#!/usr/bin/env python3
"""
IPTV Playlist Manager v6.0 — REFERENCE LIST FORMAT FIX
Парсер эталона: номер → название → пометка срока
Пропускает числа, "3 дня"/"7 дней", дубликаты

Гарантии:
  1. В плейлист попадают ТОЛЬКО каналы из spisok.txt
  2. Нерабочие ссылки автоматически заменяются рабочими из пула источников
  3. Первый рабочий URL выбирается по приоритету источника
"""

import asyncio
import aiohttp
import aiofiles
import concurrent.futures
import logging
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
from difflib import SequenceMatcher
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import yaml
from aiohttp import web
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# =============================================================================
# CONFIGURATION
# =============================================================================
DEFAULT_CONFIG = {
    "sources_dir": r"C:\Users\medlexx\Downloads\Telegram Desktop",
    "reference_file": r"C:\Users\medlexx\Downloads\spisok.txt",
    "output_playlist": r"C:\Users\medlexx\Downloads\eternal_playlist.m3u8",
    "cache_db": r"C:\Users\medlexx\Downloads\validation_cache.db",
    "max_workers": 30,
    "http_timeout": 10,
    "ffprobe_timeout": 8,
    "cache_ttl": 3600,
    "update_interval": 1800,
    "http_server_port": 8080,
    "fuzzy_threshold": 0.70,
    "source_priority": {
        "premium": 100,
        "main": 50,
        "default": 10,
    },
    "name_aliases": {},
    "diagnostic_mode": True,
}


def load_config(path: str = "config.yaml") -> dict:
    cfg = DEFAULT_CONFIG.copy()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                user_cfg = yaml.safe_load(f) or {}
            for key, value in user_cfg.items():
                if key in cfg and isinstance(cfg[key], dict) and isinstance(value, dict):
                    cfg[key].update(value)
                else:
                    cfg[key] = value
            logging.info(f"Конфиг загружен из {path}")
        except Exception as e:
            logging.warning(f"Ошибка чтения {path}, используем дефолт: {e}")
    else:
        try:
            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)
            logging.info(f"Создан шаблон конфига: {path}")
        except Exception:
            pass
    return cfg


# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("iptv_manager.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# =============================================================================
# SQLITE CACHE (WAL + BATCH WRITES)
# =============================================================================
class PersistentCache:
    def __init__(self, db_path: str, ttl: int):
        self.ttl = ttl
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                url TEXT PRIMARY KEY,
                is_valid INTEGER NOT NULL,
                checked_at REAL NOT NULL
            )
        """)
        self.conn.commit()
        self._buffer: List[Tuple[str, int, float]] = []
        self._buffer_lock = asyncio.Lock()
        logger.info(f"SQLite кэш (WAL+batch) инициализирован: {db_path}")

    def get(self, url: str) -> Optional[bool]:
        row = self.conn.execute(
            "SELECT is_valid, checked_at FROM cache WHERE url = ?", (url,)
        ).fetchone()
        if row and (time.time() - row[1]) < self.ttl:
            return bool(row[0])
        return None

    async def set(self, url: str, is_valid: bool) -> None:
        async with self._buffer_lock:
            self._buffer.append((url, int(is_valid), time.time()))
            if len(self._buffer) >= 50:
                self._flush_sync()

    def _flush_sync(self) -> None:
        if not self._buffer:
            return
        self.conn.executemany(
            "INSERT OR REPLACE INTO cache (url, is_valid, checked_at) VALUES (?, ?, ?)",
            self._buffer,
        )
        self.conn.commit()
        self._buffer.clear()

    async def flush(self) -> None:
        async with self._buffer_lock:
            self._flush_sync()

    def cleanup_expired(self) -> None:
        cutoff = time.time() - self.ttl
        deleted = self.conn.execute(
            "DELETE FROM cache WHERE checked_at < ?", (cutoff,)
        ).rowcount
        self.conn.commit()
        if deleted:
            logger.debug(f"Удалено {deleted} устаревших записей кэша")

    def close(self) -> None:
        self._flush_sync()
        self.conn.close()


# =============================================================================
# NAME NORMALIZATION
# =============================================================================
STRIP_WORDS = re.compile(
    r'\b(hd|sd|fhd|uhd|4k|hevc|h264|h265|1080p?|720p?|'
    r'tv|channel|канал|тв|rus|ru|eng|backup|rezerv|резерв\d*|'
    r'online|онлайн|live|прямой|эфир|orig)\b',
    re.I
)
CLEAN_PATTERN = re.compile(r'[^a-zа-яё0-9]')


def normalize_name(name: str) -> str:
    """
    Агрессивная нормализация:
    'VF Сериалы Турции' → 'vfсериалытурции'
    '.black HD' → 'black'
    '2×2' → '22'
    'Viasat Kino World orig' → 'viasatkinoworld'
    """
    name = name.lower().strip()
    name = STRIP_WORDS.sub('', name)
    name = CLEAN_PATTERN.sub('', name)
    return name


# =============================================================================
# REFERENCE PARSER (формат: номер → название → пометка срока)
# =============================================================================
class ReferenceParser:
    """
    Парсит spisok.txt в формате:
        371
        VF Сериалы Турции
        3 дня
        372
        VF Вестерн
        ...

    Извлекает ТОЛЬКО названия каналов, пропуская:
      - Чисто числовые строки (номера каналов)
      - Пометки "3 дня", "7 дней", "1 день" и т.п.
      - Пустые строки, URL, комментарии
    """

    # Паттерн для пометок о сроке: "3 дня", "7 дней", "1 день", "3д"
    DURATION_PATTERN = re.compile(
        r'^\s*\d+\s*(день|дня|дней|дн|д)\s*$', re.I
    )

    # Паттерн для чисто числовых строк (номера каналов)
    NUMBER_PATTERN = re.compile(r'^\s*\d+\s*$')

    @classmethod
    async def parse(cls, filepath: str) -> Tuple[Dict[str, str], Set[str]]:
        """
        Возвращает:
          - reference_urls: {} (пустой, в этом формате нет URL)
          - reference_names: {оригинальное имя канала}
        """
        reference_urls: Dict[str, str] = {}
        reference_names: Set[str] = set()

        try:
            async with aiofiles.open(filepath, "r", encoding="utf-8") as f:
                lines = await f.readlines()
        except Exception as e:
            logger.critical(f"Не удалось открыть эталонный файл: {e}")
            return {}, set()

        skipped_numbers = 0
        skipped_durations = 0
        parsed_count = 0

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Пропускаем чисто числовые строки (номера каналов)
            if cls.NUMBER_PATTERN.match(line):
                skipped_numbers += 1
                continue

            # Пропускаем битые номера типа "98371" (слияние двух номеров)
            if cls.NUMBER_PATTERN.match(line):
                try:
                    num = int(line)
                    if num > 2000:
                        skipped_numbers += 1
                        continue
                except ValueError:
                    pass

            # Пропускаем пометки о сроке ("3 дня", "7 дней" и т.п.)
            if cls.DURATION_PATTERN.match(line):
                skipped_durations += 1
                continue

            # Пропускаем комментарии и заголовки
            if line.startswith("#"):
                continue

            # Пропускаем URL если вдруг затесались
            if line.startswith(("http", "rtmp", "rtsp", "udp")):
                continue

            # Всё остальное — название канала
            name = line
            norm = normalize_name(name)
            if norm and len(norm) >= 2:
                reference_names.add(name)
                parsed_count += 1

        logger.info(
            f"Эталонный список: {parsed_count} каналов извлечено, "
            f"пропущено {skipped_numbers} номеров, "
            f"{skipped_durations} пометок срока"
        )

        if not reference_names:
            logger.critical("⚠️ Эталонный список пуст или не распознан!")
        else:
            logger.info("Примеры извлечённых имён:")
            for name in list(reference_names)[:10]:
                logger.info(f"   ✅ '{name}' → '{normalize_name(name)}'")

        return reference_urls, reference_names


# =============================================================================
# CHANNEL MATCHER (fuzzy + aliases)
# =============================================================================
class ChannelMatcher:
    def __init__(
            self,
            reference_names: Set[str],
            threshold: float = 0.70,
            aliases: Optional[Dict[str, str]] = None,
    ):
        self.threshold = threshold
        self._norm_to_ref: Dict[str, str] = {
            normalize_name(n): n for n in reference_names
        }

        if aliases:
            for alias, ref_name in aliases.items():
                norm_alias = normalize_name(alias)
                norm_ref = normalize_name(ref_name)
                if norm_ref in self._norm_to_ref:
                    self._norm_to_ref[norm_alias] = self._norm_to_ref[norm_ref]
                elif ref_name in reference_names:
                    self._norm_to_ref[norm_alias] = ref_name

        self._norm_names = list(self._norm_to_ref.keys())
        logger.info(
            f"Матчер: {len(self._norm_names)} уникальных вариантов, "
            f"threshold={threshold}, алиасов={len(aliases or {})}"
        )

    def match(self, raw_name: str) -> Optional[str]:
        """Возвращает оригинальное имя из эталона или None."""
        norm = normalize_name(raw_name)

        # Точное совпадение (быстрый путь)
        if norm in self._norm_to_ref:
            return self._norm_to_ref[norm]

        # Fuzzy поиск
        best_ratio = 0.0
        best_ref_norm = None
        for ref_norm in self._norm_names:
            ratio = SequenceMatcher(None, norm, ref_norm).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_ref_norm = ref_norm

        if best_ratio >= self.threshold and best_ref_norm is not None:
            return self._norm_to_ref[best_ref_norm]

        return None


# =============================================================================
# SOURCE PRIORITY
# =============================================================================
def get_source_priority(filepath: str, priority_map: dict) -> int:
    fname = Path(filepath).stem.lower()
    for keyword, weight in priority_map.items():
        if keyword.lower() in fname:
            return weight
    return priority_map.get("default", 10)


# =============================================================================
# M3U PARSER (источники из директории)
# =============================================================================
class M3UParser:
    EXTINF_PATTERN = re.compile(r"#EXTINF:-?\d+\s*(?:[^,]*,)?(.+)", re.IGNORECASE)

    @classmethod
    async def parse_file(cls, filepath: Path) -> Dict[str, str]:
        channels: Dict[str, str] = {}
        try:
            async with aiofiles.open(
                    filepath, "r", encoding="utf-8", errors="ignore"
            ) as f:
                lines = await f.readlines()

            last_name: Optional[str] = None
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                extinf_match = cls.EXTINF_PATTERN.match(line)
                if extinf_match:
                    last_name = extinf_match.group(1).strip()
                elif line.startswith(("http", "rtmp", "rtsp", "udp")):
                    name = last_name or line.split("/")[-1]
                    norm = normalize_name(name)
                    if norm and norm not in channels:
                        channels[norm] = line
                    last_name = None
        except Exception as e:
            logger.error(f"Ошибка парсинга {filepath}: {e}")
        return channels

    @classmethod
    async def parse_file_limited(
            cls, filepath: Path, sem: asyncio.Semaphore
    ) -> Tuple[str, Dict[str, str]]:
        async with sem:
            result = await cls.parse_file(filepath)
            return str(filepath), result

    @classmethod
    async def load_all_sources(
            cls, source_dir: str, priority_map: dict, max_io: int = 20
    ) -> Dict[str, List[Tuple[int, str]]]:
        """Возвращает {normalized_name: [(priority, url), ...]}"""
        base_path = Path(source_dir)
        if not base_path.exists():
            logger.critical(f"Директория источников не найдена: {source_dir}")
            return {}

        extensions = {".m3u", ".m3u8", ".txt"}
        files = [f for f in base_path.rglob("*") if f.suffix.lower() in extensions]
        logger.info(f"Найдено {len(files)} файлов источников")

        if not files:
            logger.warning("⚠️ В директории источников нет файлов .m3u/.m3u8/.txt!")
            return {}

        sem = asyncio.Semaphore(max_io)
        tasks = [cls.parse_file_limited(f, sem) for f in files]
        results = await asyncio.gather(*tasks)

        grouped: Dict[str, List[Tuple[int, str]]] = {}
        total_parsed = 0
        for filepath_str, channels in results:
            priority = get_source_priority(filepath_str, priority_map)
            total_parsed += len(channels)
            for norm_name, url in channels.items():
                if norm_name not in grouped:
                    grouped[norm_name] = []
                grouped[norm_name].append((priority, url))

        logger.info(f"Распознано {total_parsed} записей из {len(files)} файлов")

        for name in grouped:
            grouped[name].sort(key=lambda x: x[0], reverse=True)

        return grouped


# =============================================================================
# STREAM VALIDATOR (HTTP + FFprobe в ThreadPool)
# =============================================================================
class StreamValidator:
    def __init__(self, config: dict, cache: PersistentCache):
        self.config = config
        self.cache = cache
        self.semaphore = asyncio.Semaphore(config["max_workers"])
        self.ffprobe_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=config["max_workers"],
            thread_name_prefix="ffprobe",
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def _check_http(self, session: aiohttp.ClientSession, url: str) -> bool:
        headers = {"Range": "bytes=0-8192"}
        async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.config["http_timeout"]),
        ) as resp:
            if resp.status not in (200, 206):
                return False
            chunk = await resp.content.read(8192)
            return len(chunk) >= 1024

    def _run_ffprobe_sync(self, url: str) -> bool:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0,a:0",
            "-show_entries", "stream=codec_type",
            "-of", "csv=p=0",
            "-timeout", str(self.config["ffprobe_timeout"] * 1_000_000),
            url,
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True,
                timeout=self.config["ffprobe_timeout"],
            )
            output = result.stdout.strip()
            return "video" in output or "audio" in output
        except subprocess.TimeoutExpired:
            return False
        except FileNotFoundError:
            logger.critical("FFprobe не найден! Установите FFmpeg.")
            return False
        except Exception:
            return False

    async def validate_url(self, session: aiohttp.ClientSession, url: str) -> bool:
        cached = self.cache.get(url)
        if cached is not None:
            return cached

        async with self.semaphore:
            try:
                http_ok = await self._check_http(session, url)
                if not http_ok:
                    await self.cache.set(url, False)
                    return False

                loop = asyncio.get_running_loop()
                media_ok = await loop.run_in_executor(
                    self.ffprobe_pool, partial(self._run_ffprobe_sync, url)
                )
                await self.cache.set(url, media_ok)
                return media_ok
            except Exception as e:
                logger.debug(f"Validation failed: {url} | {e}")
                await self.cache.set(url, False)
                return False

    def shutdown(self):
        self.ffprobe_pool.shutdown(wait=False)


# =============================================================================
# PLAYLIST GENERATOR
# =============================================================================
class PlaylistGenerator:
    @staticmethod
    async def generate(output_path: str, channels: Dict[str, str]) -> None:
        tmp_path = f"{output_path}.tmp"
        lines = ["#EXTM3U"]
        for name, url in sorted(channels.items()):
            lines.append(f"#EXTINF:-1,{name}")
            lines.append(url)

        try:
            async with aiofiles.open(tmp_path, "w", encoding="utf-8") as f:
                await f.write("\n".join(lines) + "\n")
            os.replace(tmp_path, output_path)
            logger.info(
                f"✅ Плейлист обновлён: {len(channels)} каналов -> {output_path}"
            )
        except Exception as e:
            logger.error(f"Ошибка записи: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


# =============================================================================
# HTTP SERVER
# =============================================================================
class PlaylistServer:
    def __init__(self, playlist_path: str, port: int):
        self.playlist_path = playlist_path
        self.port = port
        self.runner: Optional[web.AppRunner] = None

    async def start(self):
        app = web.Application()
        app.router.add_get("/playlist.m3u8", self._serve)
        app.router.add_get("/health", self._health)

        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "0.0.0.0", self.port)
        await site.start()
        logger.info(f"🌐 HTTP-сервер запущен на порту {self.port}")
        logger.info(f"   Плейлист: http://localhost:{self.port}/playlist.m3u8")

    async def _serve(self, request):
        if os.path.exists(self.playlist_path):
            return web.FileResponse(self.playlist_path)
        return web.Response(status=503, text="Playlist not ready yet")

    async def _health(self, request):
        return web.json_response({"status": "ok"})

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================
class IPTVManager:
    def __init__(self):
        self.config = load_config()
        self.cache = PersistentCache(
            self.config["cache_db"], self.config["cache_ttl"]
        )
        self.validator = StreamValidator(self.config, self.cache)
        self.server = PlaylistServer(
            self.config["output_playlist"], self.config["http_server_port"]
        )
        self.reference_urls: Dict[str, str] = {}
        self.reference_names: Set[str] = set()
        self.matcher: Optional[ChannelMatcher] = None

    async def load_reference(self):
        self.reference_urls, self.reference_names = await ReferenceParser.parse(
            self.config["reference_file"]
        )

        if not self.reference_names:
            logger.critical("Эталонный список пуст, дальнейшая работа невозможна")
            sys.exit(1)

        self.matcher = ChannelMatcher(
            self.reference_names,
            threshold=self.config["fuzzy_threshold"],
            aliases=self.config.get("name_aliases", {}),
        )

        logger.info(
            f"Эталон загружен: {len(self.reference_names)} каналов"
        )

    def _run_diagnostics(
            self,
            all_sources: Dict[str, List[Tuple[int, str]]],
            candidates: Dict[str, List[Tuple[int, str]]],
            unmatched_count: int,
    ):
        if not self.config.get("diagnostic_mode", False):
            return

        logger.warning("=== ДИАГНОСТИКА МАТЧИНГА ===")
        logger.warning(f"В источниках: {len(all_sources)} уникальных имён")
        logger.warning(f"Совпало с эталоном: {len(candidates)}")
        logger.warning(f"Не совпало: {unmatched_count}")

        unmatched_examples = []
        for src_norm in all_sources.keys():
            if self.matcher.match(src_norm) is None:
                unmatched_examples.append(src_norm)
            if len(unmatched_examples) >= 15:
                break

        if unmatched_examples:
            logger.warning("Примеры НЕраспознанных из источников:")
            for ex in unmatched_examples:
                logger.warning(f"   ❌ '{ex}'")

        matched_norms = {normalize_name(r) for r in candidates.keys()}
        orphan_refs = [
            r for r in self.reference_names
            if normalize_name(r) not in matched_norms
        ]
        if orphan_refs:
            logger.warning("Примеры эталонных каналов БЕЗ пары:")
            for oref in orphan_refs[:15]:
                logger.warning(f"   ⭕ '{oref}' (норм: '{normalize_name(oref)}')")

        logger.warning("=== КОНЕЦ ДИАГНОСТИКИ ===")
        logger.warning(
            "Совет: добавьте несовпадающие пары в config.yaml -> name_aliases"
        )

    async def update_cycle(self):
        start = time.time()
        logger.info("=" * 50)
        logger.info("Начало цикла обновления")

        self.cache.cleanup_expired()

        # Парсинг источников
        all_sources = await M3UParser.load_all_sources(
            self.config["sources_dir"],
            self.config.get("source_priority", {}),
            max_io=20,
        )

        if not all_sources:
            logger.warning("Источники не найдены или пусты")
            return

        # Fuzzy-матчинг
        candidates: Dict[str, List[Tuple[int, str]]] = {}
        unmatched = 0

        for src_norm_name, urls_with_prio in all_sources.items():
            ref_name = self.matcher.match(src_norm_name)
            if ref_name:
                if ref_name not in candidates:
                    candidates[ref_name] = []
                candidates[ref_name].extend(urls_with_prio)
            else:
                unmatched += 1

        # Дедупликация и сортировка по приоритету
        for ref_name in candidates:
            seen: Set[str] = set()
            unique: List[Tuple[int, str]] = []
            for prio, url in candidates[ref_name]:
                if url not in seen:
                    seen.add(url)
                    unique.append((prio, url))
            unique.sort(key=lambda x: x[0], reverse=True)
            candidates[ref_name] = unique

        logger.info(
            f"Кандидатов по эталону: {len(candidates)}/{len(self.reference_names)} "
            f"(не распознано из источников: {unmatched})"
        )

        if unmatched > 0 or len(candidates) < len(self.reference_names):
            self._run_diagnostics(all_sources, candidates, unmatched)

        if not candidates:
            logger.error(
                "❌ НИ ОДИН канал не совпал с эталоном! "
                "Проверьте названия в источниках"
            )
            return

        # ================================================================
        # ВАЛИДАЦИЯ: pre-filter кэша + параллельная проверка
        # ================================================================
        valid_channels: Dict[str, str] = {}

        connector = aiohttp.TCPConnector(
            limit=self.config["max_workers"] * 2,
            limit_per_host=10,
            force_close=False,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )
        timeout = aiohttp.ClientTimeout(total=self.config["http_timeout"])

        async with aiohttp.ClientSession(
                connector=connector, timeout=timeout
        ) as session:

            tasks: List = []
            task_meta: List[Tuple[str, int, str]] = []
            pre_cached_valid: Dict[str, Tuple[int, str]] = {}

            for ref_name, urls_with_prio in candidates.items():
                found_in_cache = False
                for prio, url in urls_with_prio:
                    cached = self.cache.get(url)
                    if cached is True:
                        if ref_name not in pre_cached_valid:
                            pre_cached_valid[ref_name] = (prio, url)
                        elif prio > pre_cached_valid[ref_name][0]:
                            pre_cached_valid[ref_name] = (prio, url)
                        found_in_cache = True
                        break
                    elif cached is False:
                        continue

                if not found_in_cache:
                    for prio, url in urls_with_prio:
                        c = self.cache.get(url)
                        if c is None:
                            tasks.append(
                                self.validator.validate_url(session, url)
                            )
                            task_meta.append((ref_name, prio, url))

            valid_channels.update(
                {k: v[1] for k, v in pre_cached_valid.items()}
            )
            logger.info(
                f"Из кэша: {len(pre_cached_valid)} каналов | "
                f"На проверку: {len(tasks)} URL"
            )

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                channel_results: Dict[str, List[Tuple[int, str]]] = {}
                for (ref_name, prio, url), result in zip(task_meta, results):
                    if result is True:
                        if ref_name not in channel_results:
                            channel_results[ref_name] = []
                        channel_results[ref_name].append((prio, url))

                for ref_name, valid_urls in channel_results.items():
                    valid_urls.sort(key=lambda x: x[0], reverse=True)
                    best_url = valid_urls[0][1]
                    best_prio = valid_urls[0][0]

                    if ref_name in valid_channels:
                        existing_prio = pre_cached_valid.get(ref_name, (0, ""))[0]
                        if best_prio > existing_prio:
                            valid_channels[ref_name] = best_url
                    else:
                        valid_channels[ref_name] = best_url

        await self.cache.flush()

        await PlaylistGenerator.generate(
            self.config["output_playlist"], valid_channels
        )

        elapsed = time.time() - start
        lost = len(self.reference_names) - len(valid_channels)
        logger.info(
            f"Цикл завершён за {elapsed:.1f}с | "
            f"Рабочих: {len(valid_channels)}/{len(self.reference_names)} | "
            f"Потеряно: {max(0, lost)}"
        )

    async def run_forever(self):
        await self.load_reference()
        await self.server.start()

        stop_event = asyncio.Event()

        def _shutdown(sig, frame):
            logger.info(f"Получен сигнал {sig}, graceful shutdown...")
            stop_event.set()

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        try:
            while not stop_event.is_set():
                try:
                    await self.update_cycle()
                except Exception as e:
                    logger.exception(f"Ошибка в цикле: {e}")

                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self.config["update_interval"],
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            self.validator.shutdown()
            self.cache.close()
            await self.server.stop()
            logger.info("Все ресурсы освобождены")


if __name__ == "__main__":
    try:
        manager = IPTVManager()
        asyncio.run(manager.run_forever())
    except KeyboardInterrupt:
        logger.info("Остановка пользователем")
    except Exception as e:
        logger.critical(f"Fatal: {e}")
        sys.exit(1)