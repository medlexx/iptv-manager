#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV Playlist Manager v7.0
==========================

Назначение:
    Формирует eternal_playlist.m3u8 строго по эталонному списку spisok.txt.

Главный принцип:
    spisok.txt = MASTER LIST.

То есть:
    1. В итоговом плейлисте могут быть ТОЛЬКО каналы из spisok.txt.
    2. Каждый канал из spisok.txt должен иметь запись в итоговом плейлисте.
    3. URL для канала ищется во всех доступных M3U/M3U8/TXT источниках.
    4. Сначала выбирается наиболее подходящий канал по имени.
    5. Затем проверяются все найденные URL.
    6. Рабочий URL с максимальным приоритетом выбирается первым.
    7. Если новый URL не найден, используется старый URL из предыдущего
       eternal_playlist.m3u8, если он существует.
    8. Новый канал из источников НИКОГДА самостоятельно не добавляется.
    9. Канал из spisok.txt НИКОГДА не удаляется только потому, что источник
       временно недоступен.
   10. Если для канала вообще никогда не существовало URL, в плейлист
       записывается диагностическая запись с пустым URL-комментарием.
       Такой канал присутствует в списке, но физически не может быть
       воспроизведён без источника.

Поддерживаются:
    HTTP
    HTTPS
    RTMP
    RTMPS
    RTSP
    UDP
    RTP
    MMS

Зависимости:
    pip install aiohttp aiofiles pyyaml tenacity

Для полноценной проверки потоков:
    FFmpeg / ffprobe должны быть доступны через PATH.

Запуск:
    python iptv_manager.py
"""

import asyncio
import aiofiles
import aiohttp
import concurrent.futures
import logging
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time

from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import yaml
from aiohttp import web
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


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
    "ffprobe_timeout": 12,

    "cache_ttl": 3600,

    "update_interval": 1800,

    "http_server_port": 8080,

    "fuzzy_threshold": 0.82,

    "source_priority": {
        "premium": 100,
        "main": 50,
        "default": 10,
    },

    "name_aliases": {},

    "diagnostic_mode": True,

    # Если True:
    # если новый URL не найден, программа оставляет старый URL
    # из предыдущего eternal_playlist.m3u8.
    "keep_previous_urls": True,

    # Если True, при проверке HTTP сначала выполняется быстрый HTTP check.
    "http_precheck": True,

    # Сколько URL максимум проверять одновременно.
    "validation_batch_size": 100,

    # Расширения файлов источников.
    "source_extensions": [
        ".m3u",
        ".m3u8",
        ".txt",
    ],
}


# =============================================================================
# LOGGING
# =============================================================================

LOG_FILE = "iptv_manager.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            LOG_FILE,
            encoding="utf-8",
        ),
    ],
)

logger = logging.getLogger("IPTVManager")


# =============================================================================
# CONFIG LOADER
# =============================================================================

def load_config(path: str = "config.yaml") -> dict:
    """
    Загружает config.yaml и объединяет его с DEFAULT_CONFIG.
    """

    cfg = {}

    # Глубокая копия через YAML не нужна — словари объединяем вручную.
    for key, value in DEFAULT_CONFIG.items():
        if isinstance(value, dict):
            cfg[key] = dict(value)
        elif isinstance(value, list):
            cfg[key] = list(value)
        else:
            cfg[key] = value

    if os.path.exists(path):
        try:
            with open(
                path,
                "r",
                encoding="utf-8",
            ) as f:
                user_cfg = yaml.safe_load(f) or {}

            if not isinstance(user_cfg, dict):
                raise ValueError(
                    "Корень config.yaml должен быть YAML-словарём."
                )

            for key, value in user_cfg.items():
                if (
                    key in cfg
                    and isinstance(cfg[key], dict)
                    and isinstance(value, dict)
                ):
                    cfg[key].update(value)
                else:
                    cfg[key] = value

            logger.info(
                "Конфиг загружен: %s",
                os.path.abspath(path),
            )

        except Exception as exc:
            logger.warning(
                "Ошибка чтения config.yaml: %s",
                exc,
            )
            logger.warning(
                "Используется конфигурация по умолчанию."
            )

    else:
        try:
            with open(
                path,
                "w",
                encoding="utf-8",
            ) as f:
                yaml.safe_dump(
                    cfg,
                    f,
                    allow_unicode=True,
                    sort_keys=False,
                    default_flow_style=False,
                )

            logger.info(
                "Создан config.yaml: %s",
                os.path.abspath(path),
            )

        except Exception as exc:
            logger.warning(
                "Не удалось создать config.yaml: %s",
                exc,
            )

    return cfg


# =============================================================================
# URL HELPERS
# =============================================================================

SUPPORTED_PROTOCOLS = (
    "http://",
    "https://",
    "rtmp://",
    "rtmps://",
    "rtsp://",
    "udp://",
    "rtp://",
    "mms://",
)


def is_stream_url(value: str) -> bool:
    """
    Проверяет, является ли строка URL потока.
    """

    if not value:
        return False

    value = value.strip()

    lower = value.lower()

    return lower.startswith(SUPPORTED_PROTOCOLS)


def clean_url(value: str) -> str:
    """
    Чистит URL от лишних пробелов/кавычек.
    """

    if not value:
        return ""

    value = value.strip()

    value = value.strip('"').strip("'")

    return value.strip()


# =============================================================================
# NAME NORMALIZATION
# =============================================================================

STRIP_WORDS = re.compile(
    r"""
    \b(
        hd|
        sd|
        fhd|
        uhd|
        4k|
        8k|
        hevc|
        h264|
        h265|
        avc|
        1080p?|
        720p?|
        576p?|
        480p?|
        360p?|
        tv|
        channel|
        канал|
        каналы|
        тв|
        rus|
        ru|
        russian|
        eng|
        english|
        backup|
        back-up|
        rezerv|
        reserve|
        резерв|
        резерв\d*|
        online|
        онлайн|
        live|
        прямой|
        эфир|
        orig|
        original|
        stream|
        поток
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

CLEAN_PATTERN = re.compile(
    r"[^a-zа-яё0-9]+",
    re.IGNORECASE,
)


def normalize_name(name: str) -> str:
    """
    Нормализует название канала.

    Примеры:

        VF Сериалы Турции
        -> vfсериалытурции

        .black HD
        -> black

        2×2
        -> 22

        Viasat Kino World orig
        -> viasatkinoworld
    """

    if not name:
        return ""

    name = str(name).lower().strip()

    # Unicode multiplication sign.
    name = name.replace("×", "x")

    # Убираем URL-параметры из случайно попавшего имени.
    name = name.split("?")[0]

    name = STRIP_WORDS.sub(" ", name)

    name = CLEAN_PATTERN.sub("", name)

    return name


# =============================================================================
# REFERENCE CHANNEL
# =============================================================================

@dataclass
class ReferenceChannel:
    """
    Один канал из spisok.txt.
    """

    name: str
    normalized: str


# =============================================================================
# REFERENCE PARSER
# =============================================================================

class ReferenceParser:
    """
    Парсер spisok.txt.

    Поддерживаемый формат:

        371
        VF Сериалы Турции
        3 дня

        372
        VF Вестерн
        7 дней

    Извлекается только название.

    Игнорируются:
        - номера;
        - сроки;
        - URL;
        - комментарии;
        - пустые строки.
    """

    NUMBER_PATTERN = re.compile(
        r"^\s*\d+\s*$"
    )

    DURATION_PATTERN = re.compile(
        r"""
        ^\s*
        \d+
        \s*
        (
            день|
            дня|
            дней|
            дн|
            д
        )
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    @classmethod
    async def parse(
        cls,
        filepath: str,
    ) -> List[ReferenceChannel]:

        reference: List[ReferenceChannel] = []

        seen_norms: Set[str] = set()

        path = Path(filepath)

        if not path.exists():
            logger.critical(
                "Эталонный файл не найден: %s",
                filepath,
            )
            return []

        try:
            async with aiofiles.open(
                path,
                "r",
                encoding="utf-8-sig",
                errors="ignore",
            ) as f:
                lines = await f.readlines()

        except Exception as exc:
            logger.critical(
                "Не удалось открыть spisok.txt: %s",
                exc,
            )
            return []

        skipped_numbers = 0
        skipped_durations = 0
        skipped_urls = 0
        skipped_comments = 0
        skipped_duplicates = 0

        for raw_line in lines:

            line = raw_line.strip()

            if not line:
                continue

            # -------------------------------------------------------------
            # Комментарии
            # -------------------------------------------------------------

            if line.startswith("#"):
                skipped_comments += 1
                continue

            # -------------------------------------------------------------
            # Номера
            # -------------------------------------------------------------

            if cls.NUMBER_PATTERN.fullmatch(line):
                skipped_numbers += 1
                continue

            # -------------------------------------------------------------
            # Сроки
            # -------------------------------------------------------------

            if cls.DURATION_PATTERN.fullmatch(line):
                skipped_durations += 1
                continue

            # -------------------------------------------------------------
            # URL
            # -------------------------------------------------------------

            if is_stream_url(line):
                skipped_urls += 1
                continue

            # -------------------------------------------------------------
            # Название
            # -------------------------------------------------------------

            name = line.strip()

            norm = normalize_name(name)

            if not norm:
                continue

            if len(norm) < 2:
                continue

            # Дубликаты в spisok.txt не должны создавать
            # несколько одинаковых каналов.
            if norm in seen_norms:
                skipped_duplicates += 1
                logger.warning(
                    "Дубликат в spisok.txt: '%s'",
                    name,
                )
                continue

            seen_norms.add(norm)

            reference.append(
                ReferenceChannel(
                    name=name,
                    normalized=norm,
                )
            )

        logger.info(
            "=================================================="
        )

        logger.info(
            "ЭТАЛОННЫЙ СПИСОК"
        )

        logger.info(
            "Каналов: %d",
            len(reference),
        )

        logger.info(
            "Пропущено номеров: %d",
            skipped_numbers,
        )

        logger.info(
            "Пропущено сроков: %d",
            skipped_durations,
        )

        logger.info(
            "Пропущено URL: %d",
            skipped_urls,
        )

        logger.info(
            "Пропущено комментариев: %d",
            skipped_comments,
        )

        logger.info(
            "Пропущено дублей: %d",
            skipped_duplicates,
        )

        logger.info(
            "=================================================="
        )

        if not reference:
            logger.critical(
                "SPISOK.TXT НЕ СОДЕРЖИТ НИ ОДНОГО КАНАЛА!"
            )

        else:
            logger.info(
                "Первые каналы эталона:"
            )

            for channel in reference[:10]:
                logger.info(
                    "   %s -> %s",
                    channel.name,
                    channel.normalized,
                )

        return reference


# =============================================================================
# CHANNEL MATCHER
# =============================================================================

class ChannelMatcher:
    """
    Сопоставляет название из источника с каналом эталона.

    Приоритет:

        1. exact normalized match
        2. alias
        3. fuzzy match
    """

    def __init__(
        self,
        reference: List[ReferenceChannel],
        threshold: float,
        aliases: Optional[Dict[str, str]] = None,
    ):

        self.threshold = float(threshold)

        self.reference_by_norm: Dict[
            str,
            ReferenceChannel,
        ] = {
            channel.normalized: channel
            for channel in reference
        }

        self.alias_to_reference: Dict[
            str,
            str,
        ] = {}

        aliases = aliases or {}

        for alias, reference_name in aliases.items():

            alias_norm = normalize_name(alias)
            ref_norm = normalize_name(reference_name)

            if (
                alias_norm
                and ref_norm in self.reference_by_norm
            ):
                self.alias_to_reference[
                    alias_norm
                ] = ref_norm

        self.reference_norms = list(
            self.reference_by_norm.keys()
        )

        logger.info(
            "Матчер: %d эталонных имён, %d алиасов, threshold=%.2f",
            len(self.reference_norms),
            len(self.alias_to_reference),
            self.threshold,
        )

    def match(
        self,
        raw_name: str,
    ) -> Optional[Tuple[ReferenceChannel, float, str]]:

        norm = normalize_name(raw_name)

        if not norm:
            return None

        # -------------------------------------------------------------
        # EXACT
        # -------------------------------------------------------------

        if norm in self.reference_by_norm:

            channel = self.reference_by_norm[norm]

            return (
                channel,
                1.0,
                "exact",
            )

        # -------------------------------------------------------------
        # ALIAS
        # -------------------------------------------------------------

        if norm in self.alias_to_reference:

            ref_norm = self.alias_to_reference[norm]

            channel = self.reference_by_norm.get(
                ref_norm
            )

            if channel:
                return (
                    channel,
                    1.0,
                    "alias",
                )

        # -------------------------------------------------------------
        # FUZZY
        # -------------------------------------------------------------

        best_ratio = 0.0
        best_channel: Optional[ReferenceChannel] = None

        for ref_norm in self.reference_norms:

            ratio = SequenceMatcher(
                None,
                norm,
                ref_norm,
            ).ratio()

            if ratio > best_ratio:

                best_ratio = ratio

                best_channel = self.reference_by_norm[
                    ref_norm
                ]

        if (
            best_channel is not None
            and best_ratio >= self.threshold
        ):
            return (
                best_channel,
                best_ratio,
                "fuzzy",
            )

        return None


# =============================================================================
# SOURCE PRIORITY
# =============================================================================

def get_source_priority(
    filepath: str,
    priority_map: dict,
) -> int:

    filename = Path(filepath).stem.lower()

    best_priority = priority_map.get(
        "default",
        10,
    )

    for keyword, weight in priority_map.items():

        if keyword.lower() == "default":
            continue

        if keyword.lower() in filename:

            try:
                numeric_weight = int(weight)
            except Exception:
                numeric_weight = best_priority

            if numeric_weight > best_priority:
                best_priority = numeric_weight

    return best_priority


# =============================================================================
# SOURCE URL RECORD
# =============================================================================

@dataclass
class SourceURL:
    url: str
    priority: int
    source_file: str
    source_name: str
    match_score: float
    match_type: str


# =============================================================================
# M3U PARSER
# =============================================================================

class M3UParser:
    """
    Надёжный разбор M3U/M3U8/TXT.

    Важный момент:
    один канал может иметь много URL.

    Поэтому мы НЕ теряем дубликаты URL внутри одного файла.
    """

    @staticmethod
    def extract_extinf_name(
        line: str,
    ) -> Optional[str]:

        if not line.upper().startswith("#EXTINF"):
            return None

        # Последняя запятая отделяет attributes от имени.
        comma_pos = line.find(",")

        if comma_pos < 0:
            return None

        name = line[
            comma_pos + 1:
        ].strip()

        return name or None

    @classmethod
    async def parse_file(
        cls,
        filepath: Path,
    ) -> List[Tuple[str, str]]:

        result: List[Tuple[str, str]] = []

        try:

            async with aiofiles.open(
                filepath,
                "r",
                encoding="utf-8-sig",
                errors="ignore",
            ) as f:

                lines = await f.readlines()

        except Exception as exc:

            logger.error(
                "Ошибка чтения %s: %s",
                filepath,
                exc,
            )

            return result

        pending_name: Optional[str] = None

        for raw_line in lines:

            line = raw_line.strip()

            if not line:
                continue

            # ---------------------------------------------------------
            # EXTINF
            # ---------------------------------------------------------

            if line.upper().startswith("#EXTINF"):

                pending_name = cls.extract_extinf_name(
                    line
                )

                continue

            # ---------------------------------------------------------
            # URL
            # ---------------------------------------------------------

            if is_stream_url(line):

                url = clean_url(line)

                if not url:
                    continue

                if pending_name:

                    name = pending_name

                else:

                    # Если EXTINF отсутствует.
                    # Используем имя из URL.
                    name = cls.name_from_url(
                        url
                    )

                if name:

                    result.append(
                        (
                            name,
                            url,
                        )
                    )

                pending_name = None

                continue

            # ---------------------------------------------------------
            # Другая строка
            # ---------------------------------------------------------

            # Не сбрасываем pending_name на #EXTVLCOPT и
            # прочие M3U-директивы.
            if line.startswith("#"):
                continue

        return result

    @staticmethod
    def name_from_url(
        url: str,
    ) -> str:

        value = url

        # Удаляем query.
        value = value.split("?", 1)[0]

        # Удаляем fragment.
        value = value.split("#", 1)[0]

        value = value.rstrip("/")

        if "/" in value:
            value = value.rsplit("/", 1)[-1]

        if "." in value:
            value = value.rsplit(".", 1)[0]

        return value.strip()

    @classmethod
    async def parse_file_limited(
        cls,
        filepath: Path,
        semaphore: asyncio.Semaphore,
    ) -> Tuple[str, List[Tuple[str, str]]]:

        async with semaphore:

            result = await cls.parse_file(
                filepath
            )

            return (
                str(filepath),
                result,
            )

    @classmethod
    async def load_all_sources(
        cls,
        source_dir: str,
        priority_map: dict,
        max_io: int = 20,
    ) -> List[SourceURL]:

        base_path = Path(source_dir)

        if not base_path.exists():

            logger.critical(
                "Директория источников не найдена: %s",
                source_dir,
            )

            return []

        extensions = {
            str(x).lower()
            for x in DEFAULT_CONFIG[
                "source_extensions"
            ]
        }

        files = [
            path
            for path in base_path.rglob("*")
            if path.is_file()
            and path.suffix.lower() in extensions
        ]

        logger.info(
            "Найдено файлов источников: %d",
            len(files),
        )

        if not files:

            logger.warning(
                "В директории нет M3U/M3U8/TXT файлов."
            )

            return []

        semaphore = asyncio.Semaphore(
            max_io
        )

        tasks = [
            cls.parse_file_limited(
                filepath,
                semaphore,
            )
            for filepath in files
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        output: List[SourceURL] = []

        total_entries = 0

        for result in results:

            if isinstance(
                result,
                Exception,
            ):

                logger.error(
                    "Ошибка обработки источника: %s",
                    result,
                )

                continue

            filepath_str, entries = result

            priority = get_source_priority(
                filepath_str,
                priority_map,
            )

            total_entries += len(entries)

            for raw_name, url in entries:

                if not raw_name:
                    continue

                if not url:
                    continue

                output.append(
                    SourceURL(
                        url=url,
                        priority=priority,
                        source_file=filepath_str,
                        source_name=raw_name,
                        match_score=0.0,
                        match_type="",
                    )
                )

        logger.info(
            "Распознано URL: %d",
            total_entries,
        )

        return output


# =============================================================================
# OLD PLAYLIST LOADER
# =============================================================================

class PreviousPlaylistLoader:
    """
    Загружает предыдущий eternal_playlist.m3u8.

    Используется ТОЛЬКО как резерв:
        канал -> старый URL

    Каналы из старого плейлиста НЕ добавляются в новый список,
    если их нет в spisok.txt.
    """

    @classmethod
    async def load(
        cls,
        filepath: str,
    ) -> Dict[str, str]:

        result: Dict[str, str] = {}

        path = Path(filepath)

        if not path.exists():
            return result

        try:

            async with aiofiles.open(
                path,
                "r",
                encoding="utf-8-sig",
                errors="ignore",
            ) as f:

                lines = await f.readlines()

        except Exception as exc:

            logger.warning(
                "Не удалось прочитать старый плейлист: %s",
                exc,
            )

            return result

        pending_name: Optional[str] = None

        for raw_line in lines:

            line = raw_line.strip()

            if not line:
                continue

            if line.upper().startswith(
                "#EXTINF"
            ):

                comma_pos = line.find(",")

                if comma_pos >= 0:

                    pending_name = (
                        line[
                            comma_pos + 1:
                        ].strip()
                    )

                continue

            if is_stream_url(line):

                if pending_name:

                    norm = normalize_name(
                        pending_name
                    )

                    url = clean_url(line)

                    if norm and url:

                        result[norm] = url

                pending_name = None

        logger.info(
            "Старый плейлист: %d URL загружено",
            len(result),
        )

        return result


# =============================================================================
# SQLITE CACHE
# =============================================================================

class PersistentCache:

    def __init__(
        self,
        db_path: str,
        ttl: int,
    ):

        self.ttl = int(ttl)

        self.conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
        )

        self.conn.execute(
            "PRAGMA journal_mode=WAL"
        )

        self.conn.execute(
            "PRAGMA synchronous=NORMAL"
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache (
                url TEXT PRIMARY KEY,
                is_valid INTEGER NOT NULL,
                checked_at REAL NOT NULL
            )
            """
        )

        self.conn.commit()

        self._buffer: List[
            Tuple[str, int, float]
        ] = []

        self._lock = asyncio.Lock()

        logger.info(
            "SQLite cache: %s",
            db_path,
        )

    def get(
        self,
        url: str,
    ) -> Optional[bool]:

        try:

            row = self.conn.execute(
                """
                SELECT is_valid, checked_at
                FROM cache
                WHERE url = ?
                """,
                (url,),
            ).fetchone()

        except sqlite3.Error as exc:

            logger.warning(
                "Ошибка SQLite get: %s",
                exc,
            )

            return None

        if not row:
            return None

        is_valid, checked_at = row

        if (
            time.time() - float(checked_at)
            < self.ttl
        ):
            return bool(is_valid)

        return None

    async def set(
        self,
        url: str,
        is_valid: bool,
    ) -> None:

        async with self._lock:

            self._buffer.append(
                (
                    url,
                    int(is_valid),
                    time.time(),
                )
            )

            if len(self._buffer) >= 50:
                self._flush_sync()

    def _flush_sync(self) -> None:

        if not self._buffer:
            return

        try:

            self.conn.executemany(
                """
                INSERT OR REPLACE INTO cache
                (url, is_valid, checked_at)
                VALUES (?, ?, ?)
                """,
                self._buffer,
            )

            self.conn.commit()

            self._buffer.clear()

        except sqlite3.Error as exc:

            logger.error(
                "Ошибка записи SQLite cache: %s",
                exc,
            )

    async def flush(self) -> None:

        async with self._lock:
            self._flush_sync()

    def cleanup_expired(self) -> None:

        cutoff = (
            time.time()
            - self.ttl
        )

        try:

            deleted = self.conn.execute(
                """
                DELETE FROM cache
                WHERE checked_at < ?
                """,
                (cutoff,),
            ).rowcount

            self.conn.commit()

            if deleted:
                logger.info(
                    "Удалено устаревших cache записей: %d",
                    deleted,
                )

        except sqlite3.Error as exc:

            logger.warning(
                "Ошибка очистки cache: %s",
                exc,
            )

    def close(self) -> None:

        try:
            self._flush_sync()
        finally:
            self.conn.close()


# =============================================================================
# STREAM VALIDATOR
# =============================================================================

class StreamValidator:

    def __init__(
        self,
        config: dict,
        cache: PersistentCache,
    ):

        self.config = config
        self.cache = cache

        self.semaphore = asyncio.Semaphore(
            int(config["max_workers"])
        )

        self.ffprobe_pool = (
            concurrent.futures.ThreadPoolExecutor(
                max_workers=int(
                    config["max_workers"]
                ),
                thread_name_prefix="ffprobe",
            )
        )

        self._ffprobe_logged = False

    # -------------------------------------------------------------------------
    # HTTP PRECHECK
    # -------------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(
            multiplier=1,
            min=1,
            max=4,
        ),
        retry=retry_if_exception_type(
            (
                aiohttp.ClientError,
                asyncio.TimeoutError,
            )
        ),
        reraise=True,
    )
    async def _check_http(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> bool:

        headers = {
            "User-Agent": (
                "VLC/3.0.20 "
                "(IPTV Manager)"
            ),
            "Range": "bytes=0-16384",
            "Accept": "*/*",
            "Connection": "close",
        }

        try:

            async with session.get(
                url,
                headers=headers,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(
                    total=int(
                        self.config[
                            "http_timeout"
                        ]
                    )
                ),
            ) as response:

                if response.status in (
                    200,
                    206,
                    301,
                    302,
                    307,
                    308,
                ):

                    # Читаем небольшой кусок.
                    # Не ждём весь поток.
                    try:

                        chunk = await response.content.read(
                            16384
                        )

                    except Exception:
                        chunk = b""

                    # Для некоторых IPTV endpoint'ов
                    # тело может быть пустым, но HTTP код 200.
                    if (
                        response.status
                        in (200, 206)
                        and len(chunk) > 0
                    ):
                        return True

                    if response.status in (
                        301,
                        302,
                        307,
                        308,
                    ):
                        return True

                return False

        except Exception:
            raise

    # -------------------------------------------------------------------------
    # FFPROBE
    # -------------------------------------------------------------------------

    def _run_ffprobe_sync(
        self,
        url: str,
    ) -> bool:

        timeout_seconds = int(
            self.config[
                "ffprobe_timeout"
            ]
        )

        # Более универсальная команда ffprobe.
        cmd = [
            "ffprobe",

            "-v",
            "error",

            "-hide_banner",

            "-user_agent",
            "VLC/3.0.20",

            "-rw_timeout",
            str(
                timeout_seconds
                * 1_000_000
            ),

            "-timeout",
            str(
                timeout_seconds
                * 1_000_000
            ),

            "-show_entries",
            "stream=codec_type",

            "-of",
            "default=noprint_wrappers=1:nokey=1",

            url,
        ]

        try:

            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout_seconds + 3,
                creationflags=(
                    subprocess.CREATE_NO_WINDOW
                    if os.name == "nt"
                    else 0
                ),
            )

            if result.returncode != 0:
                return False

            output = (
                result.stdout
                or ""
            ).lower()

            return (
                "video" in output
                or "audio" in output
            )

        except subprocess.TimeoutExpired:
            return False

        except FileNotFoundError:

            if not self._ffprobe_logged:

                logger.critical(
                    "FFPROBE НЕ НАЙДЕН. "
                    "Установите FFmpeg и добавьте его "
                    "в PATH."
                )

                self._ffprobe_logged = True

            return False

        except Exception:

            return False

    # -------------------------------------------------------------------------
    # VALIDATE
    # -------------------------------------------------------------------------

    async def validate_url(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> bool:

        url = clean_url(url)

        if not url:
            return False

        cached = self.cache.get(url)

        if cached is not None:
            return cached

        async with self.semaphore:

            try:

                lower = url.lower()

                # -------------------------------------------------------------
                # HTTP / HTTPS
                # -------------------------------------------------------------

                if lower.startswith(
                    (
                        "http://",
                        "https://",
                    )
                ):

                    if self.config.get(
                        "http_precheck",
                        True,
                    ):

                        try:

                            http_ok = (
                                await self._check_http(
                                    session,
                                    url,
                                )
                            )

                        except Exception:

                            http_ok = False

                        if not http_ok:

                            # Не делаем моментальный вывод,
                            # что поток мёртв:
                            # некоторые IPTV серверы плохо
                            # отвечают на обычный GET.
                            pass

                    loop = (
                        asyncio.get_running_loop()
                    )

                    media_ok = (
                        await loop.run_in_executor(
                            self.ffprobe_pool,
                            partial(
                                self._run_ffprobe_sync,
                                url,
                            ),
                        )
                    )

                # -------------------------------------------------------------
                # RTMP / RTSP / UDP / RTP / MMS
                # -------------------------------------------------------------

                else:

                    loop = (
                        asyncio.get_running_loop()
                    )

                    media_ok = (
                        await loop.run_in_executor(
                            self.ffprobe_pool,
                            partial(
                                self._run_ffprobe_sync,
                                url,
                            ),
                        )
                    )

                await self.cache.set(
                    url,
                    media_ok,
                )

                return media_ok

            except Exception as exc:

                logger.debug(
                    "Ошибка проверки URL %s: %s",
                    url,
                    exc,
                )

                await self.cache.set(
                    url,
                    False,
                )

                return False

    def shutdown(self) -> None:

        self.ffprobe_pool.shutdown(
            wait=False,
            cancel_futures=True,
        )


# =============================================================================
# PLAYLIST RESULT
# =============================================================================

@dataclass
class ChannelResult:
    name: str
    url: str
    status: str
    source: str = ""


# =============================================================================
# PLAYLIST GENERATOR
# =============================================================================

class PlaylistGenerator:

    @staticmethod
    async def generate(
        output_path: str,
        channels: List[ChannelResult],
    ) -> None:

        """
        Генерирует плейлист В ПОРЯДКЕ spisok.txt.

        Это важно:
        сортировка по имени больше НЕ используется.

        Поэтому порядок каналов соответствует эталону.
        """

        lines = [
            "#EXTM3U",
        ]

        for channel in channels:

            # -------------------------------------------------------------
            # Каждый канал из spisok.txt имеет EXTINF.
            # -------------------------------------------------------------

            lines.append(
                f"#EXTINF:-1,{channel.name}"
            )

            # -------------------------------------------------------------
            # Рабочий/старый URL.
            # -------------------------------------------------------------

            if channel.url:

                lines.append(
                    channel.url
                )

            else:

                # Нет URL вообще.
                #
                # Канал всё равно остаётся в эталонном
                # списке, но клиент не получит фальшивый
                # адрес.
                lines.append(
                    "# IPTV_MANAGER_NO_WORKING_URL"
                )

        content = (
            "\n".join(lines)
            + "\n"
        )

        tmp_path = (
            f"{output_path}.tmp"
        )

        try:

            Path(output_path).parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            async with aiofiles.open(
                tmp_path,
                "w",
                encoding="utf-8",
                newline="\n",
            ) as f:

                await f.write(
                    content
                )

            # Атомарная замена.
            os.replace(
                tmp_path,
                output_path,
            )

            logger.info(
                "Плейлист записан: %s",
                output_path,
            )

        except Exception as exc:

            logger.error(
                "Ошибка записи плейлиста: %s",
                exc,
            )

            try:

                if os.path.exists(
                    tmp_path
                ):
                    os.remove(
                        tmp_path
                    )

            except Exception:
                pass

            raise


# =============================================================================
# PLAYLIST VALIDATOR
# =============================================================================

class PlaylistIntegrityChecker:

    @staticmethod
    async def verify(
        filepath: str,
        reference: List[ReferenceChannel],
    ) -> bool:

        path = Path(filepath)

        if not path.exists():

            logger.error(
                "Проверка: плейлист не существует."
            )

            return False

        try:

            async with aiofiles.open(
                path,
                "r",
                encoding="utf-8-sig",
                errors="ignore",
            ) as f:

                lines = await f.readlines()

        except Exception as exc:

            logger.error(
                "Не удалось проверить плейлист: %s",
                exc,
            )

            return False

        playlist_names: List[str] = []

        for raw_line in lines:

            line = raw_line.strip()

            if not line.upper().startswith(
                "#EXTINF"
            ):
                continue

            comma_pos = line.find(",")

            if comma_pos < 0:
                continue

            name = line[
                comma_pos + 1:
            ].strip()

            norm = normalize_name(
                name
            )

            if norm:
                playlist_names.append(
                    norm
                )

        reference_norms = [
            channel.normalized
            for channel in reference
        ]

        # -------------------------------------------------------------
        # Проверяем количество.
        # -------------------------------------------------------------

        if len(playlist_names) != len(
            reference_norms
        ):

            logger.error(
                "ПРОВЕРКА ПЛЕЙЛИСТА: "
                "количество каналов отличается! "
                "playlist=%d reference=%d",
                len(playlist_names),
                len(reference_norms),
            )

            return False

        # -------------------------------------------------------------
        # Проверяем точное множество.
        # -------------------------------------------------------------

        if set(playlist_names) != set(
            reference_norms
        ):

            missing = [
                channel.name
                for channel in reference
                if channel.normalized
                not in set(playlist_names)
            ]

            extra = [
                name
                for name in playlist_names
                if name not in set(
                    reference_norms
                )
            ]

            logger.error(
                "ПРОВЕРКА ПЛЕЙЛИСТА НЕ ПРОЙДЕНА."
            )

            if missing:

                logger.error(
                    "Отсутствуют: %s",
                    ", ".join(
                        missing[:30]
                    ),
                )

            if extra:

                logger.error(
                    "Лишние: %s",
                    ", ".join(
                        extra[:30]
                    ),
                )

            return False

        logger.info(
            "Проверка плейлиста ПРОЙДЕНА: "
            "%d/%d каналов",
            len(playlist_names),
            len(reference),
        )

        return True


# =============================================================================
# HTTP SERVER
# =============================================================================

class PlaylistServer:

    def __init__(
        self,
        playlist_path: str,
        port: int,
    ):

        self.playlist_path = playlist_path
        self.port = int(port)

        self.runner: Optional[
            web.AppRunner
        ] = None

    async def start(self) -> None:

        app = web.Application()

        app.router.add_get(
            "/playlist.m3u8",
            self._serve,
        )

        app.router.add_get(
            "/health",
            self._health,
        )

        self.runner = web.AppRunner(
            app
        )

        await self.runner.setup()

        site = web.TCPSite(
            self.runner,
            "0.0.0.0",
            self.port,
        )

        await site.start()

        logger.info(
            "HTTP сервер: http://localhost:%d",
            self.port,
        )

        logger.info(
            "Плейлист: "
            "http://localhost:%d/playlist.m3u8",
            self.port,
        )

    async def _serve(
        self,
        request: web.Request,
    ):

        if os.path.exists(
            self.playlist_path
        ):

            return web.FileResponse(
                self.playlist_path,
                headers={
                    "Content-Type":
                        "application/vnd.apple.mpegurl",
                    "Cache-Control":
                        "no-cache, no-store, must-revalidate",
                },
            )

        return web.Response(
            status=503,
            text="Playlist not ready yet",
        )

    async def _health(
        self,
        request: web.Request,
    ):

        return web.json_response(
            {
                "status": "ok",
                "playlist_exists":
                    os.path.exists(
                        self.playlist_path
                    ),
            }
        )

    async def stop(self) -> None:

        if self.runner:

            await self.runner.cleanup()

            self.runner = None


# =============================================================================
# IPTV MANAGER
# =============================================================================

class IPTVManager:

    def __init__(self):

        self.config = load_config()

        self.reference: List[
            ReferenceChannel
        ] = []

        self.matcher: Optional[
            ChannelMatcher
        ] = None

        self.cache = PersistentCache(
            self.config[
                "cache_db"
            ],
            self.config[
                "cache_ttl"
            ],
        )

        self.validator = StreamValidator(
            self.config,
            self.cache,
        )

        self.server = PlaylistServer(
            self.config[
                "output_playlist"
            ],
            self.config[
                "http_server_port"
            ],
        )

        self.stop_event = asyncio.Event()

    # -------------------------------------------------------------------------
    # LOAD REFERENCE
    # -------------------------------------------------------------------------

    async def load_reference(
        self,
    ) -> None:

        self.reference = (
            await ReferenceParser.parse(
                self.config[
                    "reference_file"
                ]
            )
        )

        if not self.reference:

            raise RuntimeError(
                "spisok.txt пуст или не распознан."
            )

        self.matcher = ChannelMatcher(
            self.reference,
            threshold=float(
                self.config[
                    "fuzzy_threshold"
                ]
            ),
            aliases=self.config.get(
                "name_aliases",
                {},
            ),
        )

        logger.info(
            "Эталон загружен: %d каналов.",
            len(self.reference),
        )

    # -------------------------------------------------------------------------
    # MATCH SOURCES TO REFERENCE
    # -------------------------------------------------------------------------

    def build_candidates(
        self,
        sources: List[SourceURL],
    ) -> Dict[
        str,
        List[SourceURL],
    ]:

        if not self.matcher:

            raise RuntimeError(
                "Matcher не инициализирован."
            )

        candidates: Dict[
            str,
            List[SourceURL],
        ] = defaultdict(list)

        unmatched = 0

        exact_matches = 0
        alias_matches = 0
        fuzzy_matches = 0

        for source in sources:

            match = self.matcher.match(
                source.source_name
            )

            if not match:

                unmatched += 1

                continue

            (
                reference_channel,
                score,
                match_type,
            ) = match

            source.match_score = score
            source.match_type = match_type

            candidates[
                reference_channel.normalized
            ].append(
                source
            )

            if match_type == "exact":
                exact_matches += 1

            elif match_type == "alias":
                alias_matches += 1

            elif match_type == "fuzzy":
                fuzzy_matches += 1

        # -------------------------------------------------------------
        # Сортируем URL каждого канала.
        #
        # Сначала:
        #   match score
        # Затем:
        #   priority
        # -------------------------------------------------------------

        for norm_name in candidates:

            candidates[
                norm_name
            ].sort(
                key=lambda item: (
                    item.match_score,
                    item.priority,
                ),
                reverse=True,
            )

        logger.info(
            "Матчинг источников:"
        )

        logger.info(
            "  exact: %d",
            exact_matches,
        )

        logger.info(
            "  alias: %d",
            alias_matches,
        )

        logger.info(
            "  fuzzy: %d",
            fuzzy_matches,
        )

        logger.info(
            "  не совпало: %d",
            unmatched,
        )

        logger.info(
            "Эталонных каналов с кандидатами: %d/%d",
            len(candidates),
            len(self.reference),
        )

        return candidates

    # -------------------------------------------------------------------------
    # DIAGNOSTICS
    # -------------------------------------------------------------------------

    def diagnostics(
        self,
        candidates: Dict[
            str,
            List[SourceURL],
        ],
        sources: List[SourceURL],
    ) -> None:

        if not self.config.get(
            "diagnostic_mode",
            False,
        ):
            return

        logger.info(
            "================ DIAGNOSTICS ================"
        )

        logger.info(
            "Источник URL: %d",
            len(sources),
        )

        logger.info(
            "Эталон: %d",
            len(self.reference),
        )

        logger.info(
            "С кандидатами: %d",
            len(candidates),
        )

        missing = [
            channel
            for channel in self.reference
            if channel.normalized
            not in candidates
        ]

        if missing:

            logger.warning(
                "Каналы эталона без найденных URL: %d",
                len(missing),
            )

            for channel in missing[:30]:

                logger.warning(
                    "   НЕ НАЙДЕН: %s",
                    channel.name,
                )

        else:

            logger.info(
                "Для каждого канала эталона "
                "найден хотя бы один кандидат."
            )

        logger.info(
            "=============================================="
        )

    # -------------------------------------------------------------------------
    # VALIDATE CANDIDATES
    # -------------------------------------------------------------------------

    async def validate_candidates(
        self,
        candidates: Dict[
            str,
            List[SourceURL],
        ],
    ) -> Dict[str, SourceURL]:

        """
        Проверяет URL и возвращает лучший рабочий URL
        для каждого канала.

        ВАЖНО:

        Мы проверяем ВСЕ уникальные URL-кандидаты,
        а не только первый.

        Это позволяет найти рабочий URL,
        если URL с большим priority оказался мёртвым.
        """

        # -------------------------------------------------------------
        # Убираем дубли URL в рамках одного канала.
        # -------------------------------------------------------------

        unique_candidates: Dict[
            str,
            List[SourceURL],
        ] = {}

        total_urls = 0

        for norm_name, urls in candidates.items():

            seen_urls: Set[str] = set()

            unique: List[SourceURL] = []

            for item in urls:

                url = clean_url(
                    item.url
                )

                if not url:
                    continue

                url_key = url.lower()

                if url_key in seen_urls:
                    continue

                seen_urls.add(
                    url_key
                )

                item.url = url

                unique.append(
                    item
                )

            unique.sort(
                key=lambda item: (
                    item.match_score,
                    item.priority,
                ),
                reverse=True,
            )

            unique_candidates[
                norm_name
            ] = unique

            total_urls += len(unique)

        logger.info(
            "Уникальных URL-кандидатов: %d",
            total_urls,
        )

        # -------------------------------------------------------------
        # aiohttp
        # -------------------------------------------------------------

        connector = aiohttp.TCPConnector(
            limit=int(
                self.config[
                    "max_workers"
                ]
            ) * 2,

            limit_per_host=10,

            force_close=False,

            enable_cleanup_closed=True,

            ttl_dns_cache=300,

            use_dns_cache=True,
        )

        timeout = aiohttp.ClientTimeout(
            total=int(
                self.config[
                    "http_timeout"
                ]
            )
        )

        working: Dict[
            str,
            SourceURL,
        ] = {}

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        ) as session:

            tasks = []
            metadata = []

            # ---------------------------------------------------------
            # Проверяем только URL, которых нет в cache.
            # ---------------------------------------------------------

            for norm_name, urls in unique_candidates.items():

                for item in urls:

                    cached = self.cache.get(
                        item.url
                    )

                    if cached is True:

                        # Если cached true, считаем URL рабочим.
                        #
                        # Потом среди всех cached true
                        # выберем лучший.
                        self._add_working_candidate(
                            working,
                            norm_name,
                            item,
                        )

                        continue

                    if cached is False:
                        continue

                    tasks.append(
                        self.validator.validate_url(
                            session,
                            item.url,
                        )
                    )

                    metadata.append(
                        (
                            norm_name,
                            item,
                        )
                    )

            logger.info(
                "URL из cache уже рабочие: %d каналов",
                len(working),
            )

            logger.info(
                "Новых URL на проверку: %d",
                len(tasks),
            )

            # ---------------------------------------------------------
            # Проверка пачками.
            # ---------------------------------------------------------

            batch_size = int(
                self.config.get(
                    "validation_batch_size",
                    100,
                )
            )

            for start in range(
                0,
                len(tasks),
                batch_size,
            ):

                batch_tasks = tasks[
                    start:
                    start + batch_size
                ]

                batch_meta = metadata[
                    start:
                    start + batch_size
                ]

                results = await asyncio.gather(
                    *batch_tasks,
                    return_exceptions=True,
                )

                for (
                    meta,
                    result,
                ) in zip(
                    batch_meta,
                    results,
                ):

                    if isinstance(
                        result,
                        Exception,
                    ):
                        continue

                    if result is True:

                        norm_name, item = meta

                        self._add_working_candidate(
                            working,
                            norm_name,
                            item,
                        )

                logger.info(
                    "Проверено URL: %d/%d",
                    min(
                        start + batch_size,
                        len(tasks),
                    ),
                    len(tasks),
                )

        await self.cache.flush()

        logger.info(
            "Рабочих каналов после проверки: %d/%d",
            len(working),
            len(self.reference),
        )

        return working

    # -------------------------------------------------------------------------
    # ADD BEST WORKING CANDIDATE
    # -------------------------------------------------------------------------

    @staticmethod
    def _add_working_candidate(
        working: Dict[
            str,
            SourceURL,
        ],
        norm_name: str,
        candidate: SourceURL,
    ) -> None:

        current = working.get(
            norm_name
        )

        if current is None:

            working[
                norm_name
            ] = candidate

            return

        # Сначала качество совпадения.
        if candidate.match_score > current.match_score:

            working[
                norm_name
            ] = candidate

            return

        # Если качество совпадения одинаковое,
        # выбираем больший priority.
        if (
            candidate.match_score
            == current.match_score
            and candidate.priority
            > current.priority
        ):

            working[
                norm_name
            ] = candidate

    # -------------------------------------------------------------------------
    # BUILD FINAL PLAYLIST
    # -------------------------------------------------------------------------

    def build_final_playlist(
        self,
        working: Dict[
            str,
            SourceURL,
        ],
        previous_urls: Dict[str, str],
    ) -> List[ChannelResult]:

        """
        Ключевой метод.

        Идём НЕ по источникам.

        Идём строго по self.reference.

        Поэтому:
            количество EXTINF = количество каналов spisok.txt.
        """

        final: List[
            ChannelResult
        ] = []

        fresh_count = 0
        previous_count = 0
        missing_count = 0

        for channel in self.reference:

            norm = channel.normalized

            # ---------------------------------------------------------
            # Новый рабочий URL
            # ---------------------------------------------------------

            if norm in working:

                candidate = working[
                    norm
                ]

                final.append(
                    ChannelResult(
                        name=channel.name,
                        url=candidate.url,
                        status="working",
                        source=candidate.source_file,
                    )
                )

                fresh_count += 1

                continue

            # ---------------------------------------------------------
            # Старый URL
            # ---------------------------------------------------------

            if (
                self.config.get(
                    "keep_previous_urls",
                    True,
                )
                and norm in previous_urls
            ):

                old_url = previous_urls[
                    norm
                ]

                final.append(
                    ChannelResult(
                        name=channel.name,
                        url=old_url,
                        status="previous",
                        source="previous_playlist",
                    )
                )

                previous_count += 1

                continue

            # ---------------------------------------------------------
            # Вообще нет URL.
            # ---------------------------------------------------------

            final.append(
                ChannelResult(
                    name=channel.name,
                    url="",
                    status="no_url",
                    source="",
                )
            )

            missing_count += 1

        logger.info(
            "ИТОГ:"
        )

        logger.info(
            "  новые рабочие URL: %d",
            fresh_count,
        )

        logger.info(
            "  старые резервные URL: %d",
            previous_count,
        )

        logger.info(
            "  без URL: %d",
            missing_count,
        )

        logger.info(
            "  ВСЕГО каналов: %d",
            len(final),
        )

        return final

    # -------------------------------------------------------------------------
    # SAVE PLAYLIST
    # -------------------------------------------------------------------------

    async def save_playlist(
        self,
        final: List[ChannelResult],
    ) -> None:

        output_path = self.config[
            "output_playlist"
        ]

        # -------------------------------------------------------------
        # Критическая проверка ДО записи.
        # -------------------------------------------------------------

        if len(final) != len(
            self.reference
        ):

            raise RuntimeError(
                "КРИТИЧЕСКАЯ ОШИБКА: "
                "количество каналов итогового списка "
                "не совпадает с количеством каналов "
                "spisok.txt."
            )

        reference_set = {
            channel.normalized
            for channel in self.reference
        }

        final_set = {
            normalize_name(
                channel.name
            )
            for channel in final
        }

        if reference_set != final_set:

            raise RuntimeError(
                "КРИТИЧЕСКАЯ ОШИБКА: "
                "итоговый плейлист не соответствует "
                "spisok.txt."
            )

        # -------------------------------------------------------------
        # Запись.
        # -------------------------------------------------------------

        await PlaylistGenerator.generate(
            output_path,
            final,
        )

        # -------------------------------------------------------------
        # Повторная проверка уже записанного файла.
        # -------------------------------------------------------------

        ok = await PlaylistIntegrityChecker.verify(
            output_path,
            self.reference,
        )

        if not ok:

            raise RuntimeError(
                "Плейлист записан, но проверка "
                "целостности НЕ ПРОЙДЕНА."
            )

    # -------------------------------------------------------------------------
    # UPDATE CYCLE
    # -------------------------------------------------------------------------

    async def update_cycle(
        self,
    ) -> None:

        started = time.time()

        logger.info("")
        logger.info(
            "=================================================="
        )

        logger.info(
            "НАЧАЛО ЦИКЛА ОБНОВЛЕНИЯ"
        )

        logger.info(
            "=================================================="
        )

        self.cache.cleanup_expired()

        # -------------------------------------------------------------
        # Загружаем старый плейлист ПЕРЕД перезаписью.
        # -------------------------------------------------------------

        previous_urls = {}

        if self.config.get(
            "keep_previous_urls",
            True,
        ):

            previous_urls = (
                await PreviousPlaylistLoader.load(
                    self.config[
                        "output_playlist"
                    ]
                )
            )

        # -------------------------------------------------------------
        # Загружаем источники.
        # -------------------------------------------------------------

        sources = await M3UParser.load_all_sources(
            self.config[
                "sources_dir"
            ],
            self.config.get(
                "source_priority",
                {},
            ),
            max_io=20,
        )

        if not sources:

            logger.warning(
                "Источники не найдены."
            )

            # Даже если источники отсутствуют,
            # мы всё равно строим плейлист из spisok.txt
            # и старых URL.
            working = {}

        else:

            # ---------------------------------------------------------
            # Матчинг
            # ---------------------------------------------------------

            candidates = self.build_candidates(
                sources
            )

            self.diagnostics(
                candidates,
                sources,
            )

            # ---------------------------------------------------------
            # Валидация
            # ---------------------------------------------------------

            working = (
                await self.validate_candidates(
                    candidates
                )
            )

        # -------------------------------------------------------------
        # Ключевой этап:
        #
        # строим результат СТРОГО по spisok.txt.
        # -------------------------------------------------------------

        final = self.build_final_playlist(
            working,
            previous_urls,
        )

        # -------------------------------------------------------------
        # Записываем.
        # -------------------------------------------------------------

        await self.save_playlist(
            final
        )

        elapsed = (
            time.time()
            - started
        )

        working_count = sum(
            1
            for item in final
            if item.status == "working"
        )

        previous_count = sum(
            1
            for item in final
            if item.status == "previous"
        )

        no_url_count = sum(
            1
            for item in final
            if item.status == "no_url"
        )

        logger.info(
            "=================================================="
        )

        logger.info(
            "ЦИКЛ ЗАВЕРШЁН: %.1f сек.",
            elapsed,
        )

        logger.info(
            "Эталон: %d",
            len(self.reference),
        )

        logger.info(
            "Новые рабочие: %d",
            working_count,
        )

        logger.info(
            "Старые резервные: %d",
            previous_count,
        )

        logger.info(
            "Без URL: %d",
            no_url_count,
        )

        logger.info(
            "ИТОГОВЫЙ PLAYLIST: %d/%d",
            len(final),
            len(self.reference),
        )

        logger.info(
            "=================================================="
        )

    # -------------------------------------------------------------------------
    # SIGNAL
    # -------------------------------------------------------------------------

    def request_shutdown(
        self,
        sig,
    ) -> None:

        logger.info(
            "Получен сигнал %s.",
            sig,
        )

        self.stop_event.set()

    # -------------------------------------------------------------------------
    # RUN FOREVER
    # -------------------------------------------------------------------------

    async def run_forever(
        self,
    ) -> None:

        await self.load_reference()

        await self.server.start()

        # -------------------------------------------------------------
        # Windows / Linux signal handling.
        # -------------------------------------------------------------

        try:

            loop = asyncio.get_running_loop()

            for sig in (
                signal.SIGINT,
                signal.SIGTERM,
            ):

                try:

                    loop.add_signal_handler(
                        sig,
                        partial(
                            self.request_shutdown,
                            sig,
                        ),
                    )

                except (
                    NotImplementedError,
                    RuntimeError,
                ):

                    # Windows может не поддерживать
                    # add_signal_handler.
                    pass

        except Exception as exc:

            logger.debug(
                "Не удалось установить async signals: %s",
                exc,
            )

        # -------------------------------------------------------------
        # Основной цикл.
        # -------------------------------------------------------------

        try:

            while not self.stop_event.is_set():

                try:

                    await self.update_cycle()

                except Exception as exc:

                    logger.exception(
                        "ОШИБКА ЦИКЛА: %s",
                        exc,
                    )

                if self.stop_event.is_set():
                    break

                interval = int(
                    self.config[
                        "update_interval"
                    ]
                )

                logger.info(
                    "Следующее обновление через %d сек.",
                    interval,
                )

                try:

                    await asyncio.wait_for(
                        self.stop_event.wait(),
                        timeout=interval,
                    )

                except asyncio.TimeoutError:

                    pass

        finally:

            logger.info(
                "Остановка IPTV Manager..."
            )

            try:
                self.validator.shutdown()
            except Exception:
                pass

            try:
                await self.cache.flush()
            except Exception:
                pass

            try:
                self.cache.close()
            except Exception:
                pass

            try:
                await self.server.stop()
            except Exception:
                pass

            logger.info(
                "Все ресурсы освобождены."
            )


# =============================================================================
# ENTRY POINT
# =============================================================================

def main() -> int:

    logger.info(
        "=================================================="
    )

    logger.info(
        "IPTV Playlist Manager v7.0"
    )

    logger.info(
        "MASTER LIST MODE"
    )

    logger.info(
        "=================================================="
    )

    manager: Optional[
        IPTVManager
    ] = None

    try:

        manager = IPTVManager()

        asyncio.run(
            manager.run_forever()
        )

        return 0

    except KeyboardInterrupt:

        logger.info(
            "Остановлено пользователем."
        )

        return 0

    except Exception as exc:

        logger.exception(
            "FATAL ERROR: %s",
            exc,
        )

        return 1


if __name__ == "__main__":

    sys.exit(
        main()
    )