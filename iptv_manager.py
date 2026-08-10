#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV Manager — production version compatible with structured config.yaml.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

import aiohttp
import yaml

ROOT_DIR = Path(__file__).resolve().parent

DEFAULT_CONFIG = {
    "reference_file": "spisok.txt",
    "aliases_file": "aliases.yaml",
    "sources_file": "sources.yaml",
    "sources_dir": "sources",
    "output_playlist": "output/eternal_playlist.m3u8",
    "database": "data/channel_index.db",
    "report_file": "reports/status.txt",

    "fuzzy_threshold": 0.88,
    "max_candidates_per_channel": 5,

    "http_timeout": 8,
    "connect_timeout": 5,
    "read_timeout": 5,

    "max_concurrent_validation": 20,
    "max_concurrent_downloads": 8,

    "cache_ttl_seconds": 1800,
    "good_url_ttl_seconds": 7 * 24 * 3600,
    "remote_source_ttl_seconds": 6 * 3600,

    "download_remote_sources": True,
    "validate_streams": True,
    "use_ffprobe": True,
    "ffprobe_timeout": 8,

    "strict_reference": False,
    "include_unavailable": True,
    "unavailable_url": "http://127.0.0.1:9/unavailable",
    "keep_last_good_url": True,
    "allow_unvalidated_urls": False,

    "playlist_name": "IPTV Manager",
    "user_agent": "Mozilla/5.0 IPTV-Manager/1.0",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("iptv_manager")


def resolve_path(value) -> Path:
    if isinstance(value, dict):
        value = value.get("path", "")
    path = Path(str(value))
    return path if path.is_absolute() else ROOT_DIR / path


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            if os.path.exists(temp_name):
                os.remove(temp_name)
        except OSError:
            pass


def clean_text(value: str) -> str:
    return value.replace("\ufeff", "").replace("\u00a0", " ").strip()


def is_url(value: str) -> bool:
    try:
        return urlparse(value).scheme.lower() in {
            "http", "https", "rtmp", "rtsp", "udp"
        }
    except Exception:
        return False


def file_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", "ignore")).hexdigest()


def load_yaml(path: Path, default):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return default if data is None else data
    except Exception as exc:
        logger.warning("Ошибка чтения %s: %s", path, exc)
        return default


def load_config() -> dict:
    config = dict(DEFAULT_CONFIG)
    path = ROOT_DIR / "config.yaml"
    data = load_yaml(path, {})

    if isinstance(data, dict):
        # Маппинг вложенного YAML в плоские параметры скрипта
        if "sources" in data and isinstance(data["sources"], dict):
            src = data["sources"]
            if "local_directory" in src:
                config["sources_dir"] = src["local_directory"]
            if "remote_enabled" in src:
                config["download_remote_sources"] = src["remote_enabled"]

        if "database" in data and isinstance(data["database"], dict):
            if "path" in data["database"]:
                config["database"] = data["database"]["path"]

        if "output" in data and isinstance(data["output"], dict):
            out = data["output"]
            if "playlist" in out:
                config["output_playlist"] = out["playlist"]
            if "include_unavailable" in out:
                config["include_unavailable"] = out["include_unavailable"]
            if "unavailable_url" in out:
                config["unavailable_url"] = out["unavailable_url"]

        if "matching" in data and isinstance(data["matching"], dict):
            m = data["matching"]
            if "fuzzy_threshold" in m:
                config["fuzzy_threshold"] = m["fuzzy_threshold"]

        if "validation" in data and isinstance(data["validation"], dict):
            v = data["validation"]
            if "enabled" in v:
                config["validate_streams"] = v["enabled"]
            if "http_timeout" in v:
                config["http_timeout"] = v["http_timeout"]
            if "ffprobe_enabled" in v:
                config["use_ffprobe"] = v["ffprobe_enabled"]
            if "ffprobe_timeout" in v:
                config["ffprobe_timeout"] = v["ffprobe_timeout"]
            if "max_concurrent" in v:
                config["max_concurrent_validation"] = v["max_concurrent"]
            if "cache_ttl" in v:
                config["cache_ttl_seconds"] = v["cache_ttl"]

        if "remote" in data and isinstance(data["remote"], dict):
            r = data["remote"]
            if "refresh_hours" in r:
                config["remote_source_ttl_seconds"] = int(r["refresh_hours"]) * 3600

    return config


# ============================================================================
# НОРМАЛИЗАЦИЯ ИМЁН
# ============================================================================

STRIP_WORDS = re.compile(
    r"""
    \b(
        hd|sd|fhd|uhd|4k|1080p|720p|576p|480p|
        hevc|h264|h265|avc|
        tv|channel|канал|телеканал|тв|
        online|онлайн|live|прямой|эфир|
        orig|original|backup|reserve|rezerv|резерв|
        main|основной|ru|rus|russia|рус|
        eng|en|english
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

NON_ALNUM = re.compile(r"[^0-9a-zа-яё]+", re.IGNORECASE)


def normalize_name(name: str) -> str:
    value = clean_text(name).lower().replace("ё", "е")
    value = value.replace("&", " and ")
    value = value.replace("×", "x")
    value = STRIP_WORDS.sub(" ", value)
    return NON_ALNUM.sub("", value)


def tokens(name: str) -> Set[str]:
    value = clean_text(name).lower().replace("ё", "е")
    value = STRIP_WORDS.sub(" ", value)
    return {
        x for x in re.split(r"[^0-9a-zа-яё]+", value)
        if len(x) >= 2
    }


# ============================================================================
# SPISOK.TXT
# ============================================================================

class ReferenceParser:
    NUMBER = re.compile(r"^\d+$")
    DURATION = re.compile(
        r"^\d+\s*(день|дня|дней|дн|д|day|days|d)$",
        re.IGNORECASE,
    )

    @classmethod
    def parse(cls, path: Path) -> List[str]:
        if not path.exists():
            raise FileNotFoundError(f"Не найден spisok.txt: {path}")

        result: List[str] = []
        seen: Set[str] = set()

        with path.open("r", encoding="utf-8-sig", errors="replace") as f:
            for raw in f:
                line = clean_text(raw)
                if not line:
                    continue
                if line.startswith(("#", ";")):
                    continue
                if cls.NUMBER.fullmatch(line):
                    continue
                if cls.DURATION.fullmatch(line):
                    continue
                if re.match(r"^(https?|rtmp|rtsp|udp)://", line, re.I):
                    continue

                line = re.sub(r"^\s*[-*•]+\s*", "", line).strip()
                norm = normalize_name(line)

                if len(norm) < 2 or norm in seen:
                    continue

                seen.add(norm)
                result.append(line)

        if not result:
            raise RuntimeError("spisok.txt пуст или не распознан.")

        logger.info("spisok.txt: найдено обязательных каналов: %d", len(result))
        return result


# ============================================================================
# ALIASES
# ============================================================================

class AliasManager:
    def __init__(self, path: Path):
        self.path = path
        self.aliases: Dict[str, str] = {}

    def load(self) -> None:
        data = load_yaml(self.path, {})
        if not isinstance(data, dict):
            return

        for alias, reference in data.items():
            if isinstance(alias, str) and isinstance(reference, str):
                self.aliases[normalize_name(alias)] = reference

        logger.info("Загружено aliases: %d", len(self.aliases))

    def lookup(self, source_name: str) -> Optional[str]:
        return self.aliases.get(normalize_name(source_name))


# ============================================================================
# MATCHER
# ============================================================================

@dataclass
class Match:
    reference: str
    score: float
    method: str


class ChannelMatcher:
    def __init__(
        self,
        reference_names: Sequence[str],
        aliases: AliasManager,
        threshold: float,
    ):
        self.references = list(reference_names)
        self.aliases = aliases
        self.threshold = threshold
        self.norm_map = {
            normalize_name(x): x for x in self.references
        }
        self.ref_tokens = {
            x: tokens(x) for x in self.references
        }

    def match(self, source_name: str) -> Optional[Match]:
        norm = normalize_name(source_name)
        if not norm:
            return None

        if norm in self.norm_map:
            return Match(self.norm_map[norm], 1.0, "exact")

        alias = self.aliases.lookup(source_name)
        if alias:
            alias_norm = normalize_name(alias)
            if alias_norm in self.norm_map:
                return Match(self.norm_map[alias_norm], 1.0, "alias")

        if len(norm) < 4:
            return None

        best: Optional[Match] = None
        source_tokens = tokens(source_name)

        for ref in self.references:
            ref_norm = normalize_name(ref)
            if not ref_norm:
                continue

            ratio = SequenceMatcher(None, norm, ref_norm).ratio()
            ref_tokens = self.ref_tokens[ref]

            token_score = 0.0
            if source_tokens and ref_tokens:
                union = len(source_tokens | ref_tokens)
                if union:
                    token_score = len(source_tokens & ref_tokens) / union

            score = max(
                ratio,
                ratio * 0.70 + token_score * 0.30,
            )

            if best is None or score > best.score:
                best = Match(ref, score, "fuzzy")

        if best and best.score >= self.threshold:
            return best

        return None


# ============================================================================
# M3U
# ============================================================================

@dataclass
class SourceEntry:
    name: str
    url: str
    source: str
    priority: int


class M3UParser:
    @staticmethod
    def parse(
        text: str,
        source: str,
        priority: int,
    ) -> List[SourceEntry]:
        result: List[SourceEntry] = []
        current_name: Optional[str] = None

        for raw in text.splitlines():
            line = clean_text(raw)
            if not line:
                continue

            if line.startswith("#EXTINF:"):
                if "," in line:
                    _, title = line.split(",", 1)
                    current_name = clean_text(title)
                else:
                    current_name = None
                continue

            if line.startswith("#"):
                continue

            if line.lower().startswith(
                ("http://", "https://", "rtmp://", "rtsp://", "udp://")
            ):
                name = current_name or M3UParser.guess_name(line)
                if name:
                    result.append(
                        SourceEntry(
                            name=name,
                            url=line,
                            source=source,
                            priority=priority,
                        )
                    )
                current_name = None

        return result

    @staticmethod
    def guess_name(url: str) -> str:
        path = urlparse(url).path.rstrip("/")
        if not path:
            return url
        name = path.split("/")[-1]
        name = re.sub(
            r"\.(m3u8?|ts|mp4|mkv|mpd)$",
            "",
            name,
            flags=re.I,
        )
        return re.sub(r"[_\-.]+", " ", name).strip()


# ============================================================================
# ИСТОЧНИКИ
# ============================================================================

@dataclass
class SourceDefinition:
    name: str
    location: str
    priority: int
    enabled: bool


class SourceLoader:
    def __init__(self, config: dict):
        self.config = config
        self.sources_dir = resolve_path(config["sources_dir"])
        self.remote_cache = ROOT_DIR / ".cache" / "remote_sources"

    def definitions(self) -> List[SourceDefinition]:
        result: List[SourceDefinition] = []
        source_file = resolve_path(self.config["sources_file"])
        data = load_yaml(source_file, [])

        raw = data.get("sources", []) if isinstance(data, dict) else data

        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, str):
                    result.append(
                        SourceDefinition(
                            Path(item).stem or item,
                            item,
                            0,
                            True,
                        )
                    )
                elif isinstance(item, dict):
                    location = str(
                        item.get(
                            "url",
                            item.get(
                                "file",
                                item.get("path", ""),
                            ),
                        )
                    ).strip()
                    if not location:
                        continue
                    result.append(
                        SourceDefinition(
                            str(item.get("name", Path(location).stem)),
                            location,
                            int(item.get("priority", 0)),
                            bool(item.get("enabled", True)),
                        )
                    )

        if self.sources_dir.exists():
            for path in sorted(self.sources_dir.rglob("*")):
                if path.is_file() and path.suffix.lower() in {
                    ".m3u", ".m3u8", ".txt"
                }:
                    result.append(
                        SourceDefinition(
                            path.stem,
                            str(path),
                            0,
                            True,
                        )
                    )

        unique: Dict[str, SourceDefinition] = {}
        for item in result:
            key = item.location.lower()
            if item.enabled and (
                key not in unique
                or item.priority > unique[key].priority
            ):
                unique[key] = item

        return list(unique.values())

    async def load(self) -> List[SourceEntry]:
        definitions = self.definitions()
        logger.info("Источников для обработки: %d", len(definitions))

        semaphore = asyncio.Semaphore(
            int(self.config["max_concurrent_downloads"])
        )

        timeout = aiohttp.ClientTimeout(
            total=int(self.config["http_timeout"])
        )

        connector = aiohttp.TCPConnector(
            limit=int(self.config["max_concurrent_downloads"]),
            ssl=False,
        )

        self.remote_cache.mkdir(parents=True, exist_ok=True)

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"User-Agent": self.config["user_agent"]},
        ) as session:

            async def one(item: SourceDefinition) -> List[SourceEntry]:
                async with semaphore:
                    if is_url(item.location):
                        if not self.config.get("download_remote_sources", True):
                            return []
                        return await self._remote(session, item)

                    path = Path(item.location)
                    if not path.is_absolute():
                        path = ROOT_DIR / path

                    if not path.exists():
                        logger.warning("Источник не найден: %s", path)
                        return []

                    try:
                        text = await asyncio.to_thread(
                            path.read_text,
                            encoding="utf-8",
                            errors="replace",
                        )
                    except Exception as exc:
                        logger.warning("Ошибка чтения %s: %s", path, exc)
                        return []

                    return M3UParser.parse(
                        text, item.name, item.priority
                    )

            results = await asyncio.gather(
                *(one(x) for x in definitions),
                return_exceptions=True,
            )

        entries: List[SourceEntry] = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Ошибка источника: %s", result)
            else:
                entries.extend(result)

        logger.info("Всего записей M3U: %d", len(entries))
        return entries

    async def _remote(
        self,
        session: aiohttp.ClientSession,
        item: SourceDefinition,
    ) -> List[SourceEntry]:
        cache_path = self.remote_cache / f"{file_hash(item.location)}.m3u"
        ttl = int(self.config["remote_source_ttl_seconds"])

        if cache_path.exists():
            age = time.time() - cache_path.stat().st_mtime
            if age < ttl:
                try:
                    text = cache_path.read_text(
                        encoding="utf-8",
                        errors="replace",
                    )
                    return M3UParser.parse(
                        text, item.name, item.priority
                    )
                except Exception:
                    pass

        try:
            async with session.get(
                item.location,
                allow_redirects=True,
            ) as response:
                if response.status != 200:
                    logger.warning(
                        "%s: HTTP %s",
                        item.name,
                        response.status,
                    )
                    return []

                data = await response.read()
                text = data.decode("utf-8", errors="replace")

                try:
                    cache_path.write_text(
                        text,
                        encoding="utf-8",
                    )
                except Exception:
                    pass

                return M3UParser.parse(
                    text, item.name, item.priority
                )

        except Exception as exc:
            logger.warning(
                "Не удалось скачать %s: %s",
                item.name,
                exc,
            )

            if cache_path.exists():
                try:
                    text = cache_path.read_text(
                        encoding="utf-8",
                        errors="replace",
                    )
                    return M3UParser.parse(
                        text, item.name, item.priority
                    )
                except Exception:
                    pass

            return []


# ============================================================================
# SQLITE
# ============================================================================

class Database:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None
        self._open()

    def _open(self) -> None:
        try:
            self.conn = sqlite3.connect(
                str(self.path),
                timeout=30,
            )
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self._tables()
        except sqlite3.DatabaseError as exc:
            logger.error(
                "Файл %s не является корректной SQLite БД: %s",
                self.path,
                exc,
            )

            try:
                if self.conn:
                    self.conn.close()
            except Exception:
                pass

            self.conn = None

            stamp = time.strftime("%Y%m%d-%H%M%S")
            corrupt = self.path.with_name(
                f"{self.path.name}.corrupt-{stamp}"
            )

            try:
                if self.path.exists():
                    shutil.move(str(self.path), str(corrupt))
                    logger.warning(
                        "Повреждённая БД перемещена: %s",
                        corrupt,
                    )
            except Exception:
                try:
                    self.path.unlink(missing_ok=True)
                except Exception:
                    pass

            self.conn = sqlite3.connect(
                str(self.path),
                timeout=30,
            )
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self._tables()

            logger.info("Создана новая SQLite БД: %s", self.path)

    def _tables(self) -> None:
        assert self.conn is not None
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS validation (
                url TEXT PRIMARY KEY,
                valid INTEGER NOT NULL,
                checked_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS last_good (
                channel TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                source TEXT NOT NULL,
                priority INTEGER NOT NULL,
                checked_at REAL NOT NULL
            );
            """
        )
        self.conn.commit()

    def validation(
        self,
        url: str,
        ttl: int,
    ) -> Optional[bool]:
        assert self.conn is not None
        row = self.conn.execute(
            "SELECT valid, checked_at FROM validation WHERE url=?",
            (url,),
        ).fetchone()

        if not row:
            return None

        if time.time() - row[1] > ttl:
            return None

        return bool(row[0])

    def set_validation(
        self,
        url: str,
        valid: bool,
    ) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            INSERT OR REPLACE INTO validation
            (url, valid, checked_at)
            VALUES (?, ?, ?)
            """,
            (url, int(valid), time.time()),
        )
        self.conn.commit()

    def last_good(
        self,
        channel: str,
        ttl: int,
    ) -> Optional[Tuple[str, str, int]]:
        assert self.conn is not None
        row = self.conn.execute(
            """
            SELECT url, source, priority, checked_at
            FROM last_good
            WHERE channel=?
            """,
            (channel,),
        ).fetchone()

        if not row:
            return None

        if time.time() - row[3] > ttl:
            return None

        return row[0], row[1], int(row[2])

    def set_last_good(
        self,
        channel: str,
        url: str,
        source: str,
        priority: int,
    ) -> None:
        assert self.conn is not None
        self.conn.execute(
            """
            INSERT OR REPLACE INTO last_good
            (channel, url, source, priority, checked_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (channel, url, source, priority, time.time()),
        )
        self.conn.commit()

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.commit()
            finally:
                self.conn.close()
                self.conn = None


# ============================================================================
# ПРОВЕРКА ССЫЛОК
# ============================================================================

class Validator:
    def __init__(
        self,
        config: dict,
        database: Database,
    ):
        self.config = config
        self.db = database
        self.sem = asyncio.Semaphore(
            int(config["max_concurrent_validation"])
        )
        self.ffprobe = (
            self._has_ffprobe()
            if config["use_ffprobe"]
            else False
        )

    @staticmethod
    def _has_ffprobe() -> bool:
        try:
            return subprocess.run(
                ["ffprobe", "-version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
            ).returncode == 0
        except Exception:
            logger.warning(
                "FFprobe не найден. Используется HTTP-проверка."
            )
            return False

    async def _http(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> bool:
        try:
            timeout = aiohttp.ClientTimeout(
                total=int(self.config["http_timeout"]),
                connect=int(self.config["connect_timeout"]),
                sock_read=int(self.config["read_timeout"]),
            )

            async with session.get(
                url,
                headers={
                    "User-Agent": self.config["user_agent"],
                    "Accept": "*/*",
                    "Range": "bytes=0-16384",
                },
                timeout=timeout,
                allow_redirects=True,
            ) as response:
                if response.status not in (200, 206):
                    return False

                content = await response.content.read(16384)
                content_type = response.headers.get(
                    "Content-Type", ""
                ).lower()

                return bool(content) or "mpegurl" in content_type

        except (
            asyncio.TimeoutError,
            aiohttp.ClientError,
            OSError,
        ):
            return False

    def _probe(self, url: str) -> bool:
        command = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "stream=codec_type",
            "-of", "csv=p=0",
            "-timeout",
            str(int(self.config["ffprobe_timeout"]) * 1000000),
            "-user_agent",
            self.config["user_agent"],
            url,
        ]

        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=int(self.config["ffprobe_timeout"]) + 3,
            )
            output = (result.stdout or "").lower()
            return "video" in output or "audio" in output
        except Exception:
            return False

    async def check(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> bool:
        cached = self.db.validation(
            url,
            int(self.config["cache_ttl_seconds"]),
        )
        if cached is not None:
            return cached

        async with self.sem:
            if not await self._http(session, url):
                self.db.set_validation(url, False)
                return False

            if not self.ffprobe or not self.config["validate_streams"]:
                self.db.set_validation(url, True)
                return True

            valid = await asyncio.to_thread(
                self._probe,
                url,
            )
            self.db.set_validation(url, valid)
            return valid


# ============================================================================
# CANDIDATES
# ============================================================================

@dataclass
class Candidate:
    channel: str
    url: str
    source: str
    priority: int
    score: float
    method: str


def build_candidates(
    entries: Sequence[SourceEntry],
    matcher: ChannelMatcher,
    max_per_channel: int = 5,
) -> Dict[str, List[Candidate]]:
    result: Dict[str, List[Candidate]] = {}
    unmatched = 0

    for entry in entries:
        match = matcher.match(entry.name)
        if not match:
            unmatched += 1
            continue

        result.setdefault(match.reference, []).append(
            Candidate(
                match.reference,
                entry.url,
                entry.source,
                entry.priority,
                match.score,
                match.method,
            )
        )

    for channel in result:
        unique: Dict[str, Candidate] = {}

        for candidate in result[channel]:
            old = unique.get(candidate.url)
            if old is None or (
                candidate.priority,
                candidate.score,
            ) > (
                old.priority,
                old.score,
            ):
                unique[candidate.url] = candidate

        sorted_candidates = sorted(
            unique.values(),
            key=lambda x: (x.priority, x.score),
            reverse=True,
        )
        result[channel] = sorted_candidates[:max_per_channel]

    logger.info(
        "Совпало обязательных каналов: %d; "
        "нераспознано записей источников: %d",
        len(result),
        unmatched,
    )

    return result


# ============================================================================
# РАЗРЕШЕНИЕ КАНАЛОВ
# ============================================================================

async def resolve_channels(
    config: dict,
    reference: Sequence[str],
    candidates: Dict[str, List[Candidate]],
    db: Database,
    validator: Validator,
) -> Tuple[Dict[str, Candidate], List[str]]:

    resolved: Dict[str, Candidate] = {}
    missing: List[str] = []

    connector = aiohttp.TCPConnector(
        limit=int(config["max_concurrent_validation"]),
        limit_per_host=10,
        ssl=False,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": config["user_agent"]},
    ) as session:

        async def resolve(channel: str):
            for candidate in candidates.get(channel, []):
                if await validator.check(
                    session,
                    candidate.url,
                ):
                    db.set_last_good(
                        channel,
                        candidate.url,
                        candidate.source,
                        candidate.priority,
                    )
                    return channel, candidate

            if config["keep_last_good_url"]:
                old = db.last_good(
                    channel,
                    int(config["good_url_ttl_seconds"]),
                )

                if old:
                    url, source, priority = old
                    if await validator.check(session, url):
                        return (
                            channel,
                            Candidate(
                                channel,
                                url,
                                source,
                                priority,
                                1.0,
                                "last_good",
                            ),
                        )

            return channel, None

        results = await asyncio.gather(
            *(resolve(channel) for channel in reference),
            return_exceptions=True,
        )

    for item in results:
        if isinstance(item, Exception):
            logger.error("Ошибка разрешения канала: %s", item)
            continue

        channel, candidate = item

        if candidate is None:
            missing.append(channel)
        else:
            resolved[channel] = candidate

    return resolved, missing


# ============================================================================
# PLAYLIST AND REPORTS
# ============================================================================

def write_playlist(
    config: dict,
    reference: Sequence[str],
    resolved: Dict[str, Candidate],
) -> Path:
    output = resolve_path(config["output_playlist"])

    lines = [
        "#EXTM3U",
        f'#PLAYLIST:{config["playlist_name"]}',
    ]

    include_unavailable = config.get("include_unavailable", True)
    fallback_url = config.get("unavailable_url", "http://127.0.0.1:9/unavailable")

    for channel in reference:
        candidate = resolved.get(channel)
        if candidate:
            lines.append(f'#EXTINF:-1 tvg-name="{channel}",{channel}')
            lines.append(candidate.url)
        elif include_unavailable:
            lines.append(f'#EXTINF:-1 tvg-name="{channel}",{channel}')
            lines.append(fallback_url)

    content = "\n".join(lines) + "\n"
    atomic_write(output, content)
    logger.info("Плейлист успешно записан в: %s", output)
    return output


def write_report(
    config: dict,
    total_ref: int,
    resolved: Dict[str, Candidate],
    missing: List[str],
    playlist_updated: bool,
) -> Path:
    report_path = resolve_path(config["report_file"])
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        f"=== Отчет IPTV Manager ({timestamp}) ===",
        f"Всего обязательных каналов: {total_ref}",
        f"Успешно найдено и проверено: {len(resolved)}",
        f"Заменено заглушкой (ненайдены/нерабочие): {len(missing)}",
        f"Статус обновления плейлиста: ОБНОВЛЕН",
        "",
    ]

    if missing:
        lines.append("--- Каналы с заглушкой (unavailable_url) ---")
        for ch in missing:
            lines.append(f"- {ch}")
        lines.append("")

    lines.append("--- Детализация рабочих каналов ---")
    for ch, cand in resolved.items():
        lines.append(f"• {ch} -> {cand.source} ({cand.method}, score: {cand.score:.2f})")

    content = "\n".join(lines) + "\n"
    atomic_write(report_path, content)
    logger.info("Отчет сохранен в: %s", report_path)
    return report_path


# ============================================================================
# MAIN ENTRYPOINT
# ============================================================================

async def main() -> None:
    logger.info("Запуск IPTV Manager...")
    config = load_config()

    ref_path = resolve_path(config["reference_file"])
    reference_channels = ReferenceParser.parse(ref_path)

    alias_path = resolve_path(config["aliases_file"])
    aliases = AliasManager(alias_path)
    aliases.load()

    matcher = ChannelMatcher(
        reference_channels,
        aliases,
        float(config["fuzzy_threshold"]),
    )

    loader = SourceLoader(config)
    entries = await loader.load()

    max_candidates = int(config.get("max_candidates_per_channel", 5))
    candidates = build_candidates(entries, matcher, max_candidates)

    db_path = resolve_path(config["database"])
    db = Database(db_path)

    try:
        validator = Validator(config, db)
        resolved, missing = await resolve_channels(
            config,
            reference_channels,
            candidates,
            db,
            validator,
        )

        write_playlist(config, reference_channels, resolved)
        write_report(
            config,
            len(reference_channels),
            resolved,
            missing,
            True,
        )

    finally:
        db.close()

    logger.info("Работа IPTV Manager завершена успешно.")


if __name__ == "__main__":
    asyncio.run(main())