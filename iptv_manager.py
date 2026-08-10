#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV Manager
============

Полноценный менеджер IPTV-плейлиста.

Основные принципы:

1. spisok.txt является ЖЁСТКИМ белым списком.
2. В итоговый playlist попадают только каналы из spisok.txt.
3. Один канал может иметь множество URL из разных источников.
4. URL имеют приоритет.
5. Рабочий URL выбирается автоматически.
6. Нерабочий URL не удаляется из базы — он остаётся резервом.
7. Локальные источники индексируются инкрементально.
8. Удалённые M3U можно подключать через sources.yaml.
9. Разные названия одного канала объединяются через aliases.yaml,
   точное совпадение и безопасный fuzzy matching.
10. База SQLite хранит найденные URL и историю проверки.
11. Итоговый playlist генерируется атомарно.
12. Скрипт может работать локально и в GitHub Actions.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import aiofiles
import aiohttp
import yaml


# ============================================================================
# PATHS
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent

CONFIG_FILE = BASE_DIR / "config.yaml"
SOURCES_CONFIG_FILE = BASE_DIR / "sources.yaml"
CHANNELS_CONFIG_FILE = BASE_DIR / "channels.yaml"
ALIASES_CONFIG_FILE = BASE_DIR / "aliases.yaml"
REFERENCE_FILE = BASE_DIR / "spisok.txt"

DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

DATABASE_FILE = DATA_DIR / "channel_index.db"
PLAYLIST_FILE = OUTPUT_DIR / "eternal_playlist.m3u8"

LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "iptv_manager.log"


# ============================================================================
# DIRECTORIES
# ============================================================================

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# LOGGING
# ============================================================================

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

logger = logging.getLogger("iptv_manager")


# ============================================================================
# DEFAULT CONFIG
# ============================================================================

DEFAULT_CONFIG = {
    "sources": {
        "local_directory": "sources",
        "download_directory": "downloaded_sources",
        "remote_enabled": True,
    },

    "database": {
        "path": "data/channel_index.db",
    },

    "output": {
        "playlist": "output/eternal_playlist.m3u8",
        "include_unavailable": True,
        "unavailable_url": "http://127.0.0.1:9/unavailable",
    },

    "matching": {
        "fuzzy_enabled": True,
        "fuzzy_threshold": 0.88,
        "minimum_normalized_length": 2,
    },

    "validation": {
        "enabled": True,
        "http_timeout": 8,
        "ffprobe_enabled": True,
        "ffprobe_timeout": 8,
        "max_concurrent": 20,
        "cache_ttl": 1800,
        "retry_count": 1,
    },

    "playlist": {
        "sort_by": "reference_order",
        "default_group": "TV",
    },

    "remote": {
        "refresh_hours": 6,
    },

    "diagnostics": {
        "enabled": True,
        "unmatched_limit": 30,
    },
}


# ============================================================================
# UTILITIES
# ============================================================================

def load_yaml(path: Path, default: dict) -> dict:
    if not path.exists():
        logger.warning("Файл не найден: %s", path)
        return default.copy()

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            logger.warning("Некорректный YAML: %s", path)
            return default.copy()

        return data

    except Exception as exc:
        logger.error("Ошибка чтения YAML %s: %s", path, exc)
        return default.copy()


def merge_dicts(base: dict, override: dict) -> dict:
    result = dict(base)

    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value

    return result


def load_config() -> dict:
    user_config = load_yaml(CONFIG_FILE, {})
    return merge_dicts(DEFAULT_CONFIG, user_config)


CONFIG = load_config()


# ============================================================================
# NORMALIZATION
# ============================================================================

REMOVE_WORDS = {
    "hd",
    "sd",
    "fhd",
    "uhd",
    "4k",
    "8k",
    "1080p",
    "720p",
    "576p",
    "480p",
    "hevc",
    "h264",
    "h265",
    "avc",
    "aac",
    "mpeg",
    "mpeg2",
    "mpeg4",
    "tv",
    "channel",
    "канал",
    "каналы",
    "тв",
    "rus",
    "ru",
    "russia",
    "россия",
    "eng",
    "en",
    "uk",
    "ua",
    "backup",
    "резерв",
    "резервный",
    "reserve",
    "online",
    "онлайн",
    "live",
    "прямой",
    "эфир",
    "orig",
    "original",
    "originals",
    "default",
}


def normalize_name(name: str) -> str:
    """
    Приводит название к устойчивой форме.

    Примеры:

        Кино ТВ HD
        КИНО ТВ FHD
        Kino TV 1080p

    могут свестись к одной форме.
    """

    if not name:
        return ""

    text = str(name).strip().lower()

    text = text.replace("ё", "е")
    text = text.replace("×", "x")

    # HTML entities
    text = re.sub(r"&amp;", " ", text)
    text = re.sub(r"&quot;", " ", text)

    # Сначала отделяем слова.
    tokens = re.findall(
        r"[a-zа-я0-9]+",
        text,
        flags=re.IGNORECASE,
    )

    filtered = []

    for token in tokens:
        if token in REMOVE_WORDS:
            continue

        if re.fullmatch(r"\d{3,4}p", token):
            continue

        if re.fullmatch(r"\d{3,4}", token):
            # Не удаляем обычные названия с цифрами вроде 2x2.
            if token in {"1080", "720", "576", "480", "2160"}:
                continue

        filtered.append(token)

    return "".join(filtered)


def normalize_url(url: str) -> str:
    return url.strip()


def is_url(line: str) -> bool:
    if not line:
        return False

    value = line.strip().lower()

    return value.startswith(
        (
            "http://",
            "https://",
            "rtmp://",
            "rtsp://",
            "udp://",
            "rtp://",
            "srt://",
        )
    )


def file_hash(path: Path) -> str:
    sha = hashlib.sha256()

    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)

            if not chunk:
                break

            sha.update(chunk)

    return sha.hexdigest()


# ============================================================================
# REFERENCE CHANNEL
# ============================================================================

@dataclass
class ReferenceChannel:
    name: str
    normalized: str
    order: int
    group: str = "TV"


# ============================================================================
# REFERENCE PARSER
# ============================================================================

class ReferenceParser:

    NUMBER_PATTERN = re.compile(r"^\s*\d+\s*$")

    DURATION_PATTERN = re.compile(
        r"^\s*\d+\s*(день|дня|дней|дн|д)\s*$",
        re.IGNORECASE,
    )

    @classmethod
    async def parse(
        cls,
        path: Path,
    ) -> List[ReferenceChannel]:

        if not path.exists():
            raise FileNotFoundError(
                f"Эталонный файл не найден: {path}"
            )

        channels: List[ReferenceChannel] = []
        seen: Set[str] = set()

        async with aiofiles.open(
            path,
            "r",
            encoding="utf-8-sig",
        ) as f:

            lines = await f.readlines()

        order = 0

        for raw_line in lines:

            line = raw_line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            if cls.NUMBER_PATTERN.fullmatch(line):
                continue

            if cls.DURATION_PATTERN.fullmatch(line):
                continue

            if is_url(line):
                continue

            normalized = normalize_name(line)

            if len(normalized) < CONFIG["matching"]["minimum_normalized_length"]:
                continue

            if normalized in seen:
                continue

            seen.add(normalized)

            order += 1

            channels.append(
                ReferenceChannel(
                    name=line,
                    normalized=normalized,
                    order=order,
                    group="TV",
                )
            )

        logger.info(
            "spisok.txt: найдено обязательных каналов: %d",
            len(channels),
        )

        if not channels:
            raise RuntimeError(
                "spisok.txt пуст или не содержит распознаваемых каналов"
            )

        return channels


# ============================================================================
# CHANNEL CONFIG
# ============================================================================

class ChannelConfig:

    def __init__(self, path: Path):
        self.groups: Dict[str, str] = {}
        self.enabled: Dict[str, bool] = {}

        data = load_yaml(path, {})

        channels = data.get("channels", [])

        if isinstance(channels, list):

            for item in channels:

                if not isinstance(item, dict):
                    continue

                name = str(item.get("name", "")).strip()

                if not name:
                    continue

                normalized = normalize_name(name)

                self.groups[normalized] = str(
                    item.get("group", "TV")
                ).strip() or "TV"

                self.enabled[normalized] = bool(
                    item.get("enabled", True)
                )

    def apply(
        self,
        channels: List[ReferenceChannel],
    ) -> None:

        for channel in channels:

            group = self.groups.get(
                channel.normalized,
                channel.group,
            )

            enabled = self.enabled.get(
                channel.normalized,
                True,
            )

            channel.group = group

            if not enabled:
                channel.group = "DISABLED"


# ============================================================================
# ALIASES
# ============================================================================

class AliasMatcher:

    def __init__(
        self,
        aliases_path: Path,
        reference_channels: List[ReferenceChannel],
    ):

        self.alias_to_reference: Dict[str, str] = {}

        reference_by_normalized = {
            c.normalized: c
            for c in reference_channels
        }

        data = load_yaml(aliases_path, {})

        aliases = data.get("aliases", {})

        if not isinstance(aliases, dict):
            return

        for alias, reference_name in aliases.items():

            alias_norm = normalize_name(str(alias))
            ref_norm = normalize_name(str(reference_name))

            if not alias_norm or not ref_norm:
                continue

            if ref_norm in reference_by_normalized:
                self.alias_to_reference[alias_norm] = ref_norm

        logger.info(
            "Загружено aliases: %d",
            len(self.alias_to_reference),
        )

    def resolve_exact(
        self,
        normalized_name: str,
    ) -> Optional[str]:

        return self.alias_to_reference.get(normalized_name)


# ============================================================================
# SAFE FUZZY MATCHER
# ============================================================================

class SafeMatcher:

    def __init__(
        self,
        reference_channels: List[ReferenceChannel],
        alias_matcher: AliasMatcher,
    ):

        self.reference = reference_channels
        self.aliases = alias_matcher

        self.by_normalized = {
            c.normalized: c
            for c in reference_channels
        }

        self.threshold = float(
            CONFIG["matching"]["fuzzy_threshold"]
        )

    def match(
        self,
        raw_name: str,
    ) -> Optional[ReferenceChannel]:

        normalized = normalize_name(raw_name)

        if not normalized:
            return None

        # ------------------------------------------------------------
        # 1. EXACT
        # ------------------------------------------------------------

        exact = self.by_normalized.get(normalized)

        if exact:
            return exact

        # ------------------------------------------------------------
        # 2. ALIAS
        # ------------------------------------------------------------

        alias_norm = self.aliases.resolve_exact(normalized)

        if alias_norm:
            return self.by_normalized.get(alias_norm)

        # ------------------------------------------------------------
        # 3. FUZZY
        # ------------------------------------------------------------

        if not CONFIG["matching"]["fuzzy_enabled"]:
            return None

        best: Optional[ReferenceChannel] = None
        best_ratio = 0.0

        for channel in self.reference:

            ratio = SequenceMatcher(
                None,
                normalized,
                channel.normalized,
            ).ratio()

            if ratio > best_ratio:
                best_ratio = ratio
                best = channel

        if best and best_ratio >= self.threshold:

            # Дополнительная защита.
            # Очень короткие названия нельзя fuzzy-сопоставлять.
            if len(normalized) < 5:
                return None

            logger.debug(
                "Fuzzy: '%s' -> '%s' ratio=%.3f",
                raw_name,
                best.name,
                best_ratio,
            )

            return best

        return None


# ============================================================================
# M3U PARSER
# ============================================================================

class M3UParser:

    EXTINF_RE = re.compile(
        r"^#EXTINF\s*:\s*-?\d+(?:\s+[^,]*)?,\s*(.*)$",
        re.IGNORECASE,
    )

    EXTGRP_RE = re.compile(
        r'group-title\s*=\s*"([^"]*)"',
        re.IGNORECASE,
    )

    TVG_NAME_RE = re.compile(
        r'tvg-name\s*=\s*"([^"]*)"',
        re.IGNORECASE,
    )

    @classmethod
    async def parse(
        cls,
        path: Path,
    ) -> List[Tuple[str, str, str]]:

        result = []

        current_name: Optional[str] = None
        current_group = "TV"

        try:

            async with aiofiles.open(
                path,
                "r",
                encoding="utf-8-sig",
                errors="ignore",
            ) as f:

                async for raw_line in f:

                    line = raw_line.strip()

                    if not line:
                        continue

                    if line.upper().startswith("#EXTINF"):

                        match = cls.EXTINF_RE.match(line)

                        if match:
                            current_name = match.group(1).strip()
                        else:
                            current_name = None

                        group_match = cls.EXTGRP_RE.search(line)

                        if group_match:
                            current_group = (
                                group_match.group(1).strip()
                                or "TV"
                            )
                        else:
                            current_group = "TV"

                        tvg_match = cls.TVG_NAME_RE.search(line)

                        if tvg_match and tvg_match.group(1).strip():
                            # Название из tvg-name используем только
                            # если EXTINF-название отсутствует.
                            if not current_name:
                                current_name = (
                                    tvg_match.group(1).strip()
                                )

                        continue

                    if line.startswith("#"):
                        continue

                    if is_url(line):

                        name = current_name

                        if not name:
                            parsed = urlparse(line)
                            name = (
                                Path(parsed.path).stem
                                or "Unknown"
                            )

                        result.append(
                            (
                                name.strip(),
                                line.strip(),
                                current_group,
                            )
                        )

                        current_name = None
                        current_group = "TV"

        except Exception as exc:

            logger.error(
                "Ошибка чтения %s: %s",
                path,
                exc,
            )

        return result


# ============================================================================
# DATABASE
# ============================================================================

class Database:

    def __init__(self, path: Path):

        self.path = path

        self.conn = sqlite3.connect(
            str(path),
            check_same_thread=False,
        )

        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")

        self.create_tables()

    def create_tables(self):

        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_files (
                path TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime REAL NOT NULL,
                indexed_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reference_name TEXT NOT NULL,
                reference_normalized TEXT NOT NULL,
                url TEXT NOT NULL,
                source TEXT NOT NULL,
                source_priority INTEGER NOT NULL DEFAULT 10,
                first_seen REAL NOT NULL,
                last_seen REAL NOT NULL,
                last_checked REAL,
                is_alive INTEGER,
                response_time REAL,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                UNIQUE(reference_normalized, url)
            );

            CREATE INDEX IF NOT EXISTS idx_urls_reference
                ON urls(reference_normalized);

            CREATE INDEX IF NOT EXISTS idx_urls_alive
                ON urls(is_alive);

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

        self.conn.commit()

    def is_source_changed(
        self,
        path: Path,
    ) -> bool:

        stat = path.stat()

        row = self.conn.execute(
            """
            SELECT sha256, size, mtime
            FROM source_files
            WHERE path = ?
            """,
            (str(path),),
        ).fetchone()

        if not row:
            return True

        old_hash, old_size, old_mtime = row

        if old_size != stat.st_size:
            return True

        if abs(old_mtime - stat.st_mtime) > 0.01:
            return True

        # mtime/size одинаковые — дополнительная проверка hash
        current_hash = file_hash(path)

        return current_hash != old_hash

    def mark_source_indexed(
        self,
        path: Path,
    ):

        stat = path.stat()
        digest = file_hash(path)

        self.conn.execute(
            """
            INSERT INTO source_files
                (path, sha256, size, mtime, indexed_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                sha256=excluded.sha256,
                size=excluded.size,
                mtime=excluded.mtime,
                indexed_at=excluded.indexed_at
            """,
            (
                str(path),
                digest,
                stat.st_size,
                stat.st_mtime,
                time.time(),
            ),
        )

        self.conn.commit()

    def remove_urls_from_source(
        self,
        source: str,
    ):

        self.conn.execute(
            "DELETE FROM urls WHERE source = ?",
            (source,),
        )

        self.conn.commit()

    def add_url(
        self,
        reference_name: str,
        reference_normalized: str,
        url: str,
        source: str,
        priority: int,
    ):

        now = time.time()

        self.conn.execute(
            """
            INSERT INTO urls (
                reference_name,
                reference_normalized,
                url,
                source,
                source_priority,
                first_seen,
                last_seen
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)

            ON CONFLICT(reference_normalized, url)
            DO UPDATE SET
                reference_name=excluded.reference_name,
                source=excluded.source,
                source_priority=excluded.source_priority,
                last_seen=excluded.last_seen
            """,
            (
                reference_name,
                reference_normalized,
                url,
                source,
                priority,
                now,
                now,
            ),
        )

    def commit(self):
        self.conn.commit()

    def get_urls(
        self,
        reference_normalized: str,
    ) -> List[dict]:

        rows = self.conn.execute(
            """
            SELECT
                id,
                reference_name,
                url,
                source,
                source_priority,
                last_checked,
                is_alive,
                response_time,
                success_count,
                failure_count
            FROM urls
            WHERE reference_normalized = ?
            ORDER BY
                CASE
                    WHEN is_alive = 1 THEN 0
                    ELSE 1
                END,
                source_priority DESC,
                success_count DESC,
                failure_count ASC
            """,
            (reference_normalized,),
        ).fetchall()

        result = []

        for row in rows:

            result.append(
                {
                    "id": row[0],
                    "reference_name": row[1],
                    "url": row[2],
                    "source": row[3],
                    "priority": row[4],
                    "last_checked": row[5],
                    "is_alive": row[6],
                    "response_time": row[7],
                    "success_count": row[8],
                    "failure_count": row[9],
                }
            )

        return result

    def update_validation(
        self,
        url_id: int,
        alive: bool,
        response_time: Optional[float],
    ):

        now = time.time()

        if alive:

            self.conn.execute(
                """
                UPDATE urls
                SET
                    last_checked = ?,
                    is_alive = 1,
                    response_time = ?,
                    success_count = success_count + 1
                WHERE id = ?
                """,
                (
                    now,
                    response_time,
                    url_id,
                ),
            )

        else:

            self.conn.execute(
                """
                UPDATE urls
                SET
                    last_checked = ?,
                    is_alive = 0,
                    response_time = ?,
                    failure_count = failure_count + 1
                WHERE id = ?
                """,
                (
                    now,
                    response_time,
                    url_id,
                ),
            )

    def get_statistics(self) -> dict:

        total_urls = self.conn.execute(
            "SELECT COUNT(*) FROM urls"
        ).fetchone()[0]

        alive_urls = self.conn.execute(
            "SELECT COUNT(*) FROM urls WHERE is_alive = 1"
        ).fetchone()[0]

        channels = self.conn.execute(
            """
            SELECT COUNT(DISTINCT reference_normalized)
            FROM urls
            """
        ).fetchone()[0]

        return {
            "urls": total_urls,
            "alive_urls": alive_urls,
            "channels": channels,
        }

    def close(self):

        self.conn.commit()
        self.conn.close()


# ============================================================================
# SOURCES
# ============================================================================

@dataclass
class SourceDefinition:
    name: str
    source_type: str
    location: str
    priority: int
    enabled: bool = True


class SourceManager:

    def __init__(self):

        self.sources: List[SourceDefinition] = []

        data = load_yaml(
            SOURCES_CONFIG_FILE,
            {},
        )

        sources = data.get("sources", [])

        if not isinstance(sources, list):
            sources = []

        for item in sources:

            if not isinstance(item, dict):
                continue

            name = str(
                item.get("name", "")
            ).strip()

            source_type = str(
                item.get("type", "local")
            ).strip().lower()

            location = str(
                item.get("path")
                or item.get("url")
                or ""
            ).strip()

            priority = int(
                item.get("priority", 10)
            )

            enabled = bool(
                item.get("enabled", True)
            )

            if not name or not location:
                continue

            self.sources.append(
                SourceDefinition(
                    name=name,
                    source_type=source_type,
                    location=location,
                    priority=priority,
                    enabled=enabled,
                )
            )

    def local_files(self) -> List[Tuple[Path, str, int]]:

        result = []

        # Автоматически индексируем ВСЕ файлы в sources/.
        default_dir = BASE_DIR / CONFIG["sources"]["local_directory"]

        if default_dir.exists():

            for path in default_dir.rglob("*"):

                if not path.is_file():
                    continue

                if path.suffix.lower() not in {
                    ".m3u",
                    ".m3u8",
                    ".txt",
                }:
                    continue

                result.append(
                    (
                        path,
                        path.name,
                        10,
                    )
                )

        # Дополнительные локальные источники.
        for source in self.sources:

            if not source.enabled:
                continue

            if source.source_type != "local":
                continue

            path = Path(source.location)

            if not path.is_absolute():
                path = BASE_DIR / path

            if path.is_file():

                result.append(
                    (
                        path,
                        source.name,
                        source.priority,
                    )
                )

            elif path.is_dir():

                for file_path in path.rglob("*"):

                    if not file_path.is_file():
                        continue

                    if file_path.suffix.lower() not in {
                        ".m3u",
                        ".m3u8",
                        ".txt",
                    }:
                        continue

                    result.append(
                        (
                            file_path,
                            source.name,
                            source.priority,
                        )
                    )

        # Дедупликация.
        unique = {}
        for path, source, priority in result:
            unique[str(path.resolve())] = (
                path,
                source,
                priority,
            )

        return list(unique.values())

    async def remote_sources(
        self,
    ) -> List[SourceDefinition]:

        if not CONFIG["sources"]["remote_enabled"]:
            return []

        return [
            source
            for source in self.sources
            if source.enabled
            and source.source_type == "remote"
        ]


# ============================================================================
# REMOTE DOWNLOADER
# ============================================================================

class RemoteDownloader:

    def __init__(self):

        self.timeout = aiohttp.ClientTimeout(
            total=30
        )

    async def download(
        self,
        session: aiohttp.ClientSession,
        source: SourceDefinition,
    ) -> Optional[Path]:

        download_dir = (
            BASE_DIR
            / CONFIG["sources"]["download_directory"]
        )

        download_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        safe_name = re.sub(
            r"[^a-zA-Z0-9_.-]+",
            "_",
            source.name,
        )

        output_path = download_dir / (
            safe_name + ".m3u"
        )

        try:

            logger.info(
                "Загрузка удалённого источника: %s",
                source.name,
            )

            async with session.get(
                source.location,
                timeout=self.timeout,
                allow_redirects=True,
            ) as response:

                if response.status != 200:

                    logger.warning(
                        "Источник %s вернул HTTP %s",
                        source.name,
                        response.status,
                    )

                    return None

                content = await response.read()

                if not content:
                    return None

                async with aiofiles.open(
                    output_path,
                    "wb",
                ) as f:

                    await f.write(content)

            return output_path

        except Exception as exc:

            logger.warning(
                "Ошибка загрузки %s: %s",
                source.name,
                exc,
            )

            return None


# ============================================================================
# INDEXER
# ============================================================================

class Indexer:

    def __init__(
        self,
        database: Database,
        matcher: SafeMatcher,
    ):

        self.db = database
        self.matcher = matcher

    async def index_file(
        self,
        path: Path,
        source_name: str,
        priority: int,
        force: bool = False,
    ) -> Tuple[int, int]:

        if not path.exists():
            return 0, 0

        if not force and not self.db.is_source_changed(path):

            logger.info(
                "Без изменений: %s",
                path,
            )

            return 0, 0

        logger.info(
            "Индексирование: %s",
            path,
        )

        # Удаляем старые записи конкретного файла.
        source_key = str(path.resolve())

        self.db.remove_urls_from_source(
            source_key
        )

        entries = await M3UParser.parse(path)

        matched = 0
        unmatched = 0

        for raw_name, url, group in entries:

            channel = self.matcher.match(
                raw_name
            )

            if not channel:

                unmatched += 1

                continue

            self.db.add_url(
                reference_name=channel.name,
                reference_normalized=channel.normalized,
                url=normalize_url(url),
                source=source_key,
                priority=priority,
            )

            matched += 1

        self.db.mark_source_indexed(path)

        self.db.commit()

        logger.info(
            "Источник %s: записей=%d, совпало=%d, "
            "не совпало=%d",
            path.name,
            len(entries),
            matched,
            unmatched,
        )

        return matched, unmatched

    async def index_all(
        self,
        force: bool = False,
    ):

        manager = SourceManager()

        local_files = manager.local_files()

        logger.info(
            "Локальных файлов найдено: %d",
            len(local_files),
        )

        total_matched = 0
        total_unmatched = 0

        for path, source_name, priority in local_files:

            matched, unmatched = await self.index_file(
                path,
                source_name,
                priority,
                force=force,
            )

            total_matched += matched
            total_unmatched += unmatched

        # ------------------------------------------------------------
        # REMOTE
        # ------------------------------------------------------------

        remote_sources = await manager.remote_sources()

        if remote_sources:

            timeout = aiohttp.ClientTimeout(
                total=40
            )

            async with aiohttp.ClientSession(
                timeout=timeout
            ) as session:

                downloader = RemoteDownloader()

                for source in remote_sources:

                    downloaded = await downloader.download(
                        session,
                        source,
                    )

                    if downloaded:

                        await self.index_file(
                            downloaded,
                            source.name,
                            source.priority,
                            force=True,
                        )

        logger.info(
            "Индексирование завершено: matched=%d unmatched=%d",
            total_matched,
            total_unmatched,
        )


# ============================================================================
# VALIDATOR
# ============================================================================

class Validator:

    def __init__(
        self,
        database: Database,
    ):

        self.db = database

        self.semaphore = asyncio.Semaphore(
            CONFIG["validation"]["max_concurrent"]
        )

    async def check_http(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> Tuple[bool, float]:

        start = time.monotonic()

        try:

            headers = {
                "User-Agent": (
                    "Mozilla/5.0 "
                    "(Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 "
                    "Chrome/120 Safari/537.36"
                ),
                "Range": "bytes=0-4095",
            }

            timeout = aiohttp.ClientTimeout(
                total=CONFIG["validation"]["http_timeout"]
            )

            async with session.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
            ) as response:

                if response.status not in {
                    200,
                    206,
                }:
                    return False, time.monotonic() - start

                try:
                    data = await response.content.read(
                        4096
                    )
                except Exception:
                    return False, time.monotonic() - start

                elapsed = time.monotonic() - start

                if len(data) == 0:
                    return False, elapsed

                return True, elapsed

        except Exception:

            return False, time.monotonic() - start

    async def ffprobe(
        self,
        url: str,
    ) -> bool:

        if not CONFIG["validation"]["ffprobe_enabled"]:
            return True

        timeout = int(
            CONFIG["validation"]["ffprobe_timeout"]
        )

        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0,a:0",
            "-show_entries",
            "stream=codec_type",
            "-of",
            "csv=p=0",
            "-timeout",
            str(timeout * 1_000_000),
            url,
        ]

        try:

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, _ = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout + 2,
            )

            if process.returncode != 0:
                return False

            text = stdout.decode(
                "utf-8",
                errors="ignore",
            ).lower()

            return (
                "video" in text
                or "audio" in text
            )

        except asyncio.TimeoutError:

            try:
                process.kill()
            except Exception:
                pass

            return False

        except FileNotFoundError:

            logger.warning(
                "FFprobe не найден. "
                "HTTP-проверка продолжает работать."
            )

            return True

        except Exception:

            return False

    async def validate(
        self,
        session: aiohttp.ClientSession,
        row: dict,
    ) -> bool:

        async with self.semaphore:

            url = row["url"]

            http_ok, response_time = (
                await self.check_http(
                    session,
                    url,
                )
            )

            if not http_ok:

                self.db.update_validation(
                    row["id"],
                    False,
                    response_time,
                )

                return False

            media_ok = await self.ffprobe(
                url
            )

            alive = http_ok and media_ok

            self.db.update_validation(
                row["id"],
                alive,
                response_time,
            )

            return alive


# ============================================================================
# PLAYLIST BUILDER
# ============================================================================

class PlaylistBuilder:

    def __init__(
        self,
        database: Database,
        reference_channels: List[ReferenceChannel],
    ):

        self.db = database
        self.reference_channels = reference_channels

    @staticmethod
    def escape_m3u(text: str) -> str:

        return (
            str(text)
            .replace("\r", " ")
            .replace("\n", " ")
        )

    def choose_url(
        self,
        channel: ReferenceChannel,
    ) -> Optional[dict]:

        urls = self.db.get_urls(
            channel.normalized
        )

        if not urls:
            return None

        # ------------------------------------------------------------
        # Сначала рабочие.
        # ------------------------------------------------------------

        alive = [
            row
            for row in urls
            if row["is_alive"] == 1
        ]

        if alive:

            alive.sort(
                key=lambda row: (
                    -row["priority"],
                    -row["success_count"],
                    row["failure_count"],
                    row["response_time"]
                    if row["response_time"] is not None
                    else 999999,
                )
            )

            return alive[0]

        # ------------------------------------------------------------
        # Рабочий URL не найден.
        #
        # Используем последний известный URL.
        # Это позволяет сохранить канал в плейлисте.
        # ------------------------------------------------------------

        urls.sort(
            key=lambda row: (
                -row["priority"],
                -row["success_count"],
                row["failure_count"],
            )
        )

        return urls[0]

    async def build(
        self,
    ) -> Tuple[int, int, int]:

        lines = [
            "#EXTM3U",
            "# IPTV Manager generated playlist",
            "# Do not edit this file manually.",
        ]

        total = 0
        with_url = 0
        unavailable = 0

        for channel in self.reference_channels:

            if channel.group == "DISABLED":
                continue

            total += 1

            selected = self.choose_url(
                channel
            )

            group = self.escape_m3u(
                channel.group
            )

            name = self.escape_m3u(
                channel.name
            )

            lines.append(
                '#EXTINF:-1 group-title="{}",{}'
                .format(
                    group,
                    name,
                )
            )

            if selected:

                url = selected["url"]

                lines.append(url)

                with_url += 1

            else:

                unavailable += 1

                unavailable_url = (
                    CONFIG["output"]
                    ["unavailable_url"]
                )

                if CONFIG["output"]["include_unavailable"]:

                    lines.append(
                        unavailable_url
                    )

                else:

                    # Этот вариант фактически недостижим,
                    # поскольку обязательные каналы должны
                    # оставаться в итоговом списке.
                    lines.append(
                        unavailable_url
                    )

        tmp_path = PLAYLIST_FILE.with_suffix(
            ".m3u8.tmp"
        )

        async with aiofiles.open(
            tmp_path,
            "w",
            encoding="utf-8",
        ) as f:

            await f.write(
                "\n".join(lines)
                + "\n"
            )

        os.replace(
            tmp_path,
            PLAYLIST_FILE,
        )

        logger.info(
            "Плейлист создан: %s",
            PLAYLIST_FILE,
        )

        logger.info(
            "Каналов: %d | с URL: %d | "
            "без найденного URL: %d",
            total,
            with_url,
            unavailable,
        )

        return total, with_url, unavailable


# ============================================================================
# VALIDATION CYCLE
# ============================================================================

async def validate_all(
    database: Database,
    reference_channels: List[ReferenceChannel],
):

    if not CONFIG["validation"]["enabled"]:

        logger.info(
            "Проверка URL отключена."
        )

        return

    validator = Validator(
        database
    )

    timeout = aiohttp.ClientTimeout(
        total=CONFIG["validation"]["http_timeout"]
    )

    connector = aiohttp.TCPConnector(
        limit=CONFIG["validation"]["max_concurrent"] * 2,
        limit_per_host=10,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        tasks = []

        for channel in reference_channels:

            urls = database.get_urls(
                channel.normalized
            )

            if not urls:
                continue

            # --------------------------------------------------------
            # Проверяем несколько URL:
            #
            # 1. все URL, которые ещё никогда не проверялись;
            # 2. лучший рабочий URL;
            # 3. резервные URL, если у канала нет рабочего.
            # --------------------------------------------------------

            candidates = []

            for row in urls:

                last_checked = row["last_checked"]

                expired = (
                    last_checked is None
                    or (
                        time.time()
                        - last_checked
                        > CONFIG["validation"]["cache_ttl"]
                    )
                )

                if expired:
                    candidates.append(row)

            # Если ничего не требуется проверять,
            # оставляем кэшированные результаты.
            for row in candidates:

                tasks.append(
                    validator.validate(
                        session,
                        row,
                    )
                )

        if tasks:

            logger.info(
                "URL на проверку: %d",
                len(tasks),
            )

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

            success = sum(
                1
                for result in results
                if result is True
            )

            logger.info(
                "Проверка завершена: успешных=%d/%d",
                success,
                len(results),
            )

    database.commit()


# ============================================================================
# DIAGNOSTICS
# ============================================================================

def diagnostics(
    database: Database,
    reference_channels: List[ReferenceChannel],
):

    if not CONFIG["diagnostics"]["enabled"]:
        return

    missing = []
    no_alive = []

    for channel in reference_channels:

        urls = database.get_urls(
            channel.normalized
        )

        if not urls:

            missing.append(channel.name)
            continue

        if not any(
            row["is_alive"] == 1
            for row in urls
        ):

            no_alive.append(channel.name)

    stats = database.get_statistics()

    logger.info(
        "=== ДИАГНОСТИКА ==="
    )

    logger.info(
        "Обязательных каналов: %d",
        len(reference_channels),
    )

    logger.info(
        "Каналов с найденными URL: %d",
        stats["channels"],
    )

    logger.info(
        "Всего URL: %d",
        stats["urls"],
    )

    logger.info(
        "Рабочих URL: %d",
        stats["alive_urls"],
    )

    logger.info(
        "Без единого URL: %d",
        len(missing),
    )

    logger.info(
        "Без рабочего URL: %d",
        len(no_alive),
    )

    limit = int(
        CONFIG["diagnostics"]["unmatched_limit"]
    )

    if missing:

        logger.warning(
            "Каналы без найденного URL:"
        )

        for name in missing[:limit]:

            logger.warning(
                "  ❌ %s",
                name,
            )

    if no_alive:

        logger.warning(
            "Каналы без рабочего URL:"
        )

        for name in no_alive[:limit]:

            logger.warning(
                "  ⚠ %s",
                name,
            )

    logger.info(
        "=== КОНЕЦ ДИАГНОСТИКИ ==="
    )


# ============================================================================
# MAIN
# ============================================================================

async def main():

    force_index = (
        "--force-index"
        in sys.argv
    )

    skip_validation = (
        "--no-validation"
        in sys.argv
    )

    logger.info("=" * 70)
    logger.info("IPTV MANAGER START")
    logger.info("=" * 70)

    # ------------------------------------------------------------
    # 1. REFERENCE
    # ------------------------------------------------------------

    reference_channels = (
        await ReferenceParser.parse(
            REFERENCE_FILE
        )
    )

    # ------------------------------------------------------------
    # 2. CHANNEL CONFIG
    # ------------------------------------------------------------

    channel_config = ChannelConfig(
        CHANNELS_CONFIG_FILE
    )

    channel_config.apply(
        reference_channels
    )

    # ------------------------------------------------------------
    # 3. ALIASES
    # ------------------------------------------------------------

    alias_matcher = AliasMatcher(
        ALIASES_CONFIG_FILE,
        reference_channels,
    )

    matcher = SafeMatcher(
        reference_channels,
        alias_matcher,
    )

    # ------------------------------------------------------------
    # 4. DATABASE
    # ------------------------------------------------------------

    db_path_from_config = Path(
        CONFIG["database"]["path"]
    )

    if not db_path_from_config.is_absolute():
        db_path_from_config = (
            BASE_DIR
            / db_path_from_config
        )

    db_path_from_config.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    database = Database(
        db_path_from_config
    )

    try:

        # --------------------------------------------------------
        # 5. INDEX
        # --------------------------------------------------------

        indexer = Indexer(
            database,
            matcher,
        )

        await indexer.index_all(
            force=force_index
        )

        # --------------------------------------------------------
        # 6. VALIDATION
        # --------------------------------------------------------

        if skip_validation:

            logger.info(
                "Валидация отключена параметром."
            )

        else:

            await validate_all(
                database,
                reference_channels,
            )

        # --------------------------------------------------------
        # 7. DIAGNOSTICS
        # --------------------------------------------------------

        diagnostics(
            database,
            reference_channels,
        )

        # --------------------------------------------------------
        # 8. BUILD PLAYLIST
        # --------------------------------------------------------

        builder = PlaylistBuilder(
            database,
            reference_channels,
        )

        total, with_url, unavailable = (
            await builder.build()
        )

        # --------------------------------------------------------
        # 9. FINAL CHECK
        # --------------------------------------------------------

        if total != len(
            [
                c
                for c in reference_channels
                if c.group != "DISABLED"
            ]
        ):

            raise RuntimeError(
                "КРИТИЧЕСКАЯ ОШИБКА: "
                "количество каналов в playlist "
                "не совпадает с эталоном."
            )

        logger.info("=" * 70)

        logger.info(
            "ГОТОВО"
        )

        logger.info(
            "Обязательных каналов: %d",
            total,
        )

        logger.info(
            "С URL: %d",
            with_url,
        )

        logger.info(
            "Без URL: %d",
            unavailable,
        )

        logger.info(
            "Playlist: %s",
            PLAYLIST_FILE,
        )

        logger.info("=" * 70)

    finally:

        database.close()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        logger.info(
            "Остановлено пользователем."
        )

    except Exception as exc:

        logger.exception(
            "КРИТИЧЕСКАЯ ОШИБКА: %s",
            exc,
        )

        sys.exit(1)