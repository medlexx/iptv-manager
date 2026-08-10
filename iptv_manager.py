#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV Playlist Manager — GitHub Edition
=======================================

Назначение:
    Автоматическое формирование IPTV-плейлиста на основе spisok.txt.

Главный принцип:
    spisok.txt является АБСОЛЮТНЫМ эталоном.

Гарантии:
    1. В новый playlist попадают только каналы из spisok.txt.
    2. Лишние каналы никогда не попадают в playlist.
    3. Для каждого канала проверяются все доступные URL.
    4. URL выбирается по приоритету источника.
    5. Если URL с высоким приоритетом не работает,
       выбирается следующий рабочий URL.
    6. Перед публикацией выполняется строгая проверка состава.
    7. Если хотя бы одного эталонного канала нет рабочего URL,
       новый playlist НЕ публикуется.
    8. Предыдущий рабочий playlist сохраняется.
    9. GitHub Actions может запускать скрипт периодически.
   10. Скрипт поддерживает:
         - удалённые M3U/M3U8/TXT источники;
         - локальные файлы из sources/;
         - fuzzy matching;
         - aliases;
         - HTTP-проверку;
         - FFprobe-проверку;
         - приоритеты источников;
         - диагностический отчёт.

Python:
    3.10+

Зависимости:
    aiohttp
    aiofiles
    PyYAML
    tenacity

FFprobe:
    Если ffprobe установлен, выполняется дополнительная проверка
    медиапотока. Если ffprobe отсутствует, используется HTTP-проверка.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import aiofiles
import aiohttp
import yaml


# ============================================================================
# PATHS
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent

CONFIG_FILE = BASE_DIR / "config.yaml"
SOURCES_FILE = BASE_DIR / "sources.yaml"
REFERENCE_FILE = BASE_DIR / "spisok.txt"

PUBLIC_DIR = BASE_DIR / "public"
PLAYLIST_FILE = PUBLIC_DIR / "eternal_playlist.m3u8"

SOURCES_DIR = BASE_DIR / "sources"

LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "iptv_manager.log"

DIAGNOSTIC_FILE = PUBLIC_DIR / "iptv_diagnostics.json"


# ============================================================================
# LOGGING
# ============================================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)
PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

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
    "matching": {
        "fuzzy_threshold": 0.78,
        "min_normalized_length": 2,
    },

    "validation": {
        "http_timeout": 12,
        "connect_timeout": 7,
        "read_timeout": 10,
        "max_concurrent": 30,
        "validate_with_ffprobe": True,
        "ffprobe_timeout": 10,
        "http_chunk_size": 8192,
        "min_http_bytes": 256,
        "retries": 2,
    },

    "sources": {
        "download_timeout": 60,
        "max_download_size_mb": 100,
        "allow_local_files": True,
    },

    "playlist": {
        "output_file": "public/eternal_playlist.m3u8",
        "encoding": "utf-8",
        "include_group": True,
        "group_title": "IPTV",
    },

    "safety": {
        "require_all_reference_channels": True,
        "never_publish_incomplete": True,
        "keep_previous_playlist": True,
    },

    "diagnostic_mode": True,
}


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass(frozen=True)
class Source:
    name: str
    url: str
    priority: int
    enabled: bool = True


@dataclass
class Candidate:
    reference_name: str
    source_name: str
    source_priority: int
    url: str
    raw_name: str
    match_score: float


@dataclass
class ValidationResult:
    url: str
    valid: bool
    http_valid: bool
    media_valid: bool
    reason: str
    elapsed: float


# ============================================================================
# CONFIG LOADING
# ============================================================================

def deep_merge(base: dict, override: dict) -> dict:
    """
    Рекурсивно объединяет словари.
    """
    result = dict(base)

    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value

    return result


def load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            return {}

        return data

    except Exception as exc:
        logger.error("Ошибка чтения %s: %s", path, exc)
        return {}


def load_config() -> dict:
    config = dict(DEFAULT_CONFIG)

    user_config = load_yaml(CONFIG_FILE)

    if user_config:
        config = deep_merge(config, user_config)

    return config


# ============================================================================
# NAME NORMALIZATION
# ============================================================================

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
        1080p|
        720p|
        576p|
        480p|
        tv|
        channel|
        кан|
        канал|
        тв|
        rus|
        ru|
        eng|
        en|
        backup|
        резерв|
        rez|
        rezrv|
        online|
        онлайн|
        live|
        прямой|
        эфир|
        orig|
        original|
        stream|
        test
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

MULTISPACE_PATTERN = re.compile(r"\s+")

CLEAN_PATTERN = re.compile(
    r"[^a-zа-яё0-9]+",
    re.IGNORECASE,
)


def normalize_name(name: str) -> str:
    """
    Нормализует название канала.

    Примеры:

        "VF Сериалы Турции HD"
            ->
        "vfсериалытурции"

        "Viasat Kino World orig"
            ->
        "viasatkinoworld"

        "2×2"
            ->
        "22"
    """

    if not name:
        return ""

    value = str(name).strip().lower()

    value = value.replace("ё", "е")
    value = value.replace("×", "x")
    value = value.replace("&", "and")

    value = STRIP_WORDS.sub(" ", value)

    value = MULTISPACE_PATTERN.sub(" ", value)

    value = CLEAN_PATTERN.sub("", value)

    return value


# ============================================================================
# REFERENCE PARSER
# ============================================================================

class ReferenceParser:
    """
    spisok.txt:

        371
        VF Сериалы Турции
        3 дня

        372
        VF Вестерн
        7 дней

    Извлекаются только названия каналов.
    """

    NUMBER_PATTERN = re.compile(
        r"^\s*\d+\s*$"
    )

    DURATION_PATTERN = re.compile(
        r"^\s*\d+\s*(день|дня|дней|дн|д)\s*$",
        re.IGNORECASE,
    )

    URL_PATTERN = re.compile(
        r"^(https?|rtmp|rtsp|udp)://",
        re.IGNORECASE,
    )

    @classmethod
    async def parse(
        cls,
        path: Path,
    ) -> List[str]:

        if not path.exists():
            raise FileNotFoundError(
                f"Эталонный файл не найден: {path}"
            )

        result: List[str] = []
        seen: Set[str] = set()

        async with aiofiles.open(
            path,
            "r",
            encoding="utf-8-sig",
            errors="ignore",
        ) as f:

            lines = await f.readlines()

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

            if cls.URL_PATTERN.match(line):
                continue

            normalized = normalize_name(line)

            if len(normalized) < 2:
                continue

            if normalized in seen:
                logger.warning(
                    "Дубликат в spisok.txt пропущен: %s",
                    line,
                )
                continue

            seen.add(normalized)
            result.append(line)

        logger.info(
            "Эталонный список загружен: %d каналов",
            len(result),
        )

        if not result:
            raise RuntimeError(
                "spisok.txt не содержит ни одного канала"
            )

        return result


# ============================================================================
# SOURCE LOADER
# ============================================================================

def load_sources_config() -> List[Source]:

    data = load_yaml(SOURCES_FILE)

    source_items = data.get("sources", [])

    if not isinstance(source_items, list):
        raise ValueError(
            "sources.yaml: параметр 'sources' должен быть списком"
        )

    result: List[Source] = []

    for item in source_items:

        if not isinstance(item, dict):
            continue

        name = str(item.get("name", "")).strip()
        url = str(item.get("url", "")).strip()

        if not name or not url:
            continue

        priority = int(item.get("priority", 10))
        enabled = bool(item.get("enabled", True))

        if not enabled:
            continue

        result.append(
            Source(
                name=name,
                url=url,
                priority=priority,
                enabled=enabled,
            )
        )

    logger.info(
        "Удалённых источников в sources.yaml: %d",
        len(result),
    )

    return result


# ============================================================================
# HTTP DOWNLOAD
# ============================================================================

class SourceDownloader:

    def __init__(self, config: dict):

        self.timeout = int(
            config["sources"]["download_timeout"]
        )

        self.max_size = int(
            config["sources"]["max_download_size_mb"]
        ) * 1024 * 1024

    async def download(
        self,
        session: aiohttp.ClientSession,
        source: Source,
    ) -> Optional[str]:

        logger.info(
            "Загрузка источника: %s",
            source.name,
        )

        try:

            timeout = aiohttp.ClientTimeout(
                total=self.timeout
            )

            async with session.get(
                source.url,
                timeout=timeout,
                allow_redirects=True,
            ) as response:

                if response.status != 200:

                    logger.warning(
                        "Источник %s: HTTP %s",
                        source.name,
                        response.status,
                    )

                    return None

                content_length = response.headers.get(
                    "Content-Length"
                )

                if content_length:

                    try:
                        if int(content_length) > self.max_size:
                            logger.warning(
                                "Источник %s слишком большой",
                                source.name,
                            )
                            return None
                    except ValueError:
                        pass

                chunks: List[bytes] = []
                total = 0

                async for chunk in response.content.iter_chunked(
                    1024 * 64
                ):

                    total += len(chunk)

                    if total > self.max_size:
                        logger.warning(
                            "Источник %s превысил лимит размера",
                            source.name,
                        )
                        return None

                    chunks.append(chunk)

                raw = b"".join(chunks)

                for encoding in (
                    "utf-8",
                    "utf-8-sig",
                    "cp1251",
                    "latin-1",
                ):

                    try:
                        text = raw.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    logger.error(
                        "Не удалось определить кодировку %s",
                        source.name,
                    )
                    return None

                logger.info(
                    "Источник %s загружен: %d байт",
                    source.name,
                    len(raw),
                )

                return text

        except Exception as exc:

            logger.warning(
                "Ошибка загрузки %s: %s",
                source.name,
                exc,
            )

            return None


# ============================================================================
# LOCAL SOURCE LOADER
# ============================================================================

async def load_local_sources() -> List[Tuple[str, str, int]]:

    result: List[Tuple[str, str, int]] = []

    if not SOURCES_DIR.exists():
        return result

    extensions = {
        ".m3u",
        ".m3u8",
        ".txt",
    }

    files = [
        p
        for p in SOURCES_DIR.rglob("*")
        if p.is_file()
        and p.suffix.lower() in extensions
    ]

    for path in files:

        try:

            async with aiofiles.open(
                path,
                "r",
                encoding="utf-8-sig",
                errors="ignore",
            ) as f:

                text = await f.read()

            result.append(
                (
                    path.stem,
                    text,
                    10,
                )
            )

            logger.info(
                "Локальный источник: %s",
                path,
            )

        except Exception as exc:

            logger.warning(
                "Ошибка чтения %s: %s",
                path,
                exc,
            )

    return result


# ============================================================================
# M3U PARSER
# ============================================================================

class M3UParser:

    @staticmethod
    def parse(
        text: str,
        source_name: str,
        source_priority: int,
    ) -> List[Candidate]:

        lines = text.splitlines()

        candidates: List[Candidate] = []

        current_name: Optional[str] = None

        for raw_line in lines:

            line = raw_line.strip()

            if not line:
                continue

            if line.startswith("#EXTINF:"):

                comma_index = line.find(",")

                if comma_index >= 0:
                    current_name = line[
                        comma_index + 1:
                    ].strip()
                else:
                    current_name = None

                continue

            if line.startswith("#"):
                continue

            if not is_stream_url(line):
                continue

            if not current_name:
                current_name = infer_name_from_url(line)

            normalized = normalize_name(
                current_name
            )

            if not normalized:
                current_name = None
                continue

            candidates.append(
                Candidate(
                    reference_name="",
                    source_name=source_name,
                    source_priority=source_priority,
                    url=line,
                    raw_name=current_name,
                    match_score=0.0,
                )
            )

            current_name = None

        return candidates


# ============================================================================
# URL UTILITIES
# ============================================================================

def is_stream_url(value: str) -> bool:

    value = value.strip()

    parsed = urlparse(value)

    if parsed.scheme.lower() in {
        "http",
        "https",
        "rtmp",
        "rtmps",
        "rtsp",
        "udp",
    }:
        return True

    return False


def infer_name_from_url(url: str) -> str:

    try:

        parsed = urlparse(url)

        path = parsed.path.rstrip("/")

        if not path:
            return url

        name = Path(path).stem

        name = name.replace("_", " ")
        name = name.replace("-", " ")

        return name

    except Exception:

        return url


# ============================================================================
# MATCHER
# ============================================================================

class ChannelMatcher:

    def __init__(
        self,
        reference_names: List[str],
        threshold: float,
        aliases: Optional[Dict[str, str]] = None,
    ):

        self.threshold = threshold

        self.reference_names = reference_names

        self.norm_to_reference: Dict[str, str] = {}

        for name in reference_names:

            norm = normalize_name(name)

            if norm:
                self.norm_to_reference[norm] = name

        self.aliases: Dict[str, str] = {}

        if aliases:

            for alias, reference in aliases.items():

                alias_norm = normalize_name(alias)
                reference_norm = normalize_name(reference)

                if (
                    alias_norm
                    and reference_norm
                    in self.norm_to_reference
                ):

                    self.aliases[alias_norm] = (
                        self.norm_to_reference[
                            reference_norm
                        ]
                    )

        self.reference_norms = list(
            self.norm_to_reference.keys()
        )

        logger.info(
            "Матчер: %d эталонных каналов, threshold=%.2f, aliases=%d",
            len(self.reference_names),
            threshold,
            len(self.aliases),
        )

    def match(
        self,
        raw_name: str,
    ) -> Tuple[Optional[str], float]:

        norm = normalize_name(raw_name)

        if not norm:
            return None, 0.0

        if norm in self.norm_to_reference:

            return (
                self.norm_to_reference[norm],
                1.0,
            )

        if norm in self.aliases:

            return (
                self.aliases[norm],
                1.0,
            )

        best_reference: Optional[str] = None
        best_score = 0.0

        for ref_norm in self.reference_norms:

            score = SequenceMatcher(
                None,
                norm,
                ref_norm,
            ).ratio()

            if score > best_score:

                best_score = score

                best_reference = (
                    self.norm_to_reference[
                        ref_norm
                    ]
                )

        if (
            best_reference is not None
            and best_score >= self.threshold
        ):

            return (
                best_reference,
                best_score,
            )

        return None, best_score


# ============================================================================
# VALIDATOR
# ============================================================================

class StreamValidator:

    def __init__(self, config: dict):

        validation = config["validation"]

        self.http_timeout = int(
            validation["http_timeout"]
        )

        self.connect_timeout = int(
            validation["connect_timeout"]
        )

        self.read_timeout = int(
            validation["read_timeout"]
        )

        self.max_concurrent = int(
            validation["max_concurrent"]
        )

        self.chunk_size = int(
            validation["http_chunk_size"]
        )

        self.min_bytes = int(
            validation["min_http_bytes"]
        )

        self.retries = int(
            validation["retries"]
        )

        self.use_ffprobe = bool(
            validation["validate_with_ffprobe"]
        )

        self.ffprobe_timeout = int(
            validation["ffprobe_timeout"]
        )

        self.semaphore = asyncio.Semaphore(
            self.max_concurrent
        )

        self.ffprobe_available = (
            shutil.which("ffprobe") is not None
        )

        if self.use_ffprobe and not self.ffprobe_available:

            logger.warning(
                "ffprobe не найден. "
                "Будет использоваться только HTTP-проверка."
            )

    async def validate(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> ValidationResult:

        started = time.monotonic()

        async with self.semaphore:

            last_reason = "unknown"

            for attempt in range(
                self.retries + 1
            ):

                try:

                    http_ok = await self._validate_http(
                        session,
                        url,
                    )

                    if not http_ok:

                        last_reason = (
                            "HTTP validation failed"
                        )

                        if attempt < self.retries:
                            await asyncio.sleep(
                                1.0 * (attempt + 1)
                            )
                            continue

                        return ValidationResult(
                            url=url,
                            valid=False,
                            http_valid=False,
                            media_valid=False,
                            reason=last_reason,
                            elapsed=(
                                time.monotonic()
                                - started
                            ),
                        )

                    media_ok = True

                    if (
                        self.use_ffprobe
                        and self.ffprobe_available
                    ):

                        media_ok = await asyncio.to_thread(
                            self._validate_ffprobe,
                            url,
                        )

                    if not media_ok:

                        last_reason = (
                            "ffprobe validation failed"
                        )

                        if attempt < self.retries:

                            await asyncio.sleep(
                                1.0 * (attempt + 1)
                            )

                            continue

                        return ValidationResult(
                            url=url,
                            valid=False,
                            http_valid=True,
                            media_valid=False,
                            reason=last_reason,
                            elapsed=(
                                time.monotonic()
                                - started
                            ),
                        )

                    return ValidationResult(
                        url=url,
                        valid=True,
                        http_valid=True,
                        media_valid=True,
                        reason="OK",
                        elapsed=(
                            time.monotonic()
                            - started
                        ),
                    )

                except Exception as exc:

                    last_reason = str(exc)

                    if attempt < self.retries:

                        await asyncio.sleep(
                            1.0 * (attempt + 1)
                        )

            return ValidationResult(
                url=url,
                valid=False,
                http_valid=False,
                media_valid=False,
                reason=last_reason,
                elapsed=(
                    time.monotonic()
                    - started
                ),
            )

    async def _validate_http(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> bool:

        headers = {
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "Chrome/120 Safari/537.36"
            ),
            "Range": "bytes=0-8191",
            "Accept": "*/*",
            "Connection": "close",
        }

        timeout = aiohttp.ClientTimeout(
            total=self.http_timeout,
            connect=self.connect_timeout,
            sock_read=self.read_timeout,
        )

        try:

            async with session.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
            ) as response:

                if response.status not in {
                    200,
                    206,
                    301,
                    302,
                    307,
                    308,
                }:
                    return False

                data = await response.content.read(
                    self.chunk_size
                )

                if len(data) >= self.min_bytes:
                    return True

                # Некоторые IPTV-сервера не отдают первые
                # байты обычным HTTP способом.
                # Если статус корректный и есть данные,
                # допускаем поток.
                if len(data) > 0:
                    return True

                return False

        except Exception:
            return False

    def _validate_ffprobe(
        self,
        url: str,
    ) -> bool:

        command = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type",
            "-of",
            "csv=p=0",
            "-timeout",
            str(
                self.ffprobe_timeout
                * 1_000_000
            ),
            url,
        ]

        try:

            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=self.ffprobe_timeout + 3,
            )

            if result.returncode != 0:
                return False

            output = result.stdout.lower()

            return (
                "video" in output
                or "audio" in output
            )

        except Exception:
            return False


# ============================================================================
# PREVIOUS PLAYLIST READER
# ============================================================================

def read_previous_playlist(
    playlist_path: Path,
) -> Dict[str, str]:

    result: Dict[str, str] = {}

    if not playlist_path.exists():
        return result

    try:

        text = playlist_path.read_text(
            encoding="utf-8",
            errors="ignore",
        )

    except Exception as exc:

        logger.warning(
            "Не удалось прочитать предыдущий playlist: %s",
            exc,
        )

        return result

    current_name: Optional[str] = None

    for raw_line in text.splitlines():

        line = raw_line.strip()

        if line.startswith("#EXTINF:"):

            comma = line.find(",")

            if comma >= 0:
                current_name = (
                    line[comma + 1:].strip()
                )

        elif (
            current_name
            and is_stream_url(line)
        ):

            result[current_name] = line
            current_name = None

    logger.info(
        "Предыдущий playlist: %d каналов",
        len(result),
    )

    return result


# ============================================================================
# PLAYLIST GENERATOR
# ============================================================================

def build_playlist(
    reference_names: List[str],
    selected_urls: Dict[str, str],
    config: dict,
) -> str:

    group_title = config["playlist"].get(
        "group_title",
        "IPTV",
    )

    include_group = bool(
        config["playlist"].get(
            "include_group",
            True,
        )
    )

    lines: List[str] = [
        "#EXTM3U"
    ]

    for reference_name in reference_names:

        url = selected_urls.get(
            reference_name
        )

        if not url:
            raise RuntimeError(
                f"Нет URL для канала: {reference_name}"
            )

        if include_group:

            lines.append(
                "#EXTINF:-1 "
                f'group-title="{group_title}",'
                f"{reference_name}"
            )

        else:

            lines.append(
                f"#EXTINF:-1,{reference_name}"
            )

        lines.append(url)

    return "\n".join(lines) + "\n"


# ============================================================================
# STRICT PLAYLIST CHECK
# ============================================================================

def extract_playlist_names(
    playlist_text: str,
) -> List[str]:

    names: List[str] = []

    for line in playlist_text.splitlines():

        line = line.strip()

        if not line.startswith(
            "#EXTINF:"
        ):
            continue

        comma = line.find(",")

        if comma < 0:
            continue

        name = line[
            comma + 1:
        ].strip()

        if name:
            names.append(name)

    return names


def verify_playlist_strict(
    reference_names: List[str],
    playlist_text: str,
) -> Tuple[bool, str]:

    reference_norm = [
        normalize_name(x)
        for x in reference_names
    ]

    playlist_names = extract_playlist_names(
        playlist_text
    )

    playlist_norm = [
        normalize_name(x)
        for x in playlist_names
    ]

    reference_set = set(
        reference_norm
    )

    playlist_set = set(
        playlist_norm
    )

    missing = reference_set - playlist_set
    extra = playlist_set - reference_set

    duplicate_count = (
        len(playlist_norm)
        - len(set(playlist_norm))
    )

    if missing:

        missing_names = [
            name
            for name in reference_names
            if normalize_name(name)
            in missing
        ]

        return (
            False,
            "Отсутствуют каналы: "
            + ", ".join(missing_names[:20])
        )

    if extra:

        extra_names = [
            name
            for name in playlist_names
            if normalize_name(name)
            in extra
        ]

        return (
            False,
            "Обнаружены лишние каналы: "
            + ", ".join(extra_names[:20])
        )

    if duplicate_count > 0:

        return (
            False,
            f"Обнаружены дубликаты: "
            f"{duplicate_count}"
        )

    if len(reference_names) != len(
        playlist_names
    ):

        return (
            False,
            "Количество каналов не совпадает: "
            f"reference={len(reference_names)}, "
            f"playlist={len(playlist_names)}"
        )

    return (
        True,
        "OK"
    )


# ============================================================================
# DIAGNOSTICS
# ============================================================================

def save_diagnostics(
    diagnostics: dict,
) -> None:

    try:

        DIAGNOSTIC_FILE.write_text(
            json.dumps(
                diagnostics,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    except Exception as exc:

        logger.warning(
            "Не удалось сохранить диагностику: %s",
            exc,
        )


# ============================================================================
# SOURCE COLLECTION
# ============================================================================

async def collect_candidates(
    config: dict,
) -> Dict[str, List[Candidate]]:

    sources = load_sources_config()

    local_sources: List[
        Tuple[str, str, int]
    ] = []

    if config["sources"].get(
        "allow_local_files",
        True,
    ):

        local_sources = (
            await load_local_sources()
        )

    candidates: Dict[
        str,
        List[Candidate],
    ] = {}

    timeout = aiohttp.ClientTimeout(
        total=90
    )

    connector = aiohttp.TCPConnector(
        limit=30,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        downloader = SourceDownloader(
            config
        )

        tasks = []

        for source in sources:

            tasks.append(
                downloader.download(
                    session,
                    source,
                )
            )

        downloaded = []

        if tasks:

            downloaded = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        source_texts: List[
            Tuple[str, str, int]
        ] = []

        for source, result in zip(
            sources,
            downloaded,
        ):

            if isinstance(
                result,
                Exception,
            ):
                logger.warning(
                    "Ошибка источника %s: %s",
                    source.name,
                    result,
                )
                continue

            if result:

                source_texts.append(
                    (
                        source.name,
                        result,
                        source.priority,
                    )
                )

        source_texts.extend(
            local_sources
        )

        if not source_texts:

            logger.error(
                "Не удалось получить ни одного источника"
            )

            return {}

        for (
            source_name,
            text,
            priority,
        ) in source_texts:

            parsed = M3UParser.parse(
                text,
                source_name,
                priority,
            )

            logger.info(
                "Источник %-25s → %d URL",
                source_name,
                len(parsed),
            )

            for candidate in parsed:

                normalized = normalize_name(
                    candidate.raw_name
                )

                if not normalized:
                    continue

                candidates.setdefault(
                    normalized,
                    [],
                ).append(candidate)

    logger.info(
        "Уникальных названий в источниках: %d",
        len(candidates),
    )

    return candidates


# ============================================================================
# MATCH SOURCE CANDIDATES
# ============================================================================

def match_candidates(
    raw_candidates: Dict[
        str,
        List[Candidate],
    ],
    matcher: ChannelMatcher,
) -> Dict[
    str,
    List[Candidate],
]:

    result: Dict[
        str,
        List[Candidate],
    ] = {}

    matched_count = 0
    unmatched_count = 0

    for normalized_name, items in raw_candidates.items():

        for candidate in items:

            reference_name, score = matcher.match(
                candidate.raw_name
            )

            if not reference_name:

                unmatched_count += 1
                continue

            candidate.reference_name = (
                reference_name
            )

            candidate.match_score = score

            result.setdefault(
                reference_name,
                [],
            ).append(candidate)

            matched_count += 1

    logger.info(
        "Совпадений URL с эталоном: %d",
        matched_count,
    )

    logger.info(
        "Нераспознанных URL: %d",
        unmatched_count,
    )

    return result


# ============================================================================
# DEDUPLICATION
# ============================================================================

def deduplicate_candidates(
    candidates: Dict[
        str,
        List[Candidate],
    ],
) -> None:

    for reference_name in list(
        candidates.keys()
    ):

        seen: Set[str] = set()
        unique: List[Candidate] = []

        for candidate in candidates[
            reference_name
        ]:

            url_key = candidate.url.strip()

            if url_key in seen:
                continue

            seen.add(url_key)
            unique.append(candidate)

        unique.sort(
            key=lambda x: (
                x.source_priority,
                x.match_score,
            ),
            reverse=True,
        )

        candidates[
            reference_name
        ] = unique


# ============================================================================
# VALIDATE ALL CANDIDATES
# ============================================================================

async def select_working_urls(
    candidates: Dict[
        str,
        List[Candidate],
    ],
    reference_names: List[str],
    validator: StreamValidator,
) -> Tuple[
    Dict[str, str],
    Dict[str, dict],
]:

    selected: Dict[str, str] = {}

    diagnostics: Dict[str, dict] = {}

    timeout = aiohttp.ClientTimeout(
        total=20
    )

    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=20,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        tasks = []
        metadata = []

        for reference_name in reference_names:

            items = candidates.get(
                reference_name,
                [],
            )

            for candidate in items:

                tasks.append(
                    validator.validate(
                        session,
                        candidate.url,
                    )
                )

                metadata.append(
                    (
                        reference_name,
                        candidate,
                    )
                )

        logger.info(
            "URL на проверку: %d",
            len(tasks),
        )

        results = []

        if tasks:

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        valid_by_channel: Dict[
            str,
            List[Tuple[
                Candidate,
                ValidationResult,
            ]],
        ] = {}

        for (
            meta,
            validation,
        ) in zip(
            metadata,
            results,
        ):

            reference_name, candidate = meta

            if isinstance(
                validation,
                Exception,
            ):

                validation = ValidationResult(
                    url=candidate.url,
                    valid=False,
                    http_valid=False,
                    media_valid=False,
                    reason=str(validation),
                    elapsed=0.0,
                )

            if validation.valid:

                valid_by_channel.setdefault(
                    reference_name,
                    [],
                ).append(
                    (
                        candidate,
                        validation,
                    )
                )

        for reference_name in reference_names:

            valid_items = valid_by_channel.get(
                reference_name,
                [],
            )

            if not valid_items:

                diagnostics[
                    reference_name
                ] = {
                    "status": "NO_WORKING_URL",
                    "candidates": len(
                        candidates.get(
                            reference_name,
                            [],
                        )
                    ),
                }

                continue

            valid_items.sort(
                key=lambda pair: (
                    pair[0].source_priority,
                    pair[0].match_score,
                ),
                reverse=True,
            )

            best_candidate = valid_items[0][0]

            selected[
                reference_name
            ] = best_candidate.url

            diagnostics[
                reference_name
            ] = {
                "status": "OK",
                "source": best_candidate.source_name,
                "priority": best_candidate.source_priority,
                "url": best_candidate.url,
                "match_score": round(
                    best_candidate.match_score,
                    4,
                ),
                "valid_candidates": len(
                    valid_items
                ),
            }

    return selected, diagnostics


# ============================================================================
# MAIN UPDATE
# ============================================================================

async def update_playlist() -> bool:

    started = time.monotonic()

    config = load_config()

    logger.info("=" * 80)
    logger.info("IPTV MANAGER — НАЧАЛО ОБНОВЛЕНИЯ")
    logger.info("=" * 80)

    # ------------------------------------------------------------------------
    # 1. READ REFERENCE
    # ------------------------------------------------------------------------

    reference_names = (
        await ReferenceParser.parse(
            REFERENCE_FILE
        )
    )

    reference_set = {
        normalize_name(x)
        for x in reference_names
    }

    if len(reference_set) != len(
        reference_names
    ):

        raise RuntimeError(
            "spisok.txt содержит дубликаты "
            "после нормализации"
        )

    # ------------------------------------------------------------------------
    # 2. ALIASES
    # ------------------------------------------------------------------------

    aliases = {}

    config_aliases = load_yaml(
        CONFIG_FILE
    ).get(
        "aliases",
        {},
    )

    if isinstance(
        config_aliases,
        dict,
    ):
        aliases = config_aliases

    matcher = ChannelMatcher(
        reference_names=reference_names,
        threshold=float(
            config["matching"][
                "fuzzy_threshold"
            ]
        ),
        aliases=aliases,
    )

    # ------------------------------------------------------------------------
    # 3. LOAD SOURCES
    # ------------------------------------------------------------------------

    raw_candidates = (
        await collect_candidates(
            config
        )
    )

    if not raw_candidates:

        raise RuntimeError(
            "Не удалось загрузить ни одного IPTV-источника"
        )

    # ------------------------------------------------------------------------
    # 4. MATCH
    # ------------------------------------------------------------------------

    matched_candidates = match_candidates(
        raw_candidates,
        matcher,
    )

    deduplicate_candidates(
        matched_candidates
    )

    # ------------------------------------------------------------------------
    # 5. DIAGNOSTICS BEFORE VALIDATION
    # ------------------------------------------------------------------------

    missing_from_sources = [
        name
        for name in reference_names
        if name not in matched_candidates
    ]

    if missing_from_sources:

        logger.error(
            "Каналы из spisok.txt не найдены в источниках: %d",
            len(missing_from_sources),
        )

        for name in missing_from_sources[:50]:

            logger.error(
                "   ❌ %s",
                name,
            )

    # ------------------------------------------------------------------------
    # 6. VALIDATE
    # ------------------------------------------------------------------------

    validator = StreamValidator(
        config
    )

    selected_urls, diagnostics = (
        await select_working_urls(
            matched_candidates,
            reference_names,
            validator,
        )
    )

    # ------------------------------------------------------------------------
    # 7. MISSING WORKING URLS
    # ------------------------------------------------------------------------

    missing_working = [
        name
        for name in reference_names
        if name not in selected_urls
    ]

    logger.info(
        "Рабочих каналов: %d/%d",
        len(selected_urls),
        len(reference_names),
    )

    if missing_working:

        logger.error(
            "=" * 80
        )

        logger.error(
            "НОВЫЙ PLAYLIST НЕ БУДЕТ ОПУБЛИКОВАН!"
        )

        logger.error(
            "Не найден рабочий URL для %d каналов:",
            len(missing_working),
        )

        for name in missing_working:

            logger.error(
                "   ❌ %s",
                name,
            )

        logger.error(
            "=" * 80
        )

        diagnostics_summary = {
            "status": "FAILED",
            "reason": "missing_working_channels",
            "reference_count": len(
                reference_names
            ),
            "working_count": len(
                selected_urls
            ),
            "missing_working": missing_working,
            "elapsed": round(
                time.monotonic()
                - started,
                2,
            ),
        }

        save_diagnostics(
            diagnostics_summary
        )

        return False

    # ------------------------------------------------------------------------
    # 8. BUILD PLAYLIST
    # ------------------------------------------------------------------------

    playlist_text = build_playlist(
        reference_names,
        selected_urls,
        config,
    )

    # ------------------------------------------------------------------------
    # 9. STRICT VERIFICATION
    # ------------------------------------------------------------------------

    strict_ok, strict_message = (
        verify_playlist_strict(
            reference_names,
            playlist_text,
        )
    )

    if not strict_ok:

        logger.error(
            "СТРОГАЯ ПРОВЕРКА PLAYLIST НЕ ПРОЙДЕНА: %s",
            strict_message,
        )

        save_diagnostics(
            {
                "status": "FAILED",
                "reason": "strict_playlist_check",
                "message": strict_message,
            }
        )

        return False

    logger.info(
        "Строгая проверка состава: OK"
    )

    # ------------------------------------------------------------------------
    # 10. ATOMIC WRITE
    # ------------------------------------------------------------------------

    PUBLIC_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_file = PLAYLIST_FILE.with_suffix(
        ".m3u8.tmp"
    )

    try:

        temp_file.write_text(
            playlist_text,
            encoding="utf-8",
        )

        os.replace(
            temp_file,
            PLAYLIST_FILE,
        )

    except Exception:

        if temp_file.exists():

            try:
                temp_file.unlink()
            except Exception:
                pass

        raise

    # ------------------------------------------------------------------------
    # 11. FINAL FILE VERIFICATION
    # ------------------------------------------------------------------------

    if not PLAYLIST_FILE.exists():

        raise RuntimeError(
            "После записи playlist файл отсутствует"
        )

    final_text = PLAYLIST_FILE.read_text(
        encoding="utf-8",
        errors="ignore",
    )

    final_ok, final_message = (
        verify_playlist_strict(
            reference_names,
            final_text,
        )
    )

    if not final_ok:

        raise RuntimeError(
            "Финальная проверка playlist провалена: "
            + final_message
        )

    # ------------------------------------------------------------------------
    # 12. SAVE DIAGNOSTICS
    # ------------------------------------------------------------------------

    elapsed = time.monotonic() - started

    diagnostics_output = {
        "status": "SUCCESS",
        "reference_count": len(
            reference_names
        ),
        "playlist_count": len(
            extract_playlist_names(
                final_text
            )
        ),
        "working_count": len(
            selected_urls
        ),
        "elapsed_seconds": round(
            elapsed,
            2,
        ),
        "channels": diagnostics,
    }

    save_diagnostics(
        diagnostics_output
    )

    # ------------------------------------------------------------------------
    # 13. SUMMARY
    # ------------------------------------------------------------------------

    logger.info("=" * 80)
    logger.info(
        "ПЛЕЙЛИСТ УСПЕШНО ОБНОВЛЁН"
    )
    logger.info(
        "Каналов: %d/%d",
        len(selected_urls),
        len(reference_names),
    )
    logger.info(
        "Файл: %s",
        PLAYLIST_FILE,
    )
    logger.info(
        "Время: %.2f сек.",
        elapsed,
    )
    logger.info("=" * 80)

    return True


# ============================================================================
# ENTRY POINT
# ============================================================================

def main() -> int:

    try:

        success = asyncio.run(
            update_playlist()
        )

        if success:
            return 0

        return 2

    except KeyboardInterrupt:

        logger.info(
            "Остановка пользователем"
        )

        return 130

    except Exception as exc:

        logger.exception(
            "КРИТИЧЕСКАЯ ОШИБКА: %s",
            exc,
        )

        return 1


if __name__ == "__main__":

    sys.exit(
        main()
    )