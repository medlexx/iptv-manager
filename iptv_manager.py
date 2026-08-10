```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV Manager
============

Production-oriented IPTV playlist manager for local files, external playlists
and GitHub Actions.

Основная логика:

1. spisok.txt является ЭТАЛОНОМ.
2. Только каналы из spisok.txt могут попасть в итоговый playlist.
3. Локальные M3U/M3U8/TXT-файлы сканируются из sources_dir.
4. Внешние M3U/M3U8/TXT URL загружаются из sources.yaml.
5. aliases.yaml позволяет сопоставлять разные названия одного канала.
6. Для каждого обязательного канала собираются все найденные URL.
7. URL проверяются HTTP-проверкой и, если доступен ffprobe, медиапроверкой.
8. Для каждого канала выбирается лучший рабочий URL с учётом приоритета.
9. Если основной URL перестал работать, выбирается резервный.
10. validation cache находится в runtime/ и автоматически создаётся.
11. Если cache-файл повреждён или не является SQLite, он автоматически
    переименовывается и создаётся заново.
12. Итоговый файл записывается атомарно.
13. Сервис не должен постоянно работать на GitHub.
    GitHub Actions запускает этот файл по расписанию.

Ожидаемая структура проекта:

iptv-manager/
│
├── iptv_manager.py
├── config.yaml
├── sources.yaml
├── aliases.yaml
├── spisok.txt
├── requirements.txt
│
├── sources/
│   └── локальные_m3u_файлы
│
├── runtime/
│   └── validation_cache.db
│
└── .github/
    └── workflows/
        └── update_playlist.yml
"""

from __future__ import annotations

import asyncio
import aiofiles
import aiohttp
import concurrent.futures
import hashlib
import logging
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import time

from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import yaml


# =============================================================================
# VERSION
# =============================================================================

APP_NAME = "IPTV Manager"
APP_VERSION = "7.0.0"


# =============================================================================
# PATHS
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent

DEFAULT_CONFIG_FILE = BASE_DIR / "config.yaml"
DEFAULT_SOURCES_FILE = BASE_DIR / "sources.yaml"
DEFAULT_ALIASES_FILE = BASE_DIR / "aliases.yaml"
DEFAULT_REFERENCE_FILE = BASE_DIR / "spisok.txt"

DEFAULT_RUNTIME_DIR = BASE_DIR / "runtime"
DEFAULT_CACHE_DB = DEFAULT_RUNTIME_DIR / "validation_cache.db"

DEFAULT_OUTPUT_PLAYLIST = BASE_DIR / "eternal_playlist.m3u8"


# =============================================================================
# DEFAULT CONFIGURATION
# =============================================================================

DEFAULT_CONFIG: Dict[str, Any] = {
    "reference_file": str(DEFAULT_REFERENCE_FILE),
    "output_playlist": str(DEFAULT_OUTPUT_PLAYLIST),

    "sources_dir": str(BASE_DIR / "sources"),
    "sources_file": str(DEFAULT_SOURCES_FILE),
    "aliases_file": str(DEFAULT_ALIASES_FILE),

    "runtime_dir": str(DEFAULT_RUNTIME_DIR),
    "cache_db": str(DEFAULT_CACHE_DB),

    "http_timeout": 12,
    "connect_timeout": 8,
    "read_timeout": 10,

    "ffprobe_enabled": True,
    "ffprobe_timeout": 8,

    "max_concurrent_validations": 30,
    "max_concurrent_downloads": 10,
    "max_connections": 100,
    "max_connections_per_host": 10,

    "cache_ttl": 3600,

    "fuzzy_enabled": True,
    "fuzzy_threshold": 0.86,

    "source_priority": {
        "premium": 100,
        "main": 80,
        "official": 75,
        "backup": 50,
        "reserve": 40,
        "default": 10,
    },

    "playlist_extensions": [
        ".m3u",
        ".m3u8",
        ".txt",
    ],

    "download_user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0 Safari/537.36"
    ),

    "stream_user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0 Safari/537.36"
    ),

    "minimum_http_bytes": 256,

    "require_media_validation": False,

    "keep_previous_playlist_on_failure": True,

    "minimum_playlist_channels": 1,

    "diagnostic_mode": True,

    "strict_reference_mode": True,

    "log_unmatched_limit": 30,

    "log_missing_limit": 50,

    "external_sources_enabled": True,

    "local_sources_enabled": True,

    "verify_external_playlist_downloads": True,

    "deduplicate_urls": True,

    "prefer_cached_valid_url": True,

    "validate_all_candidates": True,
}


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging() -> logging.Logger:
    """
    Настраивает логирование.

    В GitHub Actions основной вывод идёт в stdout.
    Локально дополнительно создаётся iptv_manager.log.
    """

    logger_instance = logging.getLogger(APP_NAME)

    if logger_instance.handlers:
        return logger_instance

    logger_instance.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger_instance.addHandler(console_handler)

    try:
        file_handler = logging.FileHandler(
            BASE_DIR / "iptv_manager.log",
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger_instance.addHandler(file_handler)
    except Exception:
        pass

    logger_instance.propagate = False

    return logger_instance


logger = setup_logging()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, content: str) -> None:
    """
    Атомарная запись текста.

    Сначала создаётся временный файл, затем он заменяет старый.
    Это предотвращает появление наполовину записанного плейлиста.
    """

    ensure_directory(path.parent)

    temp_path = path.with_name(
        f"{path.name}.tmp.{os.getpid()}.{int(time.time() * 1000)}"
    )

    try:
        with open(temp_path, "w", encoding="utf-8", newline="\n") as file:
            file.write(content)

        os.replace(temp_path, path)

    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass


def normalize_url(url: str) -> str:
    """
    Минимальная нормализация URL.

    Нельзя слишком агрессивно изменять IPTV URL:
    параметры запроса могут быть критически важны.
    """

    return url.strip()


def is_stream_url(line: str) -> bool:
    """
    Определяет, похожа ли строка на URL IPTV-потока.
    """

    value = line.strip()

    if not value:
        return False

    lowered = value.lower()

    prefixes = (
        "http://",
        "https://",
        "rtmp://",
        "rtsp://",
        "udp://",
        "rtp://",
    )

    return lowered.startswith(prefixes)


def is_http_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return parsed.scheme.lower() in {"http", "https"}
    except Exception:
        return False


def normalize_path(path_value: str) -> Path:
    """
    Преобразует путь из YAML в абсолютный путь.

    Относительные пути считаются относительно BASE_DIR.
    """

    path = Path(path_value)

    if not path.is_absolute():
        path = BASE_DIR / path

    return path


# =============================================================================
# CONFIG LOADING
# =============================================================================

def deep_merge(
    original: Dict[str, Any],
    update: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Рекурсивно объединяет словари.
    """

    result = dict(original)

    for key, value in update.items():

        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = deep_merge(result[key], value)

        else:
            result[key] = value

    return result


def load_yaml_file(
    path: Path,
    default: Any,
) -> Any:
    """
    Безопасная загрузка YAML.
    """

    if not path.exists():
        return default

    try:
        with open(path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        if data is None:
            return default

        return data

    except Exception as exc:
        logger.error(
            "Ошибка чтения YAML %s: %s",
            path,
            exc,
        )
        return default


def load_config() -> Dict[str, Any]:
    """
    Загружает config.yaml.
    """

    config_path = DEFAULT_CONFIG_FILE

    config = dict(DEFAULT_CONFIG)

    if config_path.exists():

        loaded = load_yaml_file(
            config_path,
            {},
        )

        if isinstance(loaded, dict):
            config = deep_merge(
                config,
                loaded,
            )

        logger.info(
            "Конфигурация загружена: %s",
            config_path,
        )

    else:

        logger.warning(
            "config.yaml не найден. Используются настройки по умолчанию."
        )

    config["reference_file"] = str(
        normalize_path(
            str(config["reference_file"])
        )
    )

    config["output_playlist"] = str(
        normalize_path(
            str(config["output_playlist"])
        )
    )

    config["sources_dir"] = str(
        normalize_path(
            str(config["sources_dir"])
        )
    )

    config["sources_file"] = str(
        normalize_path(
            str(config["sources_file"])
        )
    )

    config["aliases_file"] = str(
        normalize_path(
            str(config["aliases_file"])
        )
    )

    runtime_dir = normalize_path(
        str(config["runtime_dir"])
    )

    ensure_directory(runtime_dir)

    configured_cache = normalize_path(
        str(config["cache_db"])
    )

    config["runtime_dir"] = str(runtime_dir)

    config["cache_db"] = str(configured_cache)

    return config


# =============================================================================
# REFERENCE PARSER
# =============================================================================

class ReferenceParser:
    """
    Парсер spisok.txt.

    Основное правило:
    всё, что не является номером, сроком, URL или комментарием,
    считается названием обязательного канала.

    Поддерживается формат:

        1
        Первый канал
        3 дня

        2
        Второй канал
        7 дней

    Также поддерживается обычный список:

        Первый канал
        Второй канал
        Третий канал
    """

    NUMBER_PATTERN = re.compile(
        r"^\s*\d+\s*$"
    )

    DURATION_PATTERN = re.compile(
        r"^\s*\d+\s*"
        r"(?:день|дня|дней|дн|д|"
        r"day|days|d)"
        r"\s*$",
        re.IGNORECASE,
    )

    @classmethod
    async def parse(
        cls,
        filepath: Path,
    ) -> List[str]:

        if not filepath.exists():

            raise FileNotFoundError(
                f"Эталонный файл не найден: {filepath}"
            )

        async with aiofiles.open(
            filepath,
            "r",
            encoding="utf-8-sig",
        ) as file:

            lines = await file.readlines()

        names: List[str] = []

        seen_normalized: Set[str] = set()

        skipped_numbers = 0
        skipped_durations = 0
        skipped_urls = 0
        skipped_comments = 0
        skipped_duplicates = 0

        for raw_line in lines:

            line = raw_line.strip()

            if not line:
                continue

            if line.startswith("#"):
                skipped_comments += 1
                continue

            if cls.NUMBER_PATTERN.fullmatch(line):

                skipped_numbers += 1
                continue

            if cls.DURATION_PATTERN.fullmatch(line):

                skipped_durations += 1
                continue

            if is_stream_url(line):

                skipped_urls += 1
                continue

            normalized = normalize_channel_name(line)

            if not normalized:
                continue

            if len(normalized) < 2:
                continue

            if normalized in seen_normalized:

                skipped_duplicates += 1
                continue

            seen_normalized.add(normalized)
            names.append(line)

        logger.info(
            "spisok.txt: найдено обязательных каналов: %d",
            len(names),
        )

        logger.info(
            "spisok.txt: номера=%d, сроки=%d, URL=%d, "
            "комментарии=%d, дубликаты=%d",
            skipped_numbers,
            skipped_durations,
            skipped_urls,
            skipped_comments,
            skipped_duplicates,
        )

        if not names:

            raise RuntimeError(
                "spisok.txt не содержит ни одного канала."
            )

        return names


# =============================================================================
# CHANNEL NAME NORMALIZATION
# =============================================================================

STRIP_WORDS_PATTERN = re.compile(
    r"""
    \b
    (?:
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
        2160p?|
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
        uk|
        backup|
        резерв|
        резервный|
        reserve|
        orig|
        original|
        online|
        онлайн|
        live|
        прямой|
        эфир|
        stream|
        test
    )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

PUNCTUATION_PATTERN = re.compile(
    r"[^a-zа-яё0-9]+",
    re.IGNORECASE,
)


def normalize_channel_name(name: str) -> str:
    """
    Приводит название канала к форме для сравнения.

    Примеры:

        ".black HD"              -> "black"
        "Кино Хит HD"            -> "кинохит"
        "KinoHit"                -> "kinohit"
        "Viasat Kino World orig" -> "viasatkinoworld"
        "2×2"                    -> "22"
    """

    value = str(name).strip().lower()

    value = value.replace("ё", "е")
    value = value.replace("×", "x")

    value = STRIP_WORDS_PATTERN.sub(" ", value)

    value = PUNCTUATION_PATTERN.sub("", value)

    return value


# =============================================================================
# ALIASES
# =============================================================================

class AliasManager:
    """
    Загружает aliases.yaml.

    Поддерживаются варианты:

        "Кино Хит":
          - "КиноХит"
          - "КИНОХИТ HD"

    и:

        aliases:
          "Кино Хит":
            - "КиноХит"
            - "Kinohit"
    """

    def __init__(
        self,
        filepath: Path,
    ):
        self.filepath = filepath

        self.alias_to_canonical: Dict[str, str] = {}

    def load(
        self,
        reference_names: Iterable[str],
    ) -> None:

        self.alias_to_canonical.clear()

        reference_by_normalized = {
            normalize_channel_name(name): name
            for name in reference_names
        }

        if not self.filepath.exists():

            logger.info(
                "aliases.yaml не найден. Используется только нормализация."
            )

            return

        data = load_yaml_file(
            self.filepath,
            {},
        )

        if not isinstance(data, dict):

            logger.warning(
                "aliases.yaml имеет неверный формат."
            )

            return

        if "aliases" in data and isinstance(
            data["aliases"],
            dict,
        ):
            aliases_data = data["aliases"]

        else:
            aliases_data = data

        total = 0

        for canonical_name, aliases in aliases_data.items():

            if not isinstance(canonical_name, str):
                continue

            canonical_match = self._find_reference(
                canonical_name,
                reference_by_normalized,
            )

            if canonical_match is None:

                logger.warning(
                    "Alias canonical '%s' отсутствует в spisok.txt.",
                    canonical_name,
                )

                continue

            if isinstance(aliases, str):

                aliases = [aliases]

            if not isinstance(aliases, list):

                continue

            for alias in aliases:

                if not isinstance(alias, str):
                    continue

                alias_norm = normalize_channel_name(alias)

                if not alias_norm:
                    continue

                self.alias_to_canonical[
                    alias_norm
                ] = canonical_match

                total += 1

            canonical_norm = normalize_channel_name(
                canonical_match
            )

            self.alias_to_canonical[
                canonical_norm
            ] = canonical_match

        logger.info(
            "Загружено aliases: %d",
            total,
        )

    @staticmethod
    def _find_reference(
        value: str,
        reference_by_normalized: Dict[str, str],
    ) -> Optional[str]:

        normalized = normalize_channel_name(value)

        if normalized in reference_by_normalized:
            return reference_by_normalized[normalized]

        return None

    def resolve(
        self,
        raw_name: str,
    ) -> Optional[str]:

        normalized = normalize_channel_name(raw_name)

        if not normalized:
            return None

        return self.alias_to_canonical.get(normalized)


# =============================================================================
# SOURCE DATA STRUCTURES
# =============================================================================

@dataclass(frozen=True)
class StreamCandidate:
    canonical_name: str
    source_name: str
    url: str
    priority: int
    source: str


# =============================================================================
# CHANNEL MATCHER
# =============================================================================

class ChannelMatcher:
    """
    Сопоставляет названия источников с обязательными названиями.

    Приоритет:

    1. alias
    2. точное нормализованное совпадение
    3. fuzzy matching

    Fuzzy matching используется только при достаточно высоком threshold.
    """

    def __init__(
        self,
        reference_names: List[str],
        aliases: AliasManager,
        threshold: float,
        fuzzy_enabled: bool,
    ):

        self.reference_names = reference_names
        self.aliases = aliases
        self.threshold = threshold
        self.fuzzy_enabled = fuzzy_enabled

        self.norm_to_reference: Dict[str, str] = {}

        for name in reference_names:

            normalized = normalize_channel_name(name)

            if normalized:
                self.norm_to_reference[
                    normalized
                ] = name

    def match(
        self,
        raw_name: str,
    ) -> Optional[str]:

        if not raw_name:
            return None

        alias_result = self.aliases.resolve(raw_name)

        if alias_result is not None:
            return alias_result

        normalized = normalize_channel_name(raw_name)

        if not normalized:
            return None

        exact = self.norm_to_reference.get(
            normalized
        )

        if exact is not None:
            return exact

        if not self.fuzzy_enabled:
            return None

        best_name: Optional[str] = None
        best_ratio = 0.0

        for ref_norm, ref_name in self.norm_to_reference.items():

            ratio = SequenceMatcher(
                None,
                normalized,
                ref_norm,
            ).ratio()

            if ratio > best_ratio:

                best_ratio = ratio
                best_name = ref_name

        if (
            best_name is not None
            and best_ratio >= self.threshold
        ):

            return best_name

        return None


# =============================================================================
# SOURCE PRIORITY
# =============================================================================

def calculate_source_priority(
    source_name: str,
    priority_map: Dict[str, Any],
) -> int:

    value = str(source_name).lower()

    best_priority: Optional[int] = None

    for keyword, weight in priority_map.items():

        if str(keyword).lower() == "default":
            continue

        if str(keyword).lower() in value:

            priority = safe_int(
                weight,
                10,
            )

            if (
                best_priority is None
                or priority > best_priority
            ):

                best_priority = priority

    if best_priority is not None:
        return best_priority

    return safe_int(
        priority_map.get("default", 10),
        10,
    )


# =============================================================================
# M3U PARSER
# =============================================================================

class M3UParser:
    """
    Парсер M3U/M3U8/TXT.

    Поддерживает:

        #EXTINF:-1,Channel
        http://url

    и:

        #EXTINF:-1 tvg-name="Channel",Channel
        http://url

    При наличии tvg-name он учитывается как дополнительный кандидат имени.
    """

    EXTINF_PATTERN = re.compile(
        r"^#EXTINF\s*:\s*(-?\d+)(.*)$",
        re.IGNORECASE,
    )

    ATTRIBUTE_PATTERN = re.compile(
        r'([A-Za-z0-9_-]+)\s*=\s*"([^"]*)"',
        re.IGNORECASE,
    )

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
            ) as file:

                lines = await file.readlines()

        except Exception as exc:

            logger.error(
                "Ошибка чтения источника %s: %s",
                filepath,
                exc,
            )

            return result

        last_extinf_name: Optional[str] = None
        last_tvg_name: Optional[str] = None

        for raw_line in lines:

            line = raw_line.strip()

            if not line:
                continue

            if line.startswith("#EXTINF"):

                match = cls.EXTINF_PATTERN.match(line)

                if not match:
                    continue

                attributes_and_name = match.group(2)

                attributes = dict(
                    cls.ATTRIBUTE_PATTERN.findall(
                        attributes_and_name
                    )
                )

                tvg_name = (
                    attributes.get("tvg-name")
                    or attributes.get("tvg_name")
                    or attributes.get("name")
                )

                if "," in attributes_and_name:

                    display_name = (
                        attributes_and_name.split(
                            ",",
                            1,
                        )[1].strip()
                    )

                else:

                    display_name = ""

                last_extinf_name = (
                    display_name
                    or tvg_name
                    or None
                )

                last_tvg_name = tvg_name

                continue

            if line.startswith("#"):
                continue

            if not is_stream_url(line):
                continue

            url = normalize_url(line)

            if last_extinf_name:

                result.append(
                    (
                        last_extinf_name,
                        url,
                    )
                )

            if (
                last_tvg_name
                and last_tvg_name != last_extinf_name
            ):

                result.append(
                    (
                        last_tvg_name,
                        url,
                    )
                )

            last_extinf_name = None
            last_tvg_name = None

        return result


# =============================================================================
# LOCAL SOURCE LOADER
# =============================================================================

class LocalSourceLoader:
    def __init__(
        self,
        sources_dir: Path,
        extensions: Set[str],
        priority_map: Dict[str, Any],
    ):

        self.sources_dir = sources_dir
        self.extensions = {
            extension.lower()
            for extension in extensions
        }
        self.priority_map = priority_map

    async def load(
        self,
    ) -> List[StreamCandidate]:

        if not self.sources_dir.exists():

            logger.warning(
                "Директория локальных источников не найдена: %s",
                self.sources_dir,
            )

            return []

        files = []

        for path in self.sources_dir.rglob("*"):

            if not path.is_file():
                continue

            if path.suffix.lower() in self.extensions:

                files.append(path)

        logger.info(
            "Локальных файлов источников найдено: %d",
            len(files),
        )

        candidates: List[StreamCandidate] = []

        semaphore = asyncio.Semaphore(10)

        async def parse_one(
            path: Path,
        ) -> List[StreamCandidate]:

            async with semaphore:

                parsed = await M3UParser.parse_file(
                    path
                )

                priority = calculate_source_priority(
                    path.name,
                    self.priority_map,
                )

                result: List[StreamCandidate] = []

                for name, url in parsed:

                    result.append(
                        StreamCandidate(
                            canonical_name="",
                            source_name=name,
                            url=url,
                            priority=priority,
                            source=str(path),
                        )
                    )

                return result

        tasks = [
            parse_one(path)
            for path in files
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        for result in results:

            if isinstance(
                result,
                Exception,
            ):

                logger.error(
                    "Ошибка обработки локального источника: %s",
                    result,
                )

                continue

            candidates.extend(result)

        logger.info(
            "Из локальных источников получено URL-записей: %d",
            len(candidates),
        )

        return candidates


# =============================================================================
# EXTERNAL SOURCES
# =============================================================================

@dataclass
class ExternalSource:
    name: str
    url: str
    priority: int
    enabled: bool = True


class ExternalSourceLoader:
    """
    Загружает внешние M3U/M3U8/TXT.

    Поддерживает sources.yaml:

        playlists:
          - name: source_1
            url: https://example.com/list.m3u
            priority: 50
            enabled: true

    Также поддерживает:

        sources:
          - name: source_1
            url: https://example.com/list.m3u
    """

    def __init__(
        self,
        config: Dict[str, Any],
    ):

        self.config = config

        self.sources_file = Path(
            config["sources_file"]
        )

        self.max_downloads = safe_int(
            config.get(
                "max_concurrent_downloads",
                10,
            ),
            10,
        )

    def load_definitions(
        self,
    ) -> List[ExternalSource]:

        if not self.sources_file.exists():

            logger.info(
                "sources.yaml пока отсутствует: %s",
                self.sources_file,
            )

            return []

        data = load_yaml_file(
            self.sources_file,
            {},
        )

        if not isinstance(data, dict):
            return []

        raw_sources = (
            data.get("playlists")
            or data.get("sources")
            or []
        )

        if isinstance(
            raw_sources,
            dict,
        ):

            converted = []

            for name, url in raw_sources.items():

                if isinstance(url, str):

                    converted.append(
                        {
                            "name": name,
                            "url": url,
                        }
                    )

            raw_sources = converted

        result: List[ExternalSource] = []

        if not isinstance(
            raw_sources,
            list,
        ):

            return result

        for index, item in enumerate(
            raw_sources,
            start=1,
        ):

            if isinstance(
                item,
                str,
            ):

                url = item

                result.append(
                    ExternalSource(
                        name=f"external_{index}",
                        url=url,
                        priority=10,
                        enabled=True,
                    )
                )

                continue

            if not isinstance(
                item,
                dict,
            ):

                continue

            url = item.get("url")

            if not isinstance(
                url,
                str,
            ):
                continue

            name = str(
                item.get(
                    "name",
                    f"external_{index}",
                )
            )

            priority = safe_int(
                item.get(
                    "priority",
                    10,
                ),
                10,
            )

            enabled = bool(
                item.get(
                    "enabled",
                    True,
                )
            )

            if not enabled:
                continue

            result.append(
                ExternalSource(
                    name=name,
                    url=url.strip(),
                    priority=priority,
                    enabled=True,
                )
            )

        logger.info(
            "Внешних источников из sources.yaml: %d",
            len(result),
        )

        return result

    async def load(
        self,
    ) -> List[StreamCandidate]:

        sources = self.load_definitions()

        if not sources:
            return []

        connector = aiohttp.TCPConnector(
            limit=self.max_downloads,
            limit_per_host=5,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
        )

        timeout = aiohttp.ClientTimeout(
            total=30,
            connect=10,
            sock_read=25,
        )

        headers = {
            "User-Agent": self.config.get(
                "download_user_agent",
                DEFAULT_CONFIG[
                    "download_user_agent"
                ],
            ),
        }

        semaphore = asyncio.Semaphore(
            self.max_downloads
        )

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
        ) as session:

            async def download_one(
                source: ExternalSource,
            ) -> List[StreamCandidate]:

                async with semaphore:

                    try:

                        logger.info(
                            "Загрузка внешнего источника: %s",
                            source.name,
                        )

                        async with session.get(
                            source.url,
                            allow_redirects=True,
                        ) as response:

                            if response.status != 200:

                                logger.warning(
                                    "Источник %s: HTTP %s",
                                    source.name,
                                    response.status,
                                )

                                return []

                            content = await response.text(
                                encoding="utf-8",
                                errors="ignore",
                            )

                        parsed = (
                            await self.parse_text(
                                content
                            )
                        )

                        result = []

                        for name, url in parsed:

                            result.append(
                                StreamCandidate(
                                    canonical_name="",
                                    source_name=name,
                                    url=url,
                                    priority=source.priority,
                                    source=source.name,
                                )
                            )

                        logger.info(
                            "Источник %s: получено записей %d",
                            source.name,
                            len(result),
                        )

                        return result

                    except Exception as exc:

                        logger.warning(
                            "Ошибка загрузки %s: %s",
                            source.name,
                            exc,
                        )

                        return []

            tasks = [
                download_one(source)
                for source in sources
            ]

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        candidates: List[StreamCandidate] = []

        for result in results:

            if isinstance(
                result,
                Exception,
            ):
                continue

            candidates.extend(result)

        logger.info(
            "Из внешних источников получено URL-записей: %d",
            len(candidates),
        )

        return candidates

    async def parse_text(
        self,
        content: str,
    ) -> List[Tuple[str, str]]:

        lines = content.splitlines()

        result: List[Tuple[str, str]] = []

        last_name: Optional[str] = None
        last_tvg_name: Optional[str] = None

        for raw_line in lines:

            line = raw_line.strip()

            if not line:
                continue

            if line.startswith("#EXTINF"):

                match = M3UParser.EXTINF_PATTERN.match(
                    line
                )

                if not match:
                    continue

                attributes_and_name = match.group(2)

                attributes = dict(
                    M3UParser.ATTRIBUTE_PATTERN.findall(
                        attributes_and_name
                    )
                )

                last_tvg_name = (
                    attributes.get("tvg-name")
                    or attributes.get("tvg_name")
                    or attributes.get("name")
                )

                if "," in attributes_and_name:

                    display_name = (
                        attributes_and_name.split(
                            ",",
                            1,
                        )[1].strip()
                    )

                else:

                    display_name = ""

                last_name = (
                    display_name
                    or last_tvg_name
                    or None
                )

                continue

            if line.startswith("#"):
                continue

            if not is_stream_url(line):
                continue

            if last_name:

                result.append(
                    (
                        last_name,
                        line,
                    )
                )

            if (
                last_tvg_name
                and last_tvg_name != last_name
            ):

                result.append(
                    (
                        last_tvg_name,
                        line,
                    )
                )

            last_name = None
            last_tvg_name = None

        return result


# =============================================================================
# SQLITE CACHE
# =============================================================================

class PersistentCache:
    """
    Надёжный SQLite cache.

    Важное исправление:

    Если существующий файл не является SQLite-базой,
    программа НЕ падает с:

        sqlite3.DatabaseError:
        file is not a database

    Вместо этого повреждённый файл переименовывается в:

        validation_cache.db.corrupt.TIMESTAMP

    и создаётся новая база.

    Также SQLite хранится в runtime/.
    """

    def __init__(
        self,
        db_path: Path,
        ttl: int,
    ):

        self.db_path = db_path
        self.ttl = ttl

        ensure_directory(
            self.db_path.parent
        )

        self.conn = self._open_database()

        self._buffer: List[
            Tuple[str, int, float]
        ] = []

        self._lock = asyncio.Lock()

    def _open_database(
        self,
    ) -> sqlite3.Connection:

        try:

            conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                timeout=30,
            )

            conn.execute(
                "PRAGMA busy_timeout=30000"
            )

            conn.execute(
                "PRAGMA journal_mode=WAL"
            )

            conn.execute(
                "PRAGMA synchronous=NORMAL"
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache (
                    url TEXT PRIMARY KEY,
                    is_valid INTEGER NOT NULL,
                    checked_at REAL NOT NULL
                )
                """
            )

            conn.commit()

            return conn

        except sqlite3.DatabaseError as exc:

            logger.warning(
                "Файл кэша повреждён или не является SQLite: %s",
                self.db_path,
            )

            logger.warning(
                "SQLite сообщает: %s",
                exc,
            )

            try:
                conn.close()
            except Exception:
                pass

            backup_path = self.db_path.with_name(
                f"{self.db_path.name}.corrupt."
                f"{int(time.time())}"
            )

            try:

                if self.db_path.exists():

                    os.replace(
                        self.db_path,
                        backup_path,
                    )

                    logger.warning(
                        "Старый cache переименован в: %s",
                        backup_path,
                    )

            except Exception as rename_exc:

                logger.error(
                    "Не удалось переименовать повреждённый cache: %s",
                    rename_exc,
                )

                try:

                    self.db_path.unlink(
                        missing_ok=True
                    )

                except Exception:
                    pass

            conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                timeout=30,
            )

            conn.execute(
                "PRAGMA busy_timeout=30000"
            )

            conn.execute(
                "PRAGMA journal_mode=WAL"
            )

            conn.execute(
                "PRAGMA synchronous=NORMAL"
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache (
                    url TEXT PRIMARY KEY,
                    is_valid INTEGER NOT NULL,
                    checked_at REAL NOT NULL
                )
                """
            )

            conn.commit()

            logger.info(
                "Создан новый SQLite cache: %s",
                self.db_path,
            )

            return conn

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

        except sqlite3.DatabaseError as exc:

            logger.error(
                "Ошибка чтения SQLite cache: %s",
                exc,
            )

            return None

        if not row:
            return None

        checked_at = float(row[1])

        if (
            time.time() - checked_at
            >= self.ttl
        ):

            return None

        return bool(row[0])

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

            if len(self._buffer) >= 100:

                self._flush_sync()

    def _flush_sync(self) -> None:

        if not self._buffer:
            return

        try:

            self.conn.executemany(
                """
                INSERT OR REPLACE INTO cache
                (
                    url,
                    is_valid,
                    checked_at
                )
                VALUES (?, ?, ?)
                """,
                self._buffer,
            )

            self.conn.commit()

            self._buffer.clear()

        except sqlite3.DatabaseError as exc:

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
                    "Удалено устаревших cache-записей: %d",
                    deleted,
                )

        except sqlite3.DatabaseError as exc:

            logger.warning(
                "Ошибка очистки SQLite cache: %s",
                exc,
            )

    def close(self) -> None:

        try:

            self._flush_sync()
            self.conn.close()

        except Exception:
            pass


# =============================================================================
# STREAM VALIDATOR
# =============================================================================

class StreamValidator:
    """
    Проверка IPTV URL.

    Сначала выполняется HTTP-проверка.
    Если включён ffprobe и найден ffprobe, выполняется медиапроверка.

    require_media_validation=False означает:
    отсутствие ffprobe не уничтожает весь плейлист.

    Если ffprobe включён и работает, результат используется для повышения
    качества проверки.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        cache: PersistentCache,
    ):

        self.config = config
        self.cache = cache

        self.semaphore = asyncio.Semaphore(
            safe_int(
                config.get(
                    "max_concurrent_validations",
                    30,
                ),
                30,
            )
        )

        self.ffprobe_enabled = bool(
            config.get(
                "ffprobe_enabled",
                True,
            )
        )

        self.require_media_validation = bool(
            config.get(
                "require_media_validation",
                False,
            )
        )

        self.ffprobe_timeout = safe_int(
            config.get(
                "ffprobe_timeout",
                8,
            ),
            8,
        )

        self.http_timeout = safe_int(
            config.get(
                "http_timeout",
                12,
            ),
            12,
        )

        self.connect_timeout = safe_int(
            config.get(
                "connect_timeout",
                8,
            ),
            8,
        )

        self.minimum_http_bytes = safe_int(
            config.get(
                "minimum_http_bytes",
                256,
            ),
            256,
        )

        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=safe_int(
                config.get(
                    "max_concurrent_validations",
                    30,
                ),
                30,
            ),
            thread_name_prefix="ffprobe",
        )

        self._ffprobe_available: Optional[bool] = None

    def _detect_ffprobe(
        self,
    ) -> bool:

        if self._ffprobe_available is not None:
            return self._ffprobe_available

        try:

            result = subprocess.run(
                [
                    "ffprobe",
                    "-version",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )

            self._ffprobe_available = (
                result.returncode == 0
            )

        except Exception:

            self._ffprobe_available = False

        if self._ffprobe_available:

            logger.info(
                "FFprobe найден."
            )

        else:

            logger.warning(
                "FFprobe не найден. "
                "Медиапроверка отключена."
            )

        return self._ffprobe_available

    async def _check_http(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> bool:

        if not is_http_url(url):

            return True

        timeout = aiohttp.ClientTimeout(
            total=self.http_timeout,
            connect=self.connect_timeout,
            sock_read=self.http_timeout,
        )

        headers = {
            "User-Agent": self.config.get(
                "stream_user_agent",
                DEFAULT_CONFIG[
                    "stream_user_agent"
                ],
            ),
            "Range": "bytes=0-4095",
            "Accept": "*/*",
            "Connection": "keep-alive",
        }

        try:

            async with session.get(
                url,
                headers=headers,
                allow_redirects=True,
                timeout=timeout,
            ) as response:

                if response.status not in (
                    200,
                    206,
                    301,
                    302,
                    307,
                    308,
                ):

                    return False

                if response.status in (
                    301,
                    302,
                    307,
                    308,
                ):

                    return True

                try:

                    chunk = await response.content.read(
                        self.minimum_http_bytes
                    )

                    if len(chunk) == 0:

                        return False

                    return True

                except Exception:

                    return True

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            OSError,
        ):

            return False

    def _run_ffprobe_sync(
        self,
        url: str,
    ) -> bool:

        if not self._detect_ffprobe():
            return False

        command = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
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
                capture_output=True,
                text=True,
                timeout=self.ffprobe_timeout + 2,
            )

            output = (
                result.stdout
                + "\n"
                + result.stderr
            ).lower()

            return (
                result.returncode == 0
                and (
                    "video" in output
                    or "audio" in output
                )
            )

        except (
            subprocess.TimeoutExpired,
            OSError,
        ):

            return False

        except Exception:

            return False

    async def validate_url(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> bool:

        cached = self.cache.get(url)

        if cached is not None:

            return cached

        async with self.semaphore:

            try:

                http_ok = await self._check_http(
                    session,
                    url,
                )

                if not http_ok:

                    await self.cache.set(
                        url,
                        False,
                    )

                    return False

                if not is_http_url(url):

                    await self.cache.set(
                        url,
                        True,
                    )

                    return True

                ffprobe_available = (
                    self._detect_ffprobe()
                    if self.ffprobe_enabled
                    else False
                )

                if (
                    self.ffprobe_enabled
                    and ffprobe_available
                ):

                    loop = asyncio.get_running_loop()

                    media_ok = await loop.run_in_executor(
                        self.executor,
                        self._run_ffprobe_sync,
                        url,
                    )

                    if media_ok:

                        await self.cache.set(
                            url,
                            True,
                        )

                        return True

                    if self.require_media_validation:

                        await self.cache.set(
                            url,
                            False,
                        )

                        return False

                await self.cache.set(
                    url,
                    True,
                )

                return True

            except Exception as exc:

                logger.debug(
                    "Validation error %s: %s",
                    url,
                    exc,
                )

                await self.cache.set(
                    url,
                    False,
                )

                return False

    def shutdown(self) -> None:

        try:

            self.executor.shutdown(
                wait=False,
                cancel_futures=True,
            )

        except TypeError:

            self.executor.shutdown(
                wait=False,
            )


# =============================================================================
# CANDIDATE COLLECTION
# =============================================================================

class CandidateCollector:
    """
    Превращает сырые StreamCandidate в:

        canonical channel -> list of URLs

    Только каналы из reference_names разрешаются в итоговую структуру.
    """

    def __init__(
        self,
        reference_names: List[str],
        matcher: ChannelMatcher,
        priority_map: Dict[str, Any],
    ):

        self.reference_names = reference_names
        self.matcher = matcher
        self.priority_map = priority_map

    def collect(
        self,
        raw_candidates: List[StreamCandidate],
    ) -> Dict[
        str,
        List[StreamCandidate],
    ]:

        result: Dict[
            str,
            List[StreamCandidate],
        ] = {}

        matched = 0
        unmatched = 0

        for candidate in raw_candidates:

            canonical = self.matcher.match(
                candidate.source_name
            )

            if canonical is None:

                unmatched += 1
                continue

            priority = candidate.priority

            if priority is None:

                priority = calculate_source_priority(
                    candidate.source,
                    self.priority_map,
                )

            normalized_candidate = StreamCandidate(
                canonical_name=canonical,
                source_name=candidate.source_name,
                url=normalize_url(
                    candidate.url
                ),
                priority=priority,
                source=candidate.source,
            )

            result.setdefault(
                canonical,
                [],
            ).append(
                normalized_candidate
            )

            matched += 1

        if result:

            for name in result:

                result[name] = (
                    self._deduplicate_and_sort(
                        result[name]
                    )
                )

        logger.info(
            "Сопоставлено URL-записей: %d",
            matched,
        )

        logger.info(
            "Не сопоставлено URL-записей: %d",
            unmatched,
        )

        return result

    @staticmethod
    def _deduplicate_and_sort(
        candidates: List[StreamCandidate],
    ) -> List[StreamCandidate]:

        seen: Set[str] = set()
        result: List[StreamCandidate] = []

        for candidate in candidates:

            url = candidate.url

            if not url:
                continue

            if url in seen:
                continue

            seen.add(url)

            result.append(candidate)

        result.sort(
            key=lambda item: (
                item.priority,
                item.source,
            ),
            reverse=True,
        )

        return result


# =============================================================================
# PLAYLIST GENERATOR
# =============================================================================

class PlaylistGenerator:
    """
    Создаёт итоговый M3U.

    Ключевой принцип:

    Итоговые имена берутся ТОЛЬКО из spisok.txt.

    Никакие имена из внешнего источника напрямую в плейлист не попадают.
    """

    @staticmethod
    def generate_content(
        reference_names: List[str],
        valid_channels: Dict[str, str],
    ) -> str:

        lines: List[str] = [
            "#EXTM3U",
        ]

        for reference_name in reference_names:

            url = valid_channels.get(
                reference_name
            )

            if not url:
                continue

            lines.append(
                f"#EXTINF:-1,{reference_name}"
            )

            lines.append(url)

        return (
            "\n".join(lines)
            + "\n"
        )

    @classmethod
    async def generate(
        cls,
        output_path: Path,
        reference_names: List[str],
        valid_channels: Dict[str, str],
        keep_previous_on_failure: bool,
        minimum_channels: int,
    ) -> bool:

        valid_count = len(valid_channels)

        if valid_count < minimum_channels:

            logger.error(
                "Слишком мало рабочих каналов: %d. "
                "Требуется минимум: %d.",
                valid_count,
                minimum_channels,
            )

            if keep_previous_on_failure and output_path.exists():

                logger.warning(
                    "Существующий плейлист сохранён."
                )

            return False

        content = cls.generate_content(
            reference_names,
            valid_channels,
        )

        try:

            atomic_write_text(
                output_path,
                content,
            )

            logger.info(
                "Плейлист записан: %s",
                output_path,
            )

            logger.info(
                "Каналов в итоговом плейлисте: %d",
                valid_count,
            )

            return True

        except Exception as exc:

            logger.error(
                "Ошибка записи плейлиста: %s",
                exc,
            )

            return False


# =============================================================================
# DIAGNOSTICS
# =============================================================================

class Diagnostics:
    @staticmethod
    def report(
        reference_names: List[str],
        candidates: Dict[
            str,
            List[StreamCandidate],
        ],
        raw_candidates: List[StreamCandidate],
        matcher: ChannelMatcher,
        limit: int,
    ) -> None:

        matched_names = set(
            candidates.keys()
        )

        missing = [
            name
            for name in reference_names
            if name not in matched_names
        ]

        logger.info(
            "============================================================"
        )

        logger.info(
            "ДИАГНОСТИКА"
        )

        logger.info(
            "Обязательных каналов: %d",
            len(reference_names),
        )

        logger.info(
            "Каналов с найденными URL: %d",
            len(matched_names),
        )

        logger.info(
            "Каналов без URL: %d",
            len(missing),
        )

        if missing:

            logger.warning(
                "Первые %d каналов без найденной ссылки:",
                limit,
            )

            for name in missing[:limit]:

                logger.warning(
                    "  ❌ %s",
                    name,
                )

        unmatched_names: List[str] = []

        seen: Set[str] = set()

        for candidate in raw_candidates:

            if candidate.source_name in seen:
                continue

            seen.add(
                candidate.source_name
            )

            if (
                matcher.match(
                    candidate.source_name
                )
                is None
            ):

                unmatched_names.append(
                    candidate.source_name
                )

            if len(unmatched_names) >= limit:
                break

        if unmatched_names:

            logger.warning(
                "Примеры названий источников без сопоставления:"
            )

            for name in unmatched_names:

                logger.warning(
                    "  ❓ %s",
                    name,
                )

        logger.info(
            "============================================================"
        )


# =============================================================================
# IPTV MANAGER
# =============================================================================

class IPTVManager:
    def __init__(
        self,
        config: Dict[str, Any],
    ):

        self.config = config

        self.reference_file = Path(
            config["reference_file"]
        )

        self.output_playlist = Path(
            config["output_playlist"]
        )

        self.sources_dir = Path(
            config["sources_dir"]
        )

        self.aliases_file = Path(
            config["aliases_file"]
        )

        self.cache_db = Path(
            config["cache_db"]
        )

        self.reference_names: List[str] = []

        self.alias_manager = AliasManager(
            self.aliases_file
        )

        self.matcher: Optional[
            ChannelMatcher
        ] = None

        self.cache = PersistentCache(
            self.cache_db,
            safe_int(
                config.get(
                    "cache_ttl",
                    3600,
                ),
                3600,
            ),
        )

        self.validator = StreamValidator(
            config,
            self.cache,
        )

        self.stop_requested = False

    async def load_reference(self) -> None:

        self.reference_names = (
            await ReferenceParser.parse(
                self.reference_file
            )
        )

        self.alias_manager.load(
            self.reference_names
        )

        self.matcher = ChannelMatcher(
            self.reference_names,
            self.alias_manager,
            safe_float(
                self.config.get(
                    "fuzzy_threshold",
                    0.86,
                ),
                0.86,
            ),
            bool(
                self.config.get(
                    "fuzzy_enabled",
                    True,
                )
            ),
        )

        logger.info(
            "Эталон полностью загружен: %d каналов.",
            len(self.reference_names),
        )

    async def load_sources(
        self,
    ) -> List[StreamCandidate]:

        all_candidates: List[
            StreamCandidate
        ] = []

        extensions = set(
            self.config.get(
                "playlist_extensions",
                [
                    ".m3u",
                    ".m3u8",
                    ".txt",
                ],
            )
        )

        priority_map = self.config.get(
            "source_priority",
            {},
        )

        if self.config.get(
            "local_sources_enabled",
            True,
        ):

            local_loader = LocalSourceLoader(
                self.sources_dir,
                extensions,
                priority_map,
            )

            local_candidates = (
                await local_loader.load()
            )

            all_candidates.extend(
                local_candidates
            )

        if self.config.get(
            "external_sources_enabled",
            True,
        ):

            external_loader = (
                ExternalSourceLoader(
                    self.config
                )
            )

            external_candidates = (
                await external_loader.load()
            )

            all_candidates.extend(
                external_candidates
            )

        logger.info(
            "Всего найдено сырых URL-записей: %d",
            len(all_candidates),
        )

        return all_candidates

    async def validate_candidates(
        self,
        candidates: Dict[
            str,
            List[StreamCandidate],
        ],
    ) -> Dict[str, str]:

        valid_channels: Dict[str, str] = {}

        if not candidates:

            return valid_channels

        connector = aiohttp.TCPConnector(
            limit=safe_int(
                self.config.get(
                    "max_connections",
                    100,
                ),
                100,
            ),
            limit_per_host=safe_int(
                self.config.get(
                    "max_connections_per_host",
                    10,
                ),
                10,
            ),
            force_close=False,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
        )

        timeout = aiohttp.ClientTimeout(
            total=safe_int(
                self.config.get(
                    "http_timeout",
                    12,
                ),
                12,
            ),
        )

        headers = {
            "User-Agent": self.config.get(
                "stream_user_agent",
                DEFAULT_CONFIG[
                    "stream_user_agent"
                ],
            ),
        }

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
        ) as session:

            tasks = []
            metadata: List[
                Tuple[
                    str,
                    StreamCandidate,
                ]
            ] = []

            for channel_name in self.reference_names:

                channel_candidates = candidates.get(
                    channel_name,
                    [],
                )

                if not channel_candidates:
                    continue

                for candidate in channel_candidates:

                    tasks.append(
                        self.validator.validate_url(
                            session,
                            candidate.url,
                        )
                    )

                    metadata.append(
                        (
                            channel_name,
                            candidate,
                        )
                    )

            logger.info(
                "URL поставлено на проверку: %d",
                len(tasks),
            )

            if not tasks:

                return valid_channels

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

            valid_by_channel: Dict[
                str,
                List[StreamCandidate],
            ] = {}

            for (
                (channel_name, candidate),
                result,
            ) in zip(
                metadata,
                results,
            ):

                if result is True:

                    valid_by_channel.setdefault(
                        channel_name,
                        [],
                    ).append(
                        candidate
                    )

            for channel_name in self.reference_names:

                valid_list = valid_by_channel.get(
                    channel_name,
                    [],
                )

                if not valid_list:
                    continue

                valid_list.sort(
                    key=lambda item: (
                        item.priority,
                        item.source,
                    ),
                    reverse=True,
                )

                valid_channels[
                    channel_name
                ] = valid_list[0].url

        return valid_channels

    async def update_cycle(
        self,
    ) -> bool:

        started = time.time()

        logger.info(
            "======================================================================"
        )

        logger.info(
            "Начало обновления IPTV Manager %s",
            APP_VERSION,
        )

        logger.info(
            "======================================================================"
        )

        self.cache.cleanup_expired()

        if not self.reference_names:

            await self.load_reference()

        if self.matcher is None:

            raise RuntimeError(
                "ChannelMatcher не инициализирован."
            )

        raw_candidates = await self.load_sources()

        if not raw_candidates:

            logger.error(
                "Не найдено ни одного URL в источниках."
            )

            if (
                self.config.get(
                    "keep_previous_playlist_on_failure",
                    True,
                )
                and self.output_playlist.exists()
            ):

                logger.warning(
                    "Существующий плейлист сохранён."
                )

            return False

        collector = CandidateCollector(
            self.reference_names,
            self.matcher,
            self.config.get(
                "source_priority",
                {},
            ),
        )

        candidates = collector.collect(
            raw_candidates
        )

        Diagnostics.report(
            self.reference_names,
            candidates,
            raw_candidates,
            self.matcher,
            safe_int(
                self.config.get(
                    "log_unmatched_limit",
                    30,
                ),
                30,
            ),
        )

        logger.info(
            "Начинается проверка рабочих ссылок..."
        )

        valid_channels = (
            await self.validate_candidates(
                candidates
            )
        )

        await self.cache.flush()

        logger.info(
            "Рабочих каналов: %d/%d",
            len(valid_channels),
            len(self.reference_names),
        )

        missing_after_validation = [
            name
            for name in self.reference_names
            if name not in valid_channels
        ]

        if missing_after_validation:

            logger.warning(
                "Каналов без рабочей ссылки: %d",
                len(missing_after_validation),
            )

            for name in missing_after_validation[
                :safe_int(
                    self.config.get(
                        "log_missing_limit",
                        50,
                    ),
                    50,
                )
            ]:

                logger.warning(
                    "  ❌ %s",
                    name,
                )

        minimum_channels = safe_int(
            self.config.get(
                "minimum_playlist_channels",
                1,
            ),
            1,
        )

        playlist_written = (
            await PlaylistGenerator.generate(
                self.output_playlist,
                self.reference_names,
                valid_channels,
                bool(
                    self.config.get(
                        "keep_previous_playlist_on_failure",
                        True,
                    )
                ),
                minimum_channels,
            )
        )

        elapsed = time.time() - started

        logger.info(
            "Обновление завершено за %.1f секунд.",
            elapsed,
        )

        logger.info(
            "Итог: %d/%d обязательных каналов имеют рабочий URL.",
            len(valid_channels),
            len(self.reference_names),
        )

        if playlist_written:

            logger.info(
                "Итоговый playlist: %s",
                self.output_playlist,
            )

        else:

            logger.warning(
                "Итоговый playlist не был заменён."
            )

        return playlist_written

    def request_stop(
        self,
    ) -> None:

        self.stop_requested = True

    def shutdown(
        self,
    ) -> None:

        try:
            self.validator.shutdown()
        except Exception:
            pass

        try:
            self.cache.close()
        except Exception:
            pass


# =============================================================================
# GITHUB ACTIONS / SINGLE RUN
# =============================================================================

async def async_main() -> int:

    logger.info(
        "======================================================================"
    )

    logger.info(
        "%s %s",
        APP_NAME,
        APP_VERSION,
    )

    logger.info(
        "BASE_DIR: %s",
        BASE_DIR,
    )

    logger.info(
        "======================================================================"
    )

    config = load_config()

    manager = IPTVManager(
        config
    )

    try:

        await manager.load_reference()

        success = await manager.update_cycle()

        if success:

            return 0

        return 1

    except FileNotFoundError as exc:

        logger.critical(
            "Файл не найден: %s",
            exc,
        )

        return 1

    except Exception as exc:

        logger.exception(
            "КРИТИЧЕСКАЯ ОШИБКА: %s",
            exc,
        )

        return 1

    finally:

        manager.shutdown()

        logger.info(
            "Ресурсы освобождены."
        )


# =============================================================================
# OPTIONAL CONTINUOUS MODE
# =============================================================================

async def async_continuous_main() -> int:
    """
    Дополнительный режим для локального компьютера.

    На GitHub Actions используется обычный single-run режим.
    """

    config = load_config()

    manager = IPTVManager(
        config
    )

    stop_event = asyncio.Event()

    def signal_handler() -> None:

        logger.info(
            "Получен сигнал остановки."
        )

        manager.request_stop()

        stop_event.set()

    try:

        loop = asyncio.get_running_loop()

        for sig in (
            signal.SIGINT,
            signal.SIGTERM,
        ):

            try:

                loop.add_signal_handler(
                    sig,
                    signal_handler,
                )

            except (
                NotImplementedError,
                RuntimeError,
            ):

                pass

        await manager.load_reference()

        interval = safe_int(
            config.get(
                "update_interval",
                1800,
            ),
            1800,
        )

        while not manager.stop_requested:

            try:

                await manager.update_cycle()

            except Exception as exc:

                logger.exception(
                    "Ошибка цикла обновления: %s",
                    exc,
                )

            if manager.stop_requested:
                break

            try:

                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=interval,
                )

            except asyncio.TimeoutError:

                pass

    finally:

        manager.shutdown()

    return 0


# =============================================================================
# COMMAND LINE
# =============================================================================

def main() -> int:

    continuous = (
        "--continuous"
        in sys.argv
    )

    if continuous:

        return asyncio.run(
            async_continuous_main()
        )

    return asyncio.run(
        async_main()
    )


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":

    try:

        exit_code = main()

        sys.exit(
            exit_code
        )

    except KeyboardInterrupt:

        logger.info(
            "Остановка пользователем."
        )

        sys.exit(0)

    except Exception as exc:

        logger.exception(
            "Непредвиденная ошибка: %s",
            exc,
        )

        sys.exit(1)
```
