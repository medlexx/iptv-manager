#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import sqlite3
import logging
from pathlib import Path

# ==========================================
# Настройки приложения
# ==========================================
MAX_LINKS_PER_CHANNEL = 3  # Ограничение: не более 3 лучших ссылок на канал

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
REPORTS_DIR = BASE_DIR / "reports"

for folder in [DATA_DIR, OUTPUT_DIR, REPORTS_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "channel_index.db"
SPISOK_PATH = BASE_DIR / "spisok.txt"
ALIASES_PATH = BASE_DIR / "aliases.txt"
SOURCES_DIR = BASE_DIR / "sources"  # Папка с файлами .m3u / .m3u8
OUTPUT_PLAYLIST = OUTPUT_DIR / "eternal_playlist.m3u8"
REPORT_FILE = REPORTS_DIR / "status.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)


# ==========================================
# Инициализация и автовосстановление БД
# ==========================================
def get_db_connection(db_file: Path) -> sqlite3.Connection:
    """Подключается к БД. Если файл повреждён — автоматически пересоздает его."""
    if db_file.exists():
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check;")
            res = cursor.fetchone()
            if not res or res[0] != "ok":
                raise sqlite3.DatabaseError("Integrity check failed")
            return conn
        except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
            logging.warning(f"Файл БД {db_file} поврежден ({e}). Пересоздаем...")
            if 'conn' in locals() and conn:
                conn.close()
            try:
                os.remove(db_file)
            except OSError:
                pass

    conn = sqlite3.connect(db_file)
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection):
    """Создает структуру таблиц."""
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT UNIQUE,
                norm_title TEXT
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_name TEXT,
                extinf TEXT,
                url TEXT,
                UNIQUE(channel_name, url)
            );
        """)


# ==========================================
# Нормализация наименований каналов
# ==========================================
def normalize_name(name: str) -> str:
    """Очищает название канала от спецсимволов и кавычек для точного сопоставления."""
    name = re.sub(r'\(.*?\)|\[.*?\]', '', name)  # Удаляем скобки
    name = re.sub(r'[^a-zA-Zа-яА-Я0-9]', '', name)  # Только буквы и цифры
    return name.lower().strip()


def load_list(file_path: Path) -> list:
    """Загружает список обязательных каналов из файла."""
    if not file_path.exists():
        logging.warning(f"Файл {file_path.name} не найден!")
        return []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


# ==========================================
# Основная логика работы
# ==========================================
def main():
    logging.info("Запуск IPTV Manager...")

    # 1. Загрузка обязательных каналов и алиасов
    required_channels = load_list(SPISOK_PATH)
    logging.info(f"spisok.txt: найдено обязательных каналов: {len(required_channels)}")

    aliases = load_list(ALIASES_PATH)
    logging.info(f"Загружено aliases: {len(aliases)}")

    # Формируем карту нормализованных названий для быстрого поиска
    targets = {normalize_name(ch): ch for ch in required_channels}

    # 2. Инициализация рабочей БД
    conn = get_db_connection(DB_PATH)

    # 3. Сканирование M3U источников
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    source_files = list(SOURCES_DIR.glob("*.m3u")) + list(SOURCES_DIR.glob("*.m3u8"))
    logging.info(f"Источников для обработки: {len(source_files)}")

    total_m3u_records = 0
    unrecognized_count = 0

    valid_matches = []  # Хранилище кортежей (extinf, url)

    for src_file in source_files:
        try:
            with open(src_file, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()

            i = 0
            while i < len(lines):
                line = lines[i].strip()
                if line.startswith("#EXTINF:"):
                    total_m3u_records += 1
                    extinf = line
                    
                    # Получаем URL со следующей строки
                    url = ""
                    j = i + 1
                    while j < len(lines):
                        next_line = lines[j].strip()
                        if next_line and not next_line.startswith("#"):
                            url = next_line
                            break
                        j += 1

                    if url:
                        # Извлекаем имя канала из тега #EXTINF
                        channel_name = extinf.split(",")[-1].strip()
                        norm_name = normalize_name(channel_name)

                        if norm_name in targets:
                            matched_name = targets[norm_name]
                            valid_matches.append((matched_name, extinf, url))
                        else:
                            unrecognized_count += 1
                    i = j
                i += 1
        except Exception as e:
            logging.error(f"Ошибка при чтении {src_file.name}: {e}")

    logging.info(f"Всего записей M3U: {total_m3u_records}")

    # 4. Сохранение в БД и группировка
    matched_channels_set = set()

    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM matches;")  # Сброс старых совпадений
            
            for item in valid_matches:
                ch_name, extinf, url = item
                cursor.execute("""
                    INSERT OR IGNORE INTO matches (channel_name, extinf, url)
                    VALUES (?, ?, ?)
                """, (ch_name, extinf, url))
                matched_channels_set.add(ch_name)

        matched_count = len(matched_channels_set)
        logging.info(
            f"Совпало обязательных каналов: {matched_count}; "
            f"нераспознано записей источников: {unrecognized_count}"
        )

        # 5. Запись итогового M3U8 плейлиста (с ограничением до 3 ссылок на канал)
        total_links_written = 0
        with open(OUTPUT_PLAYLIST, "w", encoding="utf-8") as out:
            out.write("#EXTM3U\n")
            
            # Для каждого обязательного канала достаем не более MAX_LINKS_PER_CHANNEL ссылок
            for channel in required_channels:
                cursor.execute(
                    "SELECT extinf, url FROM matches WHERE channel_name = ? LIMIT ?",
                    (channel, MAX_LINKS_PER_CHANNEL)
                )
                rows = cursor.fetchall()
                for extinf, url in rows:
                    out.write(f"{extinf}\n{url}\n")
                    total_links_written += 1

        logging.info(
            f"Плейлист успешно записан в: {OUTPUT_PLAYLIST} "
            f"(Всего ссылок записано: {total_links_written}, максимум по {MAX_LINKS_PER_CHANNEL} на канал)"
        )

        # 6. Запись отчета
        with open(REPORT_FILE, "w", encoding="utf-8") as rep:
            rep.write(f"Обязательных каналов: {len(required_channels)}\n")
            rep.write(f"Обработано M3U записей: {total_m3u_records}\n")
            rep.write(f"Найдено уникальных каналов: {matched_count}\n")
            rep.write(f"Записано ссылок в плейлист: {total_links_written}\n")
            rep.write(f"Ограничение ссылок на канал: {MAX_LINKS_PER_CHANNEL}\n")
            rep.write(f"Нераспознано записей: {unrecognized_count}\n")

        logging.info(f"Отчет сохранен в: {REPORT_FILE}")

    except sqlite3.Error as e:
        logging.error(f"Ошибка при работе с SQLite: {e}")
    finally:
        conn.close()

    logging.info("Работа IPTV Manager завершена успешно.")


if __name__ == "__main__":
    main()