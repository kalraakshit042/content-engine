"""
database/setup.py

Creates all three SQLite databases and their tables.
Run once on first use: python database/setup.py
"""

import sqlite3
import os
from pathlib import Path

DB_DIR = Path(__file__).parent


def get_db_path(name: str) -> str:
    return str(DB_DIR / f"{name}.db")


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    cols = {
        row[1]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def setup_channels_db():
    conn = sqlite3.connect(get_db_path("channels"))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS channels (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            name                TEXT NOT NULL,
            slug                TEXT NOT NULL UNIQUE,
            description         TEXT NOT NULL,
            status              TEXT DEFAULT 'pending',
            is_live             INTEGER DEFAULT 0,
            error_msg           TEXT,
            youtube_channel_url TEXT,
            youtube_channel_id  TEXT,
            tiktok_username     TEXT,
            tiktok_channel_url  TEXT,
            setup_checklist     TEXT DEFAULT '{}',
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    _ensure_column(conn, "channels", "art_status", "TEXT DEFAULT 'none'")
    _ensure_column(conn, "channels", "setup_checklist", "TEXT DEFAULT '{}'")
    conn.commit()
    conn.close()
    print("channels.db ready")


def setup_videos_db():
    conn = sqlite3.connect(get_db_path("videos"))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS videos (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_slug        TEXT NOT NULL,
            status              TEXT DEFAULT 'generating',
            title               TEXT,
            subject             TEXT,
            tone_used           TEXT,
            visual_style_used   TEXT,
            voice_style_used    TEXT,
            music_mood_used     TEXT,
            script_path         TEXT,
            image_path          TEXT,
            audio_path          TEXT,
            final_video_path    TEXT,
            youtube_video_id    TEXT,
            youtube_url         TEXT,
            youtube_views       INTEGER DEFAULT 0,
            youtube_comments    INTEGER DEFAULT 0,
            youtube_likes       INTEGER DEFAULT 0,
            tiktok_posted       INTEGER DEFAULT 0,
            tiktok_url          TEXT,
            stats_refreshed_at  TIMESTAMP,
            posted_at           TIMESTAMP,
            scheduled_for       TIMESTAMP,
            comment_posted      INTEGER DEFAULT 0,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    _ensure_column(conn, "videos", "video_path", "TEXT")
    _ensure_column(conn, "videos", "scheduled_for", "TIMESTAMP")
    _ensure_column(conn, "videos", "comment_posted", "INTEGER DEFAULT 0")
    _ensure_column(conn, "videos", "youtube_status", "TEXT")
    _ensure_column(conn, "videos", "youtube_error", "TEXT")
    _ensure_column(conn, "videos", "youtube_posted_at", "TIMESTAMP")
    _ensure_column(conn, "videos", "tiktok_status", "TEXT")
    _ensure_column(conn, "videos", "tiktok_error", "TEXT")
    _ensure_column(conn, "videos", "tiktok_posted_at", "TIMESTAMP")
    _ensure_column(conn, "videos", "tiktok_views", "INTEGER DEFAULT 0")
    _ensure_column(conn, "videos", "tiktok_comments", "INTEGER DEFAULT 0")
    _ensure_column(conn, "videos", "tiktok_likes", "INTEGER DEFAULT 0")
    _ensure_column(conn, "videos", "tiktok_scheduled_for", "TIMESTAMP")
    conn.commit()
    conn.close()
    print("videos.db ready")


def setup_costs_db():
    conn = sqlite3.connect(get_db_path("costs"))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS costs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_slug  TEXT NOT NULL,
            video_id      INTEGER,
            service       TEXT NOT NULL,
            model         TEXT,
            tokens_input  INTEGER,
            tokens_output INTEGER,
            cost_usd      REAL DEFAULT 0,
            called_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()
    print("costs.db ready")


def setup_ops_db():
    conn = sqlite3.connect(get_db_path("ops"))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cron_runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at   TIMESTAMP,
            status        TEXT DEFAULT 'running',
            summary       TEXT,
            error_msg     TEXT,
            triggered_by  TEXT DEFAULT 'cron'
        );

        CREATE TABLE IF NOT EXISTS cron_events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id        INTEGER NOT NULL,
            level         TEXT DEFAULT 'info',
            channel_slug  TEXT,
            video_id      INTEGER,
            action        TEXT,
            message       TEXT NOT NULL,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()
    print("ops.db ready")


if __name__ == "__main__":
    setup_channels_db()
    setup_videos_db()
    setup_costs_db()
    setup_ops_db()
    print("All databases initialised.")
