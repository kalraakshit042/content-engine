"""
database/setup.py

Creates all tables in Postgres.
Run once: python database/setup.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def setup_postgres():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id                  SERIAL PRIMARY KEY,
            name                TEXT NOT NULL,
            slug                TEXT NOT NULL UNIQUE,
            description         TEXT NOT NULL,
            status              TEXT DEFAULT 'pending',
            is_live             BOOLEAN DEFAULT FALSE,
            error_msg           TEXT,
            youtube_channel_url TEXT,
            youtube_channel_id  TEXT,
            tiktok_username     TEXT,
            tiktok_channel_url  TEXT,
            setup_checklist     TEXT DEFAULT '{}',
            art_status              TEXT DEFAULT 'none',
            avg_view_duration_secs  FLOAT DEFAULT NULL,
            avg_view_percentage     FLOAT DEFAULT NULL,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id                  SERIAL PRIMARY KEY,
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
            video_path          TEXT,
            youtube_video_id    TEXT,
            youtube_url         TEXT,
            youtube_views       INTEGER DEFAULT 0,
            youtube_comments    INTEGER DEFAULT 0,
            youtube_likes       INTEGER DEFAULT 0,
            youtube_status      TEXT,
            youtube_error       TEXT,
            youtube_posted_at   TIMESTAMP,
            tiktok_posted       BOOLEAN DEFAULT FALSE,
            tiktok_url          TEXT,
            tiktok_status       TEXT,
            tiktok_error        TEXT,
            tiktok_posted_at    TIMESTAMP,
            tiktok_views        INTEGER DEFAULT 0,
            tiktok_comments     INTEGER DEFAULT 0,
            tiktok_likes        INTEGER DEFAULT 0,
            tiktok_scheduled_for TIMESTAMP,
            stats_refreshed_at  TIMESTAMP,
            posted_at           TIMESTAMP,
            scheduled_for       TIMESTAMP,
            comment_posted      BOOLEAN DEFAULT FALSE,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS costs (
            id            SERIAL PRIMARY KEY,
            channel_slug  TEXT NOT NULL,
            video_id      INTEGER,
            service       TEXT NOT NULL,
            model         TEXT,
            tokens_input  INTEGER,
            tokens_output INTEGER,
            cost_usd      REAL DEFAULT 0,
            called_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cron_runs (
            id            SERIAL PRIMARY KEY,
            started_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at   TIMESTAMP,
            status        TEXT DEFAULT 'running',
            summary       TEXT,
            error_msg     TEXT,
            triggered_by  TEXT DEFAULT 'cron'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cron_events (
            id            SERIAL PRIMARY KEY,
            run_id        INTEGER NOT NULL,
            level         TEXT DEFAULT 'info',
            channel_slug  TEXT,
            video_id      INTEGER,
            action        TEXT,
            message       TEXT NOT NULL,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS affiliate_products (
            subject      TEXT PRIMARY KEY,
            asin         TEXT NOT NULL,
            product_name TEXT,
            price        TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("All Postgres tables ready.")


if __name__ == "__main__":
    setup_postgres()
