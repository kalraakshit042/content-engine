"""
database/migrate_sqlite_to_postgres.py

One-shot migration: copies all SQLite data → Postgres.
Run after setup.py: python database/migrate_sqlite_to_postgres.py
"""

import json
import os
import sqlite3
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DB_DIR = Path(__file__).parent


def _sqlite(name: str) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_DIR / f"{name}.db")
    conn.row_factory = sqlite3.Row
    return conn


def _rows(conn, sql) -> list[dict]:
    return [dict(r) for r in conn.execute(sql).fetchall()]


def migrate():
    pg = psycopg2.connect(os.environ["DATABASE_URL"])
    pg.autocommit = True
    cur = pg.cursor()

    # ── channels ──────────────────────────────────────────────────────────────
    with _sqlite("channels") as sq:
        channels = _rows(sq, "SELECT * FROM channels")
    print(f"Migrating {len(channels)} channels...")
    for r in channels:
        cur.execute(
            """INSERT INTO channels
               (id, name, slug, description, status, is_live, error_msg,
                youtube_channel_url, youtube_channel_id, tiktok_username,
                tiktok_channel_url, setup_checklist, art_status, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (id) DO NOTHING""",
            (
                r["id"], r["name"], r["slug"], r["description"],
                r.get("status"), bool(r.get("is_live")), r.get("error_msg"),
                r.get("youtube_channel_url"), r.get("youtube_channel_id"),
                r.get("tiktok_username"), r.get("tiktok_channel_url"),
                r.get("setup_checklist", "{}"),
                r.get("art_status", "none"),
                r.get("created_at"),
            ),
        )

    # ── videos ────────────────────────────────────────────────────────────────
    with _sqlite("videos") as sq:
        videos = _rows(sq, "SELECT * FROM videos")
    print(f"Migrating {len(videos)} videos...")
    for r in videos:
        cur.execute(
            """INSERT INTO videos
               (id, channel_slug, status, title, subject, tone_used, visual_style_used,
                voice_style_used, music_mood_used, script_path, image_path, audio_path,
                final_video_path, video_path, youtube_video_id, youtube_url,
                youtube_views, youtube_comments, youtube_likes,
                youtube_status, youtube_error, youtube_posted_at,
                tiktok_posted, tiktok_url, tiktok_status, tiktok_error, tiktok_posted_at,
                tiktok_views, tiktok_comments, tiktok_likes, tiktok_scheduled_for,
                stats_refreshed_at, posted_at, scheduled_for, comment_posted, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (id) DO NOTHING""",
            (
                r["id"], r["channel_slug"], r.get("status"), r.get("title"),
                r.get("subject"), r.get("tone_used"), r.get("visual_style_used"),
                r.get("voice_style_used"), r.get("music_mood_used"), r.get("script_path"),
                r.get("image_path"), r.get("audio_path"), r.get("final_video_path"),
                r.get("video_path"), r.get("youtube_video_id"), r.get("youtube_url"),
                r.get("youtube_views", 0), r.get("youtube_comments", 0), r.get("youtube_likes", 0),
                r.get("youtube_status"), r.get("youtube_error"), r.get("youtube_posted_at"),
                bool(r.get("tiktok_posted")), r.get("tiktok_url"),
                r.get("tiktok_status"), r.get("tiktok_error"), r.get("tiktok_posted_at"),
                r.get("tiktok_views", 0), r.get("tiktok_comments", 0), r.get("tiktok_likes", 0),
                r.get("tiktok_scheduled_for"), r.get("stats_refreshed_at"),
                r.get("posted_at"), r.get("scheduled_for"),
                bool(r.get("comment_posted")), r.get("created_at"),
            ),
        )

    # ── costs ─────────────────────────────────────────────────────────────────
    with _sqlite("costs") as sq:
        costs = _rows(sq, "SELECT * FROM costs")
    print(f"Migrating {len(costs)} cost rows...")
    for r in costs:
        cur.execute(
            """INSERT INTO costs
               (id, channel_slug, video_id, service, model, tokens_input, tokens_output, cost_usd, called_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (id) DO NOTHING""",
            (
                r["id"], r["channel_slug"], r.get("video_id"), r["service"],
                r.get("model"), r.get("tokens_input"), r.get("tokens_output"),
                r.get("cost_usd", 0), r.get("called_at"),
            ),
        )

    # ── ops ───────────────────────────────────────────────────────────────────
    with _sqlite("ops") as sq:
        cron_runs = _rows(sq, "SELECT * FROM cron_runs")
        cron_events = _rows(sq, "SELECT * FROM cron_events")
    print(f"Migrating {len(cron_runs)} cron runs, {len(cron_events)} cron events (in batches)...")
    for r in cron_runs:
        cur.execute(
            """INSERT INTO cron_runs
               (id, started_at, finished_at, status, summary, error_msg, triggered_by)
               VALUES (%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (id) DO NOTHING""",
            (
                r["id"], r.get("started_at"), r.get("finished_at"),
                r.get("status"), r.get("summary"), r.get("error_msg"),
                r.get("triggered_by", "cron"),
            ),
        )

    batch_size = 500
    for i in range(0, len(cron_events), batch_size):
        # Reconnect every batch to avoid SSL timeout on large dataset
        cur.close()
        pg.close()
        pg = psycopg2.connect(os.environ["DATABASE_URL"])
        pg.autocommit = True
        cur = pg.cursor()

        batch = cron_events[i:i + batch_size]
        for r in batch:
            cur.execute(
                """INSERT INTO cron_events
                   (id, run_id, level, channel_slug, video_id, action, message, created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (id) DO NOTHING""",
                (
                    r["id"], r["run_id"], r.get("level"), r.get("channel_slug"),
                    r.get("video_id"), r.get("action"), r["message"], r.get("created_at"),
                ),
            )
        print(f"  cron_events: {min(i + batch_size, len(cron_events))}/{len(cron_events)}")

    # Reset sequences so new inserts don't collide with migrated IDs
    for table, col in [("channels", "id"), ("videos", "id"), ("costs", "id"),
                       ("cron_runs", "id"), ("cron_events", "id")]:
        cur.execute(f"SELECT setval(pg_get_serial_sequence('{table}', '{col}'), COALESCE(MAX({col}), 1)) FROM {table}")

    cur.close()
    pg.close()
    print("Migration complete.")


if __name__ == "__main__":
    migrate()
