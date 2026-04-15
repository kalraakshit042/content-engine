"""
database/queries.py

All read/write helpers. Uses Postgres (psycopg2) via DATABASE_URL env var.
Every function opens its own connection — no shared state.
"""

import json
import os
from contextlib import contextmanager
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]


@contextmanager
def _conn():
    """Open a connection, commit on success, rollback on error, always close."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _fetchone(cursor) -> Optional[dict]:
    row = cursor.fetchone()
    return dict(row) if row else None


def _fetchall(cursor) -> list[dict]:
    return [dict(r) for r in cursor.fetchall()]


# ── Channels ──────────────────────────────────────────────────────────────────

def insert_channel(name: str, slug: str, description: str) -> None:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO channels (name, slug, description) VALUES (%s, %s, %s)",
                (name, slug, description),
            )


def get_channel(slug: str) -> Optional[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM channels WHERE slug = %s", (slug,))
            return _fetchone(cur)


def get_all_channels() -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM channels ORDER BY created_at DESC")
            return _fetchall(cur)


def get_live_channels() -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM channels WHERE status = 'active' AND is_live = TRUE ORDER BY created_at DESC"
            )
            return _fetchall(cur)


def set_art_status(slug: str, art_status: str) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE channels SET art_status = %s WHERE slug = %s",
                (art_status, slug),
            )


def set_channel_status(slug: str, status: str, error_msg: str = None) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE channels SET status = %s, error_msg = %s WHERE slug = %s",
                (status, error_msg, slug),
            )


def update_channel_analytics(slug: str, avg_duration_secs: float, avg_percentage: float) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE channels
                   SET avg_view_duration_secs = %s, avg_view_percentage = %s
                   WHERE slug = %s""",
                (avg_duration_secs, avg_percentage, slug),
            )


def update_channel_links(
    slug: str,
    is_live: int,
    youtube_channel_url: str,
    youtube_channel_id: str,
    tiktok_username: str,
    tiktok_channel_url: str,
) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE channels SET
                    is_live = %s,
                    youtube_channel_url = %s,
                    youtube_channel_id = %s,
                    tiktok_username = %s,
                    tiktok_channel_url = %s
                   WHERE slug = %s""",
                (bool(is_live), youtube_channel_url, youtube_channel_id,
                 tiktok_username, tiktok_channel_url, slug),
            )


def get_channel_checklist(slug: str) -> dict:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT setup_checklist FROM channels WHERE slug = %s", (slug,)
            )
            row = _fetchone(cur)
    if not row or not row["setup_checklist"]:
        return {}
    try:
        val = row["setup_checklist"]
        return val if isinstance(val, dict) else json.loads(val)
    except Exception:
        return {}


def update_channel_checklist(slug: str, checklist_state: dict) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE channels SET setup_checklist = %s WHERE slug = %s",
                (json.dumps(checklist_state), slug),
            )


def delete_channel(slug: str) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM channels WHERE slug = %s", (slug,))


# ── Videos ────────────────────────────────────────────────────────────────────

def insert_video(channel_slug: str) -> int:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO videos (channel_slug) VALUES (%s) RETURNING id",
                (channel_slug,),
            )
            return cur.fetchone()[0]


def get_video(video_id: int) -> Optional[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM videos WHERE id = %s", (video_id,))
            return _fetchone(cur)


def get_channel_videos(channel_slug: str) -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM videos WHERE channel_slug = %s ORDER BY created_at DESC",
                (channel_slug,),
            )
            return _fetchall(cur)


def get_all_videos() -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM videos ORDER BY created_at DESC")
            return _fetchall(cur)


def set_video_status(video_id: int, status: str) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET status = %s WHERE id = %s", (status, video_id)
            )


def get_used_subjects(channel_slug: str) -> list[str]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT subject FROM videos WHERE channel_slug = %s AND subject IS NOT NULL AND status != 'error'",
                (channel_slug,),
            )
            return [r["subject"] for r in _fetchall(cur)]


def update_video_script(
    video_id: int,
    title: str,
    subject: str,
    tone_used: str,
    visual_style_used: str,
    voice_style_used: str,
    music_mood_used: str,
    script_path: str,
) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE videos SET
                       title = %s, subject = %s, tone_used = %s, visual_style_used = %s,
                       voice_style_used = %s, music_mood_used = %s, script_path = %s, status = 'scripted'
                   WHERE id = %s""",
                (title, subject, tone_used, visual_style_used,
                 voice_style_used, music_mood_used, script_path, video_id),
            )


def update_video_path(video_id: int, video_path: str) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET final_video_path = %s, status = 'video_done' WHERE id = %s",
                (video_path, video_id),
            )


def get_published_videos(channel_slug: str) -> list:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM videos WHERE channel_slug = %s AND youtube_video_id IS NOT NULL",
                (channel_slug,),
            )
            return _fetchall(cur)


def count_generated_today(channel_slug: str) -> int:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
    _UTC = ZoneInfo("UTC")
    now_et = datetime.now(_ET)
    today_et_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    today_et_start_utc = today_et_start.astimezone(_UTC).strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) FROM videos
                   WHERE channel_slug = %s
                   AND status != 'error'
                   AND created_at >= %s""",
                (channel_slug, today_et_start_utc),
            )
            return cur.fetchone()[0]


def count_posted_today(channel_slug: str) -> int:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
    _UTC = ZoneInfo("UTC")
    now_et = datetime.now(_ET)
    today_et_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    today_et_start_utc = today_et_start.astimezone(_UTC).strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) FROM videos
                   WHERE channel_slug = %s
                   AND youtube_status = 'posted'
                   AND youtube_posted_at >= %s""",
                (channel_slug, today_et_start_utc),
            )
            return cur.fetchone()[0]


def count_tiktok_posted_today(channel_slug: str) -> int:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
    _UTC = ZoneInfo("UTC")
    now_et = datetime.now(_ET)
    today_et_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    today_et_start_utc = today_et_start.astimezone(_UTC).strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) FROM videos
                   WHERE channel_slug = %s
                   AND tiktok_status = 'posted'
                   AND tiktok_posted_at >= %s""",
                (channel_slug, today_et_start_utc),
            )
            return cur.fetchone()[0]


def update_video_stats(video_id: int, views: int, comments: int, likes: int) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE videos SET youtube_views = %s, youtube_comments = %s, youtube_likes = %s,
                   stats_refreshed_at = CURRENT_TIMESTAMP WHERE id = %s""",
                (views, comments, likes, video_id),
            )


def update_video_youtube(
    video_id: int,
    yt_video_id: str,
    yt_url: str,
    scheduled_for: str = None,
) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            if scheduled_for:
                cur.execute(
                    """UPDATE videos
                       SET youtube_video_id = %s, youtube_url = %s, status = 'scheduled',
                           youtube_status = 'scheduled',
                           scheduled_for = %s, posted_at = CURRENT_TIMESTAMP
                       WHERE id = %s""",
                    (yt_video_id, yt_url, scheduled_for, video_id),
                )
            else:
                cur.execute(
                    """UPDATE videos
                       SET youtube_video_id = %s, youtube_url = %s, status = 'published',
                           youtube_status = 'posted', youtube_posted_at = CURRENT_TIMESTAMP,
                           posted_at = CURRENT_TIMESTAMP
                       WHERE id = %s""",
                    (yt_video_id, yt_url, video_id),
                )


def set_youtube_status(video_id: int, youtube_status: str, youtube_error: str = None) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET youtube_status = %s, youtube_error = %s WHERE id = %s",
                (youtube_status, youtube_error, video_id),
            )


def update_video_tiktok(
    video_id: int,
    tiktok_url: str = None,
    tiktok_status: str = "posted",
    tiktok_error: str = None,
) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE videos
                   SET tiktok_posted = %s, tiktok_url = %s, tiktok_status = %s,
                       tiktok_error = %s, tiktok_posted_at = CASE
                           WHEN %s = 'posted' THEN CURRENT_TIMESTAMP
                           ELSE tiktok_posted_at
                       END
                   WHERE id = %s""",
                (tiktok_status == "posted", tiktok_url, tiktok_status,
                 tiktok_error, tiktok_status, video_id),
            )


def set_video_tiktok_url(video_id: int, tiktok_url: str) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE videos
                   SET tiktok_url = %s,
                       tiktok_posted = TRUE,
                       tiktok_status = 'posted',
                       tiktok_error = NULL,
                       tiktok_posted_at = COALESCE(tiktok_posted_at, CURRENT_TIMESTAMP)
                   WHERE id = %s""",
                (tiktok_url, video_id),
            )


def set_tiktok_status(video_id: int, tiktok_status: str, tiktok_error: str = None) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET tiktok_status = %s, tiktok_error = %s WHERE id = %s",
                (tiktok_status, tiktok_error, video_id),
            )


def approve_preview_video(video_id: int) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE videos
                   SET youtube_status = CASE WHEN youtube_status = 'preview' THEN 'queued' ELSE youtube_status END,
                       tiktok_status  = CASE WHEN tiktok_status  = 'preview' THEN 'queued' ELSE tiktok_status  END
                   WHERE id = %s""",
                (video_id,),
            )


def get_videos_for_schedule_window(channel_slug: str, start_iso: str, end_iso: str) -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT * FROM videos
                   WHERE channel_slug = %s
                     AND scheduled_for IS NOT NULL
                     AND scheduled_for >= %s
                     AND scheduled_for <= %s
                   ORDER BY scheduled_for ASC""",
                (channel_slug, start_iso, end_iso),
            )
            return _fetchall(cur)


def update_tiktok_stats(video_id: int, views: int, comments: int, likes: int) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE videos SET tiktok_views = %s, tiktok_comments = %s, tiktok_likes = %s,
                   stats_refreshed_at = CURRENT_TIMESTAMP WHERE id = %s""",
                (views, comments, likes, video_id),
            )


def get_videos_for_cleanup(days_old: int = 14) -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT id, channel_slug, final_video_path, audio_path, image_path
                   FROM videos
                   WHERE status IN ('published', 'scheduled')
                   AND posted_at IS NOT NULL
                   AND posted_at < NOW() - (%s || ' days')::interval
                   AND final_video_path IS NOT NULL""",
                (str(days_old),),
            )
            return _fetchall(cur)


def clear_video_local_paths(video_id: int) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET final_video_path = NULL, audio_path = NULL, image_path = NULL WHERE id = %s",
                (video_id,),
            )


def mark_comment_posted(video_id: int) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET comment_posted = TRUE WHERE id = %s", (video_id,)
            )


def get_scheduled_slots(channel_slug: str) -> list[str]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT scheduled_for FROM videos WHERE channel_slug = %s AND scheduled_for IS NOT NULL",
                (channel_slug,),
            )
            return [str(r["scheduled_for"]) for r in _fetchall(cur)]


def set_video_scheduled_for(video_id: int, scheduled_for: str) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET scheduled_for = %s WHERE id = %s",
                (scheduled_for, video_id),
            )


def set_video_tiktok_scheduled_for(video_id: int, tiktok_scheduled_for: str) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE videos SET tiktok_scheduled_for = %s WHERE id = %s",
                (tiktok_scheduled_for, video_id),
            )


def get_tiktok_scheduled_slots(channel_slug: str) -> list[str]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT tiktok_scheduled_for FROM videos WHERE channel_slug = %s AND tiktok_scheduled_for IS NOT NULL",
                (channel_slug,),
            )
            return [str(r["tiktok_scheduled_for"]) for r in _fetchall(cur)]


# ── Costs ─────────────────────────────────────────────────────────────────────

def log_cost(
    channel_slug: str,
    service: str,
    model: str,
    tokens_input: int,
    tokens_output: int,
    cost_usd: float,
    video_id: int = None,
) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO costs
                   (channel_slug, video_id, service, model, tokens_input, tokens_output, cost_usd)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (channel_slug, video_id, service, model,
                 tokens_input, tokens_output, cost_usd),
            )


def get_channel_costs(channel_slug: str) -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM costs WHERE channel_slug = %s ORDER BY called_at DESC",
                (channel_slug,),
            )
            return _fetchall(cur)


def get_channel_service_costs(channel_slug: str, service: str) -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT * FROM costs
                   WHERE channel_slug = %s AND service = %s
                   ORDER BY called_at DESC""",
                (channel_slug, service),
            )
            return _fetchall(cur)


def get_channel_service_total_usd(channel_slug: str, service: str) -> float:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COALESCE(SUM(cost_usd), 0) AS total
                   FROM costs
                   WHERE channel_slug = %s AND service = %s""",
                (channel_slug, service),
            )
            return round(float(cur.fetchone()[0]), 4)


def get_total_cost_usd(channel_slug: str) -> float:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(cost_usd), 0) as total FROM costs WHERE channel_slug = %s",
                (channel_slug,),
            )
            return round(float(cur.fetchone()[0]), 4)


def get_video_service_costs(channel_slug: str, service: str) -> dict[int, dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT * FROM costs
                   WHERE channel_slug = %s AND service = %s AND video_id IS NOT NULL
                   ORDER BY called_at DESC""",
                (channel_slug, service),
            )
            rows = _fetchall(cur)

    grouped: dict[int, dict] = {}
    for entry in rows:
        video_id = entry["video_id"]
        if video_id not in grouped:
            grouped[video_id] = {"total_usd": 0.0, "entries": []}
        grouped[video_id]["total_usd"] += entry.get("cost_usd") or 0.0
        grouped[video_id]["entries"].append(entry)

    for vid in grouped:
        grouped[vid]["total_usd"] = round(grouped[vid]["total_usd"], 4)
    return grouped


# ── Affiliate products ────────────────────────────────────────────────────────

def _ensure_affiliate_table() -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS affiliate_products (
                    subject      TEXT PRIMARY KEY,
                    asin         TEXT NOT NULL,
                    product_name TEXT,
                    price        TEXT,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"""
            )


def get_affiliate_product(subject: str) -> Optional[dict]:
    _ensure_affiliate_table()
    normalized = subject.lower().strip()
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM affiliate_products WHERE subject = %s", (normalized,)
            )
            return _fetchone(cur)


def upsert_affiliate_product(
    subject: str, asin: str, product_name: str = "", price: str = ""
) -> None:
    _ensure_affiliate_table()
    normalized = subject.lower().strip()
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO affiliate_products (subject, asin, product_name, price)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT(subject) DO UPDATE SET
                       asin = EXCLUDED.asin,
                       product_name = EXCLUDED.product_name,
                       price = EXCLUDED.price""",
                (normalized, asin, product_name, price),
            )


# ── Cron / Ops ────────────────────────────────────────────────────────────────

def create_cron_run(triggered_by: str = "cron") -> int:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cron_runs (triggered_by) VALUES (%s) RETURNING id",
                (triggered_by,),
            )
            return cur.fetchone()[0]


def finish_cron_run(run_id: int, status: str, summary: str = "", error_msg: str = None) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE cron_runs
                   SET finished_at = CURRENT_TIMESTAMP, status = %s, summary = %s, error_msg = %s
                   WHERE id = %s""",
                (status, summary, error_msg, run_id),
            )


def log_cron_event(
    run_id: int,
    message: str,
    level: str = "info",
    action: str = None,
    channel_slug: str = None,
    video_id: int = None,
) -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO cron_events (run_id, level, channel_slug, video_id, action, message)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (run_id, level, channel_slug, video_id, action, message),
            )


def get_latest_cron_run() -> Optional[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM cron_runs ORDER BY id DESC LIMIT 1")
            return _fetchone(cur)


def get_recent_cron_runs(limit: int = 8) -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM cron_runs ORDER BY id DESC LIMIT %s", (limit,)
            )
            return _fetchall(cur)


def get_recent_cron_events(limit: int = 20) -> list[dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM cron_events ORDER BY id DESC LIMIT %s", (limit,)
            )
            return _fetchall(cur)


# ── Global stats ──────────────────────────────────────────────────────────────

def get_global_stats() -> dict:
    channels = get_all_channels()
    total_channels = len(channels)

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT
                       COUNT(*) as total,
                       COUNT(*) FILTER (WHERE youtube_status = 'posted') as yt_posted,
                       COALESCE(SUM(youtube_views), 0) as youtube_views,
                       COALESCE(SUM(tiktok_views), 0) as tiktok_views,
                       COALESCE(SUM(youtube_likes), 0) as youtube_likes,
                       COALESCE(SUM(tiktok_likes), 0) as tiktok_likes,
                       COALESCE(SUM(youtube_comments), 0) as youtube_comments,
                       COALESCE(SUM(tiktok_comments), 0) as tiktok_comments
                   FROM videos"""
            )
            row = cur.fetchone()
            total_videos = row[0]
            total_posted = row[1]
            total_views = row[2] + row[3]
            total_likes = row[4] + row[5]
            total_comments = row[6] + row[7]

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(cost_usd), 0) as total FROM costs WHERE service = 'claude'"
            )
            total_claude_cost = round(float(cur.fetchone()[0]), 4)

    return {
        "total_channels": total_channels,
        "total_videos": total_videos,
        "total_posted": total_posted,
        "total_views": total_views,
        "total_likes": total_likes,
        "total_comments": total_comments,
        "total_cost_usd": total_claude_cost,
        "total_claude_cost_usd": total_claude_cost,
    }


def get_channel_rollups() -> dict[str, dict]:
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT
                       channel_slug,
                       COALESCE(SUM(youtube_likes), 0) as youtube_likes,
                       COALESCE(SUM(tiktok_likes), 0) as tiktok_likes,
                       COALESCE(SUM(youtube_comments), 0) as youtube_comments,
                       COALESCE(SUM(tiktok_comments), 0) as tiktok_comments
                   FROM videos
                   GROUP BY channel_slug"""
            )
            video_rows = _fetchall(cur)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT
                       channel_slug,
                       COALESCE(SUM(cost_usd), 0) as claude_cost_usd
                   FROM costs
                   WHERE service = 'claude'
                   GROUP BY channel_slug"""
            )
            cost_rows = _fetchall(cur)

    rollups: dict[str, dict] = {}
    for item in video_rows:
        rollups[item["channel_slug"]] = {
            "claude_cost_usd": 0.0,
            "youtube_likes": item["youtube_likes"] or 0,
            "youtube_comments": item["youtube_comments"] or 0,
            "tiktok_likes": item["tiktok_likes"] or 0,
            "tiktok_comments": item["tiktok_comments"] or 0,
            "total_likes": (item["youtube_likes"] or 0) + (item["tiktok_likes"] or 0),
            "total_comments": (item["youtube_comments"] or 0) + (item["tiktok_comments"] or 0),
        }

    for item in cost_rows:
        bucket = rollups.setdefault(
            item["channel_slug"],
            {
                "claude_cost_usd": 0.0,
                "youtube_likes": 0,
                "youtube_comments": 0,
                "tiktok_likes": 0,
                "tiktok_comments": 0,
                "total_likes": 0,
                "total_comments": 0,
            },
        )
        bucket["claude_cost_usd"] = round(float(item["claude_cost_usd"] or 0.0), 4)

    return rollups
