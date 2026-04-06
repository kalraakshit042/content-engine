"""
database/queries.py

All read/write helpers for channels.db, videos.db, costs.db.
Every function opens its own connection — no shared state.
"""

import json
import sqlite3
from pathlib import Path
from typing import Optional

DB_DIR = Path(__file__).parent


def _connect(db_name: str) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_DIR / f"{db_name}.db")
    conn.row_factory = sqlite3.Row  # rows accessible as dicts
    return conn


# ── Channels ──────────────────────────────────────────────────────────────────

def insert_channel(name: str, slug: str, description: str) -> None:
    conn = _connect("channels")
    conn.execute(
        "INSERT INTO channels (name, slug, description) VALUES (?, ?, ?)",
        (name, slug, description),
    )
    conn.commit()
    conn.close()


def get_channel(slug: str) -> Optional[dict]:
    with _connect("channels") as conn:
        row = conn.execute(
            "SELECT * FROM channels WHERE slug = ?", (slug,)
        ).fetchone()
    return dict(row) if row else None


def get_all_channels() -> list[dict]:
    with _connect("channels") as conn:
        rows = conn.execute(
            "SELECT * FROM channels ORDER BY created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_live_channels() -> list[dict]:
    with _connect("channels") as conn:
        rows = conn.execute(
            "SELECT * FROM channels WHERE status = 'active' AND is_live = 1 ORDER BY created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def set_art_status(slug: str, art_status: str) -> None:
    conn = _connect("channels")
    conn.execute(
        "UPDATE channels SET art_status = ? WHERE slug = ?",
        (art_status, slug),
    )
    conn.commit()
    conn.close()


def set_channel_status(slug: str, status: str, error_msg: str = None) -> None:
    conn = _connect("channels")
    conn.execute(
        "UPDATE channels SET status = ?, error_msg = ? WHERE slug = ?",
        (status, error_msg, slug),
    )
    conn.commit()
    conn.close()


def update_channel_links(
    slug: str,
    is_live: int,
    youtube_channel_url: str,
    youtube_channel_id: str,
    tiktok_username: str,
    tiktok_channel_url: str,
) -> None:
    conn = _connect("channels")
    conn.execute(
        """UPDATE channels SET
            is_live = ?,
            youtube_channel_url = ?,
            youtube_channel_id = ?,
            tiktok_username = ?,
            tiktok_channel_url = ?
           WHERE slug = ?""",
        (is_live, youtube_channel_url, youtube_channel_id,
         tiktok_username, tiktok_channel_url, slug),
    )
    conn.commit()
    conn.close()


def get_channel_checklist(slug: str) -> dict:
    with _connect("channels") as conn:
        row = conn.execute(
            "SELECT setup_checklist FROM channels WHERE slug = ?",
            (slug,),
        ).fetchone()
    if not row or not row["setup_checklist"]:
        return {}
    try:
        return json.loads(row["setup_checklist"])
    except Exception:
        return {}


def update_channel_checklist(slug: str, checklist_state: dict) -> None:
    conn = _connect("channels")
    conn.execute(
        "UPDATE channels SET setup_checklist = ? WHERE slug = ?",
        (json.dumps(checklist_state), slug),
    )
    conn.commit()
    conn.close()


def delete_channel(slug: str) -> None:
    conn = _connect("channels")
    conn.execute("DELETE FROM channels WHERE slug = ?", (slug,))
    conn.commit()
    conn.close()


# ── Videos ────────────────────────────────────────────────────────────────────

def insert_video(channel_slug: str) -> int:
    with _connect("videos") as conn:
        cursor = conn.execute(
            "INSERT INTO videos (channel_slug) VALUES (?)", (channel_slug,)
        )
        return cursor.lastrowid


def get_video(video_id: int) -> Optional[dict]:
    with _connect("videos") as conn:
        row = conn.execute(
            "SELECT * FROM videos WHERE id = ?", (video_id,)
        ).fetchone()
    return dict(row) if row else None


def get_channel_videos(channel_slug: str) -> list[dict]:
    with _connect("videos") as conn:
        rows = conn.execute(
            "SELECT * FROM videos WHERE channel_slug = ? ORDER BY created_at DESC",
            (channel_slug,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_videos() -> list[dict]:
    with _connect("videos") as conn:
        rows = conn.execute(
            "SELECT * FROM videos ORDER BY created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def set_video_status(video_id: int, status: str) -> None:
    with _connect("videos") as conn:
        conn.execute(
            "UPDATE videos SET status = ? WHERE id = ?", (status, video_id)
        )


def get_used_subjects(channel_slug: str) -> list[str]:
    with _connect("videos") as conn:
        rows = conn.execute(
            "SELECT subject FROM videos WHERE channel_slug = ? AND subject IS NOT NULL AND status != 'error'",
            (channel_slug,),
        ).fetchall()
    return [r["subject"] for r in rows]


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
    conn = _connect("videos")
    conn.execute(
        """UPDATE videos SET
               title = ?, subject = ?, tone_used = ?, visual_style_used = ?,
               voice_style_used = ?, music_mood_used = ?, script_path = ?, status = 'scripted'
           WHERE id = ?""",
        (title, subject, tone_used, visual_style_used,
         voice_style_used, music_mood_used, script_path, video_id),
    )
    conn.commit()
    conn.close()


def update_video_path(video_id: int, video_path: str) -> None:
    conn = _connect("videos")
    conn.execute(
        "UPDATE videos SET final_video_path = ?, status = 'video_done' WHERE id = ?",
        (video_path, video_id),
    )
    conn.commit()
    conn.close()


def get_published_videos(channel_slug: str) -> list:
    conn = _connect("videos")
    rows = conn.execute(
        "SELECT * FROM videos WHERE channel_slug = ? AND youtube_video_id IS NOT NULL",
        (channel_slug,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_video_stats(video_id: int, views: int, comments: int, likes: int) -> None:
    conn = _connect("videos")
    conn.execute(
        """UPDATE videos SET youtube_views = ?, youtube_comments = ?, youtube_likes = ?,
           stats_refreshed_at = CURRENT_TIMESTAMP WHERE id = ?""",
        (views, comments, likes, video_id),
    )
    conn.commit()
    conn.close()


def update_video_youtube(
    video_id: int,
    yt_video_id: str,
    yt_url: str,
    scheduled_for: str = None,
) -> None:
    conn = _connect("videos")
    if scheduled_for:
        conn.execute(
            """UPDATE videos
               SET youtube_video_id = ?, youtube_url = ?, status = 'scheduled',
                   youtube_status = 'scheduled',
                   scheduled_for = ?, posted_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (yt_video_id, yt_url, scheduled_for, video_id),
        )
    else:
        conn.execute(
            """UPDATE videos
               SET youtube_video_id = ?, youtube_url = ?, status = 'published',
                   youtube_status = 'posted', youtube_posted_at = CURRENT_TIMESTAMP,
                   posted_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (yt_video_id, yt_url, video_id),
        )
    conn.commit()
    conn.close()


def set_youtube_status(video_id: int, youtube_status: str, youtube_error: str = None) -> None:
    conn = _connect("videos")
    conn.execute(
        "UPDATE videos SET youtube_status = ?, youtube_error = ? WHERE id = ?",
        (youtube_status, youtube_error, video_id),
    )
    conn.commit()
    conn.close()


def update_video_tiktok(
    video_id: int,
    tiktok_url: str = None,
    tiktok_status: str = "posted",
    tiktok_error: str = None,
) -> None:
    conn = _connect("videos")
    conn.execute(
        """UPDATE videos
           SET tiktok_posted = ?, tiktok_url = ?, tiktok_status = ?,
               tiktok_error = ?, tiktok_posted_at = CASE
                   WHEN ? = 'posted' THEN CURRENT_TIMESTAMP
                   ELSE tiktok_posted_at
               END
           WHERE id = ?""",
        (1 if tiktok_status == "posted" else 0, tiktok_url, tiktok_status, tiktok_error, tiktok_status, video_id),
    )
    conn.commit()
    conn.close()


def set_video_tiktok_url(video_id: int, tiktok_url: str) -> None:
    conn = _connect("videos")
    conn.execute(
        """UPDATE videos
           SET tiktok_url = ?,
               tiktok_posted = 1,
               tiktok_status = 'posted',
               tiktok_error = NULL,
               tiktok_posted_at = COALESCE(tiktok_posted_at, CURRENT_TIMESTAMP)
           WHERE id = ?""",
        (tiktok_url, video_id),
    )
    conn.commit()
    conn.close()


def set_tiktok_status(video_id: int, tiktok_status: str, tiktok_error: str = None) -> None:
    conn = _connect("videos")
    conn.execute(
        "UPDATE videos SET tiktok_status = ?, tiktok_error = ? WHERE id = ?",
        (tiktok_status, tiktok_error, video_id),
    )
    conn.commit()
    conn.close()


def get_videos_for_schedule_window(channel_slug: str, start_iso: str, end_iso: str) -> list[dict]:
    with _connect("videos") as conn:
        rows = conn.execute(
            """SELECT * FROM videos
               WHERE channel_slug = ?
                 AND scheduled_for IS NOT NULL
                 AND scheduled_for >= ?
                 AND scheduled_for <= ?
               ORDER BY scheduled_for ASC""",
            (channel_slug, start_iso, end_iso),
        ).fetchall()
    return [dict(r) for r in rows]


def update_tiktok_stats(video_id: int, views: int, comments: int, likes: int) -> None:
    conn = _connect("videos")
    conn.execute(
        """UPDATE videos SET tiktok_views = ?, tiktok_comments = ?, tiktok_likes = ?,
           stats_refreshed_at = CURRENT_TIMESTAMP WHERE id = ?""",
        (views, comments, likes, video_id),
    )
    conn.commit()
    conn.close()


def get_videos_for_cleanup(days_old: int = 14) -> list[dict]:
    """Returns published videos older than days_old whose local files can be purged."""
    with _connect("videos") as conn:
        rows = conn.execute(
            """SELECT id, channel_slug, final_video_path, audio_path, image_path
               FROM videos
               WHERE status IN ('published', 'scheduled')
               AND posted_at IS NOT NULL
               AND posted_at < datetime('now', ?)
               AND final_video_path IS NOT NULL""",
            (f"-{days_old} days",),
        ).fetchall()
    return [dict(r) for r in rows]


def clear_video_local_paths(video_id: int) -> None:
    """Nulls out local file path columns after cleanup."""
    with _connect("videos") as conn:
        conn.execute(
            "UPDATE videos SET final_video_path = NULL, audio_path = NULL, image_path = NULL WHERE id = ?",
            (video_id,),
        )


def mark_comment_posted(video_id: int) -> None:
    with _connect("videos") as conn:
        conn.execute(
            "UPDATE videos SET comment_posted = 1 WHERE id = ?", (video_id,)
        )


def get_scheduled_slots(channel_slug: str) -> list[str]:
    """Returns all reserved scheduled_for timestamps for this channel (any status)."""
    with _connect("videos") as conn:
        rows = conn.execute(
            "SELECT scheduled_for FROM videos WHERE channel_slug = ? AND scheduled_for IS NOT NULL",
            (channel_slug,),
        ).fetchall()
    return [r["scheduled_for"] for r in rows]


def set_video_scheduled_for(video_id: int, scheduled_for: str) -> None:
    with _connect("videos") as conn:
        conn.execute(
            "UPDATE videos SET scheduled_for = ? WHERE id = ?",
            (scheduled_for, video_id),
        )


def set_video_tiktok_scheduled_for(video_id: int, tiktok_scheduled_for: str) -> None:
    with _connect("videos") as conn:
        conn.execute(
            "UPDATE videos SET tiktok_scheduled_for = ? WHERE id = ?",
            (tiktok_scheduled_for, video_id),
        )


def get_tiktok_scheduled_slots(channel_slug: str) -> list[str]:
    """Returns all reserved tiktok_scheduled_for timestamps for this channel (any status)."""
    with _connect("videos") as conn:
        rows = conn.execute(
            "SELECT tiktok_scheduled_for FROM videos WHERE channel_slug = ? AND tiktok_scheduled_for IS NOT NULL",
            (channel_slug,),
        ).fetchall()
    return [r["tiktok_scheduled_for"] for r in rows]


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
    conn = _connect("costs")
    conn.execute(
        """INSERT INTO costs
           (channel_slug, video_id, service, model, tokens_input, tokens_output, cost_usd)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (channel_slug, video_id, service, model,
         tokens_input, tokens_output, cost_usd),
    )
    conn.commit()
    conn.close()


def get_channel_costs(channel_slug: str) -> list[dict]:
    with _connect("costs") as conn:
        rows = conn.execute(
            "SELECT * FROM costs WHERE channel_slug = ? ORDER BY called_at DESC",
            (channel_slug,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_channel_service_costs(channel_slug: str, service: str) -> list[dict]:
    with _connect("costs") as conn:
        rows = conn.execute(
            """SELECT * FROM costs
               WHERE channel_slug = ? AND service = ?
               ORDER BY called_at DESC""",
            (channel_slug, service),
        ).fetchall()
    return [dict(r) for r in rows]


def get_channel_service_total_usd(channel_slug: str, service: str) -> float:
    with _connect("costs") as conn:
        row = conn.execute(
            """SELECT COALESCE(SUM(cost_usd), 0) AS total
               FROM costs
               WHERE channel_slug = ? AND service = ?""",
            (channel_slug, service),
        ).fetchone()
    return round(row["total"], 4)


def get_total_cost_usd(channel_slug: str) -> float:
    with _connect("costs") as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM costs WHERE channel_slug = ?",
            (channel_slug,),
        ).fetchone()
    return round(row["total"], 4)


def get_video_service_costs(channel_slug: str, service: str) -> dict[int, dict]:
    with _connect("costs") as conn:
        rows = conn.execute(
            """SELECT * FROM costs
               WHERE channel_slug = ? AND service = ? AND video_id IS NOT NULL
               ORDER BY called_at DESC""",
            (channel_slug, service),
        ).fetchall()

    grouped: dict[int, dict] = {}
    for row in rows:
        entry = dict(row)
        video_id = entry["video_id"]
        if video_id not in grouped:
            grouped[video_id] = {"total_usd": 0.0, "entries": []}
        grouped[video_id]["total_usd"] += entry.get("cost_usd") or 0.0
        grouped[video_id]["entries"].append(entry)

    for video_id in grouped:
        grouped[video_id]["total_usd"] = round(grouped[video_id]["total_usd"], 4)
    return grouped


# ── Cron / Ops ────────────────────────────────────────────────────────────────

def create_cron_run(triggered_by: str = "cron") -> int:
    with _connect("ops") as conn:
        cursor = conn.execute(
            "INSERT INTO cron_runs (triggered_by) VALUES (?)",
            (triggered_by,),
        )
        return cursor.lastrowid


def finish_cron_run(run_id: int, status: str, summary: str = "", error_msg: str = None) -> None:
    with _connect("ops") as conn:
        conn.execute(
            """UPDATE cron_runs
               SET finished_at = CURRENT_TIMESTAMP, status = ?, summary = ?, error_msg = ?
               WHERE id = ?""",
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
    with _connect("ops") as conn:
        conn.execute(
            """INSERT INTO cron_events (run_id, level, channel_slug, video_id, action, message)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, level, channel_slug, video_id, action, message),
        )


def get_latest_cron_run() -> Optional[dict]:
    with _connect("ops") as conn:
        row = conn.execute(
            "SELECT * FROM cron_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def get_recent_cron_runs(limit: int = 8) -> list[dict]:
    with _connect("ops") as conn:
        rows = conn.execute(
            "SELECT * FROM cron_runs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_recent_cron_events(limit: int = 20) -> list[dict]:
    with _connect("ops") as conn:
        rows = conn.execute(
            "SELECT * FROM cron_events ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Global stats ──────────────────────────────────────────────────────────────

def get_global_stats() -> dict:
    channels = get_all_channels()
    total_channels = len(channels)

    with _connect("videos") as conn:
        row = conn.execute(
            """SELECT
                   COUNT(*) as total,
                   COALESCE(SUM(youtube_views), 0) as youtube_views,
                   COALESCE(SUM(tiktok_views), 0) as tiktok_views,
                   COALESCE(SUM(youtube_likes), 0) as youtube_likes,
                   COALESCE(SUM(tiktok_likes), 0) as tiktok_likes,
                   COALESCE(SUM(youtube_comments), 0) as youtube_comments,
                   COALESCE(SUM(tiktok_comments), 0) as tiktok_comments
               FROM videos"""
        ).fetchone()
        total_videos = row["total"]
        total_views = row["youtube_views"] + row["tiktok_views"]
        total_likes = row["youtube_likes"] + row["tiktok_likes"]
        total_comments = row["youtube_comments"] + row["tiktok_comments"]

    with _connect("costs") as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(cost_usd), 0) as total FROM costs WHERE service = 'claude'"
        ).fetchone()
        total_claude_cost = round(row["total"], 4)

    return {
        "total_channels": total_channels,
        "total_videos": total_videos,
        "total_views": total_views,
        "total_likes": total_likes,
        "total_comments": total_comments,
        "total_cost_usd": total_claude_cost,
        "total_claude_cost_usd": total_claude_cost,
    }


def get_channel_rollups() -> dict[str, dict]:
    with _connect("videos") as conn:
        video_rows = conn.execute(
            """SELECT
                   channel_slug,
                   COALESCE(SUM(youtube_likes), 0) as youtube_likes,
                   COALESCE(SUM(tiktok_likes), 0) as tiktok_likes,
                   COALESCE(SUM(youtube_comments), 0) as youtube_comments,
                   COALESCE(SUM(tiktok_comments), 0) as tiktok_comments
               FROM videos
               GROUP BY channel_slug"""
        ).fetchall()

    with _connect("costs") as conn:
        cost_rows = conn.execute(
            """SELECT
                   channel_slug,
                   COALESCE(SUM(cost_usd), 0) as claude_cost_usd
               FROM costs
               WHERE service = 'claude'
               GROUP BY channel_slug"""
        ).fetchall()

    rollups: dict[str, dict] = {}
    for row in video_rows:
        item = dict(row)
        rollups[item["channel_slug"]] = {
            "claude_cost_usd": 0.0,
            "youtube_likes": item["youtube_likes"] or 0,
            "youtube_comments": item["youtube_comments"] or 0,
            "tiktok_likes": item["tiktok_likes"] or 0,
            "tiktok_comments": item["tiktok_comments"] or 0,
            "total_likes": (item["youtube_likes"] or 0) + (item["tiktok_likes"] or 0),
            "total_comments": (item["youtube_comments"] or 0) + (item["tiktok_comments"] or 0),
        }

    for row in cost_rows:
        item = dict(row)
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
        bucket["claude_cost_usd"] = round(item["claude_cost_usd"] or 0.0, 4)

    return rollups
