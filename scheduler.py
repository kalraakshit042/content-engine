#!/usr/bin/env python3
"""
scheduler.py

Hourly orchestration job for the content engine.

Responsibilities:
1. Publish any due queued videos to YouTube and TikTok.
2. Ensure each live channel has generated videos covering the next 2 days of
   planned posting slots.

Recommended cron:
  0 * * * * cd /Users/akshitkalra/Code/Content\ automation/content-engine && \
    /usr/bin/python3 scheduler.py >> logs/scheduler.log 2>&1
"""

import sys
import os
import smtplib
import traceback
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from post_storage_migration import migrate_post_storage
from storage_cleanup import run_cleanup

load_dotenv(override=True)

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from database.setup import setup_channels_db, setup_videos_db, setup_costs_db, setup_ops_db
from database.queries import (
    create_cron_run,
    finish_cron_run,
    get_channel_videos,
    get_channel,
    get_live_channels,
    get_videos_for_schedule_window,
    insert_video,
    log_cron_event,
    set_tiktok_status,
    set_video_scheduled_for,
    set_video_status,
    set_youtube_status,
)
from layer2_script_generation.script_generator import generate_script
from layer3_audio_production.audio_generator import generate_audio
from layer4_video_production.video_assembler import assemble_video
from layer5_publishing.tiktok_uploader import publish_due_queued_videos as publish_due_tiktok_videos
from layer5_publishing.youtube_uploader import (
    ET,
    PUBLISH_SLOTS,
    publish_due_queued_videos as publish_due_youtube_videos,
)

LOG_DIR = BASE_DIR / "logs"
HORIZON_DAYS = 2
MAX_GENERATE_PER_CHANNEL = 1
FAILURE_EMAIL_TO = "akskalra@wharton.upenn.edu"
MISSED_WINDOW_GRACE_MINUTES = 65


def log(run_id: int, message: str, level: str = "info", action: str = None, channel_slug: str = None, video_id: int = None) -> None:
    ts = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")
    print(f"{ts} {level.upper()} {message}", flush=True)
    log_cron_event(run_id, message, level=level, action=action, channel_slug=channel_slug, video_id=video_id)


def send_failure_email(subject: str, body: str, run_id: int) -> bool:
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_username = os.environ.get("SMTP_USERNAME", "").strip()
    smtp_password = os.environ.get("SMTP_PASSWORD", "").strip()
    smtp_from = os.environ.get("SMTP_FROM", smtp_username or "content-engine@localhost").strip()

    if not smtp_host:
        log(run_id, "Failure email skipped: SMTP_HOST not configured", level="error", action="email_skipped")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = FAILURE_EMAIL_TO
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            if smtp_username:
                server.login(smtp_username, smtp_password)
            server.send_message(msg)
        log(run_id, f"Failure email sent to {FAILURE_EMAIL_TO}", action="email_sent")
        return True
    except Exception as exc:
        log(run_id, f"Failure email failed: {exc}", level="error", action="email_failed")
        return False


def planned_slots_for_window(now_et: datetime, horizon_days: int = HORIZON_DAYS) -> list[datetime]:
    slots: list[datetime] = []
    end_et = now_et + timedelta(days=horizon_days)

    for day_offset in range(horizon_days + 1):
        candidate_date = (now_et + timedelta(days=day_offset)).date()
        for hour, minute in PUBLISH_SLOTS:
            slot_et = datetime(
                candidate_date.year,
                candidate_date.month,
                candidate_date.day,
                hour,
                minute,
                tzinfo=ET,
            )
            if slot_et <= now_et:
                continue
            if slot_et > end_et:
                continue
            slots.append(slot_et)

    return sorted(slots)


def _parse_slot_utc(scheduled_for: str | None) -> datetime | None:
    if not scheduled_for:
        return None
    try:
        slot_dt = datetime.fromisoformat(scheduled_for)
    except Exception:
        return None
    if slot_dt.tzinfo is None:
        slot_dt = slot_dt.replace(tzinfo=ET)
    return slot_dt.astimezone(timezone.utc)


def due_video_alerts() -> list[str]:
    alerts: list[str] = []
    now = datetime.now(timezone.utc)
    grace = timedelta(minutes=MISSED_WINDOW_GRACE_MINUTES)
    client_secret = BASE_DIR / "credentials" / "google_client_secret.json"

    for channel in get_live_channels():
        slug = channel["slug"]
        videos = get_channel_videos(slug)
        due_youtube = []
        due_tiktok = []
        for video in videos:
            slot_utc = _parse_slot_utc(video.get("scheduled_for"))
            if slot_utc is None or slot_utc > now:
                continue
            if not video.get("youtube_video_id") and video.get("youtube_status") in {None, "", "queued", "error"}:
                due_youtube.append(video)
                if slot_utc < now - grace:
                    alerts.append(f"[{slug}] YouTube missed post window for video {video['id']} scheduled {video.get('scheduled_for')}")
            if not video.get("tiktok_posted") and video.get("tiktok_status") in {None, "", "queued", "error"}:
                due_tiktok.append(video)
                if slot_utc < now - grace:
                    alerts.append(f"[{slug}] TikTok missed post window for video {video['id']} scheduled {video.get('scheduled_for')}")

        if due_youtube:
            token_path = BASE_DIR / "credentials" / f"{slug}_token.json"
            if not client_secret.exists():
                alerts.append(f"[{slug}] YouTube auth unavailable: missing google_client_secret.json")
            if not token_path.exists():
                alerts.append(f"[{slug}] YouTube auth unavailable: missing {slug}_token.json for {len(due_youtube)} due video(s)")

        if due_tiktok:
            cookies_path = BASE_DIR / "credentials" / f"{slug}_tiktok_cookies.txt"
            if not cookies_path.exists():
                alerts.append(f"[{slug}] TikTok auth unavailable: missing {slug}_tiktok_cookies.txt for {len(due_tiktok)} due video(s)")

    return alerts


def log_publish_details(run_id: int, platform: str, result: dict) -> None:
    for detail in result.get("details", []):
        status = detail.get("status")
        if status not in {"published", "failed"}:
            continue
        level = "error" if status == "failed" else "info"
        reason = detail.get("reason", status)
        slug = detail.get("channel_slug")
        video_id = detail.get("video_id")
        log(
            run_id,
            f"[{slug}] {platform} {status} for video {video_id}: {reason}",
            level=level,
            action=f"{platform.lower()}_{status}",
            channel_slug=slug,
            video_id=video_id,
        )


def existing_slot_map(slug: str, start_iso: str, end_iso: str) -> dict[str, dict]:
    videos = get_videos_for_schedule_window(slug, start_iso, end_iso)
    result: dict[str, dict] = {}
    for video in videos:
        if video.get("status") == "error":
            continue
        slot = video.get("scheduled_for")
        if slot and slot not in result:
            result[slot] = video
    return result


def generate_video_for_slot(run_id: int, slug: str, slot_et: datetime) -> tuple[bool, str]:
    video_id = insert_video(slug)
    slot_iso = slot_et.isoformat()
    set_video_scheduled_for(video_id, slot_iso)
    set_youtube_status(video_id, "queued", None)
    set_tiktok_status(video_id, "queued", None)
    log(run_id, f"[{slug}] reserved slot {slot_iso} for video {video_id}", action="reserve_slot", channel_slug=slug, video_id=video_id)

    try:
        set_video_status(video_id, "generating_script")
        generate_script(slug, video_id)
        set_video_status(video_id, "generating_audio")
        generate_audio(slug, video_id)
        set_video_status(video_id, "generating_video")
        assemble_video(slug, video_id)
        set_video_status(video_id, "video_done")
        set_youtube_status(video_id, "queued", None)
        set_tiktok_status(video_id, "queued", None)
        log(run_id, f"[{slug}] video {video_id} ready for {slot_iso}", action="video_ready", channel_slug=slug, video_id=video_id)
        return True, ""
    except Exception as exc:
        set_video_status(video_id, "error")
        set_youtube_status(video_id, "error", str(exc)[:500])
        set_tiktok_status(video_id, "error", str(exc)[:500])
        log(run_id, f"[{slug}] video {video_id} failed for {slot_iso}: {exc}", level="error", action="generate_failed", channel_slug=slug, video_id=video_id)
        return False, str(exc)


def _videos_per_day(channel_created_at: str | None) -> int:
    """
    Return target videos/day based on how many days since channel went live.

    Week 1 (days 0-6):   1 video/day — establish cadence
    Weeks 2-3 (days 7-27): 2 videos/day — build momentum
    Week 4+ (day 28+):   3 videos/day — sustained growth

    High-velocity (5/day) is triggered separately via high_velocity_mode in channel config.
    """
    if not channel_created_at:
        return 1
    try:
        from datetime import date as _date
        created = _date.fromisoformat(channel_created_at[:10])
        days_live = (_date.today() - created).days
    except (ValueError, TypeError):
        return 1

    if days_live < 7:
        return 1
    elif days_live < 28:
        return 2
    else:
        return 3


def _random_daily_slots(slug: str, day, n: int) -> list[tuple[int, int]]:
    """
    Generate n posting times between 10 AM and 8 PM ET for a given day.
    Seeded by slug + date — consistent across scheduler runs, unique per day/channel.
    Times are spread evenly across the window to avoid clustering.
    """
    import random as _rng_mod
    rng = _rng_mod.Random(f"{slug}-{day.isoformat()}")
    window_start = 10 * 60   # 600 minutes = 10:00 AM
    window_end = 20 * 60     # 1200 minutes = 8:00 PM
    segment = (window_end - window_start) // n
    slots = []
    for i in range(n):
        seg_start = window_start + i * segment
        seg_end = seg_start + segment
        minutes = rng.randint(seg_start, seg_end - 1)
        slots.append((minutes // 60, minutes % 60))
    return sorted(slots)


def _slots_for_channel(slug: str, now_et: datetime, horizon_days: int) -> list[datetime]:
    """
    Returns publish slots for the given channel.

    Priority:
    1. Fixed slots from channel_config.json publish_slots (if non-empty) — manual override.
    2. Dynamic schedule: random times between 10 AM and 8 PM ET, seeded by slug+date.
       Number of videos/day scales with channel age (1 → 2 → 3 per day).
       Set high_velocity_mode=true in channel_config.json for 5/day (1M realtime views tier).
    """
    import json as _json

    config_data: dict = {}
    try:
        config_path = BASE_DIR / "channels" / slug / "channel_config.json"
        config_data = _json.loads(config_path.read_text())
    except Exception:
        pass

    raw_fixed = config_data.get("publish_slots", [])
    end_et = now_et + timedelta(days=horizon_days)
    slots = []

    if raw_fixed:
        # Fixed slots configured — use them as-is
        slot_times = [tuple(map(int, s.split(":"))) for s in raw_fixed]
        for day_offset in range(horizon_days + 1):
            candidate_date = (now_et + timedelta(days=day_offset)).date()
            for hour, minute in slot_times:
                slot_et = datetime(candidate_date.year, candidate_date.month, candidate_date.day, hour, minute, tzinfo=ET)
                if slot_et <= now_et or slot_et > end_et:
                    continue
                slots.append(slot_et)
        return sorted(slots)

    # Dynamic schedule
    channel = get_channel(slug)
    n_per_day = _videos_per_day(channel.get("created_at") if channel else None)
    if config_data.get("high_velocity_mode"):
        n_per_day = 5

    for day_offset in range(horizon_days + 1):
        candidate_date = (now_et + timedelta(days=day_offset)).date()
        slot_times = _random_daily_slots(slug, candidate_date, n_per_day)
        for hour, minute in slot_times:
            slot_et = datetime(candidate_date.year, candidate_date.month, candidate_date.day, hour, minute, tzinfo=ET)
            if slot_et <= now_et or slot_et > end_et:
                continue
            slots.append(slot_et)
    return sorted(slots)


def ensure_future_coverage(run_id: int, horizon_days: int = HORIZON_DAYS) -> dict[str, int]:
    generated = 0
    skipped = 0
    failed = 0

    now_et = datetime.now(ET)

    for channel in get_live_channels():
        slug = channel["slug"]
        slots = _slots_for_channel(slug, now_et, horizon_days)
        if not slots:
            continue
        start_iso = slots[0].isoformat()
        end_iso = slots[-1].isoformat()
        existing = existing_slot_map(slug, start_iso, end_iso)
        channel_generated = 0
        for slot_et in slots:
            slot_iso = slot_et.isoformat()
            if slot_iso in existing:
                skipped += 1
                continue
            if channel_generated >= MAX_GENERATE_PER_CHANNEL:
                skipped += 1
                continue
            ok, _ = generate_video_for_slot(run_id, slug, slot_et)
            if ok:
                generated += 1
                channel_generated += 1
            else:
                failed += 1

    return {"generated": generated, "skipped": skipped, "failed": failed}


def run_hourly_job(triggered_by: str = "cron") -> int:
    setup_channels_db()
    setup_videos_db()
    setup_costs_db()
    setup_ops_db()
    migration_result = migrate_post_storage()
    LOG_DIR.mkdir(exist_ok=True)

    run_id = create_cron_run(triggered_by=triggered_by)
    summary_parts: list[str] = []

    try:
        log(run_id, "Hourly orchestrator started", action="start")
        if migration_result["moved_files"]:
            log(run_id, f"Post storage migration moved {migration_result['moved_files']} file(s)", action="storage_migration")

        alerts = due_video_alerts()
        for alert in alerts:
            log(run_id, alert, level="error", action="runtime_alert")

        yt_result = publish_due_youtube_videos()
        log(run_id, f"YouTube due publish result: {yt_result}", action="youtube_due_publish")
        log_publish_details(run_id, "YouTube", yt_result)
        summary_parts.append(f"YT published {yt_result['published']}")

        tt_result = publish_due_tiktok_videos()
        log(run_id, f"TikTok due publish result: {tt_result}", action="tiktok_due_publish")
        log_publish_details(run_id, "TikTok", tt_result)
        summary_parts.append(f"TT published {tt_result['published']}")

        cleanup_log = run_cleanup()
        for line in cleanup_log:
            log(run_id, f"[cleanup] {line}", action="storage_cleanup")

        coverage_result = ensure_future_coverage(run_id, horizon_days=HORIZON_DAYS)
        log(run_id, f"Future coverage result: {coverage_result}", action="ensure_coverage")
        summary_parts.append(f"Generated {coverage_result['generated']}")

        status = "success" if coverage_result["failed"] == 0 and yt_result["failed"] == 0 and tt_result["failed"] == 0 and not alerts else "partial"
        finish_cron_run(run_id, status=status, summary=" | ".join(summary_parts))
        log(run_id, f"Hourly orchestrator finished with status={status}", action="finish")
        if status != "success":
            send_failure_email(
                subject=f"Content Engine cron {status.upper()}",
                body=(
                    f"Run ID: {run_id}\n"
                    f"Status: {status}\n"
                    f"Summary: {' | '.join(summary_parts)}\n"
                    f"Alerts:\n" + ("\n".join(f"- {a}" for a in alerts) if alerts else "- none") + "\n"
                    f"Finished: {datetime.now(ET).strftime('%Y-%m-%d %I:%M:%S %p ET')}\n"
                ),
                run_id=run_id,
            )
    except Exception as exc:
        finish_cron_run(run_id, status="failed", summary=" | ".join(summary_parts), error_msg=str(exc)[:1000])
        log(run_id, f"Hourly orchestrator failed: {exc}", level="error", action="fatal")
        send_failure_email(
            subject="Content Engine cron FAILED",
            body=(
                f"Run ID: {run_id}\n"
                f"Status: failed\n"
                f"Error: {exc}\n"
                f"Finished: {datetime.now(ET).strftime('%Y-%m-%d %I:%M:%S %p ET')}\n"
                f"Summary so far: {' | '.join(summary_parts)}\n"
            ),
            run_id=run_id,
        )
        traceback.print_exc()

    return run_id


def main() -> None:
    run_hourly_job(triggered_by="cron")


if __name__ == "__main__":
    main()
