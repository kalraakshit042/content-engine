#!/usr/bin/env python3
from __future__ import annotations
"""
scheduler.py

Hourly orchestration job for the content engine.

Responsibilities:
1. After 6AM ET: generate all of today's videos at once, then immediately upload
   each to YouTube as a SCHEDULED post (never "Post now"). YouTube delivers at the
   designated time — the engine does not push live at runtime.
2. Posting cadence: 1 video/day in week 1, 2 videos/day after that.
   Slots are seeded-random per channel+date — deterministic across restarts.
   1 video: random 8AM-5PM ET. 2 videos: first 8-11AM ET, second 2-5PM ET.
3. Missed cron (laptop off): if a slot's time has already passed, the video is
   still generated and uploaded with publish_at = now+15min.
4. TikTok: queued after generation, picked up by publish_due_tiktok_videos().

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
    count_generated_today,
    count_posted_today,
    count_tiktok_posted_today,
    create_cron_run,
    finish_cron_run,
    get_channel,
    get_channel_videos,
    get_live_channels,
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
    publish_video as yt_publish_video,
    publish_due_queued_videos as publish_due_youtube_videos,
)

LOG_DIR = BASE_DIR / "logs"
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


def generate_video_for_slot(run_id: int, slug: str, slot_et: datetime, publish_slot: datetime = None) -> tuple[bool, str]:
    """
    Generate a complete video then immediately upload it to YouTube as a scheduled post.

    slot_et:      the logical posting slot (stored in scheduled_for, drives the seeded random)
    publish_slot: the actual YouTube publish time — equals slot_et unless the slot already
                  passed (missed cron), in which case the caller sets it to now+15min.
    """
    import json as _json_gvfs

    _cfg: dict = {}
    try:
        _cfg = _json_gvfs.loads((BASE_DIR / "channels" / slug / "channel_config.json").read_text())
    except Exception:
        pass
    _preview_mode = _cfg.get("preview_mode", False)

    if publish_slot is None:
        publish_slot = slot_et

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

        if _preview_mode:
            set_youtube_status(video_id, "preview", None)
            set_tiktok_status(video_id, "preview", None)
            log(run_id, f"[{slug}] video {video_id} held for preview (preview_mode=true)", action="preview_hold", channel_slug=slug, video_id=video_id)
        else:
            # Upload immediately to YouTube as a scheduled post.
            # YouTube handles delivery at publish_slot — we never post as "public now".
            et_str = publish_slot.astimezone(ET).strftime("%b %-d %I:%M %p ET")
            log(run_id, f"[{slug}] uploading video {video_id} — scheduled for {et_str}",
                action="yt_upload_start", channel_slug=slug, video_id=video_id)
            try:
                yt_publish_video(slug, video_id, slot=publish_slot)
                set_youtube_status(video_id, "scheduled", None)
                log(run_id, f"[{slug}] video {video_id} uploaded and scheduled for {et_str}",
                    action="yt_scheduled", channel_slug=slug, video_id=video_id)
            except Exception as yt_exc:
                set_youtube_status(video_id, "error", str(yt_exc)[:500])
                log(run_id, f"[{slug}] video {video_id} YT upload failed: {yt_exc}",
                    level="error", action="yt_upload_failed", channel_slug=slug, video_id=video_id)
                return False, str(yt_exc)

            # TikTok still goes through the normal queued → publish flow
            set_tiktok_status(video_id, "queued", None)

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

    Week 1 (days 0-6):  1 video/day — establish cadence, single 8AM-5PM ET window
    Week 2+ (days 7+):  2 videos/day — first 8-11AM ET, second 2-5PM ET

    high_velocity_mode=true in channel_config.json overrides to 5/day (8AM-8PM spread).
    """
    if not channel_created_at:
        return 1
    try:
        from datetime import date as _date
        created = _date.fromisoformat(channel_created_at[:10])
        days_live = (_date.today() - created).days
    except (ValueError, TypeError):
        return 1

    return 1 if days_live < 7 else 2


def _random_daily_slots(slug: str, day, n: int) -> list[tuple[int, int]]:
    """
    Generate n posting times seeded by slug+date — deterministic per channel/day.

    n=1: single slot anywhere between 8AM and 5PM ET
    n=2: first slot 8-11AM ET, second slot 2-5PM ET (no midday overlap)
    n>2: spread evenly across 8AM-8PM ET (high_velocity_mode only)
    """
    import random as _rng_mod
    rng = _rng_mod.Random(f"{slug}-{day.isoformat()}")

    if n == 1:
        slot_min = rng.randint(8 * 60, 17 * 60 - 1)   # 8:00AM–4:59PM
        return [(slot_min // 60, slot_min % 60)]

    if n == 2:
        first_min  = rng.randint(8 * 60, 11 * 60 - 1)  # 8:00AM–10:59AM
        second_min = rng.randint(14 * 60, 17 * 60 - 1) # 2:00PM–4:59PM
        return [(first_min // 60, first_min % 60), (second_min // 60, second_min % 60)]

    # high_velocity_mode (n>2): evenly spread 8AM-8PM
    window_start, window_end = 8 * 60, 20 * 60
    segment = (window_end - window_start) // n
    slots = []
    for i in range(n):
        seg_start = window_start + i * segment
        seg_end   = seg_start + segment
        minutes   = rng.randint(seg_start, seg_end - 1)
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


def _slots_for_today(slug: str, today) -> list[datetime]:
    """
    Returns ALL posting slots for today (past and future) for a given channel.
    Seeded-random or fixed — same logic as _random_daily_slots.
    The caller decides which slots are actionable based on current time.
    """
    import json as _json

    config_data: dict = {}
    try:
        config_data = _json.loads((BASE_DIR / "channels" / slug / "channel_config.json").read_text())
    except Exception:
        pass

    raw_fixed = config_data.get("publish_slots", [])
    if raw_fixed:
        slot_times = [tuple(map(int, s.split(":"))) for s in raw_fixed]
    else:
        channel = get_channel(slug)
        n = _videos_per_day(channel.get("created_at") if channel else None)
        if config_data.get("high_velocity_mode"):
            n = 5
        slot_times = _random_daily_slots(slug, today, n)

    return sorted([
        datetime(today.year, today.month, today.day, hour, minute, tzinfo=ET)
        for hour, minute in slot_times
    ])


def generate_due_videos(run_id: int) -> dict[str, int]:
    """
    Daily video generation — runs after 6AM ET, generates all of today's videos at once.

    Workflow per channel:
      1. Gate: only run from 6AM ET onwards (no midnight generation).
      2. Determine today's slots (seeded random: 1 video in week 1, 2 after).
      3. Count non-error video rows already created today (count_generated_today).
         This covers any status — generating, queued, scheduled, published.
         Error rows are excluded so failed attempts can be retried.
      4. Generate the remaining (slots - already_generated) videos.
      5. Each video is uploaded immediately to YouTube as a SCHEDULED post
         (never "Post now"). If the slot time is already past (missed cron), the
         publish time is bumped to now+15min so YouTube accepts it.

    The cron may run this multiple times per day — idempotent by design.
    """
    generated = 0
    failed = 0
    now_et = datetime.now(ET)
    today = now_et.date()

    # Gate: no generation before 6AM ET (avoid midnight surprises)
    if now_et.hour < 6:
        log(run_id, "Before 6AM ET — daily generation paused until 6AM", action="generate_skip_early")
        return {"generated": 0, "failed": 0}

    for channel in get_live_channels():
        slug = channel["slug"]
        all_slots = _slots_for_today(slug, today)
        if not all_slots:
            continue

        already_generated = count_generated_today(slug)
        to_generate = max(0, len(all_slots) - already_generated)
        if to_generate == 0:
            log(run_id, f"[{slug}] {len(all_slots)} slot(s) today — all {already_generated} already handled",
                action="generate_skip", channel_slug=slug)
            continue

        log(run_id,
            f"[{slug}] {len(all_slots)} slot(s) today, {already_generated} already generated, "
            f"generating {to_generate} now",
            action="generate_due", channel_slug=slug)

        for i in range(to_generate):
            slot_et = all_slots[already_generated + i]
            # If slot is already past (missed cron tick), bump publish time to now+15min
            if slot_et <= now_et:
                publish_slot = now_et + timedelta(minutes=15)
                log(run_id,
                    f"[{slug}] slot {slot_et.strftime('%H:%M')} ET already passed — "
                    f"rescheduling to now+15min ({publish_slot.strftime('%H:%M')} ET)",
                    action="slot_bumped", channel_slug=slug)
            else:
                publish_slot = slot_et

            ok, _ = generate_video_for_slot(run_id, slug, slot_et, publish_slot=publish_slot)
            if ok:
                generated += 1
            else:
                failed += 1

    return {"generated": generated, "failed": failed}


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

        # Generate videos whose posting slot has arrived — before publish so they
        # are picked up in the same cron tick.
        gen_result = generate_due_videos(run_id)
        log(run_id, f"Due video generation result: {gen_result}", action="generate_due")
        summary_parts.append(f"Generated {gen_result['generated']}")

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

        status = "success" if gen_result["failed"] == 0 and yt_result["failed"] == 0 and tt_result["failed"] == 0 and not alerts else "partial"
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
