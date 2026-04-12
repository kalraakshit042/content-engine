"""
layer5_publishing/youtube_uploader.py

Handles YouTube OAuth and video publishing for all channels.

Flow:
  1. authenticate(slug) — OAuth2 desktop flow, token cached per channel
  2. upload_video(slug, video_id, service) — resumable upload, returns yt_video_id
  3. upload_thumbnail(yt_video_id, thumbnail_path, service) — set custom thumbnail
  4. post_comment(yt_video_id, comment_text, service) — post engagement comment
  5. publish_video(slug, video_id) — orchestrates all of the above

Note: YouTube Data API v3 has no endpoint to pin a comment programmatically.
Comments are posted unpinned; pin manually in YouTube Studio after upload.
"""

from __future__ import annotations

import json
import os
import random
import socket
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from content_paths import resolve_script_json_path, resolve_thumbnail_path
from layer1_account_setup.config_schema import ChannelConfig
from database.queries import (
    get_video,
    get_channel,
    get_scheduled_slots,
    get_channel_videos,
    get_live_channels,
    set_youtube_status,
    update_video_youtube,
    count_posted_today,
    get_affiliate_product,
)

ET = ZoneInfo("America/New_York")

load_dotenv(override=True)

BASE_DIR = Path(__file__).parent.parent
CHANNELS_DIR = BASE_DIR / "channels"
CREDENTIALS_DIR = BASE_DIR / "credentials"
CLIENT_SECRET = CREDENTIALS_DIR / "google_client_secret.json"

SCOPES = [
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.force-ssl",
]

# YouTube category IDs
CATEGORY_ENTERTAINMENT = "24"


def next_publish_slot(slug: str) -> datetime:
    """
    Returns the next available publish slot using the dynamic schedule.
    Mirrors scheduler._slots_for_channel() — seeded by slug+date, 10AM–8PM ET window,
    scales 1→2→3 videos/day by channel age. Skips already-occupied slots.
    """
    import random as _rng
    from datetime import date as _date

    occupied = set(get_scheduled_slots(slug))
    now_et = datetime.now(ET)
    search_from = now_et + timedelta(minutes=5)

    # Determine videos/day based on channel age
    channel = get_channel(slug)
    created_at = channel.get("created_at") if channel else None
    days_live = 0
    if created_at:
        try:
            days_live = (_date.today() - _date.fromisoformat(created_at[:10])).days
        except (ValueError, TypeError):
            pass
    if days_live < 7:
        n_per_day = 1
    elif days_live < 28:
        n_per_day = 2
    else:
        n_per_day = 3

    for day_offset in range(30):
        candidate_date = (now_et + timedelta(days=day_offset)).date()
        rng = _rng.Random(f"{slug}-{candidate_date.isoformat()}")
        window_start, window_end = 10 * 60, 20 * 60  # 10AM–8PM in minutes
        segment = (window_end - window_start) // n_per_day
        slot_times = sorted(
            (window_start + i * segment + rng.randint(0, segment - 1))
            for i in range(n_per_day)
        )
        for total_minutes in slot_times:
            slot_et = datetime(
                candidate_date.year, candidate_date.month, candidate_date.day,
                total_minutes // 60, total_minutes % 60, 0, tzinfo=ET,
            )
            if slot_et <= search_from:
                continue
            if slot_et.isoformat() not in occupied:
                return slot_et

    raise RuntimeError("Could not find a free publish slot in the next 30 days")


def _load_channel_config(slug: str) -> ChannelConfig:
    config_path = CHANNELS_DIR / slug / "channel_config.json"
    return ChannelConfig(**json.loads(config_path.read_text()))


def authenticate(slug: str):
    """
    OAuth2 desktop flow. Token cached at credentials/{slug}_token.json.
    First call opens a browser for consent. Subsequent calls use the refresh token.
    Returns an authenticated YouTube API service resource.
    """
    token_path = CREDENTIALS_DIR / f"{slug}_token.json"
    creds = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())

    return build("youtube", "v3", credentials=creds)


def check_token_health(slug: str) -> dict:
    """
    Non-interactive token health check. Returns {"ok": True} or {"ok": False, "reason": "..."}.
    Does NOT open a browser — only validates the cached token.
    """
    token_path = CREDENTIALS_DIR / f"{slug}_token.json"
    if not token_path.exists():
        return {"ok": False, "reason": "No token file — channel not authenticated yet"}
    try:
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        if creds.valid:
            return {"ok": True}
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json())
            return {"ok": True}
        return {"ok": False, "reason": "Token expired and cannot be refreshed — re-authenticate"}
    except Exception as e:
        return {"ok": False, "reason": str(e)}


def upload_video(slug: str, video_id: int, service, publish_at: datetime = None) -> str:
    """
    Upload the video file to YouTube. Returns the YouTube video ID.
    If publish_at is given, uploads as private with that scheduled publish time.
    Retries up to 3 times on transient server errors (5xx).
    """
    script_path = resolve_script_json_path(slug, video_id)
    script_data = json.loads(script_path.read_text())

    video_row = get_video(video_id)
    video_path = Path(video_row["final_video_path"])
    if not video_path.exists():
        raise FileNotFoundError(f"Video file not found: {video_path}")

    title = script_data.get("title", f"Video {video_id}")[:100]
    description = script_data.get("description", "")
    hashtags = script_data.get("hashtags", [])
    # Always include #Shorts so YouTube classifies the vertical video correctly
    shorts_tags = ["Shorts"] + [h.lstrip("#") for h in hashtags if h.lstrip("#").lower() != "shorts"]
    description += "\n\n" + " ".join(f"#{t}" for t in shorts_tags)

    # Affiliate link injection — subject-specific Amazon link appended after hashtags
    import logging as _log
    affiliate_tag = os.getenv("AMAZON_AFFILIATE_TAG", "")
    if not affiliate_tag:
        _log.warning("AMAZON_AFFILIATE_TAG not set — skipping affiliate link injection")
    else:
        subject = script_data.get("subject", "").lower().strip()
        if subject:
            product = get_affiliate_product(subject)
            if product:
                description += (
                    f"\n\n🔗 https://www.amazon.com/dp/{product['asin']}/?tag={affiliate_tag}"
                    "\n\nAs an Amazon Associate I earn from qualifying purchases."
                )
            else:
                _log.warning("No affiliate ASIN for subject '%s' (video %d) — uploading without link", subject, video_id)

    if publish_at is not None:
        # YouTube requires RFC 3339 UTC format: 2025-04-06T09:00:00Z
        publish_at_utc = publish_at.astimezone(timezone.utc)
        publish_at_str = publish_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        status_body = {
            "privacyStatus": "private",
            "publishAt": publish_at_str,
            "selfDeclaredMadeForKids": False,
        }
    else:
        status_body = {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False,
        }

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": shorts_tags,
            "categoryId": CATEGORY_ENTERTAINMENT,
            "defaultLanguage": "en",
        },
        "status": status_body,
    }

    media = MediaFileUpload(str(video_path), mimetype="video/mp4",
                            chunksize=-1, resumable=True)

    request = service.videos().insert(part="snippet,status", body=body,
                                       media_body=media)

    response = None
    retries = 0
    max_retries = 5
    while response is None:
        try:
            _, response = request.next_chunk()
        except HttpError as e:
            if e.resp.status in (500, 502, 503, 504) and retries < max_retries:
                retries += 1
                wait = 2 ** retries
                print(f"  upload: transient HTTP error {e.resp.status}, retry {retries}/{max_retries} in {wait}s...")
                time.sleep(wait)
            else:
                raise
        except (socket.timeout, TimeoutError, ConnectionResetError) as e:
            if retries < max_retries:
                retries += 1
                wait = 2 ** retries
                print(f"  upload: network timeout ({type(e).__name__}), retry {retries}/{max_retries} in {wait}s...")
                time.sleep(wait)
            else:
                raise

    yt_video_id = response["id"]
    yt_url = f"https://youtu.be/{yt_video_id}"
    scheduled_iso = publish_at.isoformat() if publish_at else None
    update_video_youtube(video_id, yt_video_id, yt_url, scheduled_for=scheduled_iso)
    if publish_at:
        et_str = publish_at.astimezone(ET).strftime("%b %-d %I:%M %p ET")
        print(f"  upload: done → {yt_url} (scheduled for {et_str})")
    else:
        print(f"  upload: done → {yt_url}")
    return yt_video_id


def upload_thumbnail(yt_video_id: str, thumbnail_path: Path, service) -> None:
    """
    Set custom thumbnail. Requires YouTube account to be phone-verified.
    Logs a warning on failure instead of raising — upload already succeeded.
    """
    try:
        service.thumbnails().set(
            videoId=yt_video_id,
            media_body=MediaFileUpload(str(thumbnail_path), mimetype="image/jpeg"),
        ).execute()
        print(f"  thumbnail: set")
    except HttpError as e:
        print(f"  thumbnail: failed ({e.resp.status}) — set manually in YouTube Studio")


def _build_comment(base_comment: str, video_id: int) -> str:
    """
    Append affiliate link to a pinned comment if the video's subject has an ASIN.
    YouTube Shorts descriptions aren't clickable, so the comment is the only live link.
    """
    affiliate_tag = os.getenv("AMAZON_AFFILIATE_TAG", "")
    if not affiliate_tag:
        return base_comment
    try:
        from database.queries import _connect
        with _connect("videos") as _conn:
            row = _conn.execute("SELECT channel_slug FROM videos WHERE id=?", (video_id,)).fetchone()
        if not row:
            return base_comment
        slug = row["channel_slug"]
        sp = resolve_script_json_path(slug, video_id)
        if not sp.exists():
            return base_comment
        import json as _j
        data = _j.loads(sp.read_text())
        subject = data.get("subject", "").lower().strip()
        if not subject:
            return base_comment
        product = get_affiliate_product(subject)
        if not product:
            return base_comment
        link = f"https://www.amazon.com/dp/{product['asin']}/?tag={affiliate_tag}"
        return f"{base_comment}\n\n🔗 {link}\n(As an Amazon Associate I earn from qualifying purchases.)"
    except Exception:
        return base_comment


def post_comment(yt_video_id: str, comment_text: str, service) -> None:
    """
    Post an engagement comment on the video.
    Note: YouTube API v3 has no endpoint to pin comments programmatically.
    Pin the comment manually in YouTube Studio after upload.
    """
    try:
        service.commentThreads().insert(
            part="snippet",
            body={
                "snippet": {
                    "videoId": yt_video_id,
                    "topLevelComment": {
                        "snippet": {"textOriginal": comment_text}
                    },
                }
            },
        ).execute()
        print(f"  comment: posted (pin manually in Studio)")
    except HttpError as e:
        print(f"  comment: failed ({e.resp.status}) — post manually")


def refresh_channel_stats(slug: str) -> dict:
    """
    Fetch current views/comments/likes from YouTube for all published videos.
    Also posts pending engagement comments for scheduled videos that have gone live.
    Updates videos.db. Returns {video_id: {views, comments, likes}}.
    """
    from database.queries import get_published_videos, update_video_stats, mark_comment_posted, set_video_status
    videos = get_published_videos(slug)
    if not videos:
        return {}
    service = authenticate(slug)

    # Post pending comments for scheduled videos whose publish time has passed
    now = datetime.now(timezone.utc)
    config = _load_channel_config(slug)
    for v in videos:
        if v.get("comment_posted"):
            continue
        scheduled_for = v.get("scheduled_for")
        if scheduled_for:
            try:
                slot_dt = datetime.fromisoformat(scheduled_for)
                if slot_dt.tzinfo is None:
                    slot_dt = slot_dt.replace(tzinfo=ET)
                slot_utc = slot_dt.astimezone(timezone.utc)
            except Exception:
                continue
            if now < slot_utc:
                continue  # not published yet
        # Either no scheduled_for (already public) or past publish time — post comment
        comment = _build_comment(random.choice(config.cta.pinned_comment_templates), v["id"])
        try:
            post_comment(v["youtube_video_id"], comment, service)
            mark_comment_posted(v["id"])
            if v.get("status") == "scheduled":
                set_video_status(v["id"], "published")
        except Exception as e:
            print(f"  [refresh] comment post failed for video {v['id']}: {e}")

    yt_ids = [v["youtube_video_id"] for v in videos]
    resp = service.videos().list(part="statistics", id=",".join(yt_ids)).execute()
    result = {}
    for item in resp.get("items", []):
        stats = item["statistics"]
        views = int(stats.get("viewCount", 0))
        comments = int(stats.get("commentCount", 0))
        likes = int(stats.get("likeCount", 0))
        vid = next(v for v in videos if v["youtube_video_id"] == item["id"])
        update_video_stats(vid["id"], views, comments, likes)
        result[vid["id"]] = {"views": views, "comments": comments, "likes": likes}
    return result


def publish_video(slug: str, video_id: int, slot: datetime = None) -> str:
    """
    Full publish flow: authenticate → upload scheduled → set thumbnail → post comment.
    slot should be pre-computed at pipeline start. If not provided, computes now (fallback).
    Returns the YouTube video ID.
    """
    print(f"[{slug}] Authenticating with YouTube...")
    service = authenticate(slug)

    if slot is None:
        slot = next_publish_slot(slug)
    et_str = slot.astimezone(ET).strftime("%b %-d %I:%M %p ET")
    print(f"[{slug}] Scheduling video {video_id} for {et_str}...")
    yt_video_id = upload_video(slug, video_id, service, publish_at=slot)

    thumbnail_path = resolve_thumbnail_path(slug, video_id)
    if thumbnail_path and thumbnail_path.exists():
        print(f"[{slug}] Uploading thumbnail...")
        upload_thumbnail(yt_video_id, thumbnail_path, service)
    else:
        print(f"[{slug}] No thumbnail found, skipping")

    config = _load_channel_config(slug)
    comment = _build_comment(random.choice(config.cta.pinned_comment_templates), video_id)
    print(f"[{slug}] Posting comment...")
    try:
        post_comment(yt_video_id, comment, service)
        from database.queries import mark_comment_posted
        mark_comment_posted(video_id)
    except Exception as e:
        print(f"  comment: failed ({e}) — will retry on next page load")

    return yt_video_id


def publish_video_now(slug: str, video_id: int) -> str:
    """
    Upload immediately as a public video. This is used by the app-managed scheduler
    when a locally queued video reaches its scheduled_for time.
    """
    print(f"[{slug}] Authenticating with YouTube...")
    service = authenticate(slug)
    set_youtube_status(video_id, "uploading", None)

    print(f"[{slug}] Publishing video {video_id} now...")
    yt_video_id = upload_video(slug, video_id, service, publish_at=None)

    thumbnail_path = resolve_thumbnail_path(slug, video_id)
    if thumbnail_path and thumbnail_path.exists():
        print(f"[{slug}] Uploading thumbnail...")
        upload_thumbnail(yt_video_id, thumbnail_path, service)

    config = _load_channel_config(slug)
    comment = _build_comment(random.choice(config.cta.pinned_comment_templates), video_id)
    print(f"[{slug}] Posting comment...")
    try:
        post_comment(yt_video_id, comment, service)
        from database.queries import mark_comment_posted

        mark_comment_posted(video_id)
    except Exception as e:
        print(f"  comment: failed ({e}) — will retry on refresh")

    set_youtube_status(video_id, "posted", None)
    return yt_video_id


def _daily_budget(channel_slug: str) -> int:
    """Return max videos/day for this channel based on its age."""
    from datetime import date as _date
    channel = get_channel(channel_slug)
    created_at = channel.get("created_at") if channel else None
    if not created_at:
        return 1
    try:
        days_live = (_date.today() - _date.fromisoformat(created_at[:10])).days
    except (ValueError, TypeError):
        return 1
    if days_live < 7:
        return 1
    elif days_live < 28:
        return 2
    else:
        return 3


def publish_due_queued_videos(slug: str | None = None) -> dict[str, int]:
    published = 0
    skipped = 0
    failed = 0
    details: list[dict] = []
    now = datetime.now(timezone.utc)

    # Track how many we've published this run per channel (on top of what's already in DB)
    posted_today: dict[str, int] = {}

    if slug:
        videos = get_channel_videos(slug)
    else:
        videos = []
        for channel in get_live_channels():
            videos.extend(get_channel_videos(channel["slug"]))

    for video in videos:
        ch = video["channel_slug"]

        if not video.get("final_video_path"):
            skipped += 1
            details.append({"video_id": video["id"], "channel_slug": ch, "status": "skipped", "reason": "missing_video"})
            continue
        if video.get("youtube_video_id") or video.get("youtube_status") in {"posted", "uploading", "error", "scheduled"}:
            skipped += 1
            details.append({"video_id": video["id"], "channel_slug": ch, "status": "skipped", "reason": "already_posted"})
            continue
        scheduled_for = video.get("scheduled_for")
        if not scheduled_for:
            skipped += 1
            details.append({"video_id": video["id"], "channel_slug": ch, "status": "skipped", "reason": "missing_schedule"})
            continue
        try:
            slot_dt = datetime.fromisoformat(scheduled_for)
            if slot_dt.tzinfo is None:
                slot_dt = slot_dt.replace(tzinfo=ET)
            slot_utc = slot_dt.astimezone(timezone.utc)
        except Exception:
            skipped += 1
            details.append({"video_id": video["id"], "channel_slug": ch, "status": "skipped", "reason": "bad_schedule"})
            continue
        if slot_utc > now:
            skipped += 1
            details.append({"video_id": video["id"], "channel_slug": ch, "status": "skipped", "reason": "not_due"})
            continue

        # Daily budget check — count already posted today + published this run
        if ch not in posted_today:
            posted_today[ch] = count_posted_today(ch)
        budget = _daily_budget(ch)
        if posted_today[ch] >= budget:
            skipped += 1
            details.append({"video_id": video["id"], "channel_slug": ch, "status": "skipped", "reason": f"daily_budget_reached ({posted_today[ch]}/{budget})"})
            continue

        try:
            publish_video_now(ch, video["id"])
            published += 1
            posted_today[ch] += 1
            details.append({"video_id": video["id"], "channel_slug": ch, "status": "published"})
        except Exception as exc:
            failed += 1
            set_youtube_status(video["id"], "error", str(exc)[:500])
            details.append({"video_id": video["id"], "channel_slug": ch, "status": "failed", "reason": str(exc)[:500]})

    return {"published": published, "skipped": skipped, "failed": failed, "details": details}
