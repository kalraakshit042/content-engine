"""
web_dashboard.py

FastAPI app — serves the dashboard UI and handles all routes.
Run with: uvicorn web_dashboard:app --reload
"""

import re
import os
import threading
from datetime import datetime
from pathlib import Path
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

from fastapi import FastAPI, BackgroundTasks, Request, Form
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from database.setup import setup_channels_db, setup_videos_db, setup_costs_db, setup_ops_db
from post_storage_migration import migrate_post_storage
from database.queries import (
    insert_channel,
    get_channel,
    get_all_channels,
    get_channel_rollups,
    set_channel_status,
    set_art_status,
    update_channel_links,
    get_channel_checklist,
    update_channel_checklist,
    delete_channel,
    insert_video,
    get_channel_videos,
    set_video_status,
    set_video_scheduled_for,
    set_tiktok_status,
    set_video_tiktok_url,
    get_channel_costs,
    get_channel_service_costs,
    get_channel_service_total_usd,
    get_video_service_costs,
    get_total_cost_usd,
    get_global_stats,
    get_latest_cron_run,
    get_recent_cron_runs,
    get_recent_cron_events,
    approve_preview_video,
)
from layer1_account_setup.config_generator import generate_channel_config
from layer1_account_setup.channel_art_generator import generate_channel_art
from layer1_account_setup.music_setup import ensure_music_dirs, validate_music_folders, music_setup_complete
from layer1_account_setup.music_downloader import download_all_moods
from layer2_script_generation.script_generator import generate_script
from layer3_audio_production.audio_generator import generate_audio
from layer4_video_production.video_assembler import assemble_video
from content_paths import resolve_final_video_path, resolve_script_json_path
from layer5_publishing.youtube_uploader import publish_video
from layer5_publishing.tiktok_uploader import (
    publish_video_safe as publish_tiktok_video,
    refresh_channel_stats as refresh_tiktok_channel_stats,
    schedule_pending_videos as schedule_pending_tiktok_videos,
)

load_dotenv(override=True)

BASE_DIR = Path(__file__).parent
CHANNELS_DIR = BASE_DIR / "channels"
CREDENTIALS_DIR = BASE_DIR / "credentials"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure DBs exist on startup
    setup_channels_db()
    setup_videos_db()
    setup_costs_db()
    setup_ops_db()
    migrate_post_storage()
    yield


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/channel-assets", StaticFiles(directory="channels"), name="channels")

templates = Jinja2Templates(directory="templates")
ET = ZoneInfo("America/New_York")
CHANNEL_SETUP_TASKS = [
    {
        "key": "yt_handle",
        "platform": "YouTube",
        "title": "Set channel handle and display name",
        "detail": "Make the channel searchable and brand-consistent in YouTube Studio.",
    },
    {
        "key": "yt_description",
        "platform": "YouTube",
        "title": "Write channel description",
        "detail": "Add a niche-specific 2-3 sentence description with search keywords.",
    },
    {
        "key": "yt_branding",
        "platform": "YouTube",
        "title": "Upload YouTube profile photo and banner",
        "detail": "Use the generated channel art in Studio → Customization → Branding.",
    },
    {
        "key": "yt_layout",
        "platform": "YouTube",
        "title": "Set channel layout",
        "detail": "Make Shorts visible and ensure the Home tab looks intentional.",
    },
    {
        "key": "yt_defaults",
        "platform": "YouTube",
        "title": "Configure upload defaults",
        "detail": "Set category, language, and comment defaults in YouTube Studio.",
    },
    {
        "key": "yt_verify",
        "platform": "YouTube",
        "title": "Phone-verify YouTube account",
        "detail": "Required for custom thumbnails and other account features.",
    },
    {
        "key": "yt_watermark",
        "platform": "YouTube",
        "title": "Add YouTube watermark",
        "detail": "Use your profile picture as the branding watermark in Studio.",
    },
    {
        "key": "tt_username",
        "platform": "TikTok",
        "title": "Set TikTok username and display name",
        "detail": "Claim the clean @handle and make the profile name match the brand.",
    },
    {
        "key": "tt_bio",
        "platform": "TikTok",
        "title": "Write TikTok bio",
        "detail": "Add a short bio that clearly says what the channel posts.",
    },
    {
        "key": "tt_profile_photo",
        "platform": "TikTok",
        "title": "Upload TikTok profile picture",
        "detail": "Use a clean square image that still reads on mobile.",
    },
    {
        "key": "tt_link_dashboard",
        "platform": "TikTok",
        "title": "Link TikTok in dashboard",
        "detail": "Save the TikTok username and URL in Platform Links above.",
    },
    {
        "key": "tt_cookies",
        "platform": "TikTok",
        "title": "Save TikTok cookies file",
        "detail": "Export cookies for this account and save them to the expected credentials path.",
    },
]


def format_et(value) -> str:
    if not value:
        return "—"
    try:
        if isinstance(value, datetime):
            dt = value
        else:
            text = str(value).strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(text)
            except ValueError:
                dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(ET).strftime("%Y-%m-%d %I:%M:%S %p ET")
    except Exception:
        return str(value)


templates.env.filters["format_et"] = format_et


def build_setup_checklist(slug: str, platform: str | None = None) -> list[dict]:
    saved = get_channel_checklist(slug)
    items = []
    for idx, task in enumerate(CHANNEL_SETUP_TASKS):
        if platform and task["platform"] != platform:
            continue
        checked = bool(saved.get(task["key"], False))
        items.append({**task, "checked": checked, "sort_key": (1 if checked else 0, idx)})
    items.sort(key=lambda item: item["sort_key"])
    return items


def slugify(name: str) -> str:
    """Convert channel name to URL-safe slug. e.g. 'Villain Monologues' → 'villain-monologues'"""
    slug = name.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


# ── Background task ────────────────────────────────────────────────────────────

def run_account_setup(slug: str, name: str, description: str, publish_slots: list = None):
    """Runs creative director + channel art generation. Executed in background."""
    import json as _json
    config_path = BASE_DIR / "channels" / slug / "channel_config.json"

    try:
        set_channel_status(slug, "configuring")

        if config_path.exists():
            from layer1_account_setup.config_schema import ChannelConfig
            config_data = _json.loads(config_path.read_text())
            if publish_slots:
                config_data["publish_slots"] = publish_slots
            config = ChannelConfig(**config_data)
        else:
            config = generate_channel_config(slug, name, description)
            if publish_slots:
                config_data = _json.loads(config_path.read_text())
                config_data["publish_slots"] = publish_slots
                config_path.write_text(_json.dumps(config_data, indent=2))

        # Download one CC0 track per mood from Freesound — raises on any failure
        import json as _json
        moods_list = _json.loads(config.json())["music_moods"]
        download_all_moods(slug, moods_list)
        set_channel_status(slug, "active")
    except Exception as e:
        set_channel_status(slug, "error", error_msg=str(e))
        return

    # Channel art is non-critical — failure doesn't block the channel going live
    try:
        set_art_status(slug, "generating")
        result = generate_channel_art(slug, config)
        if result["profile_pic"] == "ok" and result["banner"] == "ok":
            set_art_status(slug, "done")
        else:
            set_art_status(slug, "partial")
    except Exception:
        set_art_status(slug, "failed")


# ── Page 1: Main Dashboard ─────────────────────────────────────────────────────

@app.get("/")
async def dashboard(request: Request):
    channels = get_all_channels()
    channel_rollups = get_channel_rollups()
    for channel in channels:
        rollup = channel_rollups.get(channel["slug"], {})
        channel["claude_cost_usd"] = rollup.get("claude_cost_usd", 0.0)
        channel["youtube_likes"] = rollup.get("youtube_likes", 0)
        channel["youtube_comments"] = rollup.get("youtube_comments", 0)
        channel["tiktok_likes"] = rollup.get("tiktok_likes", 0)
        channel["tiktok_comments"] = rollup.get("tiktok_comments", 0)
        channel["total_likes"] = rollup.get("total_likes", 0)
        channel["total_comments"] = rollup.get("total_comments", 0)
    stats = get_global_stats()
    latest_cron_run = get_latest_cron_run()
    recent_cron_runs = get_recent_cron_runs()
    recent_cron_events = get_recent_cron_events()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "channels": channels,
        "stats": stats,
        "latest_cron_run": latest_cron_run,
        "recent_cron_runs": recent_cron_runs,
        "recent_cron_events": recent_cron_events,
    })


@app.post("/channels")
async def create_channel(
    background_tasks: BackgroundTasks,
    name: str = Form(...),
    description: str = Form(...),
    slot_1: str = Form("09:00"),
    slot_2: str = Form("17:00"),
):
    name = name.strip()
    description = description.strip()
    if not name or not description:
        return RedirectResponse("/?error=missing_fields", status_code=303)

    slug = slugify(name)

    # Check for duplicate
    existing = get_channel(slug)
    if existing:
        return RedirectResponse(f"/?error=duplicate&slug={slug}", status_code=303)

    publish_slots = [slot_1.strip(), slot_2.strip()]
    insert_channel(name, slug, description)
    background_tasks.add_task(run_account_setup, slug, name, description, publish_slots=publish_slots)

    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.post("/channels/{slug}/retry")
async def retry_channel(slug: str, background_tasks: BackgroundTasks):
    """Retry creative director if previous attempt errored."""
    channel = get_channel(slug)
    if not channel:
        return RedirectResponse("/", status_code=303)
    set_channel_status(slug, "pending")
    background_tasks.add_task(run_account_setup, slug, channel["name"], channel["description"])
    return RedirectResponse("/", status_code=303)


@app.post("/channels/{slug}/delete")
async def remove_channel(slug: str):
    delete_channel(slug)
    return RedirectResponse("/", status_code=303)


# ── Page 2: Channel Detail ─────────────────────────────────────────────────────

@app.get("/channel/{slug}")
async def channel_detail(request: Request, slug: str):
    channel = get_channel(slug)
    if not channel:
        return RedirectResponse("/", status_code=303)

    videos = get_channel_videos(slug)
    costs = get_channel_costs(slug)
    total_cost = get_total_cost_usd(slug)
    claude_costs = get_channel_service_costs(slug, "claude")
    total_claude_cost = get_channel_service_total_usd(slug, "claude")
    config_claude_cost = round(sum((entry.get("cost_usd") or 0) for entry in claude_costs if not entry.get("video_id")), 4)
    video_claude_cost = round(sum((entry.get("cost_usd") or 0) for entry in claude_costs if entry.get("video_id")), 4)
    video_claude_costs = get_video_service_costs(slug, "claude")
    channel_total_likes = sum((v.get("youtube_likes") or 0) + (v.get("tiktok_likes") or 0) for v in videos)
    channel_total_comments = sum((v.get("youtube_comments") or 0) + (v.get("tiktok_comments") or 0) for v in videos)

    # Check if channel art exists
    has_profile_pic = (CHANNELS_DIR / slug / "profile_pic.png").exists()
    has_banner = (CHANNELS_DIR / slug / "banner.png").exists()

    # Load config for display
    import json
    config_path = CHANNELS_DIR / slug / "channel_config.json"
    channel_config = json.loads(config_path.read_text()) if config_path.exists() else None
    tiktok_cookies_path = CREDENTIALS_DIR / f"{slug}_tiktok_cookies.txt"
    has_youtube_token = (CREDENTIALS_DIR / f"{slug}_token.json").exists()
    youtube_setup_items = build_setup_checklist(slug, platform="YouTube")
    tiktok_setup_items = build_setup_checklist(slug, platform="TikTok")

    music_status_data = validate_music_folders(slug) if channel["status"] == "pending_music" else {}

    # Auto-refresh YouTube stats on page load (non-blocking)
    try:
        from layer5_publishing.youtube_uploader import refresh_channel_stats
        refresh_channel_stats(slug)
        videos = get_channel_videos(slug)  # re-fetch with updated stats
    except Exception:
        pass

    return templates.TemplateResponse("channel_detail.html", {
        "request": request,
        "channel": channel,
        "videos": videos,
        "costs": costs,
        "total_cost": total_cost,
        "claude_costs": claude_costs,
        "total_claude_cost": total_claude_cost,
        "config_claude_cost": config_claude_cost,
        "video_claude_cost": video_claude_cost,
        "video_claude_costs": video_claude_costs,
        "channel_total_likes": channel_total_likes,
        "channel_total_comments": channel_total_comments,
        "has_profile_pic": has_profile_pic,
        "has_banner": has_banner,
        "channel_config": channel_config,
        "music_status": music_status_data,
        "music_error": request.query_params.get("music_error"),
        "tiktok_cookies_path": str(tiktok_cookies_path),
        "has_tiktok_cookies": tiktok_cookies_path.exists(),
        "has_youtube_token": has_youtube_token,
        "youtube_setup_items": youtube_setup_items,
        "tiktok_setup_items": tiktok_setup_items,
    })


@app.post("/channel/{slug}/update")
async def update_channel(
    slug: str,
    youtube_channel_url: str = Form(""),
    youtube_channel_id: str = Form(""),
    tiktok_username: str = Form(""),
    tiktok_channel_url: str = Form(""),
):
    update_channel_links(
        slug=slug,
        is_live=1,
        youtube_channel_url=youtube_channel_url.strip(),
        youtube_channel_id=youtube_channel_id.strip(),
        tiktok_username=tiktok_username.strip(),
        tiktok_channel_url=tiktok_channel_url.strip(),
    )
    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.post("/channel/{slug}/checklist")
async def update_channel_checklist_route(
    slug: str,
    key: str = Form(...),
    checked: str = Form(...),
):
    channel = get_channel(slug)
    if not channel:
        return JSONResponse({"ok": False, "error": "Channel not found"}, status_code=404)

    valid_keys = {item["key"] for item in CHANNEL_SETUP_TASKS}
    if key not in valid_keys:
        return JSONResponse({"ok": False, "error": "Invalid checklist key"}, status_code=400)

    state = get_channel_checklist(slug)
    state[key] = checked.lower() == "true"
    update_channel_checklist(slug, state)

    items = build_setup_checklist(slug)
    done = sum(1 for item in items if item["checked"])
    return JSONResponse({"ok": True, "done": done, "total": len(items)})


@app.post("/channel/{slug}/regenerate-art")
async def regenerate_art(slug: str, background_tasks: BackgroundTasks):
    """Re-run channel art generation (skips already-generated images)."""
    channel = get_channel(slug)
    if not channel:
        return RedirectResponse("/", status_code=303)

    def _run_art():
        import json
        from layer1_account_setup.config_schema import ChannelConfig
        config_path = CHANNELS_DIR / slug / "channel_config.json"
        if not config_path.exists():
            set_art_status(slug, "failed")
            return
        try:
            set_art_status(slug, "generating")
            config = ChannelConfig(**json.loads(config_path.read_text()))
            result = generate_channel_art(slug, config)
            if result["profile_pic"] == "ok" and result["banner"] == "ok":
                set_art_status(slug, "done")
            elif result["profile_pic"] == "ok" or result["banner"] == "ok":
                set_art_status(slug, "partial")
            else:
                set_art_status(slug, "failed")
        except Exception:
            set_art_status(slug, "failed")

    background_tasks.add_task(_run_art)
    return RedirectResponse(f"/channel/{slug}", status_code=303)


# ── Music setup ───────────────────────────────────────────────────────────────

@app.get("/api/channel/{slug}/music-status")
async def music_status(slug: str):
    """Polled by dashboard JS every 5s during pending_music state."""
    moods = validate_music_folders(slug)
    complete = music_setup_complete(slug)
    return JSONResponse({"complete": complete, "moods": moods})


@app.post("/channel/{slug}/activate")
async def activate_channel(slug: str):
    """Validate all music folders are populated then mark channel active."""
    channel = get_channel(slug)
    if not channel or channel["status"] != "pending_music":
        return RedirectResponse(f"/channel/{slug}", status_code=303)
    if not music_setup_complete(slug):
        return RedirectResponse(f"/channel/{slug}?music_error=1", status_code=303)
    set_channel_status(slug, "active")
    return RedirectResponse(f"/channel/{slug}", status_code=303)


# ── Script generation ─────────────────────────────────────────────────────────

def run_script_generation(slug: str, video_id: int):
    """Generates a script in the background. Updates video status on success/failure."""
    try:
        generate_script(slug, video_id)
    except Exception as e:
        set_video_status(video_id, "error")
        print(f"[script_gen] Error for video {video_id}: {e}")


def run_full_pipeline(slug: str, video_id: int):
    """Runs the full pipeline (script → audio → video → upload) in the background."""
    try:
        # Reserve publish slot immediately so it's booked before generation takes time
        from layer5_publishing.youtube_uploader import next_publish_slot
        slot = next_publish_slot(slug)
        set_video_scheduled_for(video_id, slot.isoformat())

        set_video_status(video_id, "generating_script")
        generate_script(slug, video_id)
        set_video_status(video_id, "generating_audio")
        generate_audio(slug, video_id)
        set_video_status(video_id, "generating_video")
        assemble_video(slug, video_id)
        set_video_status(video_id, "uploading")
        publish_video(slug, video_id, slot=slot)
    except Exception as e:
        set_video_status(video_id, "error")
        set_video_scheduled_for(video_id, None)   # free the slot so next run can book it
        print(f"[pipeline] Error for video {video_id}: {e}")


def run_tiktok_publish(slug: str, video_id: int):
    """Upload an already-rendered video to TikTok in the background."""
    try:
        publish_tiktok_video(slug, video_id)
    except Exception as e:
        print(f"[tiktok] Error for video {video_id}: {e}")


def run_tiktok_batch_schedule(slug: str):
    """Queue all eligible rendered videos for app-managed TikTok publishing."""
    try:
        result = schedule_pending_tiktok_videos(slug)
        print(f"[tiktok batch] {slug}: {result}")
    except Exception as e:
        print(f"[tiktok batch] Error for {slug}: {e}")


def launch_detached_task(fn, *args) -> None:
    """Run blocking TikTok automation off the FastAPI event loop."""
    thread = threading.Thread(target=fn, args=args, daemon=True)
    thread.start()


@app.post("/channel/{slug}/generate-video")
async def generate_video_route(slug: str, background_tasks: BackgroundTasks):
    channel = get_channel(slug)
    if not channel or channel["status"] != "active":
        return RedirectResponse(f"/channel/{slug}", status_code=303)

    video_id = insert_video(slug)
    background_tasks.add_task(run_full_pipeline, slug, video_id)
    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.get("/channel/{slug}/youtube-auth")
async def youtube_auth_route(slug: str):
    from layer5_publishing.youtube_uploader import authenticate
    authenticate(slug)  # opens browser, saves token
    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.get("/api/channel/{slug}/youtube-health")
async def youtube_health_route(slug: str):
    from layer5_publishing.youtube_uploader import check_token_health
    return JSONResponse(check_token_health(slug))


@app.get("/channel/{slug}/video/{video_id}/reveal")
async def reveal_in_finder(slug: str, video_id: int):
    from database.queries import get_video
    import subprocess
    video = get_video(video_id)
    if video and video.get("final_video_path"):
        subprocess.Popen(["open", "-R", video["final_video_path"]])
    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.post("/channel/{slug}/video/{video_id}/retry")
async def retry_video_route(slug: str, video_id: int, background_tasks: BackgroundTasks):
    from database.queries import get_video
    video = get_video(video_id)
    if not video or video["channel_slug"] != slug:
        return RedirectResponse(f"/channel/{slug}", status_code=303)
    set_video_status(video_id, "generating_script")
    background_tasks.add_task(run_full_pipeline, slug, video_id)
    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.post("/channel/{slug}/generate-script")
async def generate_script_route(slug: str, background_tasks: BackgroundTasks):
    channel = get_channel(slug)
    if not channel or channel["status"] != "active":
        return RedirectResponse(f"/channel/{slug}", status_code=303)

    video_id = insert_video(slug)
    background_tasks.add_task(run_script_generation, slug, video_id)
    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.post("/channel/{slug}/video/{video_id}/publish-tiktok")
async def publish_tiktok_route(slug: str, video_id: int, background_tasks: BackgroundTasks):
    channel = get_channel(slug)
    if not channel:
        return RedirectResponse("/", status_code=303)

    videos = get_channel_videos(slug)
    video = next((v for v in videos if v["id"] == video_id), None)
    if not video or not video.get("final_video_path"):
        return RedirectResponse(f"/channel/{slug}", status_code=303)

    set_tiktok_status(video_id, "queued", None)
    launch_detached_task(run_tiktok_publish, slug, video_id)
    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.post("/channel/{slug}/publish-tiktok")
async def publish_tiktok_batch_route(slug: str, background_tasks: BackgroundTasks):
    channel = get_channel(slug)
    if not channel:
        return RedirectResponse("/", status_code=303)

    launch_detached_task(run_tiktok_batch_schedule, slug)
    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.post("/channel/{slug}/video/{video_id}/set-tiktok-url")
async def set_tiktok_url_route(slug: str, video_id: int, tiktok_url: str = Form("")):
    clean = tiktok_url.strip()
    if clean:
        set_video_tiktok_url(video_id, clean)
        set_tiktok_status(video_id, "posted", None)
    return RedirectResponse(f"/channel/{slug}", status_code=303)


@app.post("/channel/{slug}/video/{video_id}/approve")
async def approve_video_route(slug: str, video_id: int):
    """Approve a preview-held video for upload — flips youtube_status + tiktok_status to 'queued'."""
    approve_preview_video(video_id)
    return RedirectResponse(f"/channel/{slug}", status_code=303)


# ── API: status polling ────────────────────────────────────────────────────────

@app.get("/api/channel/{slug}/status")
async def channel_status(slug: str):
    """Polled by dashboard JS every 5s to update status badges without full page reload."""
    channel = get_channel(slug)
    if not channel:
        return JSONResponse({"status": "not_found"}, status_code=404)
    return JSONResponse({
        "status": channel["status"],
        "error_msg": channel["error_msg"],
    })


@app.get("/api/channel/{slug}/videos")
async def channel_videos_api(slug: str):
    """Returns current video list for the channel — polled by JS when a script is generating."""
    videos = get_channel_videos(slug)
    return JSONResponse([
        {
            "id": v["id"],
            "title": v["title"],
            "subject": v["subject"],
            "tone_used": v["tone_used"],
            "status": v["status"],
            "youtube_status": v.get("youtube_status"),
            "youtube_url": v.get("youtube_url"),
            "youtube_error": v.get("youtube_error"),
            "youtube_views": v.get("youtube_views"),
            "youtube_comments": v.get("youtube_comments"),
            "youtube_likes": v.get("youtube_likes"),
            "tiktok_status": v.get("tiktok_status"),
            "tiktok_posted": v.get("tiktok_posted"),
            "tiktok_url": v.get("tiktok_url"),
            "tiktok_error": v.get("tiktok_error"),
            "tiktok_views": v.get("tiktok_views"),
            "tiktok_comments": v.get("tiktok_comments"),
            "tiktok_likes": v.get("tiktok_likes"),
            "created_at": v["created_at"],
            "scheduled_for": v["scheduled_for"],
        }
        for v in videos
    ])


@app.get("/api/channel/{slug}/video/{video_id}/script")
async def get_script(slug: str, video_id: int):
    """Returns the full script JSON for inline display."""
    import json as _json
    script_path = resolve_script_json_path(slug, video_id)
    if not script_path.exists():
        return JSONResponse({"error": "Script not found"}, status_code=404)
    return JSONResponse(_json.loads(script_path.read_text()))


@app.get("/channel/{slug}/video/{video_id}/preview")
async def preview_video_route(slug: str, video_id: int):
    video_path = resolve_final_video_path(slug, video_id)
    if not video_path.exists():
        return RedirectResponse(f"/channel/{slug}", status_code=303)
    return FileResponse(video_path, media_type="video/mp4")


@app.post("/channel/{slug}/refresh-stats")
async def refresh_stats_route(slug: str):
    try:
        from layer5_publishing.youtube_uploader import refresh_channel_stats
        refresh_channel_stats(slug)
    except Exception as e:
        print(f"[stats] {e}")
    try:
        refresh_tiktok_channel_stats(slug)
    except Exception as e:
        print(f"[tiktok stats] {e}")
    return RedirectResponse(f"/channel/{slug}", status_code=303)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web_dashboard:app", host="0.0.0.0", port=8000, reload=True)
