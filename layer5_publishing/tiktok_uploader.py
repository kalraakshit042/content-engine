"""
layer5_publishing/tiktok_uploader.py

Proof-of-concept TikTok publisher using the community `tiktok-uploader`
package. This is intentionally isolated from the rest of the app so we can
swap it for the official TikTok API later.

Auth model for the POC:
  - one cookies file per channel at credentials/{slug}_tiktok_cookies.txt
  - optional headed mode for first-time debugging

This module does not touch the main `videos.status` field. TikTok progress is
tracked separately in `tiktok_status` / `tiktok_error`.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from zoneinfo import ZoneInfo

from content_paths import (
    resolve_final_video_path,
    resolve_script_json_path,
    resolve_thumbnail_path,
)
from database.queries import (
    get_channel,
    get_channel_videos,
    get_tiktok_scheduled_slots,
    get_video,
    set_tiktok_status,
    set_video_tiktok_scheduled_for,
    update_tiktok_stats,
    update_video_tiktok,
)

from layer1_account_setup.config_schema import ChannelConfig

load_dotenv(override=True)

BASE_DIR = Path(__file__).parent.parent
CHANNELS_DIR = BASE_DIR / "channels"
CREDENTIALS_DIR = BASE_DIR / "credentials"
LOGS_DIR = BASE_DIR / "logs"
ET = ZoneInfo("America/New_York")


def _load_channel_config(slug: str) -> ChannelConfig:
    config_path = CHANNELS_DIR / slug / "channel_config.json"
    return ChannelConfig(**json.loads(config_path.read_text()))


def next_tiktok_publish_slot(slug: str) -> datetime:
    """
    Returns the next available TikTok publish slot (ET-aware datetime).
    Reads tiktok_publish_slots from channel_config.json (HH:MM ET, 24h).
    Falls back to ["18:00"] if not configured.
    Skips slots already occupied by a tiktok_scheduled_for value.
    """
    occupied = set(get_tiktok_scheduled_slots(slug))

    try:
        config = _load_channel_config(slug)
        raw_slots = config.tiktok_publish_slots or []
        slots = []
        for s in raw_slots:
            h, m = map(int, s.split(":"))
            slots.append((h, m))
    except Exception:
        slots = []

    if not slots:
        slots = [(18, 0)]

    slots = sorted(slots)
    now_et = datetime.now(ET)
    search_from = now_et + timedelta(minutes=5)

    for day_offset in range(30):
        candidate_date = (now_et + timedelta(days=day_offset)).date()
        for hour, minute in slots:
            slot_et = datetime(
                candidate_date.year, candidate_date.month, candidate_date.day,
                hour, minute, 0, tzinfo=ET,
            )
            if slot_et <= search_from:
                continue
            slot_iso = slot_et.isoformat()
            if slot_iso not in occupied:
                return slot_et

    raise RuntimeError(f"No available TikTok publish slot found for {slug} in next 30 days")


def _cookies_path(slug: str) -> Path:
    return CREDENTIALS_DIR / f"{slug}_tiktok_cookies.txt"


def _thumbnail_path(slug: str, video_id: int) -> Optional[Path]:
    path = resolve_thumbnail_path(slug, video_id)
    return path if path and path.exists() else None


def _script_data(slug: str, video_id: int) -> dict:
    path = resolve_script_json_path(slug, video_id)
    if not path.exists():
        raise FileNotFoundError(f"Script JSON not found: {path}")
    return json.loads(path.read_text())


def _build_caption(script_data: dict) -> str:
    tiktok_hook = (script_data.get("tiktok_hook") or "").strip()
    description = (script_data.get("description") or "").strip()
    hashtags = script_data.get("hashtags") or []

    tags = []
    for tag in hashtags:
        clean = tag.strip()
        if not clean:
            continue
        if not clean.startswith("#"):
            clean = f"#{clean}"
        tags.append(clean)

    # Fall back to title for old script.json files that predate tiktok_hook
    if not tiktok_hook:
        tiktok_hook = (script_data.get("title") or "").strip()

    parts = [p for p in [tiktok_hook, description] if p]
    if tags:
        parts.append(" ".join(tags[:5]))
    return "\n\n".join(parts).strip()[:2200]


def _slugify_filename(text: str) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "tiktok-upload"


def _normalize_tiktok_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    match = re.search(r"(https://www\.tiktok\.com/@[^/]+/video/\d+)", url)
    if match:
        return match.group(1)
    return url


def _extract_post_url_from_page(page, username: str = "") -> str:
    candidates = []

    if page.url:
        candidates.append(page.url)

    try:
        html = page.content()
    except Exception:
        html = ""
    if html:
        candidates.extend(
            re.findall(r"https://www\.tiktok\.com/@[^\"'<> ]+/video/\d+", html)
        )
        candidates.extend(
            re.findall(r"/@[^\"'<> ]+/video/\d+", html)
        )

    try:
        anchors = page.locator('a[href*="/video/"]')
        for i in range(min(anchors.count(), 10)):
            href = anchors.nth(i).get_attribute("href")
            if href:
                candidates.append(href)
    except Exception:
        pass

    for candidate in candidates:
        if not candidate:
            continue
        if candidate.startswith("/"):
            candidate = f"https://www.tiktok.com{candidate}"
        normalized = _normalize_tiktok_url(candidate)
        if not normalized:
            continue
        if username and f"@{username}/video/" not in normalized:
            continue
        return normalized
    return ""


def _scheduled_datetime(scheduled_for: str | None) -> Optional[datetime]:
    if not scheduled_for:
        return None
    try:
        dt = datetime.fromisoformat(scheduled_for)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _validate_tiktok_schedule(slot: Optional[datetime]) -> Optional[datetime]:
    if slot is None:
        return None
    now = datetime.now(timezone.utc)
    if slot < now + timedelta(minutes=20):
        return None
    if slot > now + timedelta(days=10):
        return None
    return slot


def _schedule_for_uploader(scheduled_for: str | None) -> Optional[datetime]:
    """
    The community uploader currently expects a naive datetime and internally
    converts it using the machine's local timezone. Our scheduled_for values are
    stored with ET offsets already, so we strip tzinfo and preserve wall time.
    """
    if not scheduled_for:
        return None
    try:
        dt = datetime.fromisoformat(scheduled_for)
    except Exception:
        return None
    return dt.replace(tzinfo=None)


def _prepare_temp_upload_file(video_path: Path, title: str) -> tuple[Path, bool]:
    """
    TikTok's web uploader appears to leak the source filename into the caption
    field. Uploading a temp copy with a human-readable title avoids `10title`
    style prefixes from numbered source files like `10.mp4`.
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="tiktok-upload-"))
    temp_path = temp_dir / f"video{video_path.suffix.lower()}"
    shutil.copy2(video_path, temp_path)
    return temp_path, True


def _cleanup_temp_upload_file(temp_path: Path, should_cleanup: bool) -> None:
    if not should_cleanup:
        return
    try:
        temp_path.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        temp_path.parent.rmdir()
    except Exception:
        pass


def _load_uploader():
    try:
        import tiktok_uploader.upload as upload_module
        from tiktok_uploader.upload import TikTokUploader
    except ImportError as exc:
        raise RuntimeError(
            "TikTok uploader package is not installed. Run `pip install -r requirements.txt` "
            "and `playwright install` before testing TikTok publishing."
        ) from exc
    # TikTok's scheduling UI is often slow to render after processing finishes.
    # Give the community uploader more room before it gives up on schedule controls.
    upload_module.config.explicit_wait = max(upload_module.config.explicit_wait, 120)

    def _dismiss_tiktok_overlays(page) -> None:
        try:
            got_it = page.get_by_role("button", name="Got it")
            if got_it.count():
                print("[tiktok ui] dismissing tutorial via 'Got it' button", flush=True)
                got_it.first.click(timeout=2000)
                page.wait_for_timeout(500)
        except Exception:
            pass

        try:
            overlay_count = page.locator("#react-joyride-portal .react-joyride__overlay").count()
            if overlay_count:
                print("[tiktok ui] removing joyride overlay", flush=True)
                page.evaluate(
                    """() => {
                        document.querySelectorAll('#react-joyride-portal, #react-joyride-step-0')
                          .forEach((el) => el.remove());
                    }"""
                )
                page.wait_for_timeout(300)
        except Exception:
            pass

    def _set_readonly_input_value(locator, value: str, label: str) -> str:
        handle = locator.element_handle()
        if handle is None:
            raise RuntimeError(f"Could not resolve {label} input handle")
        handle.scroll_into_view_if_needed()
        handle.click()
        handle.evaluate("(el) => el.removeAttribute('readonly')")
        handle.press("Control+A")
        handle.press("Backspace")
        handle.type(value, delay=20)
        handle.press("Enter")
        result = handle.evaluate("(el) => el.value || ''")
        handle.evaluate("(el) => { el.setAttribute('readonly', ''); el.blur(); }")
        print(f"[tiktok schedule] {label} input now={result}", flush=True)
        return result

    def _force_set_description(page, description: str) -> None:
        if description is None:
            return

        description = description.encode("utf-8", "ignore").decode("utf-8")
        desc_locator = page.locator(f"xpath={upload_module.config.selectors.upload.description}")
        try:
            _dismiss_tiktok_overlays(page)
            desc_locator.wait_for(state="visible", timeout=upload_module.config.implicit_wait * 1000)
            desc_locator.click()
            page.wait_for_timeout(500)

            # Clear via keyboard. TikTok pre-fills the editor with the filename slug and
            # holds it in React state — a single Ctrl+A + Backspace doesn't always remove it.
            # Do two passes to ensure React's state is fully cleared before typing.
            for _ in range(2):
                desc_locator.press("Control+A")
                page.wait_for_timeout(150)
                desc_locator.press("Delete")
                page.wait_for_timeout(150)
                desc_locator.press("Control+A")
                page.wait_for_timeout(150)
                desc_locator.press("Backspace")
                page.wait_for_timeout(300)

            # Type content line-by-line using press_sequentially (real keyboard events).
            # Use Enter keypress for newlines. Hashtags (#) trigger TikTok's autocomplete
            # dropdown — wait for it then dismiss with Enter.
            lines = description.split("\n")
            for line_idx, line in enumerate(lines):
                for word in line.split(" "):
                    if not word:
                        continue
                    if word.startswith("#"):
                        desc_locator.press_sequentially(word, delay=50)
                        page.wait_for_timeout(400)
                        mention_box = page.locator(f"xpath={upload_module.config.selectors.upload.mention_box}")
                        try:
                            mention_box.wait_for(state="visible", timeout=upload_module.config.add_hashtag_wait * 1000)
                            # Escape dismisses the autocomplete without selecting a suggestion,
                            # keeping exactly what we typed. Enter would select the first suggestion
                            # which may differ (e.g. #monologue → #monologuedeo residue).
                            desc_locator.press("Escape")
                            page.wait_for_timeout(100)
                        except Exception:
                            pass
                        desc_locator.press_sequentially(" ")
                    elif word.startswith("@"):
                        desc_locator.press_sequentially(word)
                        page.wait_for_timeout(1000)
                        mention_box_user_id = page.locator(
                            f"xpath={upload_module.config.selectors.upload.mention_box_user_id}"
                        )
                        try:
                            mention_box_user_id.first.wait_for(state="visible", timeout=5000)
                            found = False
                            user_ids = mention_box_user_id.all()
                            target_username = word[1:].lower()
                            for i, user_el in enumerate(user_ids):
                                if user_el.is_visible():
                                    text = user_el.inner_text().split(" ")[0]
                                    if text.lower() == target_username:
                                        found = True
                                        for _ in range(i):
                                            desc_locator.press("ArrowDown")
                                        desc_locator.press("Enter")
                                        break
                            if not found:
                                desc_locator.press_sequentially(" ")
                        except Exception:
                            desc_locator.press_sequentially(" ")
                    else:
                        desc_locator.press_sequentially(word + " ")

                if line_idx < len(lines) - 1:
                    desc_locator.press("Enter")

            page.wait_for_timeout(500)

            # Read back to verify (short timeout — non-blocking if element shifts)
            try:
                actual = desc_locator.inner_text(timeout=5000).strip()
                print(
                    f"[tiktok description] typed via keyboard\n"
                    f"  first 120 chars: {actual[:120]!r}",
                    flush=True,
                )
            except Exception:
                print("[tiktok description] could not read back editor content", flush=True)

        except Exception as exception:
            print(f"[tiktok description] failed: {exception}", flush=True)
            try:
                upload_module._clear(desc_locator)
                desc_locator.fill(description)
            except Exception:
                pass

    def _verbose_set_schedule_video(page, schedule):
        timezone_str = page.evaluate("Intl.DateTimeFormat().resolvedOptions().timeZone")
        driver_timezone = upload_module.pytz.timezone(timezone_str)
        localized = schedule.astimezone(driver_timezone)

        month = localized.month
        day = localized.day
        hour = localized.hour
        minute = localized.minute

        schedule_cfg = upload_module.config.selectors.schedule
        print(
            f"[tiktok schedule] target={localized.isoformat()} "
            f"switch={schedule_cfg.switch} date_picker={schedule_cfg.date_picker} "
            f"time_picker={schedule_cfg.time_picker}",
            flush=True,
        )

        try:
            _dismiss_tiktok_overlays(page)
            print("[tiktok schedule] waiting for schedule switch...", flush=True)
            # TikTok changed the old switch to a radio group: name=postSchedule.
            # Prefer the current UI, fall back to the legacy selector.
            radio = page.locator("input[name='postSchedule'][value='schedule']")
            if radio.count():
                radio.first.wait_for(state="attached", timeout=upload_module.config.explicit_wait * 1000)
                print("[tiktok schedule] clicking modern schedule radio", flush=True)
                page.evaluate(
                    """() => {
                        const el = document.querySelector("input[name='postSchedule'][value='schedule']");
                        if (el) el.click();
                    }"""
                )
            else:
                switch = page.locator(f"xpath={schedule_cfg.switch}")
                switch.wait_for(state="visible", timeout=upload_module.config.explicit_wait * 1000)
                print("[tiktok schedule] clicking legacy schedule switch", flush=True)
                switch.click()

            page.wait_for_timeout(1000)
            _dismiss_tiktok_overlays(page)
            schedule_inputs = page.locator("input.TUXTextInputCore-input[readonly]")
            input_count = schedule_inputs.count()
            current_values = [schedule_inputs.nth(i).input_value() for i in range(min(input_count, 4))]
            print(f"[tiktok schedule] current readonly inputs={current_values}", flush=True)

            if input_count >= 2:
                time_input = None
                date_input = None
                for i in range(input_count):
                    candidate = schedule_inputs.nth(i)
                    value = candidate.input_value()
                    if re.fullmatch(r"\d{2}:\d{2}", value or ""):
                        time_input = candidate
                    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""):
                        date_input = candidate

                if date_input is None or time_input is None:
                    raise RuntimeError(f"Could not identify modern schedule inputs from values={current_values}")

                target_date = localized.strftime("%Y-%m-%d")
                target_time = localized.strftime("%H:%M")
                print(f"[tiktok schedule] setting modern date input to {target_date}", flush=True)
                actual_date = _set_readonly_input_value(date_input, target_date, "date")
                print(f"[tiktok schedule] setting modern time input to {target_time}", flush=True)
                actual_time = _set_readonly_input_value(time_input, target_time, "time")
                if actual_date != target_date or actual_time != target_time:
                    raise RuntimeError(
                        f"Modern schedule inputs did not update to target values "
                        f"(date={actual_date}, time={actual_time})"
                    )
            else:
                print("[tiktok schedule] opening date picker", flush=True)
                upload_module.__date_picker(page, month, day)

                print("[tiktok schedule] opening time picker", flush=True)
                upload_module.__time_picker(page, hour, minute)
            _dismiss_tiktok_overlays(page)
            print("[tiktok schedule] schedule controls set successfully", flush=True)
        except Exception as e:
            LOGS_DIR.mkdir(exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            shot = LOGS_DIR / f"tiktok-schedule-fail-{stamp}.png"
            dump = LOGS_DIR / f"tiktok-schedule-fail-{stamp}.html"
            txt = LOGS_DIR / f"tiktok-schedule-fail-{stamp}.txt"
            try:
                page.screenshot(path=str(shot), full_page=True)
                dump.write_text(page.content())
                txt.write_text(page.locator("body").inner_text()[:12000])
                print(f"[tiktok schedule] saved screenshot: {shot}", flush=True)
                print(f"[tiktok schedule] saved html dump: {dump}", flush=True)
                print(f"[tiktok schedule] saved text dump: {txt}", flush=True)
            except Exception as diag_exc:
                print(f"[tiktok schedule] failed to save diagnostics: {diag_exc}", flush=True)
            print(f"[tiktok schedule] failed while handling schedule UI: {e}", flush=True)
            raise upload_module.FailedToUpload()

    # Wrap the cover setter to dismiss joyride overlays before clicking.
    # TikTok's tutorial overlay intercepts pointer events and causes the cover
    # click to time out if we haven't dismissed it yet (cover is set before description).
    _original_set_cover = upload_module._set_cover

    def _overlay_safe_set_cover(page, cover):
        _dismiss_tiktok_overlays(page)
        return _original_set_cover(page, cover)

    upload_module._set_cover = _overlay_safe_set_cover
    upload_module._set_description = _force_set_description
    upload_module._set_schedule_video = _verbose_set_schedule_video
    return TikTokUploader


def publish_video(slug: str, video_id: int) -> str:
    """
    Publish a finished local video to TikTok using browser-cookie auth.

    Returns a placeholder URL when upload succeeds. The community uploader does
    not reliably expose the final post URL, so we store the channel URL when
    available and use `tiktok_status='posted'` as the main success signal.
    """
    set_tiktok_status(video_id, "uploading", None)

    cookies_path = _cookies_path(slug)
    if not cookies_path.exists():
        raise FileNotFoundError(
            f"TikTok cookies file not found: {cookies_path}. "
            f"Save a cookies export there before publishing."
        )

    video_row = get_video(video_id)
    if not video_row or not video_row.get("final_video_path"):
        raise FileNotFoundError(f"Final video path not found for video {video_id}")

    video_path = resolve_final_video_path(slug, video_id)
    if not video_path.exists():
        raise FileNotFoundError(f"Video file not found: {video_path}")

    script_data = _script_data(slug, video_id)
    caption = _build_caption(script_data)
    thumbnail = _thumbnail_path(slug, video_id)

    TikTokUploader = _load_uploader()

    headless = os.environ.get("TIKTOK_HEADLESS", "0").strip() not in {"0", "false", "False"}
    browser = os.environ.get("TIKTOK_BROWSER", "chrome").strip() or "chrome"

    uploader = TikTokUploader(cookies=str(cookies_path), headless=headless, browser=browser)
    channel = get_channel(slug) or {}
    username = (channel.get("tiktok_username") or "").lstrip("@").strip()

    kwargs = {
        "description": caption,
        "comment": True,
        "stitch": True,
        "duet": True,
        "visibility": "everyone",
    }
    if thumbnail is not None:
        kwargs["cover"] = str(thumbnail)

    upload_path, cleanup_upload = _prepare_temp_upload_file(video_path, script_data.get("title") or video_row.get("title") or "")
    try:
        success = uploader.upload_video(str(upload_path), **kwargs)
        if not success:
            raise RuntimeError("TikTok uploader reported failure during publish")

        tiktok_url = _extract_post_url_from_page(uploader.page, username=username)
        if not tiktok_url:
            tiktok_url = channel.get("tiktok_channel_url") or video_row.get("tiktok_url") or ""
        update_video_tiktok(video_id, tiktok_url=tiktok_url, tiktok_status="posted", tiktok_error=None)

        # Verify what actually got posted.
        # Prefer the direct video URL if we extracted one — it's the most accurate.
        # Fall back to scraping the profile only if no direct URL is available.
        verify_url = _normalize_tiktok_url(tiktok_url) if tiktok_url else ""
        if verify_url:
            print(f"[tiktok verify] reading caption from posted video: {verify_url}", flush=True)
            _verify_posted_caption(uploader.page, username, video_url=verify_url)
        elif username:
            _verify_posted_caption(uploader.page, username)
    finally:
        uploader.close()
        _cleanup_temp_upload_file(upload_path, cleanup_upload)
    return tiktok_url


def schedule_video(slug: str, video_id: int, scheduled_for: str) -> None:
    set_tiktok_status(video_id, "uploading", None)

    cookies_path = _cookies_path(slug)
    if not cookies_path.exists():
        raise FileNotFoundError(
            f"TikTok cookies file not found: {cookies_path}. "
            f"Save a cookies export there before publishing."
        )

    video_row = get_video(video_id)
    if not video_row or not video_row.get("final_video_path"):
        raise FileNotFoundError(f"Final video path not found for video {video_id}")

    video_path = resolve_final_video_path(slug, video_id)
    if not video_path.exists():
        raise FileNotFoundError(f"Video file not found: {video_path}")

    slot_check = _validate_tiktok_schedule(_scheduled_datetime(scheduled_for))
    if slot_check is None:
        raise RuntimeError(
            "TikTok scheduling only supports times between 20 minutes and 10 days from now."
        )
    slot_for_uploader = _schedule_for_uploader(scheduled_for)
    if slot_for_uploader is None:
        raise RuntimeError("Could not parse scheduled_for timestamp for TikTok scheduling.")

    script_data = _script_data(slug, video_id)
    caption = _build_caption(script_data)
    thumbnail = _thumbnail_path(slug, video_id)
    TikTokUploader = _load_uploader()
    headless = os.environ.get("TIKTOK_HEADLESS", "0").strip() not in {"0", "false", "False"}
    browser = os.environ.get("TIKTOK_BROWSER", "chrome").strip() or "chrome"
    uploader = TikTokUploader(cookies=str(cookies_path), headless=headless, browser=browser)

    kwargs = {
        "description": caption,
        "comment": True,
        "stitch": True,
        "duet": True,
        "visibility": "everyone",
        "schedule": slot_for_uploader,
    }
    if thumbnail is not None:
        kwargs["cover"] = str(thumbnail)

    upload_path, cleanup_upload = _prepare_temp_upload_file(video_path, script_data.get("title") or video_row.get("title") or "")
    try:
        success = uploader.upload_video(str(upload_path), **kwargs)
        if not success:
            raise RuntimeError("TikTok uploader reported failure during scheduling")
        update_video_tiktok(video_id, tiktok_url=video_row.get("tiktok_url") or "", tiktok_status="scheduled", tiktok_error=None)
    finally:
        uploader.close()
        _cleanup_temp_upload_file(upload_path, cleanup_upload)


def publish_video_safe(slug: str, video_id: int) -> None:
    try:
        publish_video(slug, video_id)
    except Exception as exc:
        message = str(exc).strip() or exc.__class__.__name__
        set_tiktok_status(video_id, "error", message[:500])
        raise


def schedule_video_safe(slug: str, video_id: int, scheduled_for: str) -> None:
    try:
        schedule_video(slug, video_id, scheduled_for)
    except Exception as exc:
        message = str(exc).strip() or exc.__class__.__name__
        set_tiktok_status(video_id, "error", message[:500])
        raise


def schedule_pending_videos(slug: str) -> dict[str, int]:
    """
    Assign a tiktok_scheduled_for slot and queue TikTok publishes for eligible videos.

    Each video that has a final video but no TikTok slot yet gets the next
    available tiktok_publish_slots time (from channel_config.json). Videos are
    then set to status='queued' so publish_due_queued_videos() picks them up.
    """
    queued = 0
    skipped = 0
    failed = 0

    videos = get_channel_videos(slug)
    for video in videos:
        if not video.get("final_video_path"):
            skipped += 1
            continue
        if video.get("tiktok_status") in {"queued", "scheduled", "posted", "uploading"} or video.get("tiktok_posted"):
            skipped += 1
            continue

        try:
            # Assign a TikTok slot if not already set
            if not video.get("tiktok_scheduled_for"):
                slot_et = next_tiktok_publish_slot(slug)
                slot_iso = slot_et.isoformat()
                set_video_tiktok_scheduled_for(video["id"], slot_iso)
            else:
                slot_et = _scheduled_datetime(video["tiktok_scheduled_for"])
                if slot_et is None:
                    skipped += 1
                    continue
                slot_iso = slot_et.isoformat()

            valid_slot = _validate_tiktok_schedule(_scheduled_datetime(slot_iso))
            if valid_slot is None:
                skipped += 1
                continue

            update_video_tiktok(
                video["id"],
                tiktok_url=video.get("tiktok_url") or "",
                tiktok_status="queued",
                tiktok_error=None,
            )
            queued += 1
        except Exception as exc:
            set_tiktok_status(video["id"], "error", str(exc)[:500])
            failed += 1

    return {"queued": queued, "skipped": skipped, "failed": failed}


def publish_due_queued_videos(slug: str | None = None) -> dict[str, int]:
    """
    Publish all due queued TikTok videos.

    Each video is uploaded in an isolated subprocess via _tiktok_worker.py so
    that Playwright's sync API never shares event-loop state between consecutive
    uploads (which causes 'Playwright Sync API inside asyncio loop' errors).
    """
    import subprocess
    import sys

    published = 0
    skipped = 0
    failed = 0
    details: list[dict] = []
    now = datetime.now(timezone.utc)

    worker = Path(__file__).parent / "_tiktok_worker.py"

    if slug:
        videos = get_channel_videos(slug)
    else:
        from database.queries import get_all_channels

        videos = []
        for channel in get_all_channels():
            videos.extend(get_channel_videos(channel["slug"]))

    for video in videos:
        if video.get("tiktok_status") != "queued":
            skipped += 1
            details.append({"video_id": video["id"], "channel_slug": video["channel_slug"], "status": "skipped", "reason": "not_queued"})
            continue

        slot = _scheduled_datetime(video.get("tiktok_scheduled_for") or video.get("scheduled_for"))
        if slot is None or slot > now:
            skipped += 1
            details.append({"video_id": video["id"], "channel_slug": video["channel_slug"], "status": "skipped", "reason": "not_due"})
            continue

        vid_id = video["id"]
        chan_slug = video["channel_slug"]
        try:
            result = subprocess.run(
                [sys.executable, str(worker), chan_slug, str(vid_id)],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                published += 1
                details.append({"video_id": vid_id, "channel_slug": chan_slug, "status": "published"})
            else:
                err = (result.stderr or "").strip() or f"worker exited {result.returncode}"
                set_tiktok_status(vid_id, "error", err[:500])
                failed += 1
                details.append({"video_id": vid_id, "channel_slug": chan_slug, "status": "failed", "reason": err[:500]})
            # Always surface subprocess stdout/stderr to parent so logs capture it
            if result.stdout:
                print(result.stdout, end="", flush=True)
            if result.stderr:
                print(result.stderr, end="", flush=True)
        except Exception as exc:
            err = str(exc).strip() or exc.__class__.__name__
            set_tiktok_status(vid_id, "error", err[:500])
            failed += 1
            details.append({"video_id": vid_id, "channel_slug": chan_slug, "status": "failed", "reason": err[:500]})

    return {"published": published, "skipped": skipped, "failed": failed, "details": details}


def _verify_posted_caption(page, username: str, video_url: str = "") -> str:
    """
    After a successful upload, read the caption of the posted video from TikTok's
    embedded page JSON. If a direct video_url is provided, navigate there first.
    Falls back to scraping the profile for the first video link.
    Returns the caption string, or empty string if extraction fails.
    """
    try:
        if video_url:
            # Direct URL — most accurate, go straight to the video page
            target = video_url
        else:
            # No direct URL — try the profile and grab the first video link
            profile_url = f"https://www.tiktok.com/@{username}"
            print(f"[tiktok verify] no direct URL, navigating to profile: {profile_url}", flush=True)
            page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(4000)
            first_link = page.locator('a[href*="/video/"]').first
            if not first_link.count():
                print("[tiktok verify] no video links found on profile", flush=True)
                return ""
            href = first_link.get_attribute("href") or ""
            target = href if href.startswith("http") else f"https://www.tiktok.com{href}"

        print(f"[tiktok verify] reading from: {target}", flush=True)
        page.goto(target, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)
        html = page.content()

        # TikTok embeds video metadata as JSON. "desc" holds the caption text.
        desc_matches = re.findall(r'"desc"\s*:\s*"([^"]{5,})"', html)
        if desc_matches:
            caption = desc_matches[0]
            print(
                f"[tiktok verify] ✓ POSTED CAPTION ({len(caption)} chars):\n{caption}\n",
                flush=True,
            )
            return caption

        print("[tiktok verify] could not extract caption from page JSON", flush=True)
    except Exception as e:
        print(f"[tiktok verify] failed: {e}", flush=True)
    return ""


def _scrape_video_stats(tiktok_url: str) -> dict[str, int]:
    normalized_url = _normalize_tiktok_url(tiktok_url)
    if not normalized_url:
        raise ValueError("Invalid TikTok video URL")

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        page = browser.new_page()
        page.goto(f"{normalized_url}?lang=en", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(4000)
        html = page.content()
        browser.close()

    match = re.search(
        r'"statsV2":\{"diggCount":"(?P<likes>\d+)","shareCount":"(?P<shares>\d+)","commentCount":"(?P<comments>\d+)","playCount":"(?P<views>\d+)"',
        html,
    )
    if not match:
        match = re.search(
            r'"diggCount":(?P<likes>\d+),"shareCount":(?P<shares>\d+),"commentCount":(?P<comments>\d+),"playCount":(?P<views>\d+)',
            html,
        )
    if not match:
        raise RuntimeError("Could not parse TikTok stats from page HTML")

    return {
        "views": int(match.group("views")),
        "comments": int(match.group("comments")),
        "likes": int(match.group("likes")),
    }


def refresh_channel_stats(slug: str) -> dict[int, dict[str, int]]:
    results: dict[int, dict[str, int]] = {}
    for video in get_channel_videos(slug):
        tiktok_url = _normalize_tiktok_url(video.get("tiktok_url") or "")
        if not tiktok_url:
            continue
        try:
            stats = _scrape_video_stats(tiktok_url)
            update_tiktok_stats(video["id"], stats["views"], stats["comments"], stats["likes"])
            results[video["id"]] = stats
        except Exception as exc:
            set_tiktok_status(video["id"], "error", str(exc)[:500])
    return results
