"""
Recovery script — Apr 19 + Apr 20 YouTube upload failures.

Both YouTube tokens expired. This script:
  1. Re-auths YouTube for digital-overlords (browser)
  2. Re-auths YouTube for villian-monologues (browser)
  3. Uploads 6 backlogged videos to YouTube
  4. Resets TikTok to queued for all 8 affected videos
  5. Sets created_at so scheduler skips Apr 21 generation (avoids double-posting)

Backlog:
  168  digital-overlords  Apr19  already on YouTube  → TikTok only
  169  digital-overlords  Apr19  already on YouTube  → TikTok only
  170  villian-monologues Apr19  never uploaded       → YouTube + TikTok
  171  villian-monologues Apr19  never uploaded       → YouTube + TikTok
  172  digital-overlords  Apr20  never uploaded       → YouTube + TikTok
  173  digital-overlords  Apr20  never uploaded       → YouTube + TikTok
  174  villian-monologues Apr20  never uploaded       → YouTube + TikTok
  175  villian-monologues Apr20  never uploaded       → YouTube + TikTok

Apr 21 upload schedule (spread across the day):
  10:00 AM  digital-overlords  172
   2:00 PM  digital-overlords  173
   6:00 PM  villian-monologues 174
  10:00 PM  villian-monologues 175

Apr 22 (Apr 19 villian catch-up):
   10:00 AM  villian-monologues 170
    2:00 PM  villian-monologues 171

TikTok will auto-post at the scheduled_for time via hourly cron.
Apr 21 + Apr 22 generation is blocked (created_at faked to that day).

Usage:
  python scripts/recover_apr19.py
"""

import sys
from pathlib import Path
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# video_id, slug, slot (ET), needs_yt_upload
PLAN = [
    # Apr 21
    (172, "digital-overlords",   datetime(2026, 4, 21, 10,  0, tzinfo=ET), True),
    (173, "digital-overlords",   datetime(2026, 4, 21, 14,  0, tzinfo=ET), True),
    (174, "villian-monologues",  datetime(2026, 4, 21, 18,  0, tzinfo=ET), True),
    (175, "villian-monologues",  datetime(2026, 4, 21, 22,  0, tzinfo=ET), True),
    # Apr 22 (Apr 19 villian-monologues catch-up)
    (170, "villian-monologues",  datetime(2026, 4, 22, 10,  0, tzinfo=ET), True),
    (171, "villian-monologues",  datetime(2026, 4, 22, 14,  0, tzinfo=ET), True),
    # Apr 19 digital-overlords already on YouTube — TikTok only
    (168, "digital-overlords",   None, False),
    (169, "digital-overlords",   None, False),
]

def main():
    from layer5_publishing.youtube_uploader import authenticate, upload_video, upload_thumbnail
    from content_paths import resolve_thumbnail_path
    from database.queries import set_youtube_status, set_tiktok_status, set_video_scheduled_for

    print("\n=== Step 1: Re-authenticate YouTube for digital-overlords ===")
    print("Browser will open — log in and approve.\n")
    do_service = authenticate("digital-overlords")
    print("✓ digital-overlords authenticated\n")

    print("=== Step 2: Re-authenticate YouTube for villian-monologues ===")
    print("Browser will open again — log in and approve.\n")
    vm_service = authenticate("villian-monologues")
    print("✓ villian-monologues authenticated\n")

    services = {
        "digital-overlords": do_service,
        "villian-monologues": vm_service,
    }

    db_path = str(BASE_DIR / "database" / "videos.db")

    print("=== Step 3: Upload backlog to YouTube + reset TikTok ===\n")
    for video_id, slug, slot_et, needs_yt in PLAN:
        service = services[slug]

        if needs_yt:
            et_str = slot_et.strftime("%b %-d %-I:%M %p ET")
            print(f"[{slug}] video {video_id} → uploading for {et_str}")
            set_youtube_status(video_id, "uploading", None)
            try:
                yt_id = upload_video(slug, video_id, service, publish_at=slot_et)
                set_youtube_status(video_id, "scheduled", None)
                set_video_scheduled_for(video_id, slot_et.isoformat())
                print(f"  YouTube: ✓ https://youtu.be/{yt_id}  (publishes {et_str})")
            except Exception as e:
                set_youtube_status(video_id, "error", str(e)[:500])
                print(f"  YouTube: ✗ FAILED — {e}")
                continue

            thumb = resolve_thumbnail_path(slug, video_id)
            if thumb and thumb.exists():
                try:
                    upload_thumbnail(yt_id, thumb, service)
                    print(f"  Thumbnail: ✓")
                except Exception as e:
                    print(f"  Thumbnail: skipped ({e})")

            # Reset TikTok to queued at same slot time
            set_tiktok_status(video_id, "queued", None)
            set_video_scheduled_for(video_id, slot_et.isoformat())
            print(f"  TikTok: ✓ queued for {et_str}")

        else:
            # Already on YouTube — just reset TikTok
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT scheduled_for, youtube_video_id FROM videos WHERE id=?", (video_id,)).fetchone()
            conn.close()
            sched = row["scheduled_for"] if row else "unknown"
            yt_id = row["youtube_video_id"] if row else "?"
            print(f"[{slug}] video {video_id} — already on YouTube ({yt_id}), resetting TikTok")
            set_tiktok_status(video_id, "queued", None)
            print(f"  TikTok: ✓ queued (slot: {sched})")

        print()

    # === Step 4: Fake created_at so scheduler skips Apr 21 + Apr 22 generation ===
    print("=== Step 4: Block Apr 21 + Apr 22 scheduler generation ===")
    conn = sqlite3.connect(db_path)
    # Apr 21 videos (172-175) → created_at = Apr 21
    conn.executemany(
        "UPDATE videos SET created_at = '2026-04-21 08:00:00' WHERE id = ?",
        [(172,), (173,), (174,), (175,)],
    )
    # Apr 22 videos (170, 171) → created_at = Apr 22
    conn.executemany(
        "UPDATE videos SET created_at = '2026-04-22 08:00:00' WHERE id = ?",
        [(170,), (171,)],
    )
    conn.commit()
    conn.close()
    print("  172, 173, 174, 175 → created_at Apr 21 (blocks today's generation)")
    print("  170, 171           → created_at Apr 22 (blocks Apr 22 generation)")

    print("\n✓ All done.")
    print("  Apr 21: 4 videos publishing 10AM / 2PM / 6PM / 10PM ET")
    print("  Apr 22: 2 catch-up villian-monologues at 10AM / 2PM ET")
    print("  Apr 23: normal generation resumes for both channels")


if __name__ == "__main__":
    main()
