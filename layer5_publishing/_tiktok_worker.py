#!/usr/bin/env python3
"""
_tiktok_worker.py

Isolated subprocess worker for a single TikTok upload.

Each TikTok publish is run in its own process so Playwright's sync API
never shares event-loop state between consecutive uploads (which causes
"Playwright Sync API inside asyncio loop" errors in the parent process).

Usage:
    python3 _tiktok_worker.py <slug> <video_id>

Exit codes:
    0 — published successfully
    1 — failed (error written to stderr)
"""
import sys
from pathlib import Path

# Python automatically prepends the script's own directory (layer5_publishing/)
# to sys.path when running a file. This causes `import tiktok_uploader.upload`
# inside _load_uploader() to find OUR tiktok_uploader.py instead of the
# installed tiktok-uploader package, giving "not a package" errors.
# Strip it out, then add the true project root instead.
BASE_DIR = Path(__file__).parent.parent.resolve()
script_dir = Path(__file__).parent.resolve()
sys.path = [p for p in sys.path if Path(p).resolve() != script_dir]
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env", override=True)

from layer5_publishing.tiktok_uploader import publish_video
from database.queries import set_tiktok_status


def main() -> int:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <slug> <video_id>", file=sys.stderr)
        return 1

    slug = sys.argv[1]
    try:
        video_id = int(sys.argv[2])
    except ValueError:
        print(f"video_id must be an integer, got: {sys.argv[2]}", file=sys.stderr)
        return 1

    try:
        publish_video(slug, video_id)
        return 0
    except Exception as exc:
        message = str(exc).strip() or exc.__class__.__name__
        print(message, file=sys.stderr)
        set_tiktok_status(video_id, "error", message[:500])
        return 1


if __name__ == "__main__":
    sys.exit(main())
