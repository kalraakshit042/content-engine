"""
storage_cleanup.py

Purges local video/audio/image files for videos that have been
published for more than CLEANUP_AFTER_DAYS days.

The YouTube video stays live. Only local disk files are removed.
DB records are kept — just the file path columns are nulled out.

Called from scheduler.py and from the dashboard on page load.
"""

import shutil
from pathlib import Path

from content_paths import channel_dir
from database.queries import get_videos_for_cleanup, clear_video_local_paths

CLEANUP_AFTER_DAYS = 14


def run_cleanup(dry_run: bool = False) -> list[str]:
    """
    Finds published videos older than CLEANUP_AFTER_DAYS and deletes their
    local post directory (images, audio, final video, thumbnails).
    Returns a list of log messages.
    """
    videos = get_videos_for_cleanup(days_old=CLEANUP_AFTER_DAYS)
    log = []

    for v in videos:
        slug = v["channel_slug"]
        vid_id = v["id"]

        # The post directory contains all artifacts for this video
        post_dir = channel_dir(slug) / "posts" / str(vid_id)

        if post_dir.exists():
            size_mb = sum(f.stat().st_size for f in post_dir.rglob("*") if f.is_file()) / (1024 * 1024)
            if dry_run:
                log.append(f"[dry-run] would delete {post_dir} ({size_mb:.1f} MB)")
            else:
                shutil.rmtree(post_dir)
                clear_video_local_paths(vid_id)
                log.append(f"cleaned {slug}/video {vid_id} ({size_mb:.1f} MB freed)")
        else:
            # Directory already gone — just null the DB paths
            if not dry_run:
                clear_video_local_paths(vid_id)

    if not log:
        log.append("nothing to clean up")

    return log


if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    for line in run_cleanup(dry_run=dry):
        print(line)
