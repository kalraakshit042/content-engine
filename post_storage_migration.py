"""
post_storage_migration.py

Moves legacy per-layer files into channels/{slug}/posts/{video_id}/...
and updates videos.db paths accordingly. Safe to run repeatedly.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from content_paths import (
    BASE_DIR,
    CHANNELS_DIR,
    audio_acts_path,
    audio_wav_path,
    channel_dir,
    ensure_post_dirs,
    final_video_path,
    parse_video_id_from_name,
    post_images_dir,
    resolve_audio_wav_path,
    resolve_final_video_path,
    resolve_script_json_path,
    scene_image_path,
    script_json_path,
    thumbnail_path,
)

DB_PATH = BASE_DIR / "database" / "videos.db"


def _move_if_needed(src: Path, dst: Path) -> bool:
    if not src.exists():
        return False
    if dst.exists():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    src.rename(dst)
    return True


def _migrate_channel_files(slug: str) -> int:
    moved = 0
    root = channel_dir(slug)
    if not root.exists():
        return 0

    for path in sorted((root / "scripts").glob("*.json")):
        video_id = parse_video_id_from_name(path)
        if video_id is None:
            continue
        ensure_post_dirs(slug, video_id)
        if _move_if_needed(path, script_json_path(slug, video_id)):
            moved += 1

    for path in sorted((root / "audio").glob("*.wav")):
        video_id = parse_video_id_from_name(path)
        if video_id is None:
            continue
        ensure_post_dirs(slug, video_id)
        if _move_if_needed(path, audio_wav_path(slug, video_id)):
            moved += 1

    for path in sorted((root / "audio").glob("*_acts.json")):
        video_id = parse_video_id_from_name(path)
        if video_id is None:
            continue
        ensure_post_dirs(slug, video_id)
        if _move_if_needed(path, audio_acts_path(slug, video_id)):
            moved += 1

    for path in sorted((root / "videos").glob("*.mp4")):
        video_id = parse_video_id_from_name(path)
        if video_id is None:
            continue
        ensure_post_dirs(slug, video_id)
        if _move_if_needed(path, final_video_path(slug, video_id)):
            moved += 1

    for path in sorted((root / "thumbnails").glob("*")):
        video_id = parse_video_id_from_name(path)
        if video_id is None or not path.is_file():
            continue
        ensure_post_dirs(slug, video_id)
        if _move_if_needed(path, thumbnail_path(slug, video_id, path.suffix.lower() or ".jpg")):
            moved += 1

    scene_pattern = re.compile(r"^(?P<video>\d+)_scene(?P<scene>\d+)(?P<raw>_raw)?(?P<ext>\.[^.]+)$")
    for path in sorted((root / "images").glob("*")):
        if not path.is_file():
            continue
        match = scene_pattern.match(path.name)
        if not match:
            continue
        video_id = int(match.group("video"))
        scene_idx = int(match.group("scene"))
        ext = match.group("ext")
        ensure_post_dirs(slug, video_id)
        if match.group("raw"):
            dst = post_images_dir(slug, video_id) / f"scene{scene_idx}_raw{ext}"
        else:
            dst = scene_image_path(slug, video_id, scene_idx, ext)
        if _move_if_needed(path, dst):
            moved += 1

    return moved


def _sync_db_paths() -> int:
    updated = 0
    if not DB_PATH.exists():
        return 0

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT id, channel_slug FROM videos").fetchall()
    for video_id, slug in rows:
        script_path = resolve_script_json_path(slug, video_id)
        audio_path = resolve_audio_wav_path(slug, video_id)
        final_path = resolve_final_video_path(slug, video_id)
        conn.execute(
            """UPDATE videos
               SET script_path = CASE WHEN ? THEN ? ELSE script_path END,
                   audio_path = CASE WHEN ? THEN ? ELSE audio_path END,
                   final_video_path = CASE WHEN ? THEN ? ELSE final_video_path END
               WHERE id = ?""",
            (
                1 if script_path.exists() else 0,
                str(script_path) if script_path.exists() else "",
                1 if audio_path.exists() else 0,
                str(audio_path) if audio_path.exists() else "",
                1 if final_path.exists() else 0,
                str(final_path) if final_path.exists() else "",
                video_id,
            ),
        )
        updated += 1
    conn.commit()
    conn.close()
    return updated


def migrate_post_storage() -> dict[str, int]:
    moved = 0
    for channel_root in sorted(CHANNELS_DIR.iterdir()) if CHANNELS_DIR.exists() else []:
        if channel_root.is_dir():
            moved += _migrate_channel_files(channel_root.name)
    synced = _sync_db_paths()
    return {"moved_files": moved, "db_rows_synced": synced}
