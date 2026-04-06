"""
content_paths.py

Canonical on-disk layout helpers for channel and per-post assets.

Each channel keeps channel-level assets at the root and stores post-level assets
under channels/{slug}/posts/{video_id}/...
"""

from __future__ import annotations

import re
from pathlib import Path

BASE_DIR = Path(__file__).parent
CHANNELS_DIR = BASE_DIR / "channels"


def channel_dir(slug: str) -> Path:
    return CHANNELS_DIR / slug


def channel_posts_dir(slug: str) -> Path:
    return channel_dir(slug) / "posts"


def post_dir(slug: str, video_id: int) -> Path:
    return channel_posts_dir(slug) / str(video_id)


def post_images_dir(slug: str, video_id: int) -> Path:
    return post_dir(slug, video_id) / "images"


def ensure_post_dirs(slug: str, video_id: int) -> None:
    post_dir(slug, video_id).mkdir(parents=True, exist_ok=True)
    post_images_dir(slug, video_id).mkdir(parents=True, exist_ok=True)


def script_json_path(slug: str, video_id: int) -> Path:
    return post_dir(slug, video_id) / "script.json"


def audio_wav_path(slug: str, video_id: int) -> Path:
    return post_dir(slug, video_id) / "audio.wav"


def audio_acts_path(slug: str, video_id: int) -> Path:
    return post_dir(slug, video_id) / "audio_acts.json"


def final_video_path(slug: str, video_id: int) -> Path:
    return post_dir(slug, video_id) / "final.mp4"


def thumbnail_path(slug: str, video_id: int, ext: str = ".jpg") -> Path:
    ext = ext if ext.startswith(".") else f".{ext}"
    return post_dir(slug, video_id) / f"thumbnail{ext.lower()}"


def scene_image_path(slug: str, video_id: int, scene_index: int, ext: str = ".png") -> Path:
    ext = ext if ext.startswith(".") else f".{ext}"
    return post_images_dir(slug, video_id) / f"scene{scene_index}{ext.lower()}"


def scene_raw_image_path(slug: str, video_id: int, scene_index: int, ext: str = ".jpg") -> Path:
    ext = ext if ext.startswith(".") else f".{ext}"
    return post_images_dir(slug, video_id) / f"scene{scene_index}_raw{ext.lower()}"


def resolve_script_json_path(slug: str, video_id: int) -> Path:
    new_path = script_json_path(slug, video_id)
    if new_path.exists():
        return new_path
    return channel_dir(slug) / "scripts" / f"{video_id}.json"


def resolve_audio_wav_path(slug: str, video_id: int) -> Path:
    new_path = audio_wav_path(slug, video_id)
    if new_path.exists():
        return new_path
    return channel_dir(slug) / "audio" / f"{video_id}.wav"


def resolve_audio_acts_path(slug: str, video_id: int) -> Path:
    new_path = audio_acts_path(slug, video_id)
    if new_path.exists():
        return new_path
    return channel_dir(slug) / "audio" / f"{video_id}_acts.json"


def resolve_final_video_path(slug: str, video_id: int) -> Path:
    new_path = final_video_path(slug, video_id)
    if new_path.exists():
        return new_path
    return channel_dir(slug) / "videos" / f"{video_id}.mp4"


def resolve_thumbnail_path(slug: str, video_id: int) -> Path | None:
    for ext in (".jpg", ".jpeg", ".png"):
        new_path = thumbnail_path(slug, video_id, ext)
        if new_path.exists():
            return new_path
    legacy_root = channel_dir(slug) / "thumbnails"
    for ext in (".jpg", ".jpeg", ".png"):
        candidate = legacy_root / f"{video_id}{ext}"
        if candidate.exists():
            return candidate
    return None


def resolve_scene_image_path(slug: str, video_id: int, scene_index: int) -> Path | None:
    for ext in (".png", ".jpg", ".jpeg"):
        new_path = scene_image_path(slug, video_id, scene_index, ext)
        if new_path.exists():
            return new_path
    legacy_root = channel_dir(slug) / "images"
    for ext in (".png", ".jpg", ".jpeg"):
        candidate = legacy_root / f"{video_id}_scene{scene_index}{ext}"
        if candidate.exists():
            return candidate
    return None


def iter_legacy_scene_paths(slug: str, video_id: int) -> list[Path]:
    legacy_root = channel_dir(slug) / "images"
    return sorted(legacy_root.glob(f"{video_id}_scene*"))


def parse_video_id_from_name(path: Path) -> int | None:
    match = re.match(r"^(\d+)", path.stem)
    if not match:
        return None
    return int(match.group(1))
