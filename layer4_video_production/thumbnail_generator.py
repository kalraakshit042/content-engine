"""
layer4_video_production/thumbnail_generator.py

Generates a 1280×720 YouTube thumbnail from the first scene image.

Pipeline:
  1. Load channels/{slug}/images/{video_id}_scene0.png (1080×1920 portrait)
  2. Center-crop to 16:9 → resize to 1280×720
  3. Dark overlay (heavier than video frames — thumbnail needs contrast)
  4. Title text in lower third, large + outlined
  5. Save as channels/{slug}/thumbnails/{video_id}.jpg (quality=92)
"""

import textwrap
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFont

from content_paths import resolve_scene_image_path, thumbnail_path

THUMB_W, THUMB_H = 1280, 720
TITLE_FONT_SIZE = 72
CHANNEL_FONT_SIZE = 30
OUTLINE_WIDTH = 3


def _load_font(size: int) -> ImageFont.FreeTypeFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Impact.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/Arial Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _draw_outlined_text(draw: ImageDraw.ImageDraw, xy: tuple, text: str,
                         font: ImageFont.FreeTypeFont, outline: int = OUTLINE_WIDTH) -> None:
    x, y = xy
    for dx in range(-outline, outline + 1):
        for dy in range(-outline, outline + 1):
            if dx != 0 or dy != 0:
                draw.text((x + dx, y + dy), text, font=font, fill="black")
    draw.text((x, y), text, font=font, fill="white")


def generate_thumbnail(slug: str, video_id: int, title: str, channels_dir: Path,
                       channel_name: str = "") -> Path:
    """
    Generate a YouTube thumbnail for the given video.
    Returns the path to the saved JPEG.
    Raises FileNotFoundError if scene 0 image is missing.
    """
    scene0 = resolve_scene_image_path(slug, video_id, 0)
    if scene0 is None:
        scene0 = channels_dir / slug / "images" / f"{video_id}_scene0.png"
    if not scene0.exists():
        raise FileNotFoundError(f"Scene 0 image not found: {scene0}")

    img = Image.open(scene0).convert("RGB")
    iw, ih = img.size  # 1080 × 1920

    # ── 1. Landscape crop to 16:9 ──────────────────────────────────────────────
    target_h = int(iw * THUMB_H / THUMB_W)  # 1080 × 607
    top = (ih - target_h) // 2
    img = img.crop((0, top, iw, top + target_h))
    img = img.resize((THUMB_W, THUMB_H), Image.LANCZOS)

    # ── 2. Brightness crush (images already dark from cinematic pass, but reinforce) ─
    arr = np.array(img, dtype=np.float32)
    arr = np.clip(arr * 0.75, 0, 255)
    img = Image.fromarray(arr.astype(np.uint8))

    # ── 3. Dark gradient overlay — heavier at bottom for text legibility ────────
    overlay = Image.new("RGBA", (THUMB_W, THUMB_H), (0, 0, 0, 0))
    draw_ov = ImageDraw.Draw(overlay)
    for y in range(THUMB_H):
        # alpha ramps from 40 at top to 200 at bottom
        alpha = int(40 + 160 * (y / THUMB_H) ** 1.5)
        draw_ov.line([(0, y), (THUMB_W, y)], fill=(0, 0, 0, alpha))
    img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")

    draw = ImageDraw.Draw(img)
    title_font = _load_font(TITLE_FONT_SIZE)
    small_font = _load_font(CHANNEL_FONT_SIZE)

    # ── 4. Title text — wrapped, centered in bottom 35% ────────────────────────
    wrapped = textwrap.fill(title, width=22)
    lines = wrapped.split("\n")
    line_h = TITLE_FONT_SIZE + 10
    total_text_h = len(lines) * line_h
    y_start = int(THUMB_H * 0.65) - total_text_h // 2

    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=title_font)
        tw = bbox[2] - bbox[0]
        x = (THUMB_W - tw) // 2
        _draw_outlined_text(draw, (x, y_start), line, title_font)
        y_start += line_h

    # ── 5. Channel name — top-left ──────────────────────────────────────────────
    if channel_name:
        _draw_outlined_text(draw, (24, 20), channel_name.upper(), small_font, outline=2)

    # ── 6. Save ─────────────────────────────────────────────────────────────────
    out_path = thumbnail_path(slug, video_id, ".jpg")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(str(out_path), "JPEG", quality=92)
    return out_path
