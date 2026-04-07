"""
layer4_video_production/image_generator.py

Fetches background images from Pexels API and applies cinematic styling.
Replaces the local SDXL diffusion approach (too slow on Apple Silicon MPS).

Pipeline per image:
  1. Extract 3-5 keywords from the scene description
  2. Search Pexels for portrait-oriented photos
  3. Download top result
  4. Apply cinematic post-processing:
       - Center crop → 1080×1920
       - Dark overlay (40% opacity)
       - Warm color grade
       - Film grain
       - Vignette
  5. Cache as images/{video_id}_scene{i}.png

Total time: ~1-2 seconds per image (network + Pillow transforms).
"""

import os
import re
import random
from pathlib import Path

import httpx
import numpy as np
from PIL import Image, ImageDraw, ImageFilter

from content_paths import post_images_dir, scene_image_path, scene_raw_image_path

VIDEO_W, VIDEO_H = 1080, 1920

# Words to strip when converting a diffusion prompt to a Pexels search query.
_STRIP_WORDS = {
    "hyperrealistic", "hyperrealism", "realistic", "photorealistic",
    "macro", "photography", "cinematic", "dramatic", "style", "lighting",
    "film", "noir", "black", "white", "shadows", "smoke", "moody",
    "close-up", "closeup", "extreme", "angle", "dutch", "overhead",
    "wide", "establishing", "shot", "scene", "image", "photo",
    "render", "rendering", "portrait", "illustration", "painting",
    "atmospheric", "fog", "depth", "silhouette", "backlit", "rim",
    "chiaroscuro", "high", "contrast", "low", "key", "volumetric",
    "bokeh", "shallow", "focus", "dark", "rich", "colors", "tones",
    "detailed", "intricate", "sharp", "lens", "flare", "epic",
    "museum", "quality", "oil", "canvas", "texture", "grain",
    "vintage", "retro", "aged", "worn", "weathered", "rustic",
    "gritty", "raw", "visceral", "haunting", "ominous", "sinister",
    "menacing", "brooding", "intense", "powerful", "striking",
    "and", "with", "the", "in", "of", "a", "an", "for", "on",
}


def _extract_keywords(scene_description: str, max_words: int = 4) -> str:
    """
    Convert a diffusion-style image prompt into a clean Pexels search query.
    Strips art-direction jargon, keeps subject nouns and meaningful adjectives.
    """
    # Content before first comma is usually the subject — focus there
    text = scene_description.split(",")[0]
    text = re.sub(r"[^\w\s]", " ", text.lower())
    words = text.split()
    kept = [w for w in words if w not in _STRIP_WORDS and len(w) > 2]
    query = " ".join(kept[:max_words])
    return query or scene_description.split(",")[0][:50]


def _fetch_pixabay(query: str, api_key: str, out_path: Path) -> bool:
    """
    Fallback image source. Pixabay free tier: 100 req/hour.
    Returns True on success.
    """
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(
                "https://pixabay.com/api/",
                params={
                    "key": api_key,
                    "q": query,
                    "image_type": "photo",
                    "orientation": "vertical",
                    "per_page": 15,
                    "safesearch": "true",
                },
            )
        if resp.status_code != 200:
            return False
        hits = resp.json().get("hits", [])
        if not hits:
            return False
        photo = random.choice(hits[:10])
        img_url = photo.get("largeImageURL") or photo.get("webformatURL")
        if not img_url:
            return False
        with httpx.Client(timeout=60.0) as client:
            img_resp = client.get(img_url)
        if img_resp.status_code != 200:
            return False
        out_path.write_bytes(img_resp.content)
        return True
    except Exception:
        return False


def _fetch_pexels(query: str, api_key: str, out_path: Path) -> bool:
    """
    Search Pexels for portrait photos matching the query.
    Downloads large resolution. Returns True on success.
    """
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(
                "https://api.pexels.com/v1/search",
                headers={"Authorization": api_key},
                params={
                    "query": query,
                    "orientation": "portrait",
                    "per_page": 15,
                    "size": "large",
                },
            )
        if resp.status_code != 200:
            return False

        photos = resp.json().get("photos", [])
        if not photos:
            return False

        # Pick randomly from top results for variety across videos
        photo = random.choice(photos[:10])
        img_url = photo["src"]["large2x"]

        with httpx.Client(timeout=60.0) as client:
            img_resp = client.get(img_url)
        if img_resp.status_code != 200:
            return False

        out_path.write_bytes(img_resp.content)
        return True

    except Exception:
        return False


def _apply_cinematic_style(img: Image.Image) -> Image.Image:
    """
    Transform a raw stock photo into a moody cinematic background:
      1. Center crop → fill 1080×1920
      2. Dark overlay (40% opacity black)
      3. Warm color grade (subtle amber tone)
      4. Film grain
      5. Vignette
    """
    # ── 1. Center crop to 9:16 ──────────────────────────────────────────────
    iw, ih = img.size
    target_ratio = VIDEO_W / VIDEO_H

    if (iw / ih) > target_ratio:
        new_w = int(ih * target_ratio)
        left = (iw - new_w) // 2
        img = img.crop((left, 0, left + new_w, ih))
    else:
        new_h = int(iw / target_ratio)
        top = (ih - new_h) // 3  # slightly above center — subjects tend high
        img = img.crop((0, top, iw, top + new_h))

    img = img.resize((VIDEO_W, VIDEO_H), Image.LANCZOS).convert("RGB")

    # ── 1b. Brightness normalization (crush bright/pastel images) ────────────
    arr = np.array(img, dtype=np.float32)
    arr = np.clip(arr * 0.80, 0, 255)
    img = Image.fromarray(arr.astype(np.uint8))

    # ── 2. Dark overlay ──────────────────────────────────────────────────────
    overlay = Image.new("RGBA", (VIDEO_W, VIDEO_H), (0, 0, 0, 130))
    img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")

    # ── 3. Warm color grade ──────────────────────────────────────────────────
    arr = np.array(img, dtype=np.float32)
    arr[:, :, 0] = np.clip(arr[:, :, 0] * 1.08 + 8, 0, 255)   # R: warm up
    arr[:, :, 1] = np.clip(arr[:, :, 1] * 1.02 + 2, 0, 255)   # G: neutral
    arr[:, :, 2] = np.clip(arr[:, :, 2] * 0.90 - 5, 0, 255)   # B: cool down
    img = Image.fromarray(arr.astype(np.uint8))

    # ── 4. Film grain ────────────────────────────────────────────────────────
    grain = np.random.normal(0, 8, (VIDEO_H, VIDEO_W, 3)).astype(np.float32)
    arr = np.clip(np.array(img, dtype=np.float32) + grain, 0, 255).astype(np.uint8)
    img = Image.fromarray(arr)

    # ── 5. Vignette ──────────────────────────────────────────────────────────
    border = Image.new("RGBA", (VIDEO_W, VIDEO_H), (0, 0, 0, 0))
    bdraw = ImageDraw.Draw(border)
    for i in range(80):
        alpha = int(180 * (i / 80) ** 0.5)
        bdraw.rectangle([i, i, VIDEO_W - i, VIDEO_H - i], outline=(0, 0, 0, alpha))
    border = border.filter(ImageFilter.GaussianBlur(radius=30))
    img = Image.alpha_composite(img.convert("RGBA"), border).convert("RGB")

    return img


def generate_scene_images(
    slug: str,
    video_id: int,
    scene_descriptions: list,
    channels_dir: Path,
    pexels_queries: list = None,
    subject: str = "",
) -> list:
    """
    Main entry point. Fetches + styles one image per scene description.
    pexels_queries: if provided, used directly as Pexels search terms (one per scene).
                   Falls back to _extract_keywords(description) if not provided or shorter.
    Returns list of Paths in scene order.
    Raises RuntimeError if any image fails (no silent fallbacks).
    """
    api_key = os.environ.get("PEXELS_API_KEY", "")
    if not api_key:
        raise RuntimeError("PEXELS_API_KEY not set in .env")
    pixabay_key = os.environ.get("PIXABAY_API_KEY", "")

    img_dir = post_images_dir(slug, video_id)
    img_dir.mkdir(parents=True, exist_ok=True)

    paths = []
    for i, description in enumerate(scene_descriptions):
        out_path = scene_image_path(slug, video_id, i, ".png")

        if out_path.exists():
            print(f"  scene {i}: cached ({out_path.name})")
            paths.append(out_path)
            continue

        if pexels_queries and i < len(pexels_queries) and pexels_queries[i].strip():
            query = pexels_queries[i].strip()
        else:
            query = _extract_keywords(description)
        print(f"  scene {i}: fetching '{query}' from Pexels...")

        raw_path = scene_raw_image_path(slug, video_id, i, ".jpg")
        success = _fetch_pexels(query, api_key, raw_path)

        if not success:
            fallback1 = f"{subject} close-up" if subject else " ".join(query.split()[:2])
            print(f"  scene {i}: retrying with '{fallback1}'...")
            success = _fetch_pexels(fallback1, api_key, raw_path)

        if not success and subject:
            print(f"  scene {i}: retrying with '{subject}'...")
            success = _fetch_pexels(subject, api_key, raw_path)

        if not success and pixabay_key:
            print(f"  scene {i}: Pexels exhausted, trying Pixabay '{query}'...")
            success = _fetch_pixabay(query, pixabay_key, raw_path)

        if not success and pixabay_key and subject:
            print(f"  scene {i}: Pixabay retrying with '{subject}'...")
            success = _fetch_pixabay(subject, pixabay_key, raw_path)

        if not success:
            raise RuntimeError(
                f"Failed to fetch image for scene {i} (query: '{query}'). "
                f"Check PEXELS_API_KEY and network connection."
            )

        raw_img = Image.open(raw_path).convert("RGB")
        styled = _apply_cinematic_style(raw_img)
        styled.save(str(out_path), "PNG")
        raw_path.unlink()

        size_kb = out_path.stat().st_size // 1024
        print(f"  scene {i}: done ({size_kb}KB)")
        paths.append(out_path)

    return paths
