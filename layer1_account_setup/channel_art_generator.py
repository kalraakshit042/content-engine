"""
layer1_account_setup/channel_art_generator.py

Generates channel profile picture and banner image using Pollinations.ai (free).
Saves to channels/{slug}/profile_pic.png and channels/{slug}/banner.png.
Each image is generated independently — one failing does not affect the other.
"""

import httpx
import time
import random
from pathlib import Path
from urllib.parse import quote

from layer1_account_setup.config_schema import ChannelConfig

CHANNELS_DIR = Path(__file__).parent.parent / "channels"
POLLINATIONS_BASE = "https://image.pollinations.ai/prompt"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 10


def _build_profile_pic_prompt(config: ChannelConfig) -> str:
    first_visual = config.visual_styles[0]
    return (
        f"YouTube channel profile picture for '{config.channel_name}', "
        f"{config.description}, "
        f"icon design, bold graphic, centered composition, "
        f"{first_visual.image_prompt_suffix}, "
        f"no text, square format, professional channel art"
    )


def _build_banner_prompt(config: ChannelConfig) -> str:
    first_visual = config.visual_styles[0]
    return (
        f"YouTube channel banner for '{config.channel_name}', "
        f"{config.description}, "
        f"wide cinematic composition, dramatic atmosphere, "
        f"{first_visual.image_prompt_suffix}, "
        f"no text overlays, professional channel art, wide landscape"
    )


def _fetch_image(prompt: str, width: int, height: int, seed: int) -> bytes:
    """Fetch one image from Pollinations.ai with retry logic."""
    encoded_prompt = quote(prompt)
    url = (
        f"{POLLINATIONS_BASE}/{encoded_prompt}"
        f"?width={width}&height={height}&seed={seed}&model=flux&nologo=true"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = httpx.get(
                url,
                timeout=httpx.Timeout(connect=15.0, read=180.0, write=15.0, pool=15.0),
                follow_redirects=True,
            )
            # 429 = rate limited, their queue is full — wait longer before retry
            if response.status_code == 429:
                if attempt == MAX_RETRIES:
                    raise RuntimeError("Rate limited by Pollinations after all retries")
                print(f"[channel_art] Rate limited (429), waiting 60s before retry {attempt+1}...")
                time.sleep(60)
                continue

            response.raise_for_status()
            if len(response.content) < 1000:
                raise ValueError("Response too small — likely an error placeholder")
            return response.content
        except RuntimeError:
            raise
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"Image generation failed after {MAX_RETRIES} attempts: {e}")
            print(f"[channel_art] Attempt {attempt} failed: {e}, retrying in {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)


def _generate_single(path: Path, prompt: str, width: int, height: int, seed: int) -> bool:
    """
    Generate one image and save it. Skips if file already exists.
    Returns True on success, False on failure (logs but does not raise).
    """
    if path.exists():
        return True  # Already generated, skip

    try:
        image_bytes = _fetch_image(prompt, width, height, seed)
        path.write_bytes(image_bytes)
        return True
    except Exception as e:
        print(f"[channel_art] Warning: could not generate {path.name}: {e}")
        return False


def generate_channel_art(slug: str, config: ChannelConfig) -> dict:
    """
    Generates profile pic (800x800) and banner (1280x720) independently.
    Returns dict indicating which files were successfully generated.
    """
    channel_dir = CHANNELS_DIR / slug
    channel_dir.mkdir(parents=True, exist_ok=True)

    seed = random.randint(1, 99999)

    profile_ok = _generate_single(
        path=channel_dir / "profile_pic.png",
        prompt=_build_profile_pic_prompt(config),
        width=800,
        height=800,
        seed=seed,
    )

    # Wait between requests — Pollinations only allows 1 queued request per IP
    if profile_ok:
        print("[channel_art] Profile pic done, waiting 30s before banner request...")
        time.sleep(30)

    banner_ok = _generate_single(
        path=channel_dir / "banner.png",
        prompt=_build_banner_prompt(config),
        width=2048,
        height=1152,
        seed=seed + 1,
    )

    return {
        "profile_pic": "ok" if profile_ok else "failed",
        "banner": "ok" if banner_ok else "failed",
    }
