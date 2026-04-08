"""
layer4_video_production/video_assembler.py

Assembles a ready-to-upload vertical Short from:
  - Hero images (3-4 scene images generated from script's scene_descriptions)
  - Narration audio (WAV from Layer 3)
  - Burned captions (Pillow renders text per-frame — no ffmpeg drawtext needed)
  - Optional background music (from channels/{slug}/music/{mood}/)
  - End screen CTA (last 2 seconds, hardcoded short text)

Output: 1080x1920 H.264 .mp4 at channels/{slug}/videos/{video_id}.mp4

Frame generation approach:
  For each video frame, Pillow composites:
    1. Ken Burns crop of the current scene image (alternating zoom-in/out/pan per scene)
    2. Current caption text (white + black outline, semi-transparent pill background)
    3. CTA text in the last 2 seconds
  Frames are piped directly into ffmpeg (no temp files on disk).
"""

import bisect
import json
import re
import subprocess
from pathlib import Path
from typing import Optional

import numpy as np
import soundfile as sf
from PIL import Image, ImageDraw, ImageFont

from content_paths import (
    audio_acts_path,
    ensure_post_dirs,
    final_video_path,
    resolve_audio_wav_path,
    resolve_script_json_path,
)
from layer1_account_setup.config_schema import ChannelConfig
from database.queries import update_video_path
from layer4_video_production.image_generator import generate_scene_images
from layer4_video_production.thumbnail_generator import generate_thumbnail

BASE_DIR = Path(__file__).parent.parent
CHANNELS_DIR = BASE_DIR / "channels"

VIDEO_W, VIDEO_H = 1080, 1920
FPS = 25
MUSIC_VOLUME = 0.432
MUSIC_PUNCH_VOLUME = 0.432
CTA_DURATION = 3.5
CTA_TEXT = "Follow for more"

# How long each scene plays before cutting to the next (cycles if needed)
SCENE_CYCLE_SECONDS = 12.0

# ffmpeg binary — full path since Homebrew may not be in shell PATH
FFMPEG = next(
    (p for p in ["/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg", "ffmpeg"]
     if Path(p).exists() or p == "ffmpeg"),
    "ffmpeg",
)

# Font sizes
CAPTION_FONT_SIZE = 68
CTA_FONT_SIZE = 54
HEADER_FONT_SIZE = 150
OUTLINE_WIDTH = 3

# Header overlay
HEADER_ACCENT_COLOR = (220, 20, 60)   # Crimson red
HEADER_BAND_TOP = 20                  # px from top edge
HEADER_BAND_PADDING_V = 20           # vertical padding inside band

# Ken Burns motion types per scene (cycles if more scenes than entries)
SCENE_MOTIONS = ["zoom_in", "zoom_out", "pan_right", "zoom_in"]




# ── Captions ──────────────────────────────────────────────────────────────────

# Words that must never appear at the end of a caption chunk — they leave the
# viewer's brain hanging, waiting for the next chunk to complete the thought.
DANGLING_WORDS = {
    "the", "a", "an", "to", "of", "and", "but", "in",
    "with", "that", "this", "or", "for", "at", "by", "from", "on",
    "it", "its", "he", "she", "they", "we", "them", "him", "her", "me", "us",
}


def _fix_dangling_words(chunks: list[str]) -> list[str]:
    """
    Post-process chunk list so no chunk ends with a dangling article/preposition.
    Strategy: pull the first word of the next chunk into the current one.
    Edge case: if next chunk is a single word, merge the two entirely.
    Re-checks after each merge in case pulling a word creates a new dangling end.
    """
    result = list(chunks)
    i = 0
    while i < len(result) - 1:
        words = result[i].split()
        last = words[-1].lower().rstrip(".,!?'\"- ") if words else ""
        if last in DANGLING_WORDS:
            next_words = result[i + 1].split()
            if len(next_words) > 1:
                # pull first word of next chunk into current
                result[i] = " ".join(words + [next_words[0]])
                result[i + 1] = " ".join(next_words[1:])
                # re-check same index — new last word might also be dangling
                continue
            else:
                # next chunk is a single word — merge entirely
                result[i] = " ".join(words + next_words)
                result.pop(i + 1)
                continue
        i += 1
    return [c for c in result if c.strip()]


def _strip_ssml(text: str) -> str:
    clean = re.sub(r"<[^>]+>", "", text).strip()
    clean = clean.replace("\u2014", " - ").replace("\u2013", " - ")
    return clean


def _chunk_text(text: str, max_words: int = 5) -> list[str]:
    """
    Split text into caption-sized chunks that never cross sentence boundaries.
    - Sentence-ending punctuation (. ! ?) always forces a new chunk.
    - Commas are used as soft split points for long sentence segments.
    - Hard max of max_words per chunk.
    """
    # Split on sentence-ending punctuation, keeping punctuation with its sentence
    parts = re.split(r'(?<=[.!?])\s+', text.strip())
    chunks = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        words = part.split()
        if len(words) <= max_words:
            chunks.append(part)
            continue
        # Long sentence: try to split on commas first
        sub_parts = re.split(r'(?<=,)\s+', part)
        current: list[str] = []
        for sub in sub_parts:
            sub_words = sub.split()
            if len(current) + len(sub_words) <= max_words:
                current.extend(sub_words)
            else:
                if current:
                    chunks.append(" ".join(current))
                # Sub itself may exceed max_words — split by count
                for i in range(0, len(sub_words), max_words):
                    piece = sub_words[i:i + max_words]
                    if i + max_words < len(sub_words):
                        chunks.append(" ".join(piece))
                    else:
                        current = piece
        if current:
            chunks.append(" ".join(current))
    return _fix_dangling_words(chunks)


def _generate_captions(script_text: str, audio_duration: float, acts_path: Optional[Path] = None) -> list:
    """
    Caption chunks timed to audio, never breaking across sentence boundaries.

    1. Splits script on SSML <break> tags to identify acts.
    2. Uses per-act timing from acts_path (saved by Layer 3) for accurate sync.
       Falls back to proportional-by-word-count if no timing file exists.
    3. Within each act, uses _chunk_text() which respects sentence boundaries.
    """
    # Split into acts on SSML break tags (handles self-closing and paired tags)
    act_texts_raw = re.split(r'<break[^>]*/>', script_text, flags=re.IGNORECASE)
    act_texts = [_strip_ssml(a).strip() for a in act_texts_raw if _strip_ssml(a).strip()]
    if not act_texts:
        return []

    # Load per-act timing windows saved by audio_generator
    act_windows: list[dict] = []
    if acts_path and acts_path.exists():
        data = json.loads(acts_path.read_text())
        act_windows = data.get("acts", [])

    # Fallback: proportional by word count when timing file is absent / mismatched
    if not act_windows or len(act_windows) != len(act_texts):
        total_words = sum(len(a.split()) for a in act_texts)
        cursor = 0.0
        act_windows = []
        for act_text in act_texts:
            frac = len(act_text.split()) / max(total_words, 1)
            duration = frac * audio_duration
            act_windows.append({"start": round(cursor, 3), "end": round(cursor + duration, 3)})
            cursor += duration

    all_captions: list[dict] = []
    for act_text, window in zip(act_texts, act_windows):
        act_start = window["start"]
        act_end   = window["end"]
        act_dur   = max(act_end - act_start, 0.01)

        chunks = _chunk_text(act_text)
        if not chunks:
            continue

        chunk_dur = act_dur / len(chunks)
        for i, chunk in enumerate(chunks):
            all_captions.append({
                "text":  chunk,
                "start": round(act_start + i * chunk_dur, 3),
                "end":   round(act_start + (i + 1) * chunk_dur, 3),
            })

    return all_captions


def _caption_at(captions: list, t: float) -> Optional[str]:
    for cap in captions:
        if cap["start"] <= t < cap["end"]:
            return cap["text"]
    return None


# ── Music ─────────────────────────────────────────────────────────────────────

def _find_music_track(slug: str, music_mood_id: str) -> Optional[Path]:
    music_dir = CHANNELS_DIR / slug / "music" / music_mood_id
    if not music_dir.exists():
        return None
    for ext in ("*.mp3", "*.wav", "*.m4a"):
        tracks = list(music_dir.glob(ext))
        if tracks:
            return tracks[0]
    return None


# ── Font loading ──────────────────────────────────────────────────────────────

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


# ── Frame rendering ───────────────────────────────────────────────────────────

def _draw_outlined_text(draw: ImageDraw.ImageDraw, xy: tuple, text: str,
                         font: ImageFont.FreeTypeFont, outline: int = OUTLINE_WIDTH,
                         color=(255, 255, 255)) -> None:
    x, y = xy
    for dx in range(-outline, outline + 1):
        for dy in range(-outline, outline + 1):
            if dx != 0 or dy != 0:
                draw.text((x + dx, y + dy), text, font=font, fill="black")
    draw.text((x, y), text, font=font, fill=color)


def _wrap_text(text: str, font: ImageFont.FreeTypeFont, max_width: int) -> list:
    words = text.split()
    lines = []
    current = ""
    dummy_img = Image.new("RGB", (1, 1))
    draw = ImageDraw.Draw(dummy_img)

    for word in words:
        test = f"{current} {word}".strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def _render_header(
    frame: Image.Image,
    header_text: str,
    header_accent_words: list,
    header_font: ImageFont.FreeTypeFont,
) -> Image.Image:
    """
    Render a full-width black header band at the top of the frame.
    Words in header_accent_words render in crimson; all others in white.
    Both use black outline for legibility over any background.
    """
    accent_set = {w.lower().strip(".,!?\"'") for w in (header_accent_words or [])}
    lines = _wrap_text(header_text, header_font, VIDEO_W - 80)
    line_height = header_font.size + 8
    total_text_h = len(lines) * line_height

    band_top = HEADER_BAND_TOP
    band_bottom = band_top + HEADER_BAND_PADDING_V + total_text_h + HEADER_BAND_PADDING_V

    # Draw solid black band
    frame_rgba = frame.convert("RGBA")
    overlay = Image.new("RGBA", frame_rgba.size, (0, 0, 0, 0))
    draw_overlay = ImageDraw.Draw(overlay)
    draw_overlay.rectangle([0, band_top, VIDEO_W, band_bottom], fill=(0, 0, 0, 240))
    frame_rgba = Image.alpha_composite(frame_rgba, overlay)
    frame = frame_rgba.convert("RGB")

    draw = ImageDraw.Draw(frame)
    dummy_draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    space_w = dummy_draw.textbbox((0, 0), " ", font=header_font)[2]

    y = band_top + HEADER_BAND_PADDING_V
    for line in lines:
        words = line.split()
        word_widths = [
            dummy_draw.textbbox((0, 0), w, font=header_font)[2]
            for w in words
        ]
        total_line_w = sum(word_widths) + space_w * max(len(words) - 1, 0)
        x = (VIDEO_W - total_line_w) // 2

        for word, ww in zip(words, word_widths):
            is_accent = word.lower().strip(".,!?\"'") in accent_set
            color = HEADER_ACCENT_COLOR if is_accent else (255, 255, 255)
            _draw_outlined_text(draw, (x, y), word, header_font, color=color)
            x += ww + space_w
        y += line_height

    return frame


def _apply_ken_burns(base_img: Image.Image, motion: str, progress: float) -> Image.Image:
    """
    Apply Ken Burns motion to base_img based on motion type and progress (0.0→1.0).
    Returns a VIDEO_W x VIDEO_H RGB image.
    """
    iw, ih = base_img.size

    # Scale to fill the video frame
    scale_base = max(VIDEO_W / iw, VIDEO_H / ih)

    if motion == "zoom_in":
        progress_eased = 1 - (1 - progress) ** 2
        zoom = 1.12 + 0.16 * progress_eased
    elif motion == "zoom_out":
        zoom = 1.28 - 0.16 * progress
    elif motion == "pan_right":
        zoom = 1.15
    else:
        progress_eased = 1 - (1 - progress) ** 2
        zoom = 1.12 + 0.16 * progress_eased

    final_scale = scale_base * zoom
    scaled_w = int(iw * final_scale)
    scaled_h = int(ih * final_scale)
    scaled = base_img.resize((scaled_w, scaled_h), Image.LANCZOS)

    if motion == "pan_right":
        # Pan: shift crop center from left to right
        max_pan = int(scaled_w * 0.12)
        left = (scaled_w - VIDEO_W) // 2 - int(max_pan * (0.5 - progress))
    else:
        left = (scaled_w - VIDEO_W) // 2

    top = (scaled_h - VIDEO_H) // 2
    left = max(0, min(left, scaled_w - VIDEO_W))
    top = max(0, min(top, scaled_h - VIDEO_H))

    cropped = scaled.crop((left, top, left + VIDEO_W, top + VIDEO_H))
    return cropped.resize((VIDEO_W, VIDEO_H), Image.LANCZOS).convert("RGB")


def _render_frame(
    scene_imgs: list,
    scene_idx: int,
    scene_progress: float,
    caption: Optional[str],
    show_cta: bool,
    caption_font: ImageFont.FreeTypeFont,
    cta_font: ImageFont.FreeTypeFont,
    cta_text: str = CTA_TEXT,
    t: float = 0.0,
    hook_font: Optional[ImageFont.FreeTypeFont] = None,
    header_text: Optional[str] = None,
    header_accent_words: Optional[list] = None,
    header_font: Optional[ImageFont.FreeTypeFont] = None,
) -> bytes:
    """
    Render one video frame as raw RGB bytes.
    Applies per-scene Ken Burns and composites caption/CTA text with pill background.
    For t < 3.0s, uses hook_font (larger) and higher y position for visual punch.
    """
    motion = SCENE_MOTIONS[scene_idx % len(SCENE_MOTIONS)]
    base_img = scene_imgs[scene_idx]
    frame = _apply_ken_burns(base_img, motion, scene_progress)

    if caption:
        is_hook = t < 3.0 and hook_font is not None
        active_font = hook_font if is_hook else caption_font
        y_anchor = 0.45 if is_hook else 0.72

        lines = _wrap_text(caption, active_font, VIDEO_W - 140)
        line_height = active_font.size + 8
        total_h = len(lines) * line_height
        y_start = int(VIDEO_H * y_anchor) - total_h

        # Draw pill backgrounds first (requires RGBA)
        frame_rgba = frame.convert("RGBA")
        overlay = Image.new("RGBA", frame_rgba.size, (0, 0, 0, 0))
        draw_overlay = ImageDraw.Draw(overlay)
        padding = 14

        y = y_start
        dummy_draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
        for line in lines:
            bbox = dummy_draw.textbbox((0, 0), line, font=active_font)
            tw = bbox[2] - bbox[0]
            x = (VIDEO_W - tw) // 2
            draw_overlay.rounded_rectangle(
                [x - padding, y - padding, x + tw + padding, y + line_height + padding],
                radius=10, fill=(0, 0, 0, 160)
            )
            y += line_height

        frame_rgba = Image.alpha_composite(frame_rgba, overlay)
        frame = frame_rgba.convert("RGB")

        # Draw text on composited frame
        draw = ImageDraw.Draw(frame)
        y = y_start
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=active_font)
            tw = bbox[2] - bbox[0]
            x = (VIDEO_W - tw) // 2
            _draw_outlined_text(draw, (x, y), line, active_font)
            y += line_height

    if show_cta:
        cta_lines = _wrap_text(cta_text, cta_font, VIDEO_W - 120)
        line_height = cta_font.size + 10
        total_h = len(cta_lines) * line_height
        y_start = int(VIDEO_H * 0.80) - total_h // 2
        padding = 18

        frame_rgba = frame.convert("RGBA")
        overlay = Image.new("RGBA", frame_rgba.size, (0, 0, 0, 0))
        draw_overlay = ImageDraw.Draw(overlay)
        dummy_draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))

        y = y_start
        for line in cta_lines:
            bbox = dummy_draw.textbbox((0, 0), line, font=cta_font)
            tw = bbox[2] - bbox[0]
            x = (VIDEO_W - tw) // 2
            draw_overlay.rounded_rectangle(
                [x - padding, y - padding, x + tw + padding, y + line_height + padding],
                radius=14, fill=(255, 185, 0, 235)
            )
            y += line_height

        frame_rgba = Image.alpha_composite(frame_rgba, overlay)
        frame = frame_rgba.convert("RGB")

        draw = ImageDraw.Draw(frame)
        y = y_start
        for line in cta_lines:
            bbox = draw.textbbox((0, 0), line, font=cta_font)
            tw = bbox[2] - bbox[0]
            x = (VIDEO_W - tw) // 2
            _draw_outlined_text(draw, (x, y), line, cta_font, outline=2)
            y += line_height

    if header_text and header_font:
        frame = _render_header(frame, header_text, header_accent_words or [], header_font)

    return frame.tobytes()


# ── Assembly ──────────────────────────────────────────────────────────────────

def _assemble_with_ffmpeg(
    scene_imgs: list,
    audio_path: Path,
    music_path: Optional[Path],
    captions: list,
    audio_duration: float,
    output_path: Path,
    cta_text: str = CTA_TEXT,
    act_windows: Optional[list] = None,
    header_text: Optional[str] = None,
    header_accent_words: Optional[list] = None,
) -> None:
    total_duration = audio_duration + CTA_DURATION
    total_frames = int(total_duration * FPS)

    caption_font = _load_font(CAPTION_FONT_SIZE)
    hook_font = _load_font(96)
    cta_font = _load_font(CTA_FONT_SIZE)
    header_font = _load_font(HEADER_FONT_SIZE) if header_text else None

    num_scenes = len(scene_imgs)

    if music_path:
        cmd = [
            FFMPEG, "-y",
            "-f", "rawvideo", "-vcodec", "rawvideo",
            "-s", f"{VIDEO_W}x{VIDEO_H}", "-pix_fmt", "rgb24",
            "-r", str(FPS), "-i", "pipe:0",
            "-i", str(audio_path),
            "-stream_loop", "-1", "-i", str(music_path),
            "-filter_complex",
            f"[1:a]volume=1.0[narr];[2:a]volume='if(lt(t,3),{MUSIC_PUNCH_VOLUME},{MUSIC_VOLUME})'[music];[narr][music]amix=inputs=2:duration=first[aout]",
            "-map", "0:v", "-map", "[aout]",
            "-t", str(total_duration),
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-pix_fmt", "yuv420p",
            str(output_path),
        ]
    else:
        cmd = [
            FFMPEG, "-y",
            "-f", "rawvideo", "-vcodec", "rawvideo",
            "-s", f"{VIDEO_W}x{VIDEO_H}", "-pix_fmt", "rgb24",
            "-r", str(FPS), "-i", "pipe:0",
            "-i", str(audio_path),
            "-map", "0:v", "-map", "1:a",
            "-t", str(total_duration),
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-pix_fmt", "yuv420p",
            str(output_path),
        ]

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)

    act_boundaries = [w["start"] for w in act_windows] if act_windows else []

    for i in range(total_frames):
        t = i / FPS

        # Determine current scene and progress within it.
        if act_boundaries:
            act_idx = max(0, bisect.bisect_right(act_boundaries, t) - 1)
            scene_idx = act_idx % num_scenes
            act_start = act_windows[act_idx]["start"]
            act_end = act_windows[act_idx]["end"]
            act_dur = max(act_end - act_start, 0.001)
            scene_progress = min((t - act_start) / act_dur, 1.0)
        else:
            cycle_idx = int(t / SCENE_CYCLE_SECONDS)
            scene_idx = cycle_idx % num_scenes
            scene_start = cycle_idx * SCENE_CYCLE_SECONDS
            scene_progress = min((t - scene_start) / SCENE_CYCLE_SECONDS, 1.0)

        caption = _caption_at(captions, t)
        show_cta = t >= audio_duration

        frame_bytes = _render_frame(
            scene_imgs, scene_idx, scene_progress,
            caption, show_cta, caption_font, cta_font, cta_text,
            t=t, hook_font=hook_font,
            header_text=header_text, header_accent_words=header_accent_words,
            header_font=header_font,
        )
        proc.stdin.write(frame_bytes)

    proc.stdin.close()
    stderr = proc.stderr.read()
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed:\n{stderr.decode()[-2000:]}")


# ── Main entry point ──────────────────────────────────────────────────────────

def assemble_video(slug: str, video_id: int) -> Path:
    """
    Main entry point. Reads script + audio, fetches scene images, assembles .mp4.
    Updates videos.db on success. Returns path to the .mp4.
    """
    script_path = resolve_script_json_path(slug, video_id)
    if not script_path.exists():
        raise FileNotFoundError(f"No script at {script_path}")

    script_data = json.loads(script_path.read_text())

    audio_path = resolve_audio_wav_path(slug, video_id)
    if not audio_path.exists():
        raise FileNotFoundError(f"No audio at {audio_path} — run Layer 3 first")

    config_path = CHANNELS_DIR / slug / "channel_config.json"
    config = ChannelConfig(**json.loads(config_path.read_text()))

    audio_info = sf.info(str(audio_path))
    audio_duration = audio_info.duration

    # Resolve scene descriptions (backwards compat: old scripts only have image_prompt)
    scene_descriptions = script_data.get("scene_descriptions")
    if not scene_descriptions:
        scene_descriptions = [script_data["image_prompt"]]

    pexels_queries = script_data.get("pexels_queries") or []
    scene_paths = generate_scene_images(
        slug, video_id, scene_descriptions, CHANNELS_DIR,
        pexels_queries, subject=script_data.get("subject", "")
    )
    scene_imgs = [Image.open(p).convert("RGB") for p in scene_paths]

    acts_path = audio_acts_path(slug, video_id)
    captions = _generate_captions(script_data["script"], audio_duration, acts_path)

    act_windows: list[dict] = []
    if acts_path and acts_path.exists():
        act_windows = json.loads(acts_path.read_text()).get("acts", [])

    music_path = _find_music_track(slug, script_data.get("music_mood_id", ""))

    ensure_post_dirs(slug, video_id)
    output_path = final_video_path(slug, video_id)

    cta_text = script_data.get("cta_text") or CTA_TEXT
    header_text = script_data.get("header_text") or None
    header_accent_words = script_data.get("header_accent_words") or []
    _assemble_with_ffmpeg(
        scene_imgs=scene_imgs,
        audio_path=audio_path,
        music_path=music_path,
        captions=captions,
        audio_duration=audio_duration,
        output_path=output_path,
        cta_text=cta_text,
        act_windows=act_windows,
        header_text=header_text,
        header_accent_words=header_accent_words,
    )

    update_video_path(video_id, str(output_path))

    try:
        generate_thumbnail(
            slug, video_id, script_data.get("title", ""),
            CHANNELS_DIR, channel_name=config.channel_name,
        )
    except Exception as e:
        print(f"  thumbnail: skipped ({e})")

    return output_path
