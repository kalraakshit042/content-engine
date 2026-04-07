"""
layer3_audio_production/audio_generator.py

Generates a WAV narration for a scripted video using Kokoro TTS.
Reads the script JSON, resolves the correct voice via the channel's
voice_strategy, splits on <break> tags into acts, applies per-act
speed variation, concatenates, and writes a .wav file.
"""

import json
import re
import sqlite3
import subprocess
from pathlib import Path

import numpy as np
import soundfile as sf
from kokoro import KPipeline

from content_paths import (
    audio_acts_path,
    audio_wav_path,
    ensure_post_dirs,
    resolve_script_json_path,
)
from layer1_account_setup.config_schema import ChannelConfig

BASE_DIR = Path(__file__).parent.parent
CHANNELS_DIR = BASE_DIR / "channels"

# Atempo constants — reserved for future TikTok assembly (not used for YouTube).
# Natural TTS duration is used directly; word-count enforcement keeps it in range.
_TIKTOK_TARGET_SECONDS = 63.0
_TIKTOK_ATEMPO_MIN = 0.75
_TIKTOK_ATEMPO_MAX = 1.15  # above 1.15 Kokoro sounds noticeably robotic

FFMPEG = next(
    (p for p in ["/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg", "ffmpeg"]
     if Path(p).exists() or p == "ffmpeg"),
    "ffmpeg",
)

# Per-act speed offsets applied on top of the voice's base speed.
# Hook is slightly faster and punchy; build is neutral; re-hook slows
# for dramatic tension; peak is slowest and most intense.
ACT_SPEED_OFFSETS = [+0.10, 0.0, -0.05, -0.08]

# Kokoro sample rate
SAMPLE_RATE = 24000

# Silence between acts (0.6 seconds of zeros)
SILENCE_FRAMES = int(SAMPLE_RATE * 0.6)


def _strip_ssml(text: str) -> str:
    """Remove all XML/SSML tags, leaving only spoken text."""
    return re.sub(r"<[^>]+>", "", text).strip()


def _resolve_voice(config: ChannelConfig, script_data: dict) -> tuple[str, float]:
    """
    Returns (kokoro_voice_id, speed) based on the channel's voice_strategy.

    For "single" channels: always returns the default voice.
    For "tone_mapped" channels: looks up the tone used in this video and
    maps it to the appropriate voice_style, falling back to the default.
    """
    strategy = config.voice_strategy
    tone_id = script_data.get("tone_id", "")

    if strategy.strategy_type == "tone_mapped" and tone_id:
        voice_style_id = strategy.tone_to_voice_map.get(tone_id, strategy.default_voice_id)
    else:
        voice_style_id = strategy.default_voice_id

    voice_style = next(
        (v for v in config.voice_styles if v.id == voice_style_id),
        config.voice_styles[0],
    )
    return voice_style.tts_settings.voice_id, voice_style.tts_settings.speed


def _synthesize_act(pipeline: KPipeline, text: str, voice_id: str, speed: float) -> np.ndarray:
    """Synthesize one act of text and return a numpy float32 array."""
    segments = []
    for _, _, audio in pipeline(text, voice=voice_id, speed=speed, split_pattern=r"\n+"):
        segments.append(audio)
    if not segments:
        return np.zeros(SILENCE_FRAMES, dtype=np.float32)
    return np.concatenate(segments)


def _normalize_duration(wav_path: Path, act_windows: list, target_seconds: float = _TIKTOK_TARGET_SECONDS) -> tuple[Path, list]:
    """
    Reserved for TikTok assembly — not called for YouTube.
    Applies ffmpeg atempo to compress audio to target_seconds and scales
    act_windows proportionally so caption sync stays correct.
    Caps atempo at _TIKTOK_ATEMPO_MAX (above that Kokoro sounds robotic).
    """
    info = sf.info(str(wav_path))
    actual = info.duration

    ratio = actual / target_seconds  # > 1 means too long → speed up
    if abs(ratio - 1.0) <= 0.05:
        return wav_path, act_windows  # within 5% tolerance, no change

    if ratio < _TIKTOK_ATEMPO_MIN or ratio > _TIKTOK_ATEMPO_MAX:
        raise RuntimeError(
            f"Audio is {actual:.1f}s but target is {target_seconds}s. "
            f"Required atempo {ratio:.3f} is outside [{_TIKTOK_ATEMPO_MIN}, {_TIKTOK_ATEMPO_MAX}]. "
            f"Script is too {'long' if ratio > 1 else 'short'} to normalize safely."
        )

    tmp = wav_path.with_suffix(".tmp.wav")
    wav_path.rename(tmp)
    result = subprocess.run(
        [FFMPEG, "-y", "-i", str(tmp), "-af", f"atempo={ratio:.6f}", str(wav_path)],
        capture_output=True,
    )
    tmp.unlink()
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg atempo failed:\n{result.stderr.decode()[-1000:]}")

    # Scale all act window timestamps by the same ratio
    scaled = [
        {"start": round(w["start"] / ratio, 3), "end": round(w["end"] / ratio, 3)}
        for w in act_windows
    ]
    return wav_path, scaled


def generate_audio(slug: str, video_id: int) -> Path:
    """
    Main entry point. Reads script JSON, generates .wav, updates videos.db.
    Returns the path to the generated audio file.
    """
    script_path = resolve_script_json_path(slug, video_id)
    if not script_path.exists():
        raise FileNotFoundError(f"No script found at {script_path}")

    script_data = json.loads(script_path.read_text())
    script_text = script_data["script"]

    config_path = CHANNELS_DIR / slug / "channel_config.json"
    config = ChannelConfig(**json.loads(config_path.read_text()))

    voice_id, base_speed = _resolve_voice(config, script_data)

    # Determine lang_code from voice_id prefix: 'b' prefix = British, else American
    lang_code = "b" if voice_id.startswith("b") else "a"
    pipeline = KPipeline(lang_code=lang_code)

    # Split into acts on <break> tags, strip all SSML from each act
    raw_acts = re.split(r"<break[^/]*/>", script_text)
    acts = [_strip_ssml(a) for a in raw_acts if _strip_ssml(a)]

    silence = np.zeros(SILENCE_FRAMES, dtype=np.float32)
    audio_parts = []
    act_durations = []  # seconds of speech per act (excluding silence gaps)

    for i, act_text in enumerate(acts):
        offset = ACT_SPEED_OFFSETS[i] if i < len(ACT_SPEED_OFFSETS) else ACT_SPEED_OFFSETS[-1]
        act_speed = round(base_speed + offset, 2)
        act_audio = _synthesize_act(pipeline, act_text, voice_id, act_speed)
        act_durations.append(len(act_audio) / SAMPLE_RATE)
        audio_parts.append(act_audio)
        if i < len(acts) - 1:
            audio_parts.append(silence)

    combined = np.concatenate(audio_parts)

    ensure_post_dirs(slug, video_id)
    output_path = audio_wav_path(slug, video_id)

    sf.write(str(output_path), combined, SAMPLE_RATE)

    # Save act timing metadata for caption sync in Layer 4
    silence_sec = SILENCE_FRAMES / SAMPLE_RATE
    act_windows = []
    cursor = 0.0
    for i, dur in enumerate(act_durations):
        act_windows.append({"start": round(cursor, 3), "end": round(cursor + dur, 3)})
        cursor += dur
        if i < len(act_durations) - 1:
            cursor += silence_sec

    # NOTE: atempo normalization is intentionally skipped for YouTube.
    # Natural TTS duration is used directly — ~45-55s at current speeds.
    # _normalize_duration() is preserved below for future TikTok assembly use.

    timing_path = audio_acts_path(slug, video_id)
    timing_path.write_text(json.dumps({"acts": act_windows, "texts": acts}))

    _update_audio_path(video_id, str(output_path))
    return output_path


def _update_audio_path(video_id: int, audio_path: str) -> None:
    db_path = BASE_DIR / "database" / "videos.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE videos SET audio_path = ?, status = 'audio_done' WHERE id = ?",
        (audio_path, video_id),
    )
    conn.commit()
    conn.close()
