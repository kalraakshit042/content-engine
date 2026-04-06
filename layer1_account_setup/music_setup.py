"""
layer1_account_setup/music_setup.py

Music folder validation for channel setup.

Music tracks are channel-level assets — 1 track per mood, sourced once when
the channel is created, reused across every video on that channel forever.

Flow:
  1. After config generation, call ensure_music_dirs() to create the folders.
  2. User drops 1 MP3/WAV/M4A per mood into each folder (from YT Audio Library).
  3. Dashboard polls /api/channel/{slug}/music-status to show live checkmarks.
  4. Once music_setup_complete() returns True, user can activate the channel.
"""

import json
from pathlib import Path

CHANNELS_DIR = Path(__file__).parent.parent / "channels"
AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".ogg", ".flac"}

# Keyword → YT Audio Library search terms mapping.
# Checked against mood_id + description (lowercase). First match wins.
_SEARCH_MAP = [
    ("noir",        ["dark jazz noir", "crime jazz", "tense piano strings"]),
    ("ominous",     ["dark ambient ominous", "horror drone", "ominous tension"]),
    ("orchestral",  ["epic orchestral dramatic", "cinematic tension strings", "dramatic film score"]),
    ("dramatic",    ["dramatic orchestral", "epic cinematic", "film score tension"]),
    ("minimalist",  ["minimal tension", "suspense drone loop", "ambient suspense"]),
    ("tense",       ["thriller suspense", "tense underscore", "psychological tension"]),
    ("synth",       ["dark synthwave", "retro horror synth", "analog synth dark"]),
    ("baroque",     ["baroque harpsichord dramatic", "classical organ dark", "baroque dramatic"]),
    ("indie",       ["indie melancholy", "moody acoustic guitar", "introspective indie"]),
    ("contemplat",  ["moody guitar sparse", "melancholy piano", "indie sad instrumental"]),
]


def _search_terms_for(mood_id: str, description: str) -> list[str]:
    # Check mood_id first (most specific), then description
    mid = mood_id.lower()
    desc = description.lower()
    for keyword, terms in _SEARCH_MAP:
        if keyword in mid:
            return terms
    for keyword, terms in _SEARCH_MAP:
        if keyword in desc:
            return terms
    # Fallback: first 3 significant words of description
    words = [w for w in description.split() if len(w) > 3][:3]
    return [" ".join(words)] if words else [mood_id.replace("_", " ")]


def ensure_music_dirs(slug: str) -> None:
    """Create all mood music directories so the user can drop files straight in."""
    config_path = CHANNELS_DIR / slug / "channel_config.json"
    if not config_path.exists():
        return
    config = json.loads(config_path.read_text())
    for mood in config.get("music_moods", []):
        (CHANNELS_DIR / slug / "music" / mood["id"]).mkdir(parents=True, exist_ok=True)


def validate_music_folders(slug: str) -> dict:
    """
    Check which mood folders have at least one audio track.

    Returns:
        {
          "noir_ominous": {
              "has_track": True,
              "description": "Dark jazzy instrumental...",
              "folder": "/abs/path/to/channels/slug/music/noir_ominous",
              "search_terms": ["dark jazz noir", "crime jazz", "tense piano strings"],
          },
          ...
        }
    """
    config_path = CHANNELS_DIR / slug / "channel_config.json"
    if not config_path.exists():
        return {}

    config = json.loads(config_path.read_text())
    result: dict = {}

    for mood in config.get("music_moods", []):
        mood_id = mood["id"]
        mood_dir = CHANNELS_DIR / slug / "music" / mood_id

        has_track = False
        if mood_dir.exists():
            has_track = any(
                f.suffix.lower() in AUDIO_EXTENSIONS
                for f in mood_dir.iterdir()
                if f.is_file()
            )

        result[mood_id] = {
            "has_track": has_track,
            "description": mood.get("description", ""),
            "folder": str(mood_dir),
            "search_terms": _search_terms_for(mood_id, mood.get("description", "")),
        }

    return result


def music_setup_complete(slug: str) -> bool:
    """True if every mood folder has at least one audio file."""
    status = validate_music_folders(slug)
    return bool(status) and all(v["has_track"] for v in status.values())
