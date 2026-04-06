"""
layer1_account_setup/music_downloader.py

Downloads one CC0-licensed background track per mood from Freesound.org.
Called once at channel setup — tracks are reused across all videos.

Fail policy: raises on any failure. No silent fallbacks.
If a mood can't be filled, channel setup errors out.

Freesound API docs: https://freesound.org/docs/api/
"""

import os
import subprocess
import time
import httpx
from pathlib import Path

FREESOUND_SEARCH = "https://freesound.org/apiv2/search/text/"

FFMPEG = next(
    (p for p in ["/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg", "ffmpeg"]
     if Path(p).exists() or p == "ffmpeg"),
    "ffmpeg",
)
CHANNELS_DIR = Path(__file__).parent.parent / "channels"
MIN_DURATION_SEC = 30   # tracks shorter than this won't loop well
MAX_DURATION_SEC = 600  # 10 min cap — anything longer is overkill

# Broad fallback terms tried after all mood-specific terms fail.
# Ordered from most-thematic to most-generic. Guaranteed to have CC0 results.
GENERIC_FALLBACKS = [
    "dark dramatic music",
    "tense atmospheric music",
    "suspense background music",
    "dark ambient loop",
    "dramatic cinematic music",
]


def _api_key() -> str:
    key = os.environ.get("FREESOUND_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "FREESOUND_API_KEY not set in .env — "
            "register at https://freesound.org/apiv2/apply/ (free)"
        )
    return key


def _search(query: str, token: str) -> dict:
    """
    Search Freesound for CC0 tracks matching query.
    Returns the API response dict.
    Raises on HTTP error or empty results.
    """
    resp = httpx.get(
        FREESOUND_SEARCH,
        params={
            "query":     query,
            "token":     token,
            "filter":    f'license:"Creative Commons 0" duration:[{MIN_DURATION_SEC} TO {MAX_DURATION_SEC}]',
            "fields":    "id,name,duration,previews,license",
            "sort":      "rating_desc",
            "page_size": 10,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _download_track(url: str, dest: Path, token: str) -> None:
    """Download MP3 from Freesound and strip any leading silence."""
    resp = httpx.get(url, params={"token": token}, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)

    # Write raw bytes to a temp file then strip leading silence via ffmpeg
    tmp = dest.with_suffix(".raw.mp3")
    tmp.write_bytes(resp.content)
    subprocess.run(
        [
            FFMPEG, "-y", "-i", str(tmp),
            "-af", "silenceremove=start_periods=1:start_duration=0.1:start_threshold=-50dB",
            str(dest),
        ],
        check=True,
        capture_output=True,
    )
    tmp.unlink()


def download_mood_track(slug: str, mood_id: str, search_terms: list[str]) -> Path:
    """
    Find and download one CC0 track for a single mood.

    Tries each search term in order until a result is found.
    Downloads the HQ MP3 preview (128kbps — fine for background at 15% vol).
    Saves to channels/{slug}/music/{mood_id}/track.mp3.

    Raises RuntimeError if no track found across all search terms.
    Raises httpx.HTTPError on network/API failure.
    """
    token = _api_key()
    dest = CHANNELS_DIR / slug / "music" / mood_id / "track.mp3"

    if dest.exists():
        print(f"  [music] {mood_id} already cached, skipping")
        return dest

    last_error: Exception | None = None

    for term in search_terms:
        print(f"  [music] {mood_id} — searching '{term}'...")
        try:
            data = _search(term, token)
        except Exception as e:
            last_error = e
            print(f"  [music] {mood_id} — search failed: {e}")
            continue

        results = data.get("results", [])
        if not results:
            print(f"  [music] {mood_id} — no CC0 results for '{term}'")
            continue

        # Pick the top-rated result that has an HQ preview
        for sound in results:
            previews = sound.get("previews", {})
            mp3_url = previews.get("preview-hq-mp3")
            if not mp3_url:
                continue

            print(f"  [music] {mood_id} — downloading '{sound['name']}' ({sound['duration']:.0f}s)...")
            _download_track(mp3_url, dest, token)
            size_kb = dest.stat().st_size // 1024
            print(f"  [music] {mood_id} saved ({size_kb}KB) ✓")
            return dest

        # Short pause between search terms to be polite to the API
        time.sleep(1)

    # Mood-specific terms exhausted — try broad fallbacks before giving up
    print(f"  [music] {mood_id} — specific terms exhausted, trying generic fallbacks...")
    for term in GENERIC_FALLBACKS:
        print(f"  [music] {mood_id} — searching '{term}'...")
        try:
            data = _search(term, token)
        except Exception as e:
            last_error = e
            continue

        for sound in data.get("results", []):
            mp3_url = sound.get("previews", {}).get("preview-hq-mp3")
            if not mp3_url:
                continue
            print(f"  [music] {mood_id} — downloading '{sound['name']}' ({sound['duration']:.0f}s) [fallback]...")
            _download_track(mp3_url, dest, token)
            size_kb = dest.stat().st_size // 1024
            print(f"  [music] {mood_id} saved ({size_kb}KB) ✓")
            return dest

        time.sleep(1)

    raise RuntimeError(
        f"Could not find any CC0 track for mood '{mood_id}' "
        f"(tried specific: {search_terms}, generic: {GENERIC_FALLBACKS}). "
        f"Last error: {last_error}"
    )


def download_all_moods(slug: str, music_moods: list[dict]) -> None:
    """
    Download one track for every mood defined in the channel config.

    music_moods: list of MusicMood dicts from channel_config.json
                 Each must have 'id', 'description', 'search_terms'.

    Raises immediately on the first failure — channel setup errors out.
    """
    from layer1_account_setup.music_setup import _search_terms_for

    print(f"  [music] downloading {len(music_moods)} mood track(s)...")
    for mood in music_moods:
        mood_id = mood["id"]
        search_terms = mood.get("search_terms") or _search_terms_for(
            mood_id, mood.get("description", "")
        )
        download_mood_track(slug, mood_id, search_terms)

    print(f"  [music] all {len(music_moods)} tracks ready ✓")
