# Content Engine — Project Log

**IMPORTANT FOR ALL LLMs:** Read this file at the start of every session. Update it at the end of every session (or when switching). This is the single source of truth for project state across multiple AI sessions.

---

## What This Project Is

A fully automated YouTube Shorts content engine. The only human input is a channel name + one-line description. Everything else — creative config, scripts, audio, images, video assembly, and eventually publishing — is automated.

**Test channel:** Villain Monologues (`villian-monologues` slug — typo is intentional, keep it)
**Dashboard:** `python3 -m uvicorn web_dashboard:app --port 8000 --reload` → localhost:8000

---

## Architecture: 5 Layers

```
Layer 1: Account Setup     → channel_config.json + channel art + music tracks
Layer 2: Script Generation → script.json (Claude Haiku API)
Layer 3: Audio Production  → audio.wav (Kokoro TTS, local, free)
Layer 4: Video Production  → scene images + final.mp4 (Pexels API + Pillow + ffmpeg)
Layer 5: Publishing        → YouTube upload (not yet built)
```

---

## Current Status (as of last update)

| Layer | Status | Notes |
|-------|--------|-------|
| 1 | ✅ Complete | config, channel art, music download all working |
| 2 | ✅ Complete | scripts with SSML, scene_descriptions, pexels_queries |
| 3 | ✅ Complete | Kokoro TTS + atempo normalization to exactly 63.0s |
| 4 | ✅ Complete | Pexels images + Pillow styling + full video assembly |
| 5 | ❌ Not started | YouTube OAuth credentials exist at credentials/google_client_secret.json |

**Last generated video:** video_id=7 (oven, theatrical_shakespearean tone)

---

## File Layout

### Per-post assets (NEW layout — use this)
```
channels/{slug}/posts/{video_id}/
├── script.json          ← Layer 2 output
├── audio.wav            ← Layer 3 output
├── audio_acts.json      ← Layer 3 output (per-act timing for captions)
├── images/
│   ├── scene0.png       ← Layer 4 (Pexels + styled)
│   ├── scene1.png
│   └── ...
├── final.mp4            ← Layer 4 output
└── thumbnail.jpg        ← Layer 4 output
```

### Legacy asset paths (still supported via resolve_* functions in content_paths.py)
```
channels/{slug}/scripts/{video_id}.json
channels/{slug}/audio/{video_id}.wav
channels/{slug}/images/{video_id}_scene{i}.png
channels/{slug}/videos/{video_id}.mp4
```

### Channel-level assets
```
channels/{slug}/
├── channel_config.json
├── profile_pic.png
├── banner.png
└── music/
    └── {mood_id}/
        └── track.mp3    ← Downloaded from Freesound at channel setup
```

### Path helpers
All path logic is in `content_paths.py`. Always use the `resolve_*` functions when reading (they check new layout first, then legacy). Use the direct `*_path()` functions when writing new files.

---

## Key Technical Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Script writer | Claude Haiku | Cheapest, fast enough |
| TTS | Kokoro (local) | Free, decent quality, no API |
| Duration guarantee | ffmpeg atempo to 63.0s | Normalize after generation |
| Image generation | Pexels API + Pillow | SDXL on MPS was too slow (2.5 min/image). Pexels is instant and free |
| Image styling | Pillow post-processing | Dark overlay + warm grade + grain + vignette makes stock photos look cinematic |
| Captions | Pillow per-frame (no ffmpeg drawtext) | ffmpeg 8.1 Homebrew build has no libfreetype |
| Video piping | rawvideo → ffmpeg stdin | No temp files, efficient |
| Music | Freesound API download at setup | Pre-downloaded per mood, royalty-free |

---

## Environment Variables (all in .env)

```
ANTHROPIC_API_KEY=...       ← Required: Claude for scripts + creative director
PEXELS_API_KEY=...          ← Required: image generation (already set)
FREESOUND_API_KEY=...       ← Required: music download (already set)
PIXABAY_API_KEY=...         ← Optional: Pixabay fallback for images
```

---

## Script JSON Schema (Layer 2 output)

```json
{
  "title": "string",
  "subject": "string",
  "script": "string (SSML with <break> tags between acts)",
  "image_prompt": "string (= scene_descriptions[0], backwards compat)",
  "scene_descriptions": ["string × 6"],
  "pexels_queries": ["3-5 keyword search string × 6"],
  "description": "string",
  "hashtags": ["string"],
  "tone_id": "string",
  "visual_style_id": "string",
  "voice_style_id": "string",
  "music_mood_id": "string"
}
```

---

## Video Assembly Pipeline (Layer 4)

**Input:** script.json + audio.wav + audio_acts.json + scene images + music track
**Output:** final.mp4 (1080×1920, 65 seconds, H.264)

1. Load 6 scene images (Pexels + styled, cached as scene0-5.png)
2. Generate captions from script SSML + act timing from audio_acts.json
3. For each frame (25fps × 65s = 1625 frames):
   - Determine current scene (cycles every 12s: `scene_idx = (t // 12) % num_scenes`)
   - Apply Ken Burns motion (zoom_in → zoom_out → pan_right → zoom_in, alternating per scene)
   - Draw caption pill + text if within caption window
   - Draw "Follow for more" CTA if t >= 63s (last 2 seconds)
4. Pipe raw RGB frames to ffmpeg stdin
5. ffmpeg mixes: video stream + narration (100%) + music (7% volume, 3s fade-in)
6. Output: final.mp4

**Total video duration:** 63s audio + 2s CTA = 65s exactly

---

## Caption System

- Script split into 4 acts on `<break time="600ms"/>` SSML tags
- Each act has a precise time window from `audio_acts.json` (saved by Layer 3)
- Within each act: `_chunk_text()` splits on sentence boundaries (never breaks mid-sentence)
- `_fix_dangling_words()` ensures no chunk ends on "the", "a", "and", etc.
- Caption position: `VIDEO_H * 0.72` (clears YouTube Shorts right-side buttons — was 0.62 which overlapped Like/Share zone)
- Style: Impact font 68px, white text, black outline, semi-transparent pill background

---

## Known Issues / Limitations

- **Pexels keyword extraction is dumb**: takes first 4 non-jargon words. Now improved: Claude also outputs `pexels_queries` array with human-specified search terms per scene.
- **Pexels free tier**: 20,000 req/month. Fine for now. At 100 channels × 2 videos × 6 images = 36,000/month → request limit increase or add Pixabay fallback (already implemented).
- **Captions still proportional within acts**: Whisper word-level timestamps would be more accurate. Not yet implemented.
- **Voice flatness**: Kokoro is decent. ElevenLabs would be better but costs money.
- **Music volume**: 7% (MUSIC_VOLUME=0.07) — reduced from 15% after it was too loud.

---

## What's Next (Priority Order)

1. **Change 3 — Scene transitions on act boundaries** (`video_assembler.py`)
   - Replace 12s timer with bisect lookup against audio_acts.json act boundaries
   - Do alongside Change 5 (both touch same code area)

2. **Change 5 — Ease-out Ken Burns** (`video_assembler.py`, `_apply_ken_burns`)
   - Start zoom_in at 1.12×, quadratic ease-out: `progress_eased = 1 - (1-progress)**2`
   - zoom = 1.12 + 0.16 * progress_eased

3. **Change 6 — Music punch at t=0** (`video_assembler.py`, ffmpeg filter_complex)
   - Hit 0.25 volume at t=0, duck to 0.07 at t=3
   - Use `volume='if(lt(t,3),0.25,0.07)'` time expression — NOT afade (wrong tool for this)

4. **Wire "Generate Video" button** in dashboard (`web_dashboard.py` + `templates/channel_detail.html`)
   - Backend route already exists at `POST /channel/{slug}/generate-video`
   - Just needs the button added to the Videos card header in the template (3 lines of HTML)

5. **Layer 5: YouTube upload**
   - OAuth credentials at `credentials/google_client_secret.json`
   - YouTube channel: @VillainMonologuesolo

6. **Whisper caption sync** — word-level timestamps instead of proportional

7. **Scheduler** — auto-generate + auto-upload on schedule

---

## How to Generate a Full Video (Terminal)

```bash
cd "/Users/akshitkalra/Code/Content automation/content-engine"

python3 -c "
from database.queries import insert_video
from layer2_script_generation.script_generator import generate_script
from layer3_audio_production.audio_generator import generate_audio
from layer4_video_production.video_assembler import assemble_video

vid = insert_video('villian-monologues')
print(f'Video ID: {vid}')

data = generate_script('villian-monologues', vid)
print(f'Subject: {data[\"subject\"]} | Tone: {data[\"tone_id\"]}')

generate_audio('villian-monologues', vid)
print('Audio done')

path = assemble_video('villian-monologues', vid)
print(f'Done: {path}')
"
```

---

## Session Log

### Session 1 (earlier)
- Built Layers 1-3 from scratch
- Layer 1: FastAPI dashboard, SQLite, channel config generation, channel art
- Layer 2: Script generation with narrative bible, SSML, 4-act structure
- Layer 3: Kokoro TTS replacing Edge TTS, VoiceStrategy, per-act speed variation

### Session 2 (earlier)
- Built Layer 4 initial version (single image, basic captions)
- First video was bad: static image, broken captions, no music

### Session 3 (this arc)
- Fixed Layer 4: multiple scenes, Ken Burns, sentence-boundary captions, CTA text-only
- Added Freesound music download at channel setup
- Added atempo normalization in Layer 3 (guaranteed 63.0s audio)
- Fixed script quality: concrete detail rule, channel-specific guardrails
- Replaced SDXL image generation with Pexels API + Pillow styling (15 min → 12 sec)
- Another LLM added: content_paths.py (new canonical path layout), thumbnail_generator.py, Pixabay fallback, pexels_queries in script JSON, MUSIC_VOLUME reduced to 0.07

### Session 4 (this session)
**Changes shipped:**
1. **Hook font at t<3s** (`video_assembler.py`): caption renders at 96px (vs 68px normal) for first 3 seconds, positioned at y_anchor=0.45. After 3s drops to normal 68px at 0.72.
2. **Caption position** (`video_assembler.py`): moved from `VIDEO_H * 0.62` → `VIDEO_H * 0.72` to clear YouTube Shorts right-side buttons (Like/Dislike/Share) which overlap at 55–62%.
3. **Script hook constraint** (`script_generator.py`): Added `HARD RULE: The very first sentence must be 6 words or fewer.` with worked examples (good and bad). Added as standalone line after Act 1 prose, not buried in the paragraph — model was ignoring it when embedded. Test confirmed: "I control your evening." (4 words), "I own your commute." (4 words).
4. **Better Pexels queries** (`script_generator.py`): Changed pexels_queries instruction from "use simple nouns" → "include one specific detail: unusual angle, precise material/texture, or lighting condition." Updated examples. Queries now like `"ethernet cable macro close-up coiled tight metal"` vs old `"cable desk"`.

**Test videos generated:** video_id 25 (digital-overlords, self-driving car), 26 (teacher surveillance system), 27 (cable provider — used for caption position test). All marked `test_only` / `youtube_status=skipped`. Test files at `/Users/akshitkalra/Code/Content automation/test_video_27_v2.mp4`.

**Pending changes (proposed by external review, not yet implemented):**

**Change 3 — Scene transitions on act boundaries** (`video_assembler.py`)
Currently scene cuts happen every 12s on a timer. Should cut when the narrative shifts (act boundaries from audio_acts.json). Implementation: replace `scene_idx = int(t / 12) % num_scenes` with bisect lookup against act boundary timestamps. Complexity: ~15 lines (not 5 as claimed). Need to handle: 4 acts → 6 scenes mapping, Ken Burns progress calculation changes from fixed 12s window to variable act duration window.

**Change 5 — Ease-out Ken Burns + start at 1.15×** (`video_assembler.py`, `_apply_ken_burns`)
Currently zoom_in goes 1.0→1.22 linearly — looks static for first ~1s because delta is imperceptible. Fix: start at 1.12×, end at 1.28×, apply quadratic ease-out: `progress_eased = 1 - (1 - progress) ** 2`. First 3 seconds should feel 3× faster. Change: `zoom = 1.12 + 0.16 * progress_eased` for zoom_in. Do this at same time as Change 3 since both touch `_apply_ken_burns`.

**Change 6 — Music punch at t=0 then duck** (`video_assembler.py`, ffmpeg filter_complex)
Currently music fades in over 3s (`afade=in:st=0:d=3`). Should hit full volume at t=0 then duck to 0.07 at t=3. ffmpeg filter for this: `volume='if(lt(t,3),0.25,0.07)'` using volume filter with time expression. The `afade` approach won't work for this pattern — need `volume` with `enable` expression or `aeval`. Exact filter string needs testing.

