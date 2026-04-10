# Content Engine

Fully automated YouTube Shorts factory — topic to upload, zero human input after setup.

**Status: Live and actively iterating.**

| Metric | Value |
|--------|-------|
| Launch date | April 5, 2026 |
| Active channels | 2 |
| Total views (as of Apr 9) | 300+ |
| Average stayed-to-watch | ~30% |
| Average view duration | ~24% of video length |

*Updated weekly.*

---

## What This Is

A solo experiment: can one person run a multi-channel short-form content
operation at scale with zero production overhead?

Two live YouTube channels. One video per day each. Every step automated —
subject selection, scripting, voiceover, video assembly, publishing,
and failure recovery. The only human input is the initial channel setup.
If the unit economics hold, the architecture is designed to add new channels
with no additional engineering — just a new config file.

---

## What I've Learned So Far

The pipeline isn't the hard part. The feedback loop is. Building the system
took four days. Understanding why the output wasn't performing took longer.

Early videos had 70%+ swipe-away rates. Diagnosing why required reading
YouTube Studio retention curves and tracing problems back through every layer:

- **Script too long** → TTS too slow → video 1:13 instead of 45s → retention collapses
- **Background music inaudible** at 7% volume → video feels sterile, no atmosphere
- **0.6s silence gaps** between TTS acts stacking with Kokoro trailing silence → dead air = instant swipe
- **Pexels queries too literal** → plain stock photos instead of atmospheric visuals
- **Word count prompts ignored by Claude Haiku** → total limits don't work, per-act budgets + validation retry loop do

Each fix required understanding the full stack — prompt engineering,
audio mixing, ffmpeg filter graphs, YouTube's seed batch mechanics,
and how the algorithm weights retention vs rewatch vs absolute watch time.

Still iterating. Numbers are moving in the right direction.

---

## Architecture

```
Layer 1: Channel Setup       → layer1_account_setup/
Layer 2: Script Generation   → layer2_script_generation/
Layer 3: Audio Production    → layer3_audio_production/
Layer 4: Video Assembly      → layer4_video_production/
Layer 5: Publishing          → layer5_publishing/
Dashboard                    → web_dashboard.py
Scheduler                    → scheduler.py
State                        → SQLite (4 databases)
```

### Layer 2 — Script Generation (Claude Haiku)
- Per-act word budgets with hard ceilings (not total word counts — Haiku ignores those)
- 3-attempt validation retry loop: if any act exceeds budget, violation is fed back into next prompt
- Fallback: pick least-bad attempt on third failure
- Output: `script.json` with 4 acts — Hook, Build, Re-hook, Peak

*Prompt design matters as much as the code — per-act hard ceilings with explicit violation feedback outperform total word count instructions for length control with Haiku.*

### Layer 3 — Audio Production (Kokoro TTS)
- Runs locally, zero per-video API cost
- Per-act speed tuning: Hook +0.10 faster, Peak -0.08 slower for dramatic effect
- 0.6s silence gaps between acts
- Output: `audio.wav` + `audio_acts.json` (exact timestamps for caption sync)

### Layer 4 — Video Assembly (ffmpeg + Pillow)
- Pexels image queries are atmospheric not literal ("dramatic shadow surveillance" not "security camera")
- Ken Burns motion: alternating zoom-in, zoom-out, pan per scene
- Persistent header overlay with channel-specific accent words in crimson
- Captions rendered via Pillow, synced to `audio_acts.json`, present from second 0
- Background music mixed at flat 0.432 volume throughout
- Output: `final.mp4` — H.264, 1080×1920

### Layer 5 — Publishing
- YouTube: OAuth2 via Google Cloud, with age-based daily budget throttle
- TikTok: Playwright cookie-based automation (deprioritized, YouTube focus for now)
- Missed posts queued for next cron run

### Scheduler
- macOS crontab, runs hourly
- Age-based posting budget: 1/day for the first two weeks to warm up new accounts
  and avoid YouTube's early-channel spam signals, scaling up as the channel
  establishes history
- Randomized posting windows within 10AM–8PM to avoid pattern detection

### Dashboard
- FastAPI, localhost:8000
- Real-time pipeline status, per-channel post history, failure logs

---

## Key Design Decisions

**Why Kokoro TTS over ElevenLabs?**
Zero per-video cost. Runs locally. At 2 videos/day that compounds fast.

**Why per-act word budgets instead of total word count?**
First attempt used a total word count instruction in the prompt ("write 90–110 words
total"). Claude Haiku consistently ignored it — scripts came back at 150–180 words,
which meant TTS ran 70+ seconds and retention collapsed. Root cause: Haiku treats
total count as a soft suggestion, not a constraint. The fix was structural: hard
per-act ceilings (Hook ≤12, Build ≤30, Re-hook ≤25, Peak ≤25), validated in Python
after each generation attempt. Violations get fed back into the next prompt turn
explicitly. Three attempts, then pick the least-bad result.

**Why SQLite over Postgres?**
Single-machine deployment. No ops overhead. Four databases for clean
separation: videos, subjects, schedules, publishing state.

---

## Stack

Claude Haiku · Kokoro TTS · ffmpeg · Pillow · Pexels API
YouTube Data API v3 · Playwright · FastAPI · SQLite · macOS cron

---

## Setup

**Requirements:**
- Python 3.9 (Google auth libs incompatible with 3.10+)
- ffmpeg (`brew install ffmpeg`)
- API keys: Anthropic, Pexels, Freesound
- YouTube OAuth2 credentials (Google Cloud Console)

```bash
git clone <repo>
cp .env.example .env        # fill in your API keys
pip install -r requirements.txt
# Add google_client_secret.json to credentials/
python3 layer1_account_setup/config_generator.py   # configure a channel
python3 -m uvicorn web_dashboard:app --port 8000 --reload
```

**Scheduler:**
```bash
python3 scheduler.py   # or add to crontab
```

---

## What's Not Included

`.env`, `credentials/`, `channels/`, `*.db` — API keys, OAuth tokens,
channel configs, music tracks, and generated content are all private.

---

## Open Questions I'm Working Through

- Who is the core audience and what emotional job does this content do for them?
- Where should human judgment stay in the loop — and what quality am I
  trading away by removing it?
- What makes one subject resonate while another dies in the first seed batch?
- If a video hits 1M views, what breaks first?
