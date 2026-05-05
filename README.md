# Content Engine

Fully automated YouTube Shorts factory — topic to upload, minimal input after setup.

**Status:** Live and actively iterating  
**Live dashboard:** [akshitkalra.com/projects/Contentautomation](https://akshitkalra.com/projects/Contentautomation)

![Views](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fakshitkalra.com%2Fprojects%2FContentautomation%2Fstats.json&query=%24.total_views&label=Total%20Views&color=6096ba&style=flat-square&cacheSeconds=0)
![Videos](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fakshitkalra.com%2Fprojects%2FContentautomation%2Fstats.json&query=%24.videos_published&label=Videos%20Published&color=6096ba&style=flat-square&cacheSeconds=0)
![Channels](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fakshitkalra.com%2Fprojects%2FContentautomation%2Fstats.json&query=%24.channels&label=Channels&color=6096ba&style=flat-square&cacheSeconds=0)
![Views/Video](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fakshitkalra.com%2Fprojects%2FContentautomation%2Fstats.json&query=%24.views_per_video&label=Views%2FVideo&color=6096ba&style=flat-square&cacheSeconds=0)

---

## Current State

Live metrics update automatically — see the badges above or the [full dashboard](https://akshitkalra.com/projects/Contentautomation).

- **Publishing cadence:** 1 video/day/channel
- **Human input per video:** None after channel setup
- **Human input in system design:** Ongoing iteration based on analytics
- **Current bottleneck:** Content resonance and retention, not production throughput

These numbers are early and noisy. I use them less as proof of product-market fit and more as directional feedback for improving pacing, retention, and content quality.

---

## Product Thesis

Short-form content is a high-frequency feedback environment. Small changes in pacing, hook structure, visuals, captions, and audio can meaningfully affect viewer retention.

This project tests whether those variables can be controlled and improved systematically through automation.

Working assumptions:

1. Production can be mostly automated.
2. Retention cannot be solved by automation alone.
3. The highest-leverage work is building a feedback loop that translates platform behavior into product changes.
4. Human taste may still be the limiting factor — especially in subject selection, emotional resonance, and narrative quality.

---

## What This Is

A solo experiment: can one person run a multi-channel short-form content operation where the marginal cost of producing and publishing each additional video approaches zero?

The first-order challenge is automation.  
The second-order challenge is quality.  
The third-order challenge is learning fast enough from viewer behavior to improve the system.

Two live YouTube channels. One video per day each. Every step automated — subject selection, scripting, voiceover, video assembly, publishing, and failure recovery. The only human input per video is the initial channel setup. If the unit economics hold, the architecture is designed to add new channels with no additional engineering — just a new config file.

---

## The Core Learning

The pipeline isn't the hard part. The feedback loop is.

Building the system took four days. Understanding why the output wasn't performing took longer.

Early videos had 70%+ swipe-away rates. Diagnosing why required reading YouTube Studio retention curves and tracing problems back through every layer:

- **Script too long** → TTS too slow → video 1:13 instead of 45s → retention collapses
- **Background music inaudible** at 7% volume → video feels sterile, no atmosphere
- **0.6s silence gaps** between TTS acts stacking with Kokoro trailing silence → dead air = instant swipe
- **Pexels queries too literal** → plain stock photos instead of atmospheric visuals
- **Word count prompts ignored by Claude Haiku** → total limits don't work, per-act budgets + validation retry loop do

Each fix required understanding the full stack — prompt engineering, audio mixing, ffmpeg filter graphs, YouTube Studio retention curves, and the tradeoffs between retention, rewatch behavior, and absolute watch time.

Still iterating. Numbers are moving in the right direction.

---

## Key Experiments

| Problem Observed | Hypothesis | Change Made | Result / Learning |
|---|---|---|---|
| Videos running 70+ seconds, retention collapsing | Runtime too long for the format | Replaced total script word limits with per-act hard ceilings (Hook ≤12, Build ≤30, Re-hook ≤25, Peak ≤25) | Average runtime dropped from ~73s to target range; per-act validation outperformed global word count prompts |
| Claude Haiku ignored total word count instructions | Model needed structural constraints, not softer prompt language | Added 3-attempt validation loop with explicit violation feedback per attempt | Output length became reliable; fallback picks least-bad attempt on third failure |
| High early swipe-away | First seconds needed tighter pacing and immediate visual anchoring | Increased hook TTS speed (+0.10), rendered captions from second 0 | Improved opening retention; eliminated dead-air window before first caption |
| Videos felt sterile and generic | Background music too quiet, visual selection too literal | Raised music mix to calibrated level; changed Pexels queries from literal ("security camera") to atmospheric ("dramatic shadow surveillance") | Higher perceived production quality; videos stopped reading as stock-footage slideshows |
| Silent transitions causing drop-off | Micro-gaps compound heavily in short-form video | Reduced and normalized TTS trailing silence and act gaps | Tighter pacing; fewer dead zones between acts |
| New channels needed stable posting history before scaling | Publishing volume should ramp with channel age | Added age-based posting limits: 1/day for first two weeks, scaling up as channel establishes history | Prioritized consistency over raw output volume |

---

## What Failed

**Total word count prompts did not work.**  
Initial prompts asked Claude Haiku to write 90–110 words total. The model regularly returned 150–180 words, pushing videos past 70 seconds. The fix was structural: hard per-act ceilings validated in Python, with violations fed back into the next generation attempt. Prompt instructions are not constraints — enforcement has to happen outside the model.

**Automation was easier than resonance.**  
The initial assumption was that automating the pipeline would be the hard part. It wasn't. Once the system ran, the harder question became why some videos held attention and others died in the first seed batch. The bottleneck shifted from production capacity to taste, pacing, and feedback interpretation.

**Literal visual prompts produced generic output.**  
Early Pexels queries matched the script too directly — a line about surveillance searched for "security camera" and returned obvious stock imagery. Switching to atmospheric queries ("dramatic shadow surveillance") made the visual layer feel intentional rather than algorithmic.

---

## Architecture

```
Layer 1: Channel Setup       → layer1_account_setup/
Layer 2: Script Generation   → layer2_script_generation/
Layer 3: Audio Production    → layer3_audio_production/
Layer 4: Video Assembly      → layer4_video_production/
Layer 5: Publishing          → layer5_publishing/
Dashboard                    → public_dashboard.py (public) · web_dashboard.py (internal)
Scheduler                    → scheduler.py
State                        → Postgres (Supabase)
Analytics                    → YouTube Analytics API
```

The system runs as a closed loop: Generate → Publish → Measure → Diagnose → Adjust → Repeat.

### Layer 2 — Script Generation (Claude Haiku)
- Per-act word budgets with hard ceilings (not total word counts — Haiku ignores those)
- 3-attempt validation retry loop: if any act exceeds budget, violation is fed back into next prompt
- Fallback: pick least-bad attempt on third failure
- Output: `script.json` with 4 acts — Hook, Build, Re-hook, Peak

*Prompt design matters as much as the code — per-act hard ceilings with explicit violation feedback outperform total word count instructions for length control with Haiku.*

### Layer 3 — Audio Production (Kokoro TTS)
- Runs locally, zero per-video API cost
- Per-act speed tuning: Hook +0.10 faster, Peak -0.08 slower for dramatic effect
- Act gaps controlled explicitly because small silences compound into retention-killing dead air
- Output: `audio.wav` + `audio_acts.json` with exact timestamps for caption sync

### Layer 4 — Video Assembly (ffmpeg + Pillow)
- Pexels image queries are atmospheric, not literal ("dramatic shadow surveillance" not "security camera")
- Ken Burns motion: alternating zoom-in, zoom-out, pan per scene
- Persistent header overlay with channel-specific accent words in crimson
- Captions rendered via Pillow, synced to `audio_acts.json`, present from second 0
- Background music mixed at a fixed calibrated level after early tests showed low-volume music made videos feel sterile
- Output: `final.mp4` — H.264, 1080×1920

### Layer 5 — Publishing
- YouTube: OAuth2 via Google Cloud, with age-based daily budget throttle
- TikTok: deprioritized prototype; current focus is YouTube because analytics and retention feedback are more actionable there
- Missed posts queued for next cron run

### Scheduler
- macOS crontab, runs hourly
- Age-based posting budget: 1/day for the first two weeks to build consistent channel history before increasing volume, scaling up as the channel establishes track record
- Randomized posting windows within 10AM–8PM to maintain natural publishing cadence

### Dashboard
- **Public:** [akshitkalra.com/projects/Contentautomation](https://akshitkalra.com/projects/Contentautomation) — read-only stats, no auth required
- **Internal:** FastAPI, localhost:8000 — full pipeline management, failure logs

---

## Key Design Decisions

**Kokoro TTS over ElevenLabs**  
At 2+ videos/day, per-video voice costs compound fast. Local generation keeps marginal production cost close to zero. Tradeoff: voice quality may be lower than premium paid models, so the system compensates through pacing, music, captions, and editing rhythm.

**Per-act budgets over total word count**  
Claude Haiku consistently treated total word limits as soft suggestions. That created long scripts, longer videos, and worse retention. Per-act budgets reduce creative flexibility, but they make runtime predictable enough to optimize retention.

**Postgres over SQLite**  
The public dashboard runs remotely and needs reliable access to production state. Supabase's connection pooler handles serverless connections cleanly. Tradeoff: slightly more infrastructure complexity in exchange for remote visibility and cleaner deployment.

**YouTube over TikTok as the primary feedback loop**  
YouTube Analytics provides more actionable retention data, making it easier to trace performance issues back to product decisions. Tradeoff: the system sacrifices multi-platform reach in the short term to improve learning speed on one platform.

---

## Open Questions

**Audience**
- Who is the core audience for each channel?
- What emotional job does this content perform — curiosity, suspense, background entertainment, identity reinforcement?
- Which subjects create durable retention rather than one-off spikes?

**Product Quality**
- Where should human judgment stay in the loop — and what quality is traded away by removing it?
- Can automated scoring detect weak scripts before publishing?
- What makes one subject resonate while another dies in the first seed batch?

**Growth**
- When does the channel reach sufficient scale to run controlled experiments rather than multi-variable debugging?
- Which variables matter most: topic, hook, pacing, voice, visuals, captions, or posting time?
- If a video hits 1M views, what breaks first?

**Strategy**
- At what point does "zero human input" become a ceiling rather than a feature — and where specifically would human taste reintroduce the most value?
- What would make this defensible if content generation becomes commoditized?

---

## Stack

Claude Haiku · Kokoro TTS · ffmpeg · Pillow · Pexels API · YouTube Data API v3 · Playwright · FastAPI · Postgres (Supabase) · macOS cron

---

## Setup

**Requirements:**
- Python 3.9 (tested environment for the current Google auth setup)
- ffmpeg (`brew install ffmpeg`)
- API keys: Anthropic, Pexels, Freesound
- YouTube OAuth2 credentials from Google Cloud Console

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

`.env`, `credentials/`, `channels/` — API keys, OAuth tokens, channel configs, music tracks, and generated content are all private.
