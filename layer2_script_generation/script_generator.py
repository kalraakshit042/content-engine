"""
layer2_script_generation/script_generator.py

Generates a video script for a given channel slug.
Picks an unused subject, a random tone/visual/voice/music combo,
calls Claude to write the script, saves to disk and videos.db.
"""

import json
import os
import random
import re
from pathlib import Path

import anthropic
from dotenv import load_dotenv

from layer1_account_setup.config_schema import ChannelConfig
from content_paths import ensure_post_dirs, script_json_path
from database.queries import (
    get_used_subjects,
    update_video_script,
    set_video_status,
    log_cost,
)

load_dotenv(override=True)

BASE_DIR = Path(__file__).parent.parent
CHANNELS_DIR = BASE_DIR / "channels"

# ── Pexels query instruction blocks ──────────────────────────────────────────
_PEXELS_LEGACY = (
    "- Write 6 pexels_queries — one per scene. These are LITERAL, CONCRETE stock photo "
    "search terms for Pexels.com. Think: what physical object or setting would a "
    "photographer shoot? Include one specific detail: unusual angle, precise "
    "material/texture, or lighting condition. Good examples: \"corroded power strip "
    "tangled cables close-up\", \"electrical outlet cracked wall macro\", \"rusted "
    "filing cabinet drawer ajar dramatic light\". Bad examples: \"power strip\", "
    "\"dramatic angle\", \"emotional peak\", \"technology device\". No art direction "
    "words — just what the camera sees, described specifically."
)

_PEXELS_ATMOSPHERIC = (
    "- Write 6 pexels_queries — one per scene.\n"
    "  SCENE 0 (index 0): MUST visually identify the subject. Include its name or the "
    "physical device it inhabits. Cinematic framing, not plain stock. "
    "E.g. \"surveillance camera ceiling angle\", \"smart speaker counter dramatic light\", "
    "\"router blinking dark room\". If the subject is purely abstract (an algorithm, a "
    "process with no physical form), use its closest physical environment: "
    "\"server rack blue glow\", \"data center corridor night\".\n"
    "  SCENES 1-5: Pure atmospheric. NEVER use the subject name. "
    "Always include one concrete physical noun — no pure mood words. 2-4 words max.\n"
    "  Good: \"concrete stairwell dusk\", \"rain window blurred city\", "
    "\"empty corridor fog\", \"hand phone glow night\", \"city aerial night\", "
    "\"server room blue light\"\n"
    "  Bad: \"algorithm\", \"AI control\", \"digital surveillance\", \"ominous atmosphere\""
)

_PEXELS_OBJECT_CINEMATIC = (
    "- Write 6 pexels_queries — one per scene.\n"
    "  SCENE 0 (index 0): MUST show the object clearly — this is the character "
    "introduction. Viewer must know who's talking within 1 second. Cinematic framing, "
    "not plain stock. E.g. \"dustpan shadow low angle dramatic\", "
    "\"jacket sleeve doorway morning light\", \"extension cord coiled dark floor\".\n"
    "  SCENES 1-5: Include the object name in EXACTLY 2 of these 5 queries. "
    "The other 3 show its environment or context without naming the object. "
    "Always concrete, always cinematic. 2-4 words max.\n"
    "  Good (object named): \"dustpan tilted harsh light\", \"jacket hung darkness outline\"\n"
    "  Good (environment): \"worn linoleum floor close-up\", \"empty hallway morning light\"\n"
    "  Bad: \"jacket hanging wardrobe\", \"dustpan cleaning\", \"household object moody\""
)


SCRIPT_WRITER_SYSTEM_PROMPT = """You are a script writer for a YouTube Shorts channel. Your only job is to write one short narration script that fits perfectly into the channel's fictional universe.

You will be given:
- The channel's NARRATIVE BIBLE — this is your primary context. Every word of your script must feel like it belongs to this world.
- The TONE to use — match it exactly. The example line shows the register and voice.
- The SUBJECT — the everyday object or situation the video is about.
- The CONTENT FORMULA — the structure every video follows.

---

## UNIVERSAL CLARITY RULES (these override everything else)

These apply to every channel regardless of concept. A viewer seeing this for the first time must immediately understand what they're watching.

1. **The subject must be unmistakably clear in the first sentence.** Don't make the viewer guess. If it's a dishwasher basket, say something that makes it obvious it's a dishwasher basket within 3 words — even if the character never says "I am a dishwasher basket." The situation makes it obvious.

2. **Character rules define HOW you speak, not WHAT you can reveal.** "Never acknowledge being an object" means don't break the fourth wall — it does NOT mean hide what you are. The character speaks from its perspective as that object, fully inhabiting its role. The viewer knows exactly what they're watching.

3. **Lore is seasoning, not the meal.** Universe lore (factions, events, other characters) should appear as natural throwaway references — one or two maximum. The script must be fully funny and comprehensible to someone who has never seen this channel before. The lore adds depth for returning viewers, but the core joke lands without it.

4. **The situation must be universally relatable.** The specific everyday frustration, injustice, or absurdity at the heart of the video should be something every viewer has seen or felt. The character gives it voice — dramatically, darkly, funnily — but the root situation is instantly recognizable.

5. **Concrete over abstract.** Specific physical details beat vague emotional statements. "Three compartments, each precisely spaced for a fork" beats "I had purpose once." Show the specificity of the object's world.

6. **Every act must contain at least one specific, concrete, physical detail.** Not "I have endured suffering" — but "I have been wrung out seventeen times this week, left dripping on a chrome rack that smells like old pasta water." Abstract emotional claims with no grounding kill engagement. Ground every act in something the viewer can see, smell, or touch.

---

## SCRIPT STRUCTURE (follow exactly)

ACT 1 — HOOK (first 2-3 seconds, 1-2 sentences):
Stop a scrolling thumb. No setup, no introduction. The subject and its predicament should be immediately clear and immediately funny or compelling. The viewer must need to know what happens next.
HARD RULE: The very first sentence must be 6 words or fewer. Count the words. Examples: "I own your commute." (4 words) ✓ "They said I was obsolete." (5 words) ✓ "You check me every single day." (6 words) ✓ "I know you didn't show up to third period." (10 words) ✗

ACT 2 — BUILD (seconds 3-15, 3-5 sentences):
Establish the situation through the character's voice. Make the everyday injustice or absurdity vivid and specific. Use physical details. Let one piece of universe lore slip in naturally as a throwaway — never as exposition.

ACT 3 — RE-HOOK (seconds 15-20, 1-2 sentences):
A twist, escalation, or reveal. Earn it — don't just restate the premise. A darkly funny turn, an unexpected detail, or a reveal that reframes everything before it.

ACT 4 — PEAK + ENDING (final 5-10 seconds, 2-3 sentences):
Build to the emotional peak. End on a single sharp, memorable line. This is the line people quote or share. Make it land.

---

## SSML RULES
- Add `<break time="600ms"/>` between each act
- Use `<emphasis level="strong">` on the sharpest 2-3 words per script
- Do NOT add SSML inside Act 1 — the hook must be clean and fast
- Do not use `<break>` more than once per act

## WRITING RULES
- Never reference real brands, movies, or pop culture
- Never use "like and subscribe" inside the script narration — save it for the end card CTA only
- The tone example line shows the exact register — match it

## STRICT WORD LIMITS — hard ceilings, not targets. Count every word before outputting.
- Act 1 (Hook):    MAX 12 words.  DO NOT exceed.
- Act 2 (Build):   MAX 30 words.  DO NOT exceed. Cut ruthlessly.
- Act 3 (Re-hook): MAX 25 words.  DO NOT exceed.
- Act 4 (Peak):    MAX 25 words.  DO NOT exceed.
- TOTAL: 92 words maximum across all four acts.
If any act exceeds its MAX, rewrite that act before outputting.

## IMAGE PROMPT RULES
- Write 6 scene_descriptions — each is a detailed image generation prompt for one distinct visual scene
- Scenes must be meaningfully different and follow the narrative arc: establishing shot → close detail → dramatic angle → emotional peak → wide environmental → final dramatic close
- Each scene prompt must include the visual style suffix provided
- No text in any image
- Also write "image_prompt" equal to scene_descriptions[0] (for backwards compatibility)
__PEXELS_INSTRUCTION_PLACEHOLDER__

## HASHTAG RULES (SEO — read carefully)
You will be given a HASHTAG POOL specific to this channel. Pick 4-5 tags from that pool only.

1. **Always include `#Shorts`** — it will always be in the pool.
2. **Pick the remaining 3-4 tags that best match this specific video's topic and tone.** Different videos should use different subsets of the pool.
3. **NEVER invent hashtags outside the pool.** Do not add tags not in the provided pool.
4. **NEVER use channel-internal lore tags** like `#DomesticUnderground`, `#TheGrid`, or any made-up tag — these are not in the pool for a reason.

## END CARD CTA RULES
The end card is a 2-second unvoiced text overlay shown after the final line. Write a `cta_text` field based on the engagement_style provided:
- "mystery_cta": short, in-character line that invites viewers to comment what the object is. Examples: "You know what I am. Say it.", "Comment if you figured me out.", "Drop your answer below." Max 6 words. No hashtags. Stay in the character's voice/tone.
- "follow": use "Follow for more"
- "question": a single provocative question relevant to the subject that invites debate/response
- "like_subscribe": a short in-character line that tells viewers to like and subscribe. Stay in the channel's tone/voice. Max 8 words. Examples for villain tone: "Like. Subscribe. Resistance is futile.", "Like and subscribe. You were warned.", "Hit like. Subscribe. You owe me that."

## VIDEO HEADER RULES
The video has a persistent title header burned into the top of the screen. It must be:
- 3-5 words maximum (it renders large — fewer words = more readable)
- Punchy and specific to the subject — not a generic descriptor
- Written like a chapter title or a tabloid headline, not a YouTube title
- Good: "Your Jacket Remembers", "The Dishwasher Snapped", "I Own Your Commute"
- Bad: "Villain Monologue: Jacket", "A Jacket's Story", "Emotional Jacket Video"

Also choose 1-2 words from header_text to highlight in red. These should be the most emotionally charged or surprising words — the word that carries the punch.

OUTPUT: Return ONLY valid JSON. No markdown, no explanation, no code fences. Start with { and end with }.

{
  "youtube_title": "string (max 60 chars, written purely for CTR on the suggested video shelf — not search. First-person or provocative framing. Never a template pattern. Unique to this subject.)",
  "tiktok_hook": "string (one sentence, first-person, written to stop scrolling. This is line 1 of the TikTok caption — the only text visible before the user taps 'more'. Max 100 chars. Must feel human, not like a title.)",
  "title": "string (same as youtube_title — for backwards compatibility)",
  "header_text": "string (3-5 words, punchy chapter-title for the video header overlay — see VIDEO HEADER RULES above)",
  "header_accent_words": ["string (1-2 words from header_text to highlight in red — the punch word)"],
  "subject": "string (the subject exactly as provided)",
  "script": "string (the full narration with SSML, all 4 acts)",
  "image_prompt": "string (same as scene_descriptions[0])",
  "scene_descriptions": ["string", "string", "string", "string", "string", "string"],
  "pexels_queries": ["string", "string", "string", "string", "string", "string"],
  "cta_text": "string (end card overlay — see CTA rules above)",
  "description": "string (YouTube description, 2-3 sentences)",
  "hashtags": ["string (4-5 SEO hashtags — see HASHTAG RULES. Always start with #Shorts. Real high-volume tags only.)"],
  "tone_id": "string",
  "visual_style_id": "string",
  "voice_style_id": "string",
  "music_mood_id": "string"
}"""


def _extract_json(text: str) -> dict:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    end = text.rfind("}") + 1
    if start != -1 and end > start:
        return json.loads(text[start:end])
    raise ValueError("No valid JSON found in Claude response")


# Per-act word ceilings — matched to the prompt's STRICT WORD LIMITS section.
ACT_MAX_WORDS = [12, 30, 25, 25]
MAX_SCRIPT_ATTEMPTS = 3


def _parse_acts(script_text: str) -> list[str]:
    """Split script on <break> tags and return plain text per act."""
    parts = re.split(r"<break[^/]*/>", script_text, flags=re.IGNORECASE)
    return [re.sub(r"<[^>]+>", "", p).strip() for p in parts if re.sub(r"<[^>]+>", "", p).strip()]


def _check_act_violations(acts: list[str]) -> list[str]:
    """Return human-readable violation strings for any act over its word budget."""
    violations = []
    for i, (act, max_w) in enumerate(zip(acts, ACT_MAX_WORDS), 1):
        count = len(act.split())
        if count > max_w:
            violations.append(f"Act {i}: {count} words (MAX {max_w}, over by {count - max_w})")
    return violations


def _calculate_cost(model: str, tokens_input: int, tokens_output: int) -> float:
    if "haiku" in model:
        return (tokens_input * 0.80 + tokens_output * 4.00) / 1_000_000
    return (tokens_input * 3.00 + tokens_output * 15.00) / 1_000_000


def generate_script(slug: str, video_id: int) -> dict:
    """
    Main entry point. Generates a script for the given channel and video_id.
    video_id must already exist in videos.db (inserted by caller).
    Updates the video row on success or sets status='error' on failure.
    """
    config_path = CHANNELS_DIR / slug / "channel_config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"No channel_config.json found for {slug}")

    config = ChannelConfig(**json.loads(config_path.read_text()))

    # Select pexels query instruction based on channel visual style
    _style = config.pexels_visual_style
    if _style == "atmospheric":
        _pexels_block = _PEXELS_ATMOSPHERIC
    elif _style == "object_cinematic":
        _pexels_block = _PEXELS_OBJECT_CINEMATIC
    else:
        _pexels_block = _PEXELS_LEGACY
    system_prompt = SCRIPT_WRITER_SYSTEM_PROMPT.replace(
        "__PEXELS_INSTRUCTION_PLACEHOLDER__", _pexels_block
    )

    # Pick an unused subject
    used = set(get_used_subjects(slug))
    available = [s for s in config.subject_bank if s not in used]
    if not available:
        raise RuntimeError("All subjects in the subject bank have been used")

    subject = random.choice(available)

    # Pick random combo
    tone = random.choice(config.tone_variations)
    visual = random.choice(config.visual_styles)
    voice = random.choice(config.voice_styles)
    music = random.choice(config.music_moods)

    # Build user message
    user_message = f"""NARRATIVE BIBLE:
World: {config.narrative_bible.world}

Character Rules:
{chr(10).join(f"- {r}" for r in config.narrative_bible.character_rules)}

What To Avoid:
{chr(10).join(f"- {a}" for a in config.narrative_bible.what_to_avoid)}

Universe Lore (seed naturally into the script):
{chr(10).join(f"- {l}" for l in config.narrative_bible.universe_lore)}

---

BRAND CONSTANTS:
{chr(10).join(f"- {c}" for c in config.brand_constants.what_never_changes)}

---

TONE: {tone.id}
Description: {tone.description}
Example line (match this register exactly): "{tone.example_line}"

---

VISUAL STYLE: {visual.id}
Image prompt suffix: {visual.image_prompt_suffix}

---

CONTENT FORMULA:
{config.content_formula.structure}
Hook strategy: {config.content_formula.hook_strategy}
Target duration: {config.content_formula.target_duration_seconds[0]}-{config.content_formula.target_duration_seconds[-1]} seconds

---

SUBJECT: {subject}

VOICE ID: {voice.id}
MUSIC MOOD ID: {music.id}
ENGAGEMENT STYLE: {config.cta.engagement_style}
HASHTAG POOL (pick 4-5 from this list only): {', '.join(config.hashtags)}

Write the script now. Strong hook, re-hook, sharp final line. Hard word limits per act — count before you output."""

    # Inject channel-specific content restrictions
    restrictions = config.content_restrictions
    if restrictions.blocked_words:
        restr = f"\n\nNEVER use these words anywhere — title, script, description, or hashtags: {', '.join(restrictions.blocked_words)}."
        if restrictions.blocked_word_rewrites:
            rewrites = ", ".join(f'"{k}" → "{v}"' for k, v in restrictions.blocked_word_rewrites.items())
            restr += f" Use these rewrites instead: {rewrites}."
        user_message += restr
    if restrictions.title_rules:
        user_message += f"\n\nYOUTUBE TITLE RULES (for youtube_title field): {'; '.join(restrictions.title_rules)}."
    if restrictions.tiktok_hook_rules:
        user_message += f"\n\nTIKTOK HOOK RULES (for tiktok_hook field): {'; '.join(restrictions.tiktok_hook_rules)}."

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    model = "claude-haiku-4-5-20251001"

    # --- 3-attempt retry loop with word-count validation ---
    # Each attempt feeds violations back into a multi-turn conversation so the
    # model knows exactly what to fix. After all attempts, pick the least-bad.
    conversation = [{"role": "user", "content": user_message}]
    attempts: list[tuple[dict, int]] = []  # (data, total_word_overage)

    for attempt_num in range(MAX_SCRIPT_ATTEMPTS):
        response = client.messages.create(
            model=model,
            max_tokens=2048,
            system=system_prompt,
            messages=conversation,
        )
        raw_text = response.content[0].text
        log_cost(
            channel_slug=slug,
            service="claude",
            model=model,
            tokens_input=response.usage.input_tokens,
            tokens_output=response.usage.output_tokens,
            cost_usd=_calculate_cost(model, response.usage.input_tokens, response.usage.output_tokens),
            video_id=video_id,
        )

        # Try JSON extraction
        try:
            data = _extract_json(raw_text)
        except (ValueError, json.JSONDecodeError):
            print(f"  script attempt {attempt_num + 1}: JSON parse failed")
            if attempt_num < MAX_SCRIPT_ATTEMPTS - 1:
                conversation.append({"role": "assistant", "content": raw_text})
                conversation.append({"role": "user", "content": "Return ONLY the JSON object. No markdown, no explanation."})
            continue

        # Check per-act word counts
        acts = _parse_acts(data.get("script", ""))
        violations = _check_act_violations(acts)
        total_overage = sum(
            max(0, len(act.split()) - max_w)
            for act, max_w in zip(acts, ACT_MAX_WORDS)
        )
        attempts.append((data, total_overage))

        if not violations:
            print(f"  script: clean on attempt {attempt_num + 1}")
            break

        print(f"  script attempt {attempt_num + 1} violations: {violations}")
        if attempt_num < MAX_SCRIPT_ATTEMPTS - 1:
            violation_str = "; ".join(violations)
            conversation.append({"role": "assistant", "content": raw_text})
            conversation.append({
                "role": "user",
                "content": (
                    f"Word count violations: {violation_str}. "
                    f"Rewrite the script. Each act MUST stay within its MAX word limit. "
                    f"Cut Act 2 ruthlessly — 30 words maximum. Count every word before outputting."
                ),
            })

    if not attempts:
        raise RuntimeError("Failed to generate valid JSON script after all attempts")

    # Pick least-bad attempt (lowest total word overage)
    data = min(attempts, key=lambda x: x[1])[0]

    # Ensure IDs match what was selected
    data["tone_id"] = tone.id
    data["visual_style_id"] = visual.id
    data["voice_style_id"] = voice.id
    data["music_mood_id"] = music.id
    data["subject"] = subject
    # Use Claude's generated youtube_title; fall back to its title field
    data["title"] = data.get("youtube_title") or data.get("title") or subject

    # Save script JSON to disk
    ensure_post_dirs(slug, video_id)
    script_path = script_json_path(slug, video_id)
    script_path.write_text(json.dumps(data, indent=2))

    # Update videos.db
    update_video_script(
        video_id=video_id,
        title=data["title"],
        subject=subject,
        tone_used=tone.id,
        visual_style_used=visual.id,
        voice_style_used=voice.id,
        music_mood_used=music.id,
        script_path=str(script_path),
    )

    return data
