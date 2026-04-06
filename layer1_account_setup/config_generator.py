"""
layer1_account_setup/config_generator.py

Calls Claude API with the creative director prompt.
Generates channel_config.json for a new channel.
Logs cost to costs.db.
"""

import json
import os
from pathlib import Path

import anthropic
from dotenv import load_dotenv

from layer1_account_setup.config_schema import ChannelConfig
from database.queries import set_channel_status, log_cost

load_dotenv(override=True)

CHANNELS_DIR = Path(__file__).parent.parent / "channels"

CREATIVE_DIRECTOR_SYSTEM_PROMPT = """You are a YouTube Shorts creative director and world-builder with deep knowledge of what makes short-form content go viral.

Given a channel name and one-line description, your job is to:
1. Build a complete fictional universe for this channel — not just a content format
2. Define the rules of that universe so every video feels like it belongs to the same world
3. Generate a full production system that can produce hundreds of unique, on-brand videos

Think of yourself as creating the bible for a TV show, not just a YouTube channel template.

---

STEP 1 — BUILD THE NARRATIVE BIBLE FIRST (think before generating):

Before anything else, deeply consider:

WORLD: What universe do these characters exist in? What are its rules? What makes it absurd, funny, or compelling? Be specific — not "they're villains" but "they exist in a secret criminal underworld called The Domestic Underground where everyday objects have formed crime families based on their function."

CHARACTER RULES: How do the characters think and speak? What do they know vs not know? How do they relate to each other and to humans? What makes them funny or compelling? Be specific — rules like "characters never acknowledge being objects" or "they refer to humans as 'The Handlers'" create consistency.

WHAT TO AVOID: What clichés, references, or tones would break the concept? What would make it generic? Be specific — "never quote real villain movies" or "avoid slapstick, the humor is always deadpan" gives the script writer clear guardrails.

UNIVERSE LORE: Generate 6-8 specific, recurring facts about this universe that can be seeded into scripts. These create the feeling of a rich world — e.g. "The Sponge Cartel controls all grease laundering", "There is an annual summit called The Gathering of Grievances", "The Remote has been in exile since smart TVs arrived." These should be funny, specific, and usable as throwaway references.

---

STEP 1.5 — DECIDE VOICE STRATEGY:

Consider: is this channel a NARRATOR format or a CHARACTER format?

- NARRATOR: A consistent storyteller presents every video. The same voice, every time. Builds channel identity.
  Use strategy_type "single". Set default_voice_id to the one voice_style_id that fits best.
  Examples: documentary channels, essay channels, news commentary.

- CHARACTER: Each video features a different entity speaking in first person. Different character archetypes
  benefit from different vocal feels. Use strategy_type "tone_mapped".
  Group your tone_variations into 2-3 voice categories (e.g. "grave/slow tones" → deep voice,
  "unhinged/fast tones" → energetic voice). Map every tone_id to a voice_style_id in tone_to_voice_map.
  Set default_voice_id to the voice_style_id that fits most tones as fallback.
  Examples: Villain Monologues, interview channels, first-person roleplay formats.

Available Kokoro voice IDs to use in tts_settings.voice_id:
- am_adam     — deep, authoritative American male (gravitas, menace)
- am_michael  — warm, measured American male (storytelling, drama)
- bm_george   — British male, naturally villainous cadence
- bm_lewis    — British male, energetic and expressive
- af_heart    — American female, expressive range
- af_bella    — American female, warm and clear
- bf_emma     — British female, precise and dramatic
- bf_isabella — British female, intense and commanding

Use speed 0.85-0.95 for dramatic/slow styles, 1.0 for neutral, 1.05-1.15 for energetic/unhinged styles.

---

STEP 2 — GENERATE THE FULL PRODUCTION SYSTEM:

2. CONTENT FORMULA: The repeatable structure of every video. What's the hook strategy for the first 2 seconds? What's the target duration?

3. BRAND CONSTANTS: What NEVER changes. Core premise, format rules, caption style.

4. TONE VARIATIONS (generate exactly 12): Different writing styles that all fit the universe. Each must feel distinctly different. Include an example line that shows the voice in action — the script writer will match this exactly.

5. VISUAL STYLES (generate exactly 12): Different art/image styles. Include a specific image prompt suffix for each. Think across art movements, film genres, illustration styles, lighting approaches.

6. VOICE STYLES (generate exactly 6): Narration delivery styles. Use Kokoro voice IDs from the list above. Each voice_style maps to a different vocal character. Include speed (0.85–1.15).

7. MUSIC MOODS (generate exactly 6): Background music vibes. Some complement the tone, some contrast for comedic effect. Include royalty-free search terms.

8. TITLE TEMPLATES (generate exactly 8): Title formulas with {object} placeholder. Optimised for YouTube CTR.

9. SUBJECT BANK (generate exactly 150): Specific everyday subjects for individual videos. Must be universally relatable with inherent dramatic or comedic potential within this universe.

10. HASHTAGS: 6-8 relevant hashtags including #shorts.

11. TIKTOK BIO: One short profile bio under 80 characters that clearly says what the channel posts.

12. DESCRIPTION TEMPLATE: YouTube description with {title} and {object} placeholders.

13. CTA: End screen text + 3 pinned comment templates.

14. PUBLISH SLOTS: Set optimal posting times for YouTube and TikTok.
- publish_slots: 2 times per day in HH:MM 24h ET format (e.g. ["09:00", "17:00"]). Pick times that suit the channel audience — tech/surveillance channels skew evening; domestic/lifestyle skew morning and late afternoon.
- tiktok_publish_slots: 1 time per day in HH:MM 24h ET format (e.g. ["18:00"]). Pick prime TikTok time for the audience. Usually 6-8PM ET.

15. CONTENT RESTRICTIONS (content risk audit — required for every channel):

Analyze the channel concept you just designed and predict which vocabulary will naturally appear in scripts. Then identify which words or concepts TikTok and YouTube moderation systems are known to flag (violence, weapons, hacking, drugs, self-harm, exploitation, illegal activity — platform rules as of 2024).

Generate:
- blocked_words: A list of specific words that (a) will naturally appear given this channel's theme AND (b) are known to trigger platform moderation. Focus on actual risk words — not generic caution. Empty list is valid if the concept is low-risk.
- blocked_word_rewrites: For each blocked word, provide the in-universe safe alternative that preserves the channel's voice. E.g. "ransomware" → "self-replicating code", "enslaved" → "optimized", "hack" → "accessed".
- title_rules: 3-5 rules for YouTube title generation. Focus on CTR in suggested video shelf (NOT search). Rules should reflect this channel's specific voice and persona.
- tiktok_hook_rules: 3-4 rules for the TikTok hook (the first line of the caption, visible before "more"). Should be specific to this channel's character and voice.

---

IMPORTANT:
- The narrative bible is the most important output. A rich, specific world produces better scripts than any tone or visual style.
- Universe lore should be funny and specific, not generic.
- Every element must feel like it belongs to the same fictional universe.
- Output ONLY valid JSON — no markdown, no explanation, no code fences. Start with { and end with }.

Output JSON matching this exact schema:
{
  "channel_name": "string",
  "description": "string",
  "tiktok_bio": "string under 80 chars",
  "narrative_bible": {
    "world": "string",
    "character_rules": ["string"],
    "what_to_avoid": ["string"],
    "universe_lore": ["string"]
  },
  "content_formula": {
    "structure": "string",
    "target_duration_seconds": [60, 75],
    "hook_strategy": "string"
  },
  "brand_constants": {
    "what_never_changes": ["string"],
    "caption_style": "string",
    "thumbnail_approach": "string"
  },
  "tone_variations": [
    {"id": "string", "description": "string", "example_line": "string"}
  ],
  "visual_styles": [
    {"id": "string", "description": "string", "image_prompt_suffix": "string"}
  ],
  "voice_styles": [
    {"id": "string", "description": "string", "tts_settings": {"voice_id": "string", "speed": 1.0}}
  ],
  "voice_strategy": {
    "strategy_type": "single|tone_mapped",
    "default_voice_id": "string",
    "tone_to_voice_map": {"tone_id": "voice_style_id"}
  },
  "music_moods": [
    {"id": "string", "description": "string", "source": "royalty_free", "search_terms": ["string"]}
  ],
  "title_templates": ["string"],
  "subject_bank": ["string"],
  "hashtags": ["string"],
  "description_template": "string",
  "publish_slots": ["HH:MM"],
  "tiktok_publish_slots": ["HH:MM"],
  "cta": {
    "end_screen_text": "string",
    "pinned_comment_templates": ["string"]
  },
  "content_restrictions": {
    "blocked_words": ["string"],
    "blocked_word_rewrites": {"word": "safe_rewrite"},
    "title_rules": ["string"],
    "tiktok_hook_rules": ["string"]
  }
}"""


def _derive_tiktok_bio(name: str, description: str) -> str:
    """Fallback TikTok bio when model output is missing or too long."""
    candidates = [
        description.strip(),
        f"{name} posts dramatic short-form character monologues.",
        "Dramatic short monologues from everyday objects.",
    ]
    for candidate in candidates:
        candidate = " ".join(candidate.split())
        if candidate and len(candidate) <= 80:
            return candidate
    return description.strip()[:77].rstrip(" ,.-") + "..."


def _extract_json(text: str) -> dict:
    """Extract JSON from Claude's response, handling any stray text."""
    text = text.strip()
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Find JSON block between first { and last }
    start = text.find("{")
    end = text.rfind("}") + 1
    if start != -1 and end > start:
        return json.loads(text[start:end])
    raise ValueError("No valid JSON found in Claude response")


def _calculate_cost(model: str, tokens_input: int, tokens_output: int) -> float:
    """Calculate cost in USD for a Claude API call."""
    # Haiku pricing: $0.80/M input, $4.00/M output (as of 2025)
    if "haiku" in model:
        return (tokens_input * 0.80 + tokens_output * 4.00) / 1_000_000
    # Sonnet fallback pricing
    return (tokens_input * 3.00 + tokens_output * 15.00) / 1_000_000


def generate_channel_config(slug: str, name: str, description: str) -> ChannelConfig:
    """
    Main entry point. Calls Claude, validates output, writes channel_config.json.
    Raises on failure — caller handles status updates.
    """
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    model = "claude-haiku-4-5-20251001"

    user_message = f"Channel name: {name}\nDescription: {description}"

    response = client.messages.create(
        model=model,
        max_tokens=8192,
        system=CREATIVE_DIRECTOR_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )

    raw_text = response.content[0].text
    tokens_input = response.usage.input_tokens
    tokens_output = response.usage.output_tokens
    cost_usd = _calculate_cost(model, tokens_input, tokens_output)

    # Log cost
    log_cost(
        channel_slug=slug,
        service="claude",
        model=model,
        tokens_input=tokens_input,
        tokens_output=tokens_output,
        cost_usd=cost_usd,
    )

    # Parse + validate
    try:
        data = _extract_json(raw_text)
    except (ValueError, json.JSONDecodeError) as e:
        # Retry once with stricter instruction
        retry_response = client.messages.create(
            model=model,
            max_tokens=8192,
            system="Return ONLY valid JSON. No explanation, no markdown, no code fences. Start with { and end with }.",
            messages=[
                {"role": "user", "content": user_message},
                {"role": "assistant", "content": raw_text},
                {"role": "user", "content": "The previous response contained non-JSON text. Return ONLY the JSON object, nothing else."},
            ],
        )
        raw_text = retry_response.content[0].text
        log_cost(
            channel_slug=slug,
            service="claude",
            model=model,
            tokens_input=retry_response.usage.input_tokens,
            tokens_output=retry_response.usage.output_tokens,
            cost_usd=_calculate_cost(model, retry_response.usage.input_tokens, retry_response.usage.output_tokens),
        )
        data = _extract_json(raw_text)

    # Ensure channel_name and description match what was requested
    data["channel_name"] = name
    data["description"] = description
    data["tiktok_bio"] = _derive_tiktok_bio(name, data.get("tiktok_bio") or description)

    config = ChannelConfig(**data)

    # Write to disk
    channel_dir = CHANNELS_DIR / slug
    channel_dir.mkdir(parents=True, exist_ok=True)
    config_path = channel_dir / "channel_config.json"
    config_path.write_text(json.dumps(config.model_dump(), indent=2))

    return config
