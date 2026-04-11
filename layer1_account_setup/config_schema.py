"""
layer1_account_setup/config_schema.py

Pydantic models for channel_config.json.
Used to validate Claude's output before writing to disk.
"""

from pydantic import BaseModel, field_validator
from typing import Dict, List, Optional


class ContentFormula(BaseModel):
    structure: str
    target_duration_seconds: List[int]
    hook_strategy: str


class BrandConstants(BaseModel):
    what_never_changes: List[str]
    caption_style: str
    thumbnail_approach: str


class ToneVariation(BaseModel):
    id: str
    description: str
    example_line: str


class VisualStyle(BaseModel):
    id: str
    description: str
    image_prompt_suffix: str


class VoiceSettings(BaseModel):
    """Generic TTS settings — provider-agnostic. Currently used by Kokoro."""
    voice_id: str   # e.g. "am_adam", "bm_george"
    speed: float = 1.0  # 0.5–2.0


class VoiceStyle(BaseModel):
    id: str
    description: str
    tts_settings: VoiceSettings


class VoiceStrategy(BaseModel):
    """
    Decides how voices are selected per video.
    - "single": one voice for every video (narrator-style channels)
    - "tone_mapped": tone_id → voice_style_id (character-driven channels)
    """
    strategy_type: str          # "single" | "tone_mapped"
    default_voice_id: str       # voice_style_id used as fallback / sole voice
    tone_to_voice_map: Dict[str, str] = {}  # tone_id → voice_style_id


class MusicMood(BaseModel):
    id: str
    description: str
    source: str
    search_terms: List[str]


class CTA(BaseModel):
    end_screen_text: str
    pinned_comment_templates: List[str]
    engagement_style: str = "follow"  # "follow" | "mystery_cta" | "question"


class ContentRestrictions(BaseModel):
    blocked_words: List[str] = []
    blocked_word_rewrites: Dict[str, str] = {}
    title_rules: List[str] = []
    tiktok_hook_rules: List[str] = []


class NarrativeBible(BaseModel):
    world: str
    character_rules: List[str]
    what_to_avoid: List[str]
    universe_lore: List[str]


class ChannelConfig(BaseModel):
    channel_name: str
    description: str
    tiktok_bio: str = ""
    publish_slots: List[str] = []  # HH:MM ET, 24h format — empty = use dynamic schedule
    tiktok_publish_slots: List[str] = []  # HH:MM ET, 1 per day recommended
    narrative_bible: NarrativeBible
    content_formula: ContentFormula
    brand_constants: BrandConstants
    tone_variations: List[ToneVariation]
    visual_styles: List[VisualStyle]
    voice_styles: List[VoiceStyle]
    voice_strategy: VoiceStrategy
    music_moods: List[MusicMood]
    title_templates: List[str]
    subject_bank: List[str]
    hashtags: List[str]
    description_template: str
    cta: CTA
    content_restrictions: ContentRestrictions = ContentRestrictions()
    captioning_mode: str = "word_highlight"   # "static" | "word_highlight" — controls assembler rendering path
    preview_mode: bool = False         # if True, assembled videos hold at 'preview' status instead of auto-queuing
    pexels_visual_style: str = "legacy"
    # "legacy"           — current behavior, object-literal queries (default, safe rollback)
    # "atmospheric"      — scene 0 identifies subject, scenes 1-5 pure mood (abstract subjects: AI, algorithms)
    # "object_cinematic" — scene 0 identifies object, scenes 1-5 mix object + atmosphere (physical object characters)

    @field_validator("tone_variations")
    @classmethod
    def enough_tones(cls, v):
        if len(v) < 10:
            raise ValueError(f"Need at least 10 tone_variations, got {len(v)}")
        return v

    @field_validator("visual_styles")
    @classmethod
    def enough_visual_styles(cls, v):
        if len(v) < 10:
            raise ValueError(f"Need at least 10 visual_styles, got {len(v)}")
        return v

    @field_validator("voice_styles")
    @classmethod
    def enough_voice_styles(cls, v):
        if len(v) < 5:
            raise ValueError(f"Need at least 5 voice_styles, got {len(v)}")
        return v

    @field_validator("music_moods")
    @classmethod
    def enough_music_moods(cls, v):
        if len(v) < 5:
            raise ValueError(f"Need at least 5 music_moods, got {len(v)}")
        return v

    @field_validator("title_templates")
    @classmethod
    def enough_title_templates(cls, v):
        if len(v) < 6:
            raise ValueError(f"Need at least 6 title_templates, got {len(v)}")
        return v

    @field_validator("subject_bank")
    @classmethod
    def enough_subjects(cls, v):
        if len(v) < 100:
            raise ValueError(f"Need at least 100 subjects, got {len(v)}")
        return v

    @field_validator("tiktok_bio")
    @classmethod
    def valid_tiktok_bio(cls, v):
        if v and len(v) > 80:
            raise ValueError(f"TikTok bio must be 80 chars or fewer, got {len(v)}")
        return v
