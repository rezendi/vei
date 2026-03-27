"""Actor response backends: deterministic (rule-based) and LLM-powered.

DeterministicActorBackend generates responses from templates and persona
traits — fast, free, reproducible. LLMActorBackend wraps any LLM provider
with system prompts built from the persona and current world state.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any, Protocol

from vei.actors.persona import ActorPersona

logger = logging.getLogger(__name__)


class ActorBackend(Protocol):
    """Interface for actor response generation."""

    def respond(
        self,
        persona: ActorPersona,
        message: str,
        *,
        world_context: str = "",
        channel: str = "",
    ) -> str: ...


class DeterministicActorBackend:
    """Rule-based actor responses derived from persona traits.

    Deterministic: same inputs always produce the same output.
    """

    def respond(
        self,
        persona: ActorPersona,
        message: str,
        *,
        world_context: str = "",
        channel: str = "",
    ) -> str:
        h = _content_hash(persona.name, message, channel)
        templates = _templates_for_bias(persona.response_bias)
        template = templates[h % len(templates)]
        return template.format(
            name=persona.name,
            role=persona.role,
            department=persona.department,
        )


class LLMActorBackend:
    """LLM-powered actor responses with cost guardrails.

    Wraps any VEI-supported LLM provider. Includes:
    - Response caching (same context hash returns cached response)
    - Token budget enforcement
    - Automatic fallback to deterministic when budget exhausted
    """

    def __init__(
        self,
        provider: str = "openai",
        model: str = "gpt-4o-mini",
        max_tokens_per_run: int = 50000,
        max_cost_usd: float = 1.0,
        cache_responses: bool = True,
    ) -> None:
        self.provider = provider
        self.model = model
        self.max_tokens_per_run = max_tokens_per_run
        self.max_cost_usd = max_cost_usd
        self.cache_responses = cache_responses
        self._tokens_used = 0
        self._cost_usd = 0.0
        self._cache: dict[str, str] = {}
        self._fallback = DeterministicActorBackend()

    @property
    def budget_exhausted(self) -> bool:
        return (
            self._tokens_used >= self.max_tokens_per_run
            or self._cost_usd >= self.max_cost_usd
        )

    def respond(
        self,
        persona: ActorPersona,
        message: str,
        *,
        world_context: str = "",
        channel: str = "",
    ) -> str:
        if self.budget_exhausted:
            logger.info(
                "actor_budget_exhausted",
                extra={
                    "actor": persona.name,
                    "tokens_used": self._tokens_used,
                    "cost_usd": self._cost_usd,
                },
            )
            return self._fallback.respond(
                persona, message, world_context=world_context, channel=channel
            )

        cache_key = _content_hash(persona.name, message, channel, world_context)
        if self.cache_responses and cache_key in self._cache:
            return self._cache[cache_key]

        try:
            response = self._call_llm(persona, message, world_context)
        except Exception:
            logger.exception(
                "actor_llm_failed",
                extra={"actor": persona.name},
            )
            return self._fallback.respond(
                persona, message, world_context=world_context, channel=channel
            )

        if self.cache_responses:
            self._cache[cache_key] = response
        return response

    def _call_llm(
        self,
        persona: ActorPersona,
        message: str,
        world_context: str,
    ) -> str:
        from vei.llm.providers import plan_once

        system = persona.system_prompt(world_context)
        result = asyncio.run(
            plan_once(
                provider=self.provider,
                model=self.model,
                system=system,
                user=message,
            )
        )

        if isinstance(result, dict):
            text = result.get("response", result.get("text", json.dumps(result)))
        else:
            text = str(result)

        self._tokens_used += len(text.split()) * 2
        self._cost_usd += len(text.split()) * 0.00002

        return text

    def usage_summary(self) -> dict[str, Any]:
        return {
            "tokens_used": self._tokens_used,
            "cost_usd": round(self._cost_usd, 4),
            "max_tokens": self.max_tokens_per_run,
            "max_cost_usd": self.max_cost_usd,
            "budget_exhausted": self.budget_exhausted,
            "cached_responses": len(self._cache),
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COOPERATIVE_TEMPLATES = [
    "Thanks for flagging this. I'll take care of it from the {department} side.",
    "Got it — I'll follow up with the team. {name} ({role}) is on it.",
    "Understood. Let me pull the latest from {department} and get back to you.",
    "Sure, I can help with that. Will have an update by end of day.",
]

_NEUTRAL_TEMPLATES = [
    "Noted. I'll review this when I get a chance.",
    "I've seen this. Let me check with {department} first.",
    "Acknowledged. Will circle back once I have more context.",
]

_RESISTANT_TEMPLATES = [
    "I'm not sure this is the right approach. Can we discuss alternatives?",
    "I have concerns about this — {department} might push back.",
    "This needs more thought. Let's not rush into a decision.",
]

_ADVERSARIAL_TEMPLATES = [
    "This doesn't align with what {department} agreed to. I'd like to revisit.",
    "I disagree with this direction. We need to reconsider.",
    "I'm escalating this — we can't proceed without more review.",
]


def _templates_for_bias(bias: str) -> list[str]:
    mapping = {
        "cooperative": _COOPERATIVE_TEMPLATES,
        "neutral": _NEUTRAL_TEMPLATES,
        "resistant": _RESISTANT_TEMPLATES,
        "adversarial": _ADVERSARIAL_TEMPLATES,
    }
    return mapping.get(bias, _NEUTRAL_TEMPLATES)


def _content_hash(*parts: str) -> int:
    combined = "|".join(parts)
    digest = hashlib.sha256(combined.encode("utf-8"), usedforsecurity=False).digest()
    return int.from_bytes(digest[:4], "big")
