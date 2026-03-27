"""Actor persona definitions for hybrid simulation.

An ActorPersona describes a simulated person in the enterprise — their
role, personality, communication style, and knowledge scope. These are
used by both deterministic and LLM-powered actor backends.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field


class ActorPersona(BaseModel):
    """A simulated person in the enterprise world."""

    name: str
    email: str = ""
    role: str = ""
    department: str = ""
    personality: str = ""
    communication_style: Literal[
        "formal", "casual", "terse", "verbose", "technical"
    ] = "formal"
    knowledge_scope: List[str] = Field(default_factory=list)
    response_bias: Literal["cooperative", "neutral", "resistant", "adversarial"] = (
        "cooperative"
    )
    backend: Literal["deterministic", "llm"] = "deterministic"
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def system_prompt(self, world_context: str = "") -> str:
        """Build a system prompt for LLM-backed responses."""
        parts = [
            f"You are {self.name}, {self.role} in the {self.department} department.",
        ]
        if self.personality:
            parts.append(f"Personality: {self.personality}")
        parts.append(f"Communication style: {self.communication_style}")
        parts.append(f"Response tendency: {self.response_bias}")
        if self.knowledge_scope:
            parts.append(f"You have knowledge about: {', '.join(self.knowledge_scope)}")
        parts.append(
            "Respond in character. Keep responses concise and realistic. "
            "Do not break character or acknowledge being an AI."
        )
        if world_context:
            parts.append(f"\nCurrent situation:\n{world_context}")
        return "\n".join(parts)
