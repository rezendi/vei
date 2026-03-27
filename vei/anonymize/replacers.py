"""Deterministic pseudonymization for PII values.

Uses hash-based mapping so the same real value always maps to the same
fake value within a session — preserving referential integrity across
surfaces (the same email appears as the same pseudonym everywhere).
"""

from __future__ import annotations

import hashlib
from typing import Any

from vei.anonymize.detectors import EMAIL_RE, IP_RE, PHONE_RE, SSN_RE

_FIRST_NAMES = [
    "Alex",
    "Jordan",
    "Taylor",
    "Morgan",
    "Casey",
    "Riley",
    "Quinn",
    "Avery",
    "Cameron",
    "Drew",
    "Sage",
    "Blake",
    "Parker",
    "Reese",
    "Finley",
    "Rowan",
    "Skyler",
    "Emery",
]
_LAST_NAMES = [
    "Smith",
    "Chen",
    "Patel",
    "Kim",
    "Nguyen",
    "Brown",
    "Lee",
    "Garcia",
    "Miller",
    "Davis",
    "Wilson",
    "Moore",
    "Taylor",
    "Clark",
    "Hall",
    "Young",
    "King",
    "Wright",
]
_DOMAINS = [
    "acme.example",
    "initech.example",
    "globex.example",
    "hooli.example",
    "piedpiper.example",
    "waystar.example",
]


class DeterministicReplacer:
    """Maps real PII to fake PII using deterministic hashing.

    Same input always produces the same output within a session,
    so referential integrity across surfaces is preserved.
    """

    def __init__(self, salt: str = "vei-anon-2024") -> None:
        self._salt = salt
        self._cache: dict[str, str] = {}

    def replace_email(self, email: str) -> str:
        key = f"email:{email.lower()}"
        if key not in self._cache:
            h = self._hash(key)
            first = _FIRST_NAMES[h % len(_FIRST_NAMES)]
            last = _LAST_NAMES[(h >> 8) % len(_LAST_NAMES)]
            domain = _DOMAINS[(h >> 16) % len(_DOMAINS)]
            self._cache[key] = f"{first.lower()}.{last.lower()}@{domain}"
        return self._cache[key]

    def replace_phone(self, phone: str) -> str:
        key = f"phone:{phone}"
        if key not in self._cache:
            h = self._hash(key)
            area = 200 + (h % 800)
            mid = 200 + ((h >> 10) % 800)
            last = 1000 + ((h >> 20) % 9000)
            self._cache[key] = f"({area}) {mid}-{last}"
        return self._cache[key]

    def replace_ssn(self, ssn: str) -> str:
        key = f"ssn:{ssn}"
        if key not in self._cache:
            h = self._hash(key)
            self._cache[key] = (
                f"{900 + h % 100:03d}-{70 + (h >> 7) % 30:02d}-{(h >> 14) % 10000:04d}"
            )
        return self._cache[key]

    def replace_ip(self, ip: str) -> str:
        key = f"ip:{ip}"
        if key not in self._cache:
            h = self._hash(key)
            self._cache[key] = f"10.{(h >> 8) % 256}.{(h >> 16) % 256}.{h % 256}"
        return self._cache[key]

    def replace_name(self, name: str) -> str:
        key = f"name:{name.lower()}"
        if key not in self._cache:
            h = self._hash(key)
            first = _FIRST_NAMES[h % len(_FIRST_NAMES)]
            last = _LAST_NAMES[(h >> 8) % len(_LAST_NAMES)]
            self._cache[key] = f"{first} {last}"
        return self._cache[key]

    def replace_text(self, text: str) -> str:
        """Replace all detected PII in a text string."""
        result = text
        result = EMAIL_RE.sub(lambda m: self.replace_email(m.group()), result)
        result = SSN_RE.sub(lambda m: self.replace_ssn(m.group()), result)
        result = PHONE_RE.sub(lambda m: self.replace_phone(m.group()), result)
        result = IP_RE.sub(lambda m: self.replace_ip(m.group()), result)
        return result

    def replace_in_dict(self, data: dict[str, Any]) -> dict[str, Any]:
        """Recursively replace PII in a dict structure."""
        return {k: self._replace_value(v) for k, v in data.items()}

    def _replace_value(self, value: Any) -> Any:
        if isinstance(value, str):
            return self.replace_text(value)
        if isinstance(value, dict):
            return self.replace_in_dict(value)
        if isinstance(value, list):
            return [self._replace_value(item) for item in value]
        return value

    def _hash(self, value: str) -> int:
        digest = hashlib.sha256(
            f"{self._salt}:{value}".encode("utf-8"),
            usedforsecurity=False,
        ).digest()
        return int.from_bytes(digest[:4], "big")
