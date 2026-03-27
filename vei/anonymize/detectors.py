"""PII detection via regex patterns.

Identifies emails, phone numbers, SSNs, and common name patterns in text.
"""

from __future__ import annotations

import re
from typing import NamedTuple

EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
PHONE_RE = re.compile(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
IP_RE = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")


class PIIMatch(NamedTuple):
    kind: str
    value: str
    start: int
    end: int


def detect_pii(text: str) -> list[PIIMatch]:
    """Find all PII matches in a string."""
    matches: list[PIIMatch] = []
    for m in EMAIL_RE.finditer(text):
        matches.append(PIIMatch("email", m.group(), m.start(), m.end()))
    for m in PHONE_RE.finditer(text):
        matches.append(PIIMatch("phone", m.group(), m.start(), m.end()))
    for m in SSN_RE.finditer(text):
        matches.append(PIIMatch("ssn", m.group(), m.start(), m.end()))
    for m in IP_RE.finditer(text):
        if not _is_version_number(m.group()):
            matches.append(PIIMatch("ip", m.group(), m.start(), m.end()))
    return sorted(matches, key=lambda x: x.start)


def has_pii(text: str) -> bool:
    """Quick check: does the text contain any detectable PII?"""
    return bool(detect_pii(text))


def _is_version_number(value: str) -> bool:
    parts = value.split(".")
    return all(p.isdigit() and int(p) < 256 for p in parts) and len(parts) == 4
