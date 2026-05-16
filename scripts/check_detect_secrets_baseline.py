from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

SecretKey = tuple[str, str, str]


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "Usage: check_detect_secrets_baseline.py BASELINE SCAN",
            file=sys.stderr,
        )
        return 2
    baseline_path = Path(sys.argv[1])
    scan_path = Path(sys.argv[2])
    baseline = _load_secret_keys(baseline_path)
    scanned = _load_secret_keys(scan_path)
    new_secrets = sorted(scanned - baseline)
    if not new_secrets:
        print("detect-secrets scan matches committed baseline.")
        return 0
    print("New potential secrets found outside .secrets.baseline:", file=sys.stderr)
    for filename, secret_type, hashed_secret in new_secrets:
        print(
            f"  - {filename}: {secret_type} ({hashed_secret[:12]})",
            file=sys.stderr,
        )
    return 1


def _load_secret_keys(path: Path) -> set[SecretKey]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    results = payload.get("results", {})
    if not isinstance(results, dict):
        raise ValueError(f"{path} does not contain detect-secrets results")
    keys: set[SecretKey] = set()
    for filename, findings in results.items():
        if not isinstance(filename, str) or not isinstance(findings, list):
            continue
        for finding in findings:
            key = _secret_key(filename, finding)
            if key is not None:
                keys.add(key)
    return keys


def _secret_key(filename: str, finding: Any) -> SecretKey | None:
    if not isinstance(finding, dict):
        return None
    secret_type = finding.get("type")
    hashed_secret = finding.get("hashed_secret")
    if not isinstance(secret_type, str) or not isinstance(hashed_secret, str):
        return None
    return (filename, secret_type, hashed_secret)


if __name__ == "__main__":
    raise SystemExit(main())
