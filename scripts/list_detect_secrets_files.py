from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

DEFAULT_MAX_BYTES = 1_048_576


def main() -> None:
    root = Path.cwd()
    exclude_pattern = re.compile(os.environ.get("DETECT_SECRETS_EXCLUDE_RE", r"^$"))
    max_bytes = int(os.environ.get("DETECT_SECRETS_MAX_BYTES", str(DEFAULT_MAX_BYTES)))
    completed = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        check=True,
        capture_output=True,
        text=True,
    )
    for relative_path in completed.stdout.splitlines():
        if exclude_pattern.search(relative_path):
            continue
        path = root / relative_path
        if not path.is_file():
            continue
        if path.stat().st_size > max_bytes:
            continue
        print(relative_path)


if __name__ == "__main__":
    main()
