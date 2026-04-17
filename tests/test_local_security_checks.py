from __future__ import annotations

import subprocess
from pathlib import Path

from scripts.run_local_security_checks import collect_changed_files


def _git(root: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True, text=True)


def test_collect_changed_files_includes_modified_and_untracked_files(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.name", "Codex")
    _git(root, "config", "user.email", "codex@example.com")

    tracked = root / "tracked.py"
    tracked.write_text("value = 1\n", encoding="utf-8")
    ignored = root / ".gitignore"
    ignored.write_text("ignored.txt\n", encoding="utf-8")
    _git(root, "add", "tracked.py", ".gitignore")
    _git(root, "commit", "-m", "init")

    tracked.write_text("value = 2\n", encoding="utf-8")
    untracked = root / "notes.txt"
    untracked.write_text("hello\n", encoding="utf-8")
    ignored_file = root / "ignored.txt"
    ignored_file.write_text("skip\n", encoding="utf-8")

    changed = [
        path.relative_to(root).as_posix() for path in collect_changed_files(root)
    ]

    assert "tracked.py" in changed
    assert "notes.txt" in changed
    assert "ignored.txt" not in changed
