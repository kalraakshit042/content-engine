#!/usr/bin/env python3
"""
backup_content_engine.py

Create a timestamped tar.gz backup of the key Content Engine state.

Default destination:
  ~/Library/Application Support/ContentEngineBackups
"""

from __future__ import annotations

import tarfile
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).parent
DEFAULT_BACKUP_DIR = Path.home() / "Library" / "Application Support" / "ContentEngineBackups"
INCLUDE_PATHS = [
    BASE_DIR / "database" / "channels.db",
    BASE_DIR / "database" / "videos.db",
    BASE_DIR / "database" / "costs.db",
    BASE_DIR / "database" / "ops.db",
    BASE_DIR / "channels",
    BASE_DIR / "credentials",
]


def create_backup(backup_dir: Path = DEFAULT_BACKUP_DIR) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    archive_path = backup_dir / f"content-engine-backup-{stamp}.tar.gz"

    with tarfile.open(archive_path, "w:gz") as archive:
        for path in INCLUDE_PATHS:
            if path.exists():
                archive.add(path, arcname=path.relative_to(BASE_DIR))

    return archive_path


def main() -> None:
    archive = create_backup()
    print(archive)


if __name__ == "__main__":
    main()
