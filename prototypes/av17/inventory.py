# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file.
# SPDX-License-Identifier: Apache-2.0

"""Read-only, bounded inventory. Run on Gadi; write JSON to stdout."""

import collections
import datetime
import json
import os
import time
from pathlib import Path

COLLECTION_DEPTH = 2
EXAMPLE_LIMIT = 3


def inventory(root, seconds=45):
    root = Path(root)
    started = time.monotonic()
    groups = {}
    cryosphere_files = []
    errors = []
    complete = True
    for directory, dirs, files in os.walk(
        root, followlinks=False, onerror=lambda e: errors.append(str(e))
    ):
        dirs.sort()
        for name in sorted(files):
            if time.monotonic() - started > seconds:
                complete = False
                break
            path = Path(directory) / name
            parts = path.relative_to(root).parts
            key = (
                "/".join(parts[:COLLECTION_DEPTH])
                if len(parts) > COLLECTION_DEPTH
                else parts[0]
            )
            group = groups.setdefault(
                key,
                {
                    "files": 0,
                    "bytes": 0,
                    "extensions": collections.Counter(),
                    "examples": [],
                },
            )
            try:
                stat = path.lstat()
                ext = path.suffix.lower() or "(none)"
                group["files"] += 1
                group["bytes"] += stat.st_size
                group["extensions"][ext] += 1
                if len(group["examples"]) < EXAMPLE_LIMIT:
                    group["examples"].append(str(path))
                if parts[0] == "cryosphere-data-pool":
                    cryosphere_files.append(
                        {
                            "path": str(path),
                            "bytes": stat.st_size,
                            "mtime_ns": stat.st_mtime_ns,
                            "symlink": path.is_symlink(),
                        }
                    )
            except OSError as exc:
                errors.append(str(exc))
        if not complete:
            break
    return {
        "root": str(root),
        "observed_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "complete": complete,
        "elapsed_seconds": time.monotonic() - started,
        "errors": errors,
        "collections": groups,
        "cryosphere_files": cryosphere_files,
    }


if __name__ == "__main__":
    print(json.dumps(inventory("/g/data/av17/access-nri"), indent=2))
