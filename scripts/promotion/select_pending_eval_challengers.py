#!/usr/bin/env python3
"""Select pending evaluated challenger metadata files from eval root."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Select pending evaluated challengers")
    parser.add_argument("--eval-root", type=Path, required=True)
    parser.add_argument("--out-file", type=Path, required=True)
    args = parser.parse_args()

    eval_root = args.eval_root.resolve()
    out_file = args.out_file
    selected: list[str] = []

    for path in sorted((eval_root / "airflow" / "web_app_data" / "metadata").glob("*/*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        promotion = str(payload.get("promotion_status", "")).strip().lower()
        publish = str(payload.get("publish_status", "")).strip().lower()
        evaluated = str(payload.get("last_evaluated_at", "")).strip()
        rel = str(path.relative_to(eval_root)).replace("\\", "/")
        if promotion == "challenger" and publish == "merged_to_git" and evaluated:
            selected.append(rel)

    out_file.write_text("\n".join(selected) + ("\n" if selected else ""), encoding="utf-8")
    print(f"fallback_selected_count={len(selected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

