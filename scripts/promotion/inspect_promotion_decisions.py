#!/usr/bin/env python3
"""Inspect promotion decisions and emit expected promoted files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect promotion decisions")
    parser.add_argument("--decision-file", type=Path, required=True)
    parser.add_argument("--expected-out", type=Path, required=True)
    parser.add_argument("--env-out", type=Path, required=True)
    args = parser.parse_args()

    payload = json.loads(args.decision_file.read_text(encoding="utf-8"))
    decisions = payload.get("decisions", [])
    expected: list[str] = []
    failed: list[tuple[str, str, str]] = []

    for d in decisions:
        rel = str(d.get("metadata_file") or "").strip()
        eligible = bool(d.get("eligible"))
        promoted = bool(d.get("promoted"))
        reason = str(d.get("reason") or "").strip()
        details = str(d.get("details") or "").strip()
        print(f"decision metadata_file={rel} eligible={eligible} promoted={promoted} reason={reason}")
        if details:
            print(f"decision_details metadata_file={rel} details={details}")
        if rel and promoted:
            expected.append(rel)
        if rel and eligible and not promoted:
            failed.append((rel, reason or "unknown_reason", details))

    args.expected_out.write_text("\n".join(expected) + ("\n" if expected else ""), encoding="utf-8")
    print(f"expected_promotions_count={len(expected)}")

    if failed:
        print("Promotion failures detected for eligible metadata:")
        for rel, reason, details in failed:
            print(f"- metadata_file={rel} reason={reason} details={details}")
        return 1

    args.env_out.write_text(f"PROD_EXPECTED_PROMOTION_COUNT={len(expected)}\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

