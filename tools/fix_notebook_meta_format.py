"""Normalize Fabric notebook-content.py files to the canonical Git-integration v2 format.

Problem (reported by the fabric-jumpstart maintainer):
  The notebooks in this repo use a malformed per-cell metadata form::

      # CELL ********************

      # META {"language":"python"}

      <body>

  This places a single-line ``# META {...}`` *before* the cell body and omits the
  ``# METADATA ********************`` separator and the file-level kernel metadata
  block. Fabric's parser does not treat that line as metadata:
    * In MARKDOWN cells it renders ``META {"language":"markdown"}`` as literal text.
    * In CODE cells it leaves a stray ``# META {...}`` comment in every cell.

Canonical format (matches microsoft/fabric-stateful-streaming-lakehouse)::

      # Fabric notebook source

      # METADATA ********************

      # META {
      # META   "kernel_info": {
      # META     "name": "synapse_pyspark"
      # META   }
      # META }

      # MARKDOWN ********************

      # <markdown body>          <- no META block for markdown cells

      # CELL ********************

      <code body>

      # METADATA ********************

      # META {
      # META   "language": "python",
      # META   "language_group": "synapse_pyspark"
      # META }

Usage::

    python tools/fix_notebook_meta_format.py           # dry-run: list files that would change
    python tools/fix_notebook_meta_format.py --apply    # rewrite in place
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE = REPO_ROOT / "payer-provider-healthcare"

FILE_HEADER = "# Fabric notebook source"
SEP = "*" * 20
KERNEL_META = {"kernel_info": {"name": "synapse_pyspark"}}

# Map a cell language to its language_group (only the languages used in this repo).
LANGUAGE_GROUP = {
    "python": "synapse_pyspark",
    "sparksql": "synapse_pyspark",
    "scala": "synapse_pyspark",
    "csharp": "synapse_pyspark",
}

# A cell starts with: "# CELL ****..." or "# MARKDOWN ****..."
CELL_START_RE = re.compile(r"^# (?P<kind>CELL|MARKDOWN) \*{20}\s*$")
# The malformed inline meta line: # META {"language":"python"}
INLINE_META_RE = re.compile(r'^# META (?P<json>\{.*\})\s*$')


def _emit_meta_block(obj: dict) -> str:
    """Render dict as ``# META`` prefixed JSON lines (canonical Fabric form)."""
    raw = json.dumps(obj, indent=2)
    return "\n".join(f"# META {line}" if line else "# META" for line in raw.splitlines())


def already_canonical(text: str) -> bool:
    return f"# METADATA {SEP}" in text


def convert(text: str) -> str:
    """Convert the malformed single-line-meta format to canonical format."""
    if already_canonical(text):
        return text

    lines = text.splitlines()
    if not lines or lines[0].strip() != FILE_HEADER:
        raise ValueError("file does not start with the Fabric notebook header")

    # Locate every cell-start marker.
    starts = [i for i, ln in enumerate(lines) if CELL_START_RE.match(ln)]
    if not starts:
        raise ValueError("no '# CELL'/'# MARKDOWN' markers found")

    out: list[str] = [
        FILE_HEADER,
        "",
        f"# METADATA {SEP}",
        "",
        _emit_meta_block(KERNEL_META),
    ]

    for idx, start in enumerate(starts):
        kind = CELL_START_RE.match(lines[start]).group("kind")
        end = starts[idx + 1] if idx + 1 < len(starts) else len(lines)

        # Body region is everything after the marker up to the next marker.
        body_lines = lines[start + 1:end]

        # Extract & drop the inline "# META {...}" line; capture its language.
        language = "markdown" if kind == "MARKDOWN" else "python"
        cleaned: list[str] = []
        for ln in body_lines:
            m = INLINE_META_RE.match(ln)
            if m:
                try:
                    language = json.loads(m.group("json")).get("language", language)
                except json.JSONDecodeError:
                    pass
                continue  # drop the malformed meta line
            cleaned.append(ln)

        body = "\n".join(cleaned).strip("\n")

        out.append("")
        out.append(f"# {kind} {SEP}")
        out.append("")
        if body:
            out.append(body)

        if kind == "CELL":
            cell_meta = {"language": language}
            group = LANGUAGE_GROUP.get(language)
            if group:
                cell_meta["language_group"] = group
            out.append("")
            out.append(f"# METADATA {SEP}")
            out.append("")
            out.append(_emit_meta_block(cell_meta))

    return "\n".join(out).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="rewrite files in place")
    args = parser.parse_args()

    files = sorted(WORKSPACE.rglob("notebook-content.py"))
    if not files:
        print(f"No notebook-content.py files under {WORKSPACE}")
        return 1

    changed = 0
    for f in files:
        original = f.read_text(encoding="utf-8")
        try:
            converted = convert(original)
        except ValueError as e:
            print(f"  [SKIP] {f.relative_to(REPO_ROOT)}: {e}")
            continue
        if converted != original:
            changed += 1
            rel = f.relative_to(REPO_ROOT)
            if args.apply:
                f.write_text(converted, encoding="utf-8", newline="\n")
                print(f"  [FIXED] {rel}")
            else:
                print(f"  [WOULD FIX] {rel}")

    verb = "Fixed" if args.apply else "Would fix"
    print(f"\n{verb} {changed}/{len(files)} notebook(s).")
    if not args.apply and changed:
        print("Run with --apply to rewrite in place.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
