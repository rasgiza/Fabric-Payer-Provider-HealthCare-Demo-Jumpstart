"""Remove redundant post-deploy cells from Healthcare_Launcher.Notebook.

These cells are superseded by native Jumpstart / fabric-cicd features:
  * CELL 1  — uploaded healthcare_knowledge/*.md from GitHub  -> manifest file-copy
  * CELL 4b — imported a (missing) .pbix to deploy the report -> native .Report item
  * CELL 6  — patched Data Agent datasource IDs at runtime     -> parameter.yml
  * CELL 9  — created category folders via REST (orphaned)     -> native parent folder

A "cell" in canonical Fabric format starts at a `# CELL ********************`
marker and runs until the next `# CELL`/`# MARKDOWN` marker (or end of file).
We identify each target cell by a unique banner substring in its body and drop
the whole cell block.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LAUNCHER = REPO_ROOT / "payer-provider-healthcare" / "Healthcare_Launcher.Notebook" / "notebook-content.py"

# Unique banner substrings identifying the cells to remove.
TARGETS = [
    "CELL 1 — Upload healthcare knowledge docs",
    "CELL 4b — Deploy Power BI Report",
    "CELL 6 — Patch Data Agent sources",
    "CELL 9 — Organize Workspace Items into Folders",
]

MARKER_RE = re.compile(r"^# (CELL|MARKDOWN) \*{20}\s*$", re.MULTILINE)


def remove_cells(text: str, targets: list[str]) -> tuple[str, list[str]]:
    removed: list[str] = []
    for target in targets:
        markers = list(MARKER_RE.finditer(text))
        cut = None
        for i, m in enumerate(markers):
            end = markers[i + 1].start() if i + 1 < len(markers) else len(text)
            block = text[m.start():end]
            if target in block:
                cut = (m.start(), end)
                break
        if cut is None:
            print(f"  [NOT FOUND] {target}")
            continue
        start, end = cut
        text = text[:start] + text[end:]
        removed.append(target)
        print(f"  [REMOVED] {target}")
    return text, removed


def main() -> None:
    text = LAUNCHER.read_text(encoding="utf-8")
    before = len(MARKER_RE.findall(text))
    text, removed = remove_cells(text, TARGETS)

    # Drop the now-unused UPLOAD_KNOWLEDGE_DOCS config flag line.
    text = re.sub(
        r"^UPLOAD_KNOWLEDGE_DOCS\s*=.*\n", "", text, flags=re.MULTILINE
    )

    # Collapse any run of 3+ blank lines left behind into a single blank line.
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.rstrip() + "\n"

    LAUNCHER.write_text(text, encoding="utf-8", newline="\n")
    after = len(MARKER_RE.findall(text))
    print(f"\nRemoved {len(removed)} cell(s). Cell markers: {before} -> {after}.")


if __name__ == "__main__":
    main()
