"""One-off analysis: which SM sourceColumns are NOT produced by the gold notebooks.

The semantic model is the contract. For each SM table we collect its
`sourceColumn` values, then compare against the columns the gold notebooks
actually emit:
  * 06a  CREATE TABLE IF NOT EXISTS <t> ( <col type>, ... )
  * 06b  ...select( ... .alias("col") ... ).saveAsTable(<t>)   (overwriteSchema)

Prints, per table, the columns the SM needs that the notebooks don't build.
"""
from __future__ import annotations
import re
from pathlib import Path

ROOT = Path("payer-provider-healthcare")
SM_TABLES = ROOT / "HealthcareDemoHLS.SemanticModel" / "definition" / "tables"
NB_06A = ROOT / "06a_Create_Gold_Lakehouse_Tables.Notebook" / "notebook-content.py"
NB_06B = ROOT / "06b_Gold_Transform_Load_v2.Notebook" / "notebook-content.py"


def sm_source_columns(tmdl: Path) -> list[str]:
    cols: list[str] = []
    for ln in tmdl.read_text(encoding="utf-8").splitlines():
        m = re.match(r"\s*sourceColumn:\s*(.+?)\s*$", ln)
        if m:
            cols.append(m.group(1).strip("'\""))
    return cols


def create_table_columns(text: str) -> dict[str, set[str]]:
    """Parse 06a CREATE TABLE IF NOT EXISTS <t> ( col TYPE, ... )."""
    out: dict[str, set[str]] = {}
    for m in re.finditer(
        r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\)\s*\n\s*USING DELTA",
        text, re.DOTALL):
        tbl, body = m.group(1), m.group(2)
        cols: set[str] = set()
        for raw in body.splitlines():
            line = raw.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            cols.add(line.split()[0])
        out[tbl] = cols
    return out


def saveastable_columns(text: str) -> dict[str, set[str]]:
    """Approximate 06b output schema: alias("x") tokens near each saveAsTable.

    We split the file on saveAsTable(... <t> ...) and, for each segment, take the
    .alias("col") names that precede it since the previous saveAsTable. This is
    heuristic but good enough to surface obvious gaps.
    """
    out: dict[str, set[str]] = {}
    # map of TABLE_CONST -> "gold.table" if defined like NAME = f"{GOLD}.table"
    const_map = dict(re.findall(r"(\w+)\s*=\s*f?\"\{GOLD\}\.(\w+)\"", text))
    parts = re.split(r"saveAsTable\(([^)]+)\)", text)
    # parts: [seg0, arg1, seg1, arg2, seg2, ...]; segment before arg_i is parts[2*i]
    for i in range(1, len(parts), 2):
        arg = parts[i].strip()
        seg = parts[i - 1]
        # resolve table name
        tbl = None
        m = re.search(r"\{GOLD\}\.(\w+)", arg)
        if m:
            tbl = m.group(1)
        else:
            cm = re.match(r"(\w+)$", arg)
            if cm and cm.group(1) in const_map:
                tbl = const_map[cm.group(1)]
        if not tbl:
            continue
        aliases = set(re.findall(r'\.alias\("([^"]+)"\)', seg))
        cols = set(re.findall(r'col\("(?:\w+\.)?([^"]+)"\)', seg))  # passthrough cols
        out.setdefault(tbl, set()).update(aliases)
    return out


def main() -> None:
    text_a = NB_06A.read_text(encoding="utf-8")
    text_b = NB_06B.read_text(encoding="utf-8")
    created = create_table_columns(text_a)
    saved = saveastable_columns(text_b)

    print(f"{'TABLE':<26} MISSING-FROM-NOTEBOOKS (SM needs, notebook lacks)")
    print("-" * 90)
    any_gap = False
    for tmdl in sorted(SM_TABLES.glob("*.tmdl")):
        tbl = tmdl.stem
        need = set(sm_source_columns(tmdl))
        have = set(created.get(tbl, set())) | set(saved.get(tbl, set()))
        missing = sorted(need - have)
        flag = ""
        if missing:
            any_gap = True
            flag = ", ".join(missing)
        print(f"{tbl:<26} {flag}")
    if not any_gap:
        print("\nNo gaps: every SM sourceColumn is produced by the notebooks.")


if __name__ == "__main__":
    main()
