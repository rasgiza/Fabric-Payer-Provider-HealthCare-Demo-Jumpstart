"""One-time migration: embed each notebook's lakehouse dependency (currently in the
non-standard .metadata/notebook-metadata.json) into the canonical notebook-content.py
file-level META block, then delete the .metadata folder.

fabric-cicd reads lakehouse deps (default_lakehouse / default_lakehouse_workspace_id)
from the notebook definition's META block, not from .metadata/. Without this, deployed
notebooks have no default lakehouse, so pipeline/SM steps fail.
"""
import json
from pathlib import Path

ws = Path(__file__).resolve().parent.parent / "payer-provider-healthcare"
OLD = ('# META {\n'
       '# META   "kernel_info": {\n'
       '# META     "name": "synapse_pyspark"\n'
       '# META   }\n'
       '# META }')


def emit(obj: dict) -> str:
    raw = json.dumps(obj, indent=2)
    return "\n".join(f"# META {l}" if l else "# META" for l in raw.splitlines())


def main() -> None:
    embedded = 0
    for nb in sorted(ws.glob("*.Notebook")):
        meta = nb / ".metadata" / "notebook-metadata.json"
        content = nb / "notebook-content.py"
        if not meta.exists():
            print(f"  [no .metadata] {nb.name}")
            continue
        deps = json.loads(meta.read_text(encoding="utf-8"))  # {"dependencies": {...}}
        text = content.read_text(encoding="utf-8")
        if OLD not in text:
            print(f"  [WARN block not found] {nb.name}")
            continue
        file_meta = {"kernel_info": {"name": "synapse_pyspark"}, **deps}
        text = text.replace(OLD, emit(file_meta), 1)
        content.write_text(text, encoding="utf-8", newline="\n")
        meta.unlink()
        try:
            (nb / ".metadata").rmdir()
        except OSError:
            pass
        embedded += 1
        print(f"  [EMBEDDED] {nb.name}")
    print(f"\nEmbedded lakehouse deps into {embedded} notebook(s).")


if __name__ == "__main__":
    main()
