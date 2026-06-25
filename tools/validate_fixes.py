"""Post-edit validation for the maintainer-review fixes.

Checks:
  1. Every notebook-content.py compiles as valid Python (catches edit damage).
  2. parameter.yml is valid YAML, well-formed find_replace rules, regex compiles.
  3. The two manifest YAMLs parse and carry the file-copy keys.
"""
import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WS = ROOT / "payer-provider-healthcare"

errors: list[str] = []

# 1. Compile notebooks. Neutralize Jupyter magics (%pip, %%sql, !cmd) which are
#    valid in Fabric notebooks but not parseable by ast.
MAGIC_RE = re.compile(r"^(\s*)([%!].*)$")


def neutralize_magics(src: str) -> str:
    out = []
    for line in src.splitlines():
        m = MAGIC_RE.match(line)
        out.append(f"{m.group(1)}pass  # magic" if m else line)
    return "\n".join(out)


nbs = sorted(WS.rglob("notebook-content.py"))
for nb in nbs:
    src = neutralize_magics(nb.read_text(encoding="utf-8"))
    try:
        ast.parse(src)
    except SyntaxError as e:
        errors.append(f"PYTHON SYNTAX: {nb.relative_to(ROOT)}:{e.lineno}: {e.msg}")
print(f"[1] Compiled {len(nbs)} notebook(s): {'OK' if not errors else 'FAIL'}")

# 2. parameter.yml
try:
    import yaml
except ImportError:
    yaml = None
    print("[warn] pyyaml not installed; doing text-only checks")

param = WS / "parameter.yml"
ptxt = param.read_text(encoding="utf-8")
if yaml:
    pdata = yaml.safe_load(ptxt)
    rules = pdata.get("find_replace", [])
    print(f"[2] parameter.yml parsed: {len(rules)} find_replace rule(s)")
    for i, r in enumerate(rules):
        if "find_value" not in r or "replace_value" not in r:
            errors.append(f"parameter.yml rule {i}: missing find_value/replace_value")
        if str(r.get("is_regex", "")).lower() == "true":
            try:
                re.compile(r["find_value"])
            except re.error as e:
                errors.append(f"parameter.yml rule {i}: bad regex: {e}")
        rv = r.get("replace_value", {})
        if list(rv.keys()) != ["_ALL_"]:
            errors.append(f"parameter.yml rule {i}: replace_value should have single _ALL_ key, got {list(rv.keys())}")
else:
    print("[2] parameter.yml present (text-only)")

# Sanity: the SM regex actually matches the (pre-logical-id) tmdl URL
tmdl = (WS / "HealthcareDemoHLS.SemanticModel" / "definition" / "expressions.tmdl").read_text(encoding="utf-8")
sm_re = r"onelake\.dfs\.fabric\.microsoft\.com/([0-9a-fA-F-]{36})/[0-9a-fA-F-]{36}"
if not re.search(sm_re, tmdl):
    errors.append("SM regex does not match expressions.tmdl OneLake URL")
else:
    print("[2b] SM Direct Lake regex matches expressions.tmdl URL: OK")

# DataAgent placeholder still present to be replaced
da_hits = list(WS.glob("HealthcareHLSAgent.DataAgent/**/datasource.json"))
da_ph = sum("00000000-0000-0000-0000-000000000003" in p.read_text(encoding="utf-8") for p in da_hits)
print(f"[2c] DataAgent SM placeholder present in {da_ph} datasource.json file(s)")
if da_ph == 0:
    errors.append("DataAgent SM placeholder 00000000-...0003 not found (rule would be a no-op)")

# 3. Manifests
for mf in [ROOT / "payer-provider-healthcare.yml",
           ROOT / "fabric-jumpstart/src/fabric_jumpstart/fabric_jumpstart/jumpstarts/community/payer-provider-healthcare.yml"]:
    if not mf.exists():
        print(f"[3] (skip, not present) {mf.relative_to(ROOT)}")
        continue
    if yaml:
        m = yaml.safe_load(mf.read_text(encoding="utf-8"))
        has = "files_source_path" in m.get("source", {})
        print(f"[3] {mf.name} parsed; file-copy keys: {'present' if has else 'MISSING'}")
    else:
        print(f"[3] {mf.name} present")

# stray inline META across notebooks
stray = 0
for nb in nbs:
    stray += len(re.findall(r'(?m)^# META \{"language"', nb.read_text(encoding="utf-8")))
print(f"[4] stray inline language META lines: {stray}")
if stray:
    errors.append(f"{stray} stray inline language META line(s) remain")

print("\n" + ("ALL CHECKS PASSED" if not errors else "FAILURES:\n  " + "\n  ".join(errors)))
sys.exit(1 if errors else 0)
