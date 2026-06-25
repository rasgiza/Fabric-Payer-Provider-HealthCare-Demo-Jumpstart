# Contributing

Thanks for your interest in improving the **Fabric Payer/Provider Healthcare
Jumpstart**.

This repository is the source code for a Microsoft Fabric Jumpstart catalog
entry. The catalog manifest is hosted in
[microsoft/fabric-jumpstart](https://github.com/microsoft/fabric-jumpstart);
this repo is the deployment payload referenced by that manifest.

## Reporting issues

Open a GitHub issue with:

- A clear title and description.
- Steps to reproduce, expected vs actual behaviour.
- Fabric capacity SKU (e.g. F4), region, and which launcher cell failed.
- The full traceback or notebook cell output (paste as a fenced code block).

## Pull requests

1. Fork the repo and create a feature branch off `main`.
2. Follow existing conventions:
   - Fabric items live in `payer-provider-healthcare/<Name>.<ItemType>/` using
     the Fabric Git Integration `2.0.0` platform schema.
   - **No spaces in folder names.** Display names (in `.platform`
     `metadata.displayName` and `manifest.json` `agentName`) may contain
     spaces; folder names must not.
   - Notebooks are stored as Fabric `notebook-content.py` source. Do not
     commit `.ipynb` files into `payer-provider-healthcare/`.
   - Stable `logicalId` GUIDs in `.platform` files are intentional &mdash; do
     not regenerate them, as the launcher patches items by display name and
     downstream pipelines may bind by logical id.
3. Keep the diff focused. One logical change per PR.
4. Test locally end-to-end before opening the PR:
   - Install in a fresh F4+ workspace via
     `fabric_jumpstart.install('payer-provider-healthcare', ...)` once the
     catalog manifest is merged, **or** by cloning this repo and running
     `Healthcare_Launcher.Notebook` directly.
   - Run all 11 cells; confirm exit messages are clean and the Power BI
     report + Data Agent + RTI dashboard load.
5. Update `README.md` and any affected `*_GUIDE.md` files in the same PR.

## Catalog manifest changes

If you change item names, folder structure, or the entry point, you also need
to open a follow-up PR against
[microsoft/fabric-jumpstart](https://github.com/microsoft/fabric-jumpstart)
updating
`src/fabric_jumpstart/fabric_jumpstart/jumpstarts/community/payer-provider-healthcare.yml`.
Bump `repo_ref` to a new tag (e.g. `v1.1.0`) and `last_updated` to the change
date.

## Code of conduct

This project adopts the
[Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
