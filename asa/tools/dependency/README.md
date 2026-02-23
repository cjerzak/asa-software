# Dependency Docs Tooling

This folder contains scripts for generating hierarchical dependency artifacts.

## Commands

- Generate dependency docs:
  - `./asa/tools/dependency/generate_combined_dependency_docs.sh`
- Check dependency docs are current (CI-friendly):
  - `./asa/tools/dependency/check_dependency_docs.sh`

## Outputs

Generated files are written to `tracked_reports/dependency/`:

- `dependency_index.md`
- `dependency_manifest.json`
- `r_file_graph.mmd`
- `python_file_graph.mmd`
- `key_function_edges.mmd`
