#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${1:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
OUT_DIR="${2:-${REPO_ROOT}/tracked_reports/dependency}"

mkdir -p "${OUT_DIR}"

Rscript "${REPO_ROOT}/asa/tools/dependency/generate_r_dependency_map.R" "${REPO_ROOT}" "${OUT_DIR}"
python3 "${REPO_ROOT}/asa/tools/dependency/generate_python_dependency_map.py" "${REPO_ROOT}" "${OUT_DIR}"

python3 - "${REPO_ROOT}" "${OUT_DIR}" <<'PY'
from __future__ import annotations

import json
import pathlib
import sys
from collections import defaultdict
from datetime import datetime, timezone

repo_root = pathlib.Path(sys.argv[1]).resolve()
out_dir = pathlib.Path(sys.argv[2]).resolve()

r_manifest = json.loads((out_dir / "r_manifest.json").read_text(encoding="utf-8"))
py_manifest = json.loads((out_dir / "python_manifest.json").read_text(encoding="utf-8"))

generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def top_hubs(edges: list[dict], limit: int = 10) -> list[dict]:
    out_degree: dict[str, int] = defaultdict(int)
    in_degree: dict[str, int] = defaultdict(int)
    for edge in edges:
        src = edge.get("from", "")
        dst = edge.get("to", "")
        if src:
            out_degree[src] += 1
        if dst:
            in_degree[dst] += 1
    nodes = set(out_degree.keys()) | set(in_degree.keys())
    ranked = sorted(
        nodes,
        key=lambda n: (out_degree.get(n, 0), in_degree.get(n, 0), n),
        reverse=True,
    )
    return [
        {
            "node": node,
            "out_degree": out_degree.get(node, 0),
            "in_degree": in_degree.get(node, 0),
        }
        for node in ranked[:limit]
    ]

r_edges = r_manifest.get("edges", [])
py_edges = py_manifest.get("edges", [])

combined = {
    "generated_at": generated_at,
    "repo_root": str(repo_root),
    "r": r_manifest,
    "python": py_manifest,
    "summary": {
        "r_files": len(r_manifest.get("nodes", [])),
        "python_files": len(py_manifest.get("nodes", [])),
        "r_edges": len(r_edges),
        "python_edges": len(py_edges),
        "python_module_edges_from_r": len(r_manifest.get("python_module_edges", [])),
    },
    "hubs": {
        "r": top_hubs(r_edges),
        "python": top_hubs(py_edges),
    },
}

(out_dir / "dependency_manifest.json").write_text(
    json.dumps(combined, indent=2), encoding="utf-8"
)

def node_id(label: str) -> str:
    return "k_" + "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in label)

key_lines = ["graph TD"]
nodes: set[str] = set()
edges: list[str] = []

for edge in r_manifest.get("key_function_edges", []):
    src_file = edge.get("from_file", "")
    dst_file = edge.get("to_file", "")
    call = edge.get("call", "call")
    if not src_file or not dst_file:
        continue
    src_label = pathlib.Path(src_file).name
    dst_label = pathlib.Path(dst_file).name
    src_id = node_id(f"r:{src_label}")
    dst_id = node_id(f"r:{dst_label}")
    nodes.add(f'  {src_id}["{src_label}"]')
    nodes.add(f'  {dst_id}["{dst_label}"]')
    edges.append(f"  {src_id} -->|{call}| {dst_id}")

for edge in py_manifest.get("key_function_edges", []):
    src_file = edge.get("from_file", "")
    dst_file = edge.get("to_file", "")
    from_fn = edge.get("from_function", "")
    call = edge.get("call", "call")
    if not src_file or not dst_file:
        continue
    src_label = f"{pathlib.Path(src_file).name}::{from_fn}()"
    dst_label = pathlib.Path(dst_file).name
    src_id = node_id(f"py:{src_label}")
    dst_id = node_id(f"py:{dst_label}")
    nodes.add(f'  {src_id}["{src_label}"]')
    nodes.add(f'  {dst_id}["{dst_label}"]')
    edges.append(f"  {src_id} -->|{call}| {dst_id}")

key_lines.extend(sorted(nodes))
key_lines.extend(sorted(set(edges)))
(out_dir / "key_function_edges.mmd").write_text("\n".join(key_lines) + "\n", encoding="utf-8")

def fmt_hubs(items: list[dict]) -> list[str]:
    lines: list[str] = []
    for row in items:
        node = row.get("node", "")
        out_d = row.get("out_degree", 0)
        in_d = row.get("in_degree", 0)
        lines.append(f"- `{node}` (out: {out_d}, in: {in_d})")
    return lines

summary = combined["summary"]
md_lines = [
    "# Dependency Index",
    "",
    f"Generated: `{generated_at}`",
    "",
    "## Summary",
    "",
    f"- R files: `{summary['r_files']}`",
    f"- Python files: `{summary['python_files']}`",
    f"- R file-level edges: `{summary['r_edges']}`",
    f"- Python file-level edges: `{summary['python_edges']}`",
    f"- R -> Python module edges: `{summary['python_module_edges_from_r']}`",
    "",
    "## Graph Files",
    "",
    "- `r_file_graph.mmd`",
    "- `python_file_graph.mmd`",
    "- `key_function_edges.mmd`",
    "",
    "## Top R Hubs",
    "",
    *fmt_hubs(combined["hubs"]["r"]),
    "",
    "## Top Python Hubs",
    "",
    *fmt_hubs(combined["hubs"]["python"]),
    "",
    "## Machine-Readable Manifest",
    "",
    "- `dependency_manifest.json`",
    "- `r_manifest.json`",
    "- `python_manifest.json`",
    "",
]

(out_dir / "dependency_index.md").write_text("\n".join(md_lines), encoding="utf-8")
PY

echo "Wrote dependency artifacts to ${OUT_DIR}"
