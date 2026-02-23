#!/usr/bin/env python3
"""Generate Python dependency artifacts for the ASA codebase."""

from __future__ import annotations

import ast
import json
import os
import pathlib
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def node_id(path: str) -> str:
    safe = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in path)
    return f"py_{safe}"


def module_from_rel(rel: str) -> str:
    no_ext = rel[:-3] if rel.endswith(".py") else rel
    if no_ext.endswith("/__init__"):
        no_ext = no_ext[: -len("/__init__")]
    return no_ext.replace("/", ".")


def rel_from_abs(path: pathlib.Path, base: pathlib.Path) -> str:
    return path.resolve().relative_to(base.resolve()).as_posix()


def discover_python_files(py_root: pathlib.Path) -> List[pathlib.Path]:
    out: List[pathlib.Path] = []
    for path in sorted(py_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        out.append(path)
    return out


def resolve_absolute_target(
    candidate: str, module_to_file: Dict[str, str]
) -> Optional[str]:
    if not candidate:
        return None
    if candidate in module_to_file:
        return module_to_file[candidate]
    return None


def resolve_relative_base(
    *,
    current_module: str,
    is_package_file: bool,
    imported_module: Optional[str],
    level: int,
) -> str:
    if level <= 0:
        return imported_module or ""

    if is_package_file:
        current_pkg = current_module.split(".")
    else:
        current_pkg = current_module.split(".")[:-1]

    trim = max(0, level - 1)
    if trim > 0:
        if trim > len(current_pkg):
            current_pkg = []
        else:
            current_pkg = current_pkg[: len(current_pkg) - trim]

    if imported_module:
        current_pkg.extend(imported_module.split("."))
    return ".".join([p for p in current_pkg if p])


def call_name(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def main() -> int:
    repo_root = pathlib.Path(sys.argv[1]).resolve() if len(sys.argv) >= 2 else pathlib.Path.cwd().resolve()
    out_dir = pathlib.Path(sys.argv[2]).resolve() if len(sys.argv) >= 3 else (repo_root / "tracked_reports" / "dependency")
    py_root = repo_root / "asa" / "inst" / "python"
    if not py_root.exists():
        raise SystemExit(f"Missing Python tree: {py_root}")

    out_dir.mkdir(parents=True, exist_ok=True)

    files = discover_python_files(py_root)
    rel_files = [rel_from_abs(path, repo_root) for path in files]

    module_to_file: Dict[str, str] = {}
    file_to_module: Dict[str, str] = {}
    is_package_file: Dict[str, bool] = {}
    for path, rel in zip(files, rel_files):
        rel_from_py_root = rel_from_abs(path, py_root)
        module_name = module_from_rel(rel_from_py_root)
        module_to_file[module_name] = rel
        file_to_module[rel] = module_name
        is_package_file[rel] = rel_from_py_root.endswith("__init__.py")

    parse_trees: Dict[str, ast.AST] = {}
    parse_errors: Dict[str, str] = {}
    top_level_defs: Dict[str, Set[str]] = defaultdict(set)
    func_to_files: Dict[str, Set[str]] = defaultdict(set)

    for path, rel in zip(files, rel_files):
        try:
            src = path.read_text(encoding="utf-8")
            tree = ast.parse(src, filename=rel)
            parse_trees[rel] = tree
        except Exception as exc:  # pragma: no cover
            parse_errors[rel] = str(exc)
            continue

        for node in tree.body:  # type: ignore[attr-defined]
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                top_level_defs[rel].add(node.name)
                func_to_files[node.name].add(rel)

    edges: Set[Tuple[str, str, str]] = set()
    unresolved_imports: List[Dict[str, str]] = []

    for rel, tree in parse_trees.items():
        cur_mod = file_to_module[rel]
        cur_is_pkg = is_package_file[rel]

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    target = resolve_absolute_target(alias.name, module_to_file)
                    if target is not None and target != rel:
                        edges.add((rel, target, "import"))
                    else:
                        unresolved_imports.append(
                            {"from_file": rel, "import": alias.name, "kind": "import"}
                        )
            elif isinstance(node, ast.ImportFrom):
                base = resolve_relative_base(
                    current_module=cur_mod,
                    is_package_file=cur_is_pkg,
                    imported_module=node.module,
                    level=node.level,
                )

                # Capture direct module import edge (from x import y)
                target_base = resolve_absolute_target(base, module_to_file)
                if target_base is not None and target_base != rel:
                    edges.add((rel, target_base, "import_from"))

                for alias in node.names:
                    if alias.name == "*":
                        continue
                    candidate = f"{base}.{alias.name}" if base else alias.name
                    target = resolve_absolute_target(candidate, module_to_file)
                    if target is not None and target != rel:
                        edges.add((rel, target, "import_from_symbol"))
                    elif target_base is None:
                        unresolved_imports.append(
                            {
                                "from_file": rel,
                                "import": candidate,
                                "kind": "import_from_symbol",
                            }
                        )

    key_calls = {
        "create_standard_agent",
        "create_memory_folding_agent",
        "create_memory_folding_agent_with_checkpointer",
        "invoke_graph_safely",
        "configure_search",
        "configure_tor",
        "configure_tor_registry",
        "configure_anti_detection",
        "evaluate_schema_outcome",
        "evaluate_research_outcome",
        "create_research_graph",
        "run_research",
        "stream_research",
        "create_audit_graph",
        "run_audit",
    }

    key_edges: List[Dict[str, str]] = []
    for rel, tree in parse_trees.items():
        for node in tree.body:  # type: ignore[attr-defined]
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            from_fn = node.name
            for call in ast.walk(node):
                if not isinstance(call, ast.Call):
                    continue
                callee = call_name(call.func)
                if not callee or callee not in key_calls:
                    continue
                targets = sorted(func_to_files.get(callee, set()))
                if not targets:
                    key_edges.append(
                        {
                            "from_file": rel,
                            "from_function": from_fn,
                            "call": callee,
                            "to_file": "",
                        }
                    )
                else:
                    for to_file in targets:
                        if to_file == rel:
                            continue
                        key_edges.append(
                            {
                                "from_file": rel,
                                "from_function": from_fn,
                                "call": callee,
                                "to_file": to_file,
                            }
                        )

    edges_sorted = sorted(edges, key=lambda x: (x[0], x[1], x[2]))

    node_lines = [
        f'  {node_id(rel)}["{pathlib.Path(rel).name}"]'
        for rel in sorted(rel_files)
    ]
    edge_lines = [
        f"  {node_id(src)} -->|{kind}| {node_id(dst)}"
        for src, dst, kind in edges_sorted
    ]

    py_graph_path = out_dir / "python_file_graph.mmd"
    py_graph_path.write_text(
        "\n".join(["graph TD", *node_lines, *edge_lines]) + "\n",
        encoding="utf-8",
    )

    manifest = {
        "language": "Python",
        "generated_at": now_iso(),
        "nodes": [{"id": rel, "label": pathlib.Path(rel).name} for rel in sorted(rel_files)],
        "edges": [
            {"from": src, "to": dst, "kind": kind} for src, dst, kind in edges_sorted
        ],
        "key_function_edges": key_edges,
        "parse_errors": parse_errors,
        "unresolved_imports": unresolved_imports,
    }

    manifest_path = out_dir / "python_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Wrote: {py_graph_path}")
    print(f"Wrote: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
