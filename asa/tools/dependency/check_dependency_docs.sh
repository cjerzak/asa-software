#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${1:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
OUT_DIR="${2:-${REPO_ROOT}/tracked_reports/dependency}"

"${SCRIPT_DIR}/generate_combined_dependency_docs.sh" "${REPO_ROOT}" "${OUT_DIR}"

REL_OUT="${OUT_DIR#${REPO_ROOT}/}"
STATUS="$(git -C "${REPO_ROOT}" status --porcelain -- "${REL_OUT}" || true)"
if [[ -n "${STATUS}" ]]; then
  echo "Dependency artifacts are stale. Regenerate and commit tracked_reports/dependency outputs."
  git -C "${REPO_ROOT}" --no-pager diff -- "${REL_OUT}" || true
  echo "${STATUS}"
  exit 1
fi

echo "Dependency artifacts are up to date."
