#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_SOURCE="${BASH_SOURCE[0]}"
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_SOURCE}")" && pwd -P)"
SCRIPT_PATH="${SCRIPT_DIR}/$(basename "${SCRIPT_SOURCE}")"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
R_SCRIPT="${SCRIPT_DIR}/trace_test_real_15units.R"
CONDA_EXE="$(command -v conda)"
CONDA_ENV_NAME="${ASA_CONDA_ENV:-asa_env}"
TRACE_ARTIFACT_BASE_DIR="${ASA_TRACE_ARTIFACT_BASE_DIR:-${REPO_ROOT}/tracked_reports/trace_real_15units}"
AUTO_RESUME_MAX_ATTEMPTS="${ASA_TRACE_AUTO_RESUME_MAX_ATTEMPTS:-1}"

N_JOBS=15
SOCKS_PORT_START=9050
CTRL_PORT_START=9150

SUPERVISOR_MODE=false
RESTART_MODE=false
RUN_ID=""

RUN_DIR=""
UNITS_ROOT_DIR=""
LOGS_DIR=""
SAMPLE_MANIFEST_PATH=""
RUN_CONTEXT_PATH=""
RUN_STARTED_AT_UTC=""
PARALLEL_JOBLOG_PATH=""
PARALLEL_STDOUT_PATH=""
PARALLEL_STDERR_PATH=""
SUPERVISOR_STDOUT_PATH=""
SUPERVISOR_STDERR_PATH=""
SUPERVISOR_PID_PATH=""
PARALLEL_INDICES_PATH=""
TOR_DATA_PREFIX=""
CAFFEINATE_PID=""

usage() {
  cat <<'EOF'
Usage:
  ./tracked_reports/trace_test_real_15units.sh
  bash ./tracked_reports/trace_test_real_15units.sh
  ./tracked_reports/trace_test_real_15units.sh --restart --run-id <trace_real_15units_...>
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

require_commands() {
  command -v parallel >/dev/null 2>&1 || {
    echo "GNU parallel is required." >&2
    exit 1
  }
  command -v tor >/dev/null 2>&1 || {
    echo "tor is required." >&2
    exit 1
  }
  command -v Rscript >/dev/null 2>&1 || {
    echo "Rscript is required." >&2
    exit 1
  }
  command -v "${CONDA_EXE}" >/dev/null 2>&1 || {
    echo "conda is required." >&2
    exit 1
  }
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --supervisor)
        SUPERVISOR_MODE=true
        shift
        ;;
      --restart|-r)
        RESTART_MODE=true
        shift
        ;;
      --run-id)
        [[ $# -ge 2 ]] || {
          echo "--run-id requires a value." >&2
          exit 1
        }
        RUN_ID="$2"
        shift 2
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done

  if [[ "${RESTART_MODE}" == true && -z "${RUN_ID}" ]]; then
    echo "--restart requires --run-id." >&2
    exit 1
  fi
  if [[ "${SUPERVISOR_MODE}" == true && -z "${RUN_ID}" ]]; then
    echo "--supervisor requires --run-id." >&2
    exit 1
  fi
}

fetch_run_metadata() {
  local setup_output
  local -a setup_args=(--mode setup)
  if [[ -n "${RUN_ID}" ]]; then
    setup_args+=(--run-id "${RUN_ID}")
  fi

  setup_output="$(ASA_TRACE_REPO_ROOT="${REPO_ROOT}" \
    ASA_TRACE_ARTIFACT_BASE_DIR="${TRACE_ARTIFACT_BASE_DIR}" \
    "${CONDA_EXE}" run --no-capture-output -n "${CONDA_ENV_NAME}" \
    Rscript --no-save "${R_SCRIPT}" "${setup_args[@]}")"

  while IFS='=' read -r key value; do
    [[ -z "${key}" ]] && continue
    case "${key}" in
      RUN_ID) RUN_ID="${value}" ;;
      RUN_DIR) RUN_DIR="${value}" ;;
      UNITS_ROOT_DIR) UNITS_ROOT_DIR="${value}" ;;
      LOGS_DIR) LOGS_DIR="${value}" ;;
      SAMPLE_MANIFEST_PATH) SAMPLE_MANIFEST_PATH="${value}" ;;
      RUN_CONTEXT_PATH) RUN_CONTEXT_PATH="${value}" ;;
      RUN_STARTED_AT_UTC) RUN_STARTED_AT_UTC="${value}" ;;
      PARALLEL_JOBLOG_PATH) PARALLEL_JOBLOG_PATH="${value}" ;;
      PARALLEL_STDOUT_PATH) PARALLEL_STDOUT_PATH="${value}" ;;
      PARALLEL_STDERR_PATH) PARALLEL_STDERR_PATH="${value}" ;;
      SUPERVISOR_STDOUT_PATH) SUPERVISOR_STDOUT_PATH="${value}" ;;
      SUPERVISOR_STDERR_PATH) SUPERVISOR_STDERR_PATH="${value}" ;;
      SUPERVISOR_PID_PATH) SUPERVISOR_PID_PATH="${value}" ;;
    esac
  done <<< "${setup_output}"

  [[ -n "${RUN_ID}" ]] || {
    echo "Failed to resolve RUN_ID from setup output." >&2
    exit 1
  }
  [[ -n "${RUN_DIR}" ]] || {
    echo "Failed to resolve RUN_DIR from setup output." >&2
    exit 1
  }

  mkdir -p "${LOGS_DIR}"
  PARALLEL_INDICES_PATH="${LOGS_DIR}/parallel_indices.txt"
  TOR_DATA_PREFIX="/tmp/asa_trace_real_15units_${RUN_ID}_tor"
}

pid_is_running() {
  local pid="$1"
  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" 2>/dev/null
}

assert_no_active_supervisor() {
  if [[ -f "${SUPERVISOR_PID_PATH}" ]]; then
    local existing_pid
    existing_pid="$(cat "${SUPERVISOR_PID_PATH}" 2>/dev/null || true)"
    if pid_is_running "${existing_pid}"; then
      echo "Run ${RUN_ID} already has an active supervisor process: ${existing_pid}" >&2
      exit 1
    fi
  fi
}

start_keepawake() {
  if [[ "$(uname)" == "Darwin" ]] && command -v caffeinate >/dev/null 2>&1; then
    caffeinate -i -u -m >/dev/null 2>&1 &
    CAFFEINATE_PID=$!
  fi
}

stop_keepawake() {
  if [[ -n "${CAFFEINATE_PID}" ]]; then
    kill "${CAFFEINATE_PID}" 2>/dev/null || true
    wait "${CAFFEINATE_PID}" 2>/dev/null || true
  fi
}

stop_tor_processes() {
  local pids=""

  pids="$(
    {
      command -v lsof >/dev/null 2>&1 && lsof -ti "tcp:${SOCKS_PORT_START}-$((SOCKS_PORT_START + N_JOBS - 1))" 2>/dev/null || true
      command -v lsof >/dev/null 2>&1 && lsof -ti "tcp:${CTRL_PORT_START}-$((CTRL_PORT_START + N_JOBS - 1))" 2>/dev/null || true
      command -v pgrep >/dev/null 2>&1 && pgrep -f "${TOR_DATA_PREFIX}" 2>/dev/null || true
    } | sort -u
  )"

  if [[ -z "${pids}" ]]; then
    return 0
  fi

  while IFS= read -r pid; do
    [[ -z "${pid}" ]] && continue
    kill "${pid}" 2>/dev/null || true
  done <<< "${pids}"

  sleep 1

  while IFS= read -r pid; do
    [[ -z "${pid}" ]] && continue
    if kill -0 "${pid}" 2>/dev/null; then
      kill -9 "${pid}" 2>/dev/null || true
    fi
  done <<< "${pids}"
}

cleanup_on_exit() {
  stop_tor_processes
  stop_keepawake
}

launch_tor_instances() {
  log "Cleaning up Tor instances on trace ports"
  if [[ "$(uname)" == "Darwin" ]]; then
    brew services stop tor >/dev/null 2>&1 || true
  fi
  stop_tor_processes

  log "Launching ${N_JOBS} Tor instances"
  for ((i=0; i<N_JOBS; i++)); do
    local socks_port=$((SOCKS_PORT_START + i))
    local ctrl_port=$((CTRL_PORT_START + i))
    local data_dir="${TOR_DATA_PREFIX}_${socks_port}"

    mkdir -p "${data_dir}"

    tor \
      --RunAsDaemon 1 \
      --SocksPort "${socks_port} IsolateClientAddr" \
      --ControlPort "${ctrl_port}" \
      --DataDirectory "${data_dir}" \
      --MaxCircuitDirtiness 300 \
      --NewCircuitPeriod 30 \
      --Log "notice file ${data_dir}/tor.log"

    sleep 1
  done

  log "Waiting for Tor bootstrap (20s)"
  sleep 20
}

prepare_parallel_inputs() {
  seq 1 "${N_JOBS}" > "${PARALLEL_INDICES_PATH}"
}

run_parallel_command() {
  export TRACE_RUN_ID="${RUN_ID}"
  export TRACE_TOR_SOCKS_PORT_START="${SOCKS_PORT_START}"
  export TRACE_TOR_CONTROL_PORT_START="${CTRL_PORT_START}"
  export TRACE_CONDA_EXE="${CONDA_EXE}"
  export TRACE_CONDA_ENV_NAME="${CONDA_ENV_NAME}"
  export TRACE_R_SCRIPT="${R_SCRIPT}"
  export ASA_TRACE_REPO_ROOT="${REPO_ROOT}"
  export ASA_TRACE_ARTIFACT_BASE_DIR="${TRACE_ARTIFACT_BASE_DIR}"

  parallel --jobs "${N_JOBS}" \
    --joblog "${PARALLEL_JOBLOG_PATH}" \
    "$@" \
    --load 90% \
    --delay 0.5 \
    -a "${PARALLEL_INDICES_PATH}" \
    '
    IDX=$(( {%} - 1 ))
    PORT=$(( TRACE_TOR_SOCKS_PORT_START + IDX ))
    CTRL_PORT=$(( TRACE_TOR_CONTROL_PORT_START + IDX ))

    export ASA_PROXY="socks5h://127.0.0.1:${PORT}"
    unset HTTP_PROXY http_proxy HTTPS_PROXY https_proxy ALL_PROXY all_proxy
    export TOR_CONTROL_PORT="${CTRL_PORT}"
    export no_proxy=localhost,127.0.0.1
    export NO_PROXY=localhost,127.0.0.1

    "${TRACE_CONDA_EXE}" run --no-capture-output -n "${TRACE_CONDA_ENV_NAME}" \
      Rscript --no-save "${TRACE_R_SCRIPT}" --mode worker --run-id "${TRACE_RUN_ID}" --unit-index "{}"
    ' \
    > "${PARALLEL_STDOUT_PATH}" \
    2> "${PARALLEL_STDERR_PATH}"
}

run_parallel_workers() {
  if [[ "${RESTART_MODE}" == true ]]; then
    if [[ ! -f "${PARALLEL_JOBLOG_PATH}" ]]; then
      echo "Resume requested but joblog is missing: ${PARALLEL_JOBLOG_PATH}" >&2
      exit 1
    fi
    run_parallel_command --resume-failed
    return 0
  fi

  run_parallel_command
}

resume_failed_workers() {
  local attempt=0
  local resume_status=0

  if [[ "${RESTART_MODE}" == true ]]; then
    return 0
  fi
  if [[ "${AUTO_RESUME_MAX_ATTEMPTS}" -le 0 ]]; then
    return 1
  fi

  if [[ ! -f "${PARALLEL_JOBLOG_PATH}" ]]; then
    return 1
  fi

  while [[ "${attempt}" -lt "${AUTO_RESUME_MAX_ATTEMPTS}" ]]; do
    attempt=$((attempt + 1))
    log "Resuming failed workers (attempt ${attempt}/${AUTO_RESUME_MAX_ATTEMPTS})"
    resume_status=0
    run_parallel_command --resume-failed || resume_status=$?
    if [[ "${resume_status}" -eq 0 ]]; then
      return 0
    fi
    log "Resume attempt ${attempt} exited with status ${resume_status}"
  done

  return "${resume_status}"
}

run_aggregate() {
  ASA_TRACE_REPO_ROOT="${REPO_ROOT}" \
    ASA_TRACE_ARTIFACT_BASE_DIR="${TRACE_ARTIFACT_BASE_DIR}" \
    "${CONDA_EXE}" run --no-capture-output -n "${CONDA_ENV_NAME}" \
    Rscript --no-save "${R_SCRIPT}" --mode aggregate --run-id "${RUN_ID}"
}

run_supervisor() {
  fetch_run_metadata
  trap cleanup_on_exit EXIT INT TERM

  start_keepawake
  launch_tor_instances
  prepare_parallel_inputs

  log "Starting parallel workers for ${RUN_ID}"
  local parallel_status=0
  run_parallel_workers || parallel_status=$?
  if [[ "${parallel_status}" -ne 0 ]]; then
    log "Parallel workers exited with status ${parallel_status}"
    local resume_status=0
    if resume_failed_workers; then
      parallel_status=0
    else
      resume_status=$?
      if [[ "${AUTO_RESUME_MAX_ATTEMPTS}" -gt 0 ]]; then
        parallel_status="${resume_status}"
      fi
    fi
  fi

  log "Aggregating run outputs for ${RUN_ID}"
  local aggregate_status=0
  run_aggregate || aggregate_status=$?
  if [[ "${aggregate_status}" -ne 0 ]]; then
    log "Aggregate step exited with status ${aggregate_status}"
  fi

  if [[ "${parallel_status}" -ne 0 ]]; then
    exit "${parallel_status}"
  fi
  if [[ "${aggregate_status}" -ne 0 ]]; then
    exit "${aggregate_status}"
  fi
}

launch_background_supervisor() {
  local -a supervisor_cmd

  fetch_run_metadata
  assert_no_active_supervisor

  supervisor_cmd=(bash "${SCRIPT_PATH}" --supervisor --run-id "${RUN_ID}")
  if [[ "${RESTART_MODE}" == true ]]; then
    supervisor_cmd+=(--restart)
  fi

  nohup "${supervisor_cmd[@]}" \
    > "${SUPERVISOR_STDOUT_PATH}" \
    2> "${SUPERVISOR_STDERR_PATH}" \
    < /dev/null &

  local supervisor_pid=$!
  printf '%s\n' "${supervisor_pid}" > "${SUPERVISOR_PID_PATH}"

  cat <<EOF
RUN_ID=${RUN_ID}
RUN_DIR=${RUN_DIR}
RUN_STARTED_AT_UTC=${RUN_STARTED_AT_UTC}
SAMPLE_MANIFEST_PATH=${SAMPLE_MANIFEST_PATH}
PARALLEL_JOBLOG_PATH=${PARALLEL_JOBLOG_PATH}
PARALLEL_STDOUT_PATH=${PARALLEL_STDOUT_PATH}
PARALLEL_STDERR_PATH=${PARALLEL_STDERR_PATH}
SUPERVISOR_STDOUT_PATH=${SUPERVISOR_STDOUT_PATH}
SUPERVISOR_STDERR_PATH=${SUPERVISOR_STDERR_PATH}
SUPERVISOR_PID_PATH=${SUPERVISOR_PID_PATH}
SUPERVISOR_PID=${supervisor_pid}
EOF
}

main() {
  parse_args "$@"
  require_commands

  if [[ "${SUPERVISOR_MODE}" == true ]]; then
    run_supervisor
    exit 0
  fi

  if [[ "${RESTART_MODE}" == false && -n "${RUN_ID}" ]]; then
    echo "--run-id is only supported together with --restart." >&2
    exit 1
  fi

  launch_background_supervisor

  cat <<EOF

Monitor progress with:
  tail -f "${SUPERVISOR_STDOUT_PATH}"
  tail -f "${SUPERVISOR_STDERR_PATH}"
  tail -f "${PARALLEL_STDOUT_PATH}"
  tail -f "${PARALLEL_STDERR_PATH}"
  tail -n 50 "${PARALLEL_JOBLOG_PATH}"

Stop the run with:
  kill "$(cat "${SUPERVISOR_PID_PATH}")"
EOF
}

main "$@"
