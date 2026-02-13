#!/usr/bin/env bash
# ==============================================================================
# run_batch.sh - Parallel ASA Agent Execution via Tor
# ==============================================================================
# Scales AI search agent tasks to extreme parallelism using multiple Tor circuits
# for IP rotation and anonymity.
#
# Usage:
#   ./run_batch.sh <machine_profile> [start_idx] [stop_idx]
#
# Machine Profiles:
#   Studio  - 75 parallel jobs (high-end workstation)
#   M4      - 50 parallel jobs (Apple Silicon)
#   Pop     - 26 parallel jobs (Linux desktop)
#   Mini    - 8 parallel jobs (Mac Mini / laptop)
#   Custom  - Uses ASA_PARALLEL_JOBS env var
#
# Examples:
#   ./run_batch.sh Studio              # Run all tasks with Studio profile
#   ./run_batch.sh M4 1 1000           # Run tasks 1-1000 with M4 profile
#   ASA_PARALLEL_JOBS=16 ./run_batch.sh Custom 1 500
#
# Requirements:
#   - tor (brew install tor / apt install tor)
#   - GNU parallel (brew install parallel / apt install parallel)
#   - R with asa package installed
#   - Input file: ./tasks.csv (or set ASA_TASKS_FILE)
#
# ==============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${SCRIPT_DIR}/logs"
TOR_DATA_PREFIX="/tmp/asa_tor_instance"

# Default task file (CSV with columns: id, prompt, [optional fields])
ASA_TASKS_FILE="${ASA_TASKS_FILE:-${SCRIPT_DIR}/tasks.csv}"

# Parse arguments
PROFILE="${1:-Mini}"
START_IDX="${2:-1}"
STOP_IDX="${3:-}"

# Determine parallel jobs based on profile
case "$PROFILE" in
    Studio)
        N_JOBS=75
        OS_TYPE="macos"
        ;;
    M4)
        N_JOBS=50
        OS_TYPE="macos"
        ;;
    Pop)
        N_JOBS=26
        OS_TYPE="linux"
        ;;
    Mini)
        N_JOBS=8
        OS_TYPE="macos"
        ;;
    Custom)
        N_JOBS="${ASA_PARALLEL_JOBS:-4}"
        OS_TYPE="${ASA_OS_TYPE:-macos}"
        ;;
    *)
        echo "Unknown profile: $PROFILE"
        echo "Available profiles: Studio, M4, Pop, Mini, Custom"
        exit 1
        ;;
esac

# If STOP_IDX not provided, count lines in tasks file
if [ -z "$STOP_IDX" ]; then
    if [ -f "$ASA_TASKS_FILE" ]; then
        STOP_IDX=$(wc -l < "$ASA_TASKS_FILE" | tr -d ' ')
        # Subtract 1 for header
        STOP_IDX=$((STOP_IDX - 1))
    else
        echo "Error: Tasks file not found: $ASA_TASKS_FILE"
        echo "Create a CSV with columns: id,prompt"
        exit 1
    fi
fi

echo "=============================================="
echo "ASA Batch Runner"
echo "=============================================="
echo "Profile:      $PROFILE"
echo "Parallel:     $N_JOBS jobs"
echo "Task range:   $START_IDX - $STOP_IDX"
echo "Tasks file:   $ASA_TASKS_FILE"
echo "Log dir:      $LOG_DIR"
echo "=============================================="

# Create log directory
mkdir -p "$LOG_DIR"

# System info
echo ""
echo "System Information:"
ulimit -a 2>/dev/null | head -5 || true
parallel --number-of-cpus 2>/dev/null || echo "CPUs: unknown"
echo ""

# ----------------------------------------
# Prevent system sleep
# ----------------------------------------
echo "Preventing system sleep..."
pkill caffeinate 2>/dev/null || true

if [ "$OS_TYPE" = "macos" ]; then
    nohup caffeinate -i -u -m >/dev/null 2>&1 &
elif [ "$OS_TYPE" = "linux" ]; then
    nohup systemd-inhibit --why="asa_batch" --what=sleep:idle sleep infinity >/dev/null 2>&1 &
fi

# ----------------------------------------
# Stop existing Tor services and instances
# ----------------------------------------
echo "Cleaning up existing Tor instances..."

# Kill processes on Tor ports
for p in $(lsof -ti tcp:9050-9199 2>/dev/null); do
    kill "$p" 2>/dev/null || true
done

# Stop system Tor service
if [ "$OS_TYPE" = "macos" ]; then
    brew services stop tor 2>/dev/null || true
elif [ "$OS_TYPE" = "linux" ]; then
    sudo systemctl stop tor 2>/dev/null || true
fi

killall -q tor 2>/dev/null || true
sleep 2

# ----------------------------------------
# Launch Tor instances (one per parallel job)
# ----------------------------------------
echo "Launching $N_JOBS Tor instances..."

for ((i=0; i<N_JOBS; i++)); do
    SOCKS_PORT=$((9050 + i))
    CTRL_PORT=$((9150 + i))
    DATA_DIR="${TOR_DATA_PREFIX}_${SOCKS_PORT}"

    mkdir -p "$DATA_DIR"

    tor \
        --RunAsDaemon 1 \
        --SocksPort "${SOCKS_PORT} IsolateClientAddr" \
        --ControlPort "${CTRL_PORT}" \
        --DataDirectory "${DATA_DIR}" \
        --MaxCircuitDirtiness 300 \
        --NewCircuitPeriod 30 \
        --Log "notice file ${DATA_DIR}/tor.log" \
        2>/dev/null

    if [ $((i % 10)) -eq 0 ]; then
        echo "  Launched Tor on port $SOCKS_PORT..."
    fi
done

echo "Waiting for Tor circuits to establish..."
sleep 20
echo "All $N_JOBS Tor instances ready."

# ----------------------------------------
# Run parallel tasks
# ----------------------------------------
echo ""
echo "Starting parallel execution..."
echo "Logs: ${LOG_DIR}/batch_${PROFILE}_*.log"
echo ""

# Export variables for parallel jobs
export ASA_PACKAGE_DIR="$PACKAGE_DIR"
export ASA_TASKS_FILE
export ASA_LOG_DIR="$LOG_DIR"

seq "$START_IDX" "$STOP_IDX" | \
    nohup parallel \
        --jobs "$N_JOBS" \
        --joblog "${LOG_DIR}/batch_${PROFILE}_joblog.txt" \
        --load 90% \
        --delay 0.25 \
        '
        IDX=$(( {%} - 1 ))
        PORT=$(( 9050 + IDX ))
        CTRL_PORT=$(( 9150 + IDX ))

        # Set proxy environment for this job
        export HTTP_PROXY="socks5h://127.0.0.1:${PORT}"
        export http_proxy="$HTTP_PROXY"
        export HTTPS_PROXY="$HTTP_PROXY"
        export https_proxy="$HTTP_PROXY"
        export ALL_PROXY="$HTTP_PROXY"
        export all_proxy="$HTTP_PROXY"
        export no_proxy=localhost,127.0.0.1
        export NO_PROXY=localhost,127.0.0.1
        # Keep Tor circuit rotation bound to this worker'"'"'s Tor instance.
        export TOR_CONTROL_PORT="${CTRL_PORT}"

        # Disable browser crash reporters
        export MOZ_CRASHREPORTER_DISABLE=1
        export NO_EM_RESTART=1
        export MOZ_DISABLE_FONT_HOST_DB=1
        export MOZ_DISABLE_AUTO_SAFE_MODE=1
        export MOZ_DISABLE_CONTENT_SANDBOX=1
        export MOZ_DISABLE_GPU_SANDBOX=1
        export MOZ_DISABLE_RDD_SANDBOX=1
        export MOZ_DISABLE_SOCKET_PROCESS_SANDBOX=1
        export MOZ_DISABLE_GMP_SANDBOX=1

        Rscript --no-save "${ASA_PACKAGE_DIR}/bash_scripts/run_single_task.R" {}
        ' \
    > "${LOG_DIR}/batch_${PROFILE}_stdout.log" \
    2> "${LOG_DIR}/batch_${PROFILE}_stderr.log" &

PARALLEL_PID=$!
echo "Parallel execution started (PID: $PARALLEL_PID)"
echo ""
echo "Monitor progress:"
echo "  tail -f ${LOG_DIR}/batch_${PROFILE}_joblog.txt"
echo ""
echo "To stop:"
echo "  kill $PARALLEL_PID"
echo ""
