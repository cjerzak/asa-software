#!/usr/bin/env bash
set -ex
cd ~/Dropbox/APIs/Elites2/Analysis

chmod u+r+x ./LLM_GetPredictions.R

# kill and restart caffeinate (no sleep)
pkill caffeinate 2>/dev/null || true
nohup caffeinate -i -u -m >/dev/null 2>&1 &

# print some helpful contextual information 
ulimit -a 
parallel --number-of-cpus
parallel --number-of-cores
parallel --number-of-threads
parallel --number-of-sockets

# cleanup old tor instances 
for p in $(lsof -ti tcp:9050-9065); do kill $p; done

export NameTag=$1 # first positional argument to .sh
if [ "$NameTag" = "Studio" ]; then
    export nParallelJobs=75
    export StartAt=1
    export StopAt=77421
    brew services stop tor # stop tor services
elif [ "$NameTag" = "M4" ]; then
    export nParallelJobs=50
    export StartAt=1
    export StopAt=77421
    brew services stop tor # stop tor services
elif [ "$NameTag" = "Pop" ]; then
    export nParallelJobs=26
    export StartAt=1
    export StopAt=77421
    nohup systemd-inhibit --why="caffeinate" --what=sleep:idle sleep infinity >/dev/null 2>&1 &
    # pkill -f 'systemd-inhibit.*sleep infinity # kill nosleep 
    sudo systemctl stop tor  # stop tor services
elif [ "$NameTag" = "Mini" ]; then
    export nParallelJobs=8
    export StartAt=1
    export StopAt=77421    
    brew services stop tor # stop tor services 
fi

# Force kill any existing instances again
killall -q tor 2>/dev/null || true

# loop generating tor instances
N=$nParallelJobs
for ((i=0; i<N; i++)); do
  SOCKS_PORT=$((9050 + i))
  CTRL_PORT=$((9150 + i))
  DATA_DIR="/tmp/ELITES_tor_instance_${SOCKS_PORT}"

  mkdir -p $DATA_DIR

  tor \
    --RunAsDaemon 1 \
    --SocksPort "${SOCKS_PORT} IsolateClientAddr" \
    --ControlPort ${CTRL_PORT} \
    --DataDirectory ${DATA_DIR} \
    --MaxCircuitDirtiness 300 \
    --NewCircuitPeriod 30 \
    --Log "notice file ${DATA_DIR}/tor.log"
  echo "Waiting for Tor to bootstrap…" && sleep 2
done

sleep 20
# wait # -> waits indefinitely for caffeinate to end (which it doesn't)
echo "All Tor instances launched."

# add nohup and trailing & for screen and disconnect use
seq ${StartAt} ${StopAt} | nohup parallel --jobs ${nParallelJobs} --joblog ./../BashScripts/logs/PredictRun_${NameTag}_log.txt --load 90% --delay 0.25 \
      '
    IDX=$(( {%} - 1 ))
    PORT=$(( 9050 + IDX ))
    export HTTP_PROXY="socks5h://127.0.0.1:${PORT}"
    export http_proxy="$HTTP_PROXY"
    export HTTPS_PROXY="$HTTP_PROXY"
    export https_proxy="$HTTP_PROXY"
    export ALL_PROXY="$HTTP_PROXY"
    export all_proxy="$HTTP_PROXY"
    export no_proxy=localhost,127.0.0.1
    export NO_PROXY=localhost,127.0.0.1
     
    export MOZ_CRASHREPORTER_DISABLE=1
    export NO_EM_RESTART=1
    export MOZ_DISABLE_FONT_HOST_DB=1
    export MOZ_DISABLE_AUTO_SAFE_MODE=1
    export MOZ_DISABLE_CONTENT_SANDBOX=1
    export MOZ_DISABLE_GPU_SANDBOX=1
    export MOZ_DISABLE_RDD_SANDBOX=1
    export MOZ_DISABLE_SOCKET_PROCESS_SANDBOX=1
    export MOZ_DISABLE_GMP_SANDBOX=1

    Rscript --no-save ./LLM_GetPredictions.R {} ${NameTag}' \
      > ./../BashScripts/logs/PredictRun_${NameTag}_out.out \
      2> ./../BashScripts/logs/PredictRun_${NameTag}_err.err &

# note: this approach feeds sequence in via pipe (not via expansion) to avoid blowup with too many args 

# Notes:
# NO_PROXY needed to ensure Selenium/Chromedriver HTTP calls to localhost bypass the SOCKS proxy

# note: on pop os, run via bash *not* sh
