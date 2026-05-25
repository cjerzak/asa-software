# Ollama Local Agent Runbook

This document records the Mac Studio local-agent bakeoff and the supported
environment-driven setup for ASA/OpenCode jobs. Keep task scripts such as
`InstRunAgent.R` configurable through environment variables; do not bake these
local paths, ports, or model names into the task script.

## Bakeoff Summary

Recommended default:

- `lfm2:24b-a2b`: best new challenger. It was the fastest candidate and
  produced the strongest new non-Unknown extraction signal.
- Keep `mistral-small3.2:24b` installed as the quality fallback.

Useful comparison models:

- `mistral-small3.2:24b`: best incumbent evidence baseline.
- `devstral-small-2`: reliable fast control.

Observed failure modes:

- Laguna: timeout or invalid tool calls.
- GLM: prose responses instead of JSON.
- Granite: zero tool calls.
- Qwen: empty-output and full-timeout failures.

## Mac Studio Constraints

Use the external SSD for Ollama model storage:

```bash
/Volumes/Jerzak-xSSD-01/asa-ollama-models
```

Local jobs should run against an SSD-backed Ollama process on port `11436`.
This avoids competing with any default Ollama service on port `11434` and keeps
large model files off the internal disk.

## Prepare The Host

```bash
ssh connors-mac-studio

export OLLAMA_HOST=127.0.0.1:11436
export OLLAMA_MODELS=/Volumes/Jerzak-xSSD-01/asa-ollama-models
export OLLAMA_API_BASE=http://127.0.0.1:11436/v1
export NO_PROXY=127.0.0.1,localhost
export no_proxy=127.0.0.1,localhost

ollama pull lfm2:24b-a2b
ollama pull mistral-small3.2:24b   # optional fallback
```

Start SSD-backed Ollama if needed:

```bash
nohup env \
  OLLAMA_HOST=127.0.0.1:11436 \
  OLLAMA_MODELS=/Volumes/Jerzak-xSSD-01/asa-ollama-models \
  OLLAMA_KEEP_ALIVE=-1 \
  OLLAMA_NO_CLOUD=1 \
  /usr/local/bin/ollama serve \
  > "$HOME/Dropbox/GLP/Electoral systems/Tmp/ollama-11436.log" 2>&1 &
```

## Run Local ASA/OpenCode Jobs

```bash
cd "$HOME/Dropbox/GLP/Electoral systems"

export ASA_LLM_BACKEND=ollama
export ASA_MODEL_NAME=lfm2:24b-a2b
export ASA_AGENT_BACKEND=opencode
export ASA_RUN_NAME=MESEliteSearch_local_lfm2
export ASA_OUTPUT_SCRATCH_ROOT=/Volumes/Jerzak-xSSD-01/asa-local-scratch

Rscript "Analysis/AgentPipeline/InstRunAgent.R" 1 LOCAL_LFM2_TEST
```

Recommended fallback run:

```bash
export ASA_MODEL_NAME=mistral-small3.2:24b
export ASA_RUN_NAME=MESEliteSearch_local_mistral_small32

Rscript "Analysis/AgentPipeline/InstRunAgent.R" 1 LOCAL_MISTRAL_TEST
```

## Health Checks

```bash
curl http://127.0.0.1:11436/api/version
OLLAMA_HOST=127.0.0.1:11436 ollama ps
curl http://127.0.0.1:11436/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ollama-local" \
  -d '{
    "model": "lfm2:24b-a2b",
    "messages": [{"role": "user", "content": "Say ready."}],
    "stream": false
  }'
```
