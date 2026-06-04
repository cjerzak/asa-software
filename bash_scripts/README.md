# ASA Batch Scripts

Scripts for running AI search agent tasks at extreme scale using parallel Tor circuits.

## Overview

These scripts enable massive parallelization of asa agent tasks by:
- Spawning multiple Tor instances for IP rotation
- Using GNU parallel for job distribution
- Routing each job through a dedicated Tor circuit

## Files

| File | Description |
|------|-------------|
| `run_batch.sh` | Main orchestration script |
| `run_single_task.R` | R script for individual task execution |
| `tasks_example.csv` | Example task input format |
| `logs/` | Job logs and output (created at runtime) |
| `results/` | JSON output files (created at runtime) |

## Quick Start

```bash
# 1. Create your tasks file
cp tasks_example.csv tasks.csv
# Edit tasks.csv with your prompts

# 2. Run with appropriate profile
./run_batch.sh Mini        # 8 parallel jobs
./run_batch.sh M4          # 50 parallel jobs
./run_batch.sh Studio      # 75 parallel jobs

# 3. Monitor progress
tail -f logs/batch_Mini_joblog.txt

# 4. Results appear in results/task_NNNNNN.json
```

## Machine Profiles

| Profile | Jobs | Target Hardware |
|---------|------|-----------------|
| `Studio` | 75 | High-end workstation |
| `M4` | 50 | Apple Silicon Mac |
| `Pop` | 26 | Linux desktop |
| `Mini` | 8 | Mac Mini / Laptop |
| `Custom` | env | Set `ASA_PARALLEL_JOBS` |

## Tasks CSV Format

```csv
id,prompt,output_format,template_vars
1,"What is X?",text,
2,"Find {{thing}}",json,"{""thing"":""value""}"
```

| Column | Required | Description |
|--------|----------|-------------|
| `id` | Yes | Unique task identifier |
| `prompt` | Yes | The prompt text (supports `{{var}}` templates) |
| `output_format` | No | `text` (default) or `json` |
| `template_vars` | No | JSON object for template substitution |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ASA_TASKS_FILE` | `./tasks.csv` | Input tasks file |
| `ASA_OUTPUT_DIR` | `./results` | Output directory |
| `ASA_BACKEND` | `openai` | LLM backend |
| `ASA_MODEL` | `gpt-4.1-mini` | Model name, or Azure deployment name for `azure-openai` |
| `ASA_CONDA_ENV` | `asa_env` | Conda environment |
| `ASA_PARALLEL_JOBS` | profile | Jobs for Custom profile |

## Requirements

### System Dependencies

```bash
# macOS
brew install tor parallel

# Linux (Debian/Ubuntu)
sudo apt install tor parallel
```

### R Package

```r
devtools::install_github("cjerzak/asa-software/asa")
library(asa)
build_backend()  # First time only
```

### API Keys

Set your LLM API key:
```bash
export OPENAI_API_KEY="sk-..."
# or
export GROQ_API_KEY="..."
# or for Azure OpenAI
export ASA_BACKEND="azure-openai"
export AZURE_OPENAI_ENDPOINT="https://<resource>.openai.azure.com"
export AZURE_OPENAI_API_KEY="..."
export AZURE_OPENAI_DEPLOYMENT="gpt-5-mini"
export ASA_MODEL="$AZURE_OPENAI_DEPLOYMENT"
```

For Azure OpenAI batches, ASA coordinates model calls through a shared SQLite
limiter across local workers. Set `ASA_AZURE_RPM`, `ASA_AZURE_TPM`, and
`ASA_AZURE_MAX_CONCURRENT_REQUESTS` to match the deployment quota; the default
database path is `${TMPDIR:-/tmp}/asa_azure_rate_limit.sqlite`.

## How It Works

1. **Tor Setup**: The script spawns N Tor instances on ports 9050-905N, each with isolated circuits

2. **Job Distribution**: GNU parallel distributes task IDs across workers, each assigned a unique Tor port

3. **Proxy Routing**: Each R process uses its assigned SOCKS5 proxy for all HTTP traffic

4. **Result Collection**: Individual JSON results are written to `results/`

## Scaling Guidelines

| Tasks | Profile | Est. Time | Notes |
|-------|---------|-----------|-------|
| 100 | Mini | ~30 min | Testing |
| 1,000 | M4 | ~2-4 hours | Medium batch |
| 10,000 | Studio | ~12-24 hours | Large batch |
| 100,000+ | Multiple machines | Days | Use job ranges |

For very large jobs, split across machines:
```bash
# Machine 1
./run_batch.sh Studio 1 50000

# Machine 2
./run_batch.sh Studio 50001 100000
```

## Troubleshooting

### Tor won't start
```bash
# Check if ports are in use
lsof -i tcp:9050-9100

# Kill stuck processes
for p in $(lsof -ti tcp:9050-9100); do kill $p; done
```

### Jobs failing with proxy errors
- Check Tor logs: `cat /tmp/asa_tor_instance_9050/tor.log`
- Some sites block Tor exit nodes; the agent has fallback mechanisms

### Out of memory
- Reduce `ASA_PARALLEL_JOBS`
- Add `--memfree 2G` to parallel command in run_batch.sh

## Output Format

Each task produces `results/task_NNNNNN.json`:
```json
{
  "id": 1,
  "prompt": "What is the population of Tokyo?",
  "message": "The population of Tokyo is approximately 13.96 million...",
  "status": "success",
  "elapsed_time": 0.42,
  "timestamp": "2025-01-15 14:32:01"
}
```

## Aggregating Results

```r
library(jsonlite)

# Read all results
files <- list.files("results", pattern = "\\.json$", full.names = TRUE)
results <- lapply(files, fromJSON)
df <- do.call(rbind, lapply(results, as.data.frame))

# Summary
table(df$status)
mean(df$elapsed_time, na.rm = TRUE)
```
