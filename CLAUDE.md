# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**asa** is an R package for performing AI search agent tasks at large scales. It uses a ReAct (Reasoning + Acting) agent pattern with web search capabilities, implemented via LangGraph in Python and orchestrated from R using reticulate.

The package provides a general-purpose framework for running LLM-powered research tasks that require web search and information extraction.

## Repository Structure

```
asa-software/
├── asa/                    # R Package (main)
│   ├── DESCRIPTION         # Package metadata
│   ├── NAMESPACE           # Exported functions
│   ├── R/                  # R source files
│   │   ├── asa-package.R   # Package docs & environment
│   │   ├── build_backend.R # Conda env setup
│   │   ├── initialize_agent.R  # Agent initialization
│   │   ├── run_agent.R     # Low-level agent invocation
│   │   ├── run_task.R      # High-level task execution
│   │   ├── process_outputs.R   # Output extraction
│   │   ├── helpers.R       # Utility functions
│   │   └── s3_methods.R    # S3 class definitions
│   ├── inst/
│   │   ├── python/         # Python modules (custom_ddg_production.py)
│   │   └── CITATION
│   ├── tests/testthat/     # Unit tests
│   ├── Internal/           # Legacy scripts (pre-package)
│   └── PredictRun.sh       # Batch processing script
├── reports/                # Documentation & plans
└── README.md
```

## Installation

```r
# Install from GitHub
devtools::install_github("cjerzak/asa-software/asa")

# Or install locally
devtools::install("./asa")
```

## Quick Start (Package API)

```r
library(asa)

# First time: build the Python backend
build_backend(conda_env = "asa_env")

# Initialize the agent
agent <- initialize_agent(
  backend = "openai",
  model = "gpt-4.1-mini",
  conda_env = "asa_env"
)

# Run a simple task
result <- run_task(
  prompt = "What is the population of Tokyo?",
  output_format = "text",
  agent = agent
)
print(result)

# Run a structured task with JSON output
result <- run_task(
  prompt = "Find information about Marie Curie and return JSON with fields: birth_year, nationality, field_of_study",
  output_format = "json",
  agent = agent
)
print(result$parsed)

# Build prompts from templates
prompt <- build_prompt(
  template = "Find information about {{name}} in {{year}}",
  name = "Albert Einstein",
  year = 1905
)
```

## Key Package Functions

| Function | Description |
|----------|-------------|
| `build_backend()` | Create conda environment with Python dependencies |
| `initialize_agent()` | Initialize the LangGraph agent with tools |
| `run_task()` | Run a structured research task |
| `run_task_batch()` | Run multiple tasks in batch |
| `run_agent()` | Low-level agent invocation with custom prompt |
| `build_prompt()` | Build prompts from templates |
| `extract_agent_results()` | Parse raw agent traces |

## Configuration

Environment variables:
- `OPENAI_API_KEY`: OpenAI API key
- `GROQ_API_KEY`: Groq API key
- `XAI_API_KEY`: xAI API key

Agent options (passed to `initialize_agent()`):
- `backend`: "openai", "groq", "xai", or "exo"
- `model`: Model identifier (e.g., "gpt-4.1-mini")
- `proxy`: SOCKS5 proxy URL (default: "socks5h://127.0.0.1:9050")
- `use_memory_folding`: Enable DeepAgent-style memory compression (default: TRUE)
- `memory_threshold`: Messages before folding triggers (default: 4)

## Tor Proxy Setup

For anonymous searching:
```bash
# macOS
brew install tor
brew services start tor

# Linux
sudo apt install tor
sudo systemctl start tor
```

## Legacy Scripts

The `asa/Internal/` and `asa/*.R` files contain the original non-package implementation. These are kept for backward compatibility and batch processing workflows.

- `asa/PredictRun.sh`: Parallel batch processing with multiple Tor instances

## Search Architecture

The `custom_ddg_production.py` implements 4-tier search fallback:

1. **PRIMP**: HTTP/2 with browser TLS fingerprint, CAPTCHA detection
2. **Selenium**: Headless browser for JS-rendered content
3. **DDGS**: Standard ddgs Python library
4. **Requests**: Raw POST to DuckDuckGo HTML endpoint

## Memory Folding

DeepAgent-style memory folding for long conversations:
- Compresses old messages into summary when threshold exceeded
- Injects summary into system prompt as "long-term memory"
- Only folds at safe boundaries (complete conversation rounds)

## Development

```r
# Load for development
devtools::load_all("./asa")

# Run tests
devtools::test("./asa")

# Check package
devtools::check("./asa")

# Regenerate documentation
devtools::document("./asa")
```

## Known Issues

See `reports/BUGS_AgentFunctions.md`:
- BUG-001: xAI backend needs proper initialization
- BUG-003: Selenium driver type annotation mismatch
- BUG-007: async_client resource leak (minor)
