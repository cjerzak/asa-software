# asa

**AI Search Agent for Large-Scale Research Automation**

An R package for running LLM-powered research tasks at scale. Uses a ReAct (Reasoning + Acting) agent pattern with web search capabilities, implemented via LangGraph in Python and orchestrated from R.

## Installation

```r
# Install from GitHub
devtools::install_github("cjerzak/asa-software/asa")

# Or install locally
devtools::install("path/to/asa")
```

## Quick Start

```r
library(asa)

# First time only: build the Python backend
build_backend()

# Initialize the agent
agent <- initialize_agent(

  backend = "openai",
  model = "gpt-4.1-mini"
)

# Run a simple task
result <- run_task(
  prompt = "What is the population of Tokyo?",
  agent = agent
)
print(result)
```

## Key Functions

| Function | Description |
|----------|-------------|
| `build_backend()` | Create conda environment with Python dependencies |
| `initialize_agent()` | Initialize the search agent with LLM backend |
| `run_task()` | Execute a research task and get structured results |
| `run_task_batch()` | Run multiple tasks in batch |
| `build_prompt()` | Build prompts from templates with variable substitution |

## Structured Output

Request JSON-formatted responses for programmatic use:

```r
result <- run_task(
  prompt = "Find Marie Curie's birth year, nationality, and field of study. Return as JSON.",
  output_format = "json",
  agent = agent
)

# Access parsed fields
result$parsed$birth_year
result$parsed$nationality
```

## Template-Based Prompts

Build prompts dynamically using `{{variable}}` syntax:

```r
prompt <- build_prompt(

  template = "Find information about {{person}} and their work in {{year}}.",
  person = "Albert Einstein",

  year = 1905
)

result <- run_task(prompt, agent = agent)
```

## Batch Processing

Process multiple queries efficiently:

```r
prompts <- c(

  "What is the capital of France?",
  "What is the capital of Japan?",
  "What is the capital of Brazil?"
)

results <- run_task_batch(prompts, agent = agent)
```

## Configuration

### LLM Backends

| Backend | Model Examples | API Key Variable |
|---------|----------------|------------------|
| `openai` | `gpt-4.1-mini`, `gpt-4o` | `OPENAI_API_KEY` |
| `groq` | `llama-3.3-70b-versatile` | `GROQ_API_KEY` |
| `xai` | `grok-beta` | `XAI_API_KEY` |
| `exo` | Local models | (none) |

### Agent Options

```r
agent <- initialize_agent(

  backend = "openai",
  model = "gpt-4.1-mini",
)
```

### Memory Folding

Long conversations automatically compress older messages into summaries, following the DeepAgent paper. Configure with:

```r
agent <- initialize_agent(
  use_memory_folding = TRUE,
  memory_threshold = 4,      # Messages before folding
  memory_keep_recent = 2     # Recent messages to preserve
)
```

## Search Architecture

The agent uses two search tools:

- **Wikipedia**: Encyclopedic information lookup
- **DuckDuckGo**: Web search with 4-tier fallback (PRIMP -> Selenium -> DDGS -> raw requests)

## S3 Classes

The package uses S3 classes for clean output handling:

- `asa_agent`: Initialized agent object
- `asa_response`: Raw agent response with trace
- `asa_result`: Structured task result with parsed output

```r
# All classes have print and summary methods
print(agent)
summary(result)

# Convert results to data frame
df <- as.data.frame(result)
```

## Requirements

- R >= 4.0
- Python >= 3.10 (managed via conda)
- reticulate, jsonlite, rlang

## License

MIT
