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
# First time only: build the Python backend
asa::build_backend()

# Initialize the agent
agent <- asa::initialize_agent(

  backend = "openai",
  model = "gpt-4.1-mini"
)

# Run a simple task
result <- asa::run_task(
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
| `configure_search()` | Configure search timing and retry behavior |
| `extract_search_tiers()` | Get which search tier was used from traces |

## Structured Output

Request JSON-formatted responses for programmatic use:

```r
result <- asa::run_task(
  prompt = "Find Marie Curie's birth year, nationality, and field of study. Return as JSON.",
  output_format = "json",
  agent = agent
)

# Access parsed fields
result$parsed$birth_year
result$parsed$nationality
```

**Result object fields:**

| Field | Description |
|-------|-------------|
| `$message` | The agent's response text |
| `$parsed` | Parsed JSON as a list (when `output_format = "json"`) |
| `$status` | `"success"` or `"error"` |
| `$elapsed_time` | Execution time in minutes |
| `$raw_output` | Full agent trace for debugging |

## Template-Based Prompts

Build prompts dynamically using `{{variable}}` syntax and named arguments:

```r
prompt <- asa::build_prompt(
  template = "Find information about {{person}} and their work in {{year}}.",
  person = "Albert Einstein",
  year = 1905
)
```

**Key behaviors:**
- All substitution arguments must be named
- Numeric values are automatically converted to strings
- Missing variables remain as `{{variable}}` in output (useful for debugging)

**Batch processing with templates** - the primary use case:

```r
# Create prompts for multiple entities
people <- c("Marie Curie", "Nikola Tesla", "Ada Lovelace")
prompts <- sapply(people, function(p) {
  build_prompt("Research {{person}}'s major contributions.", person = p)
})

results <- asa::run_task_batch(prompts, agent = agent)
```

## Batch Processing

Process multiple queries efficiently:

```r
prompts <- c(
  "What is the capital of France?",
  "What is the capital of Japan?",
  "What is the capital of Brazil?"
)

results <- asa::run_task_batch(prompts, agent = agent)

# Results is a list of asa_result objects
results[[1]]$message   # First response text
results[[2]]$status    # "success" or "error"
```

**Data frame input/output** - pass a data frame with a `prompt` column to get results appended:

```r
df <- data.frame(
  country = c("France", "Japan"),
  prompt = c("Capital of France?", "Capital of Japan?")
)
df <- asa::run_task_batch(df, agent = agent)
# Returns df with added columns: response, status, elapsed_time
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
agent <- asa::initialize_agent(
  backend = "openai",
  model = "gpt-4.1-mini",
  proxy = "socks5h://127.0.0.1:9050",  # Tor proxy (NULL to disable)
  timeout = 120,                        # Request timeout in seconds
  rate_limit = 0.2                      # Requests per second
)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `proxy` | `"socks5h://127.0.0.1:9050"` | SOCKS5 proxy URL, or `NULL` to disable |
| `timeout` | `120` | Request timeout in seconds |
| `rate_limit` | `0.2` | Max requests per second to avoid rate limits |
| `verbose` | `TRUE` | Print initialization status messages |

### Memory Folding

Long conversations automatically compress older messages into summaries, following the DeepAgent paper. Configure with:

```r
agent <- asa::initialize_agent(
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

## Advanced Use

### Memory Folding Configuration

Memory folding compresses older conversation messages into summaries, enabling longer research sessions without exceeding context limits. This follows the DeepAgent paper methodology.

```r
# Custom memory settings for extended research sessions
agent <- asa::initialize_agent(
  backend = "openai",
  model = "gpt-4.1-mini",
  use_memory_folding = TRUE,   # Enable memory compression (default)
  memory_threshold = 6,         # Fold after 6 messages (default: 4)
  memory_keep_recent = 3        # Keep 3 recent messages after fold (default: 2)
)

# Monitor folding activity in responses
response <- asa::run_agent("Complex multi-step research query...", agent = agent)
print(response$fold_count)  # Number of times memory was folded

# Disable memory folding for short, single-turn tasks
agent_simple <- asa::initialize_agent(
  backend = "openai",
  model = "gpt-4.1-mini",
  use_memory_folding = FALSE   # Uses standard agent with lower recursion limit
)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `use_memory_folding` | `TRUE` | Enable DeepAgent-style memory compression |
| `memory_threshold` | `4` | Number of messages that trigger folding |
| `memory_keep_recent` | `2` | Recent messages to preserve after each fold |

**When to disable:** For simple single-turn queries where conversation history isn't needed, disabling memory folding reduces overhead.

### Parallel Batch Processing

Process large batches efficiently using parallel workers:

```r
# Parallel processing with custom worker count
results <- asa::run_task_batch(
  prompts = prompts,
  output_format = "json",
  agent = agent,
  parallel = TRUE,      # Enable parallel execution
  workers = 8,          # Number of parallel workers (default: 4)
  progress = TRUE       # Show progress messages
)
```

**Data frame workflow with automatic JSON field extraction:**

```r
# Prepare structured batch input
df <- data.frame(
  entity = c("Marie Curie", "Nikola Tesla", "Ada Lovelace"),
  prompt = c(
    "Find birth_year and primary_field for Marie Curie. Return as JSON.",
    "Find birth_year and primary_field for Nikola Tesla. Return as JSON.",
    "Find birth_year and primary_field for Ada Lovelace. Return as JSON."
  )
)

# Run batch with JSON parsing
results_df <- asa::run_task_batch(
  df,
  output_format = "json",
  agent = agent,
  parallel = TRUE,
  workers = 4
)

# Results include original columns plus:
# - response: Full agent response text
# - status: "success" or "error"
# - elapsed_time: Execution time in minutes
# - birth_year, primary_field: Parsed JSON fields as columns
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `parallel` | `FALSE` | Enable parallel execution (requires `future.apply`) |
| `workers` | `4` | Number of parallel workers |
| `progress` | `TRUE` | Show `[1/N] Processing...` messages |

### Search Configuration

Fine-tune search behavior globally:

```r
# Configure search parameters for reliability
asa::configure_search(
  max_results = 15,          # Results per search (default: 10)
  timeout = 20,              # Request timeout in seconds (default: 15)
  max_retries = 5,           # Retry attempts (default: 3)
  retry_delay = 3,           # Seconds between retries (default: 2)
  backoff_multiplier = 2.0,  # Exponential backoff factor (default: 1.5)
  inter_search_delay = 1.0   # Delay between searches in seconds (default: 0.5)
)

# Enable debug logging for troubleshooting
asa::configure_search_logging("DEBUG")  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `inter_search_delay` | `0.5` | Seconds to wait between consecutive searches (avoids rate limiting) |
| `max_results` | `10` | Maximum results per search |
| `timeout` | `15` | HTTP request timeout in seconds |
| `max_retries` | `3` | Retry attempts on failure |
| `retry_delay` | `2` | Initial delay between retries |
| `backoff_multiplier` | `1.5` | Exponential backoff factor |

### Tor/Proxy Management

Manage anonymous searching through Tor:

```r
# Check if Tor is running
if (is_tor_running()) {
  # Get current exit IP
  current_ip <- get_tor_ip()
  message("Current Tor IP: ", current_ip)

  # Rotate to new circuit between sensitive batches
  asa::rotate_tor_circuit(
    method = "brew",   # "brew" (macOS), "systemctl" (Linux), or "signal"
    wait = 15          # Seconds to wait for new circuit (default: 12)
  )

  new_ip <- get_tor_ip()
  message("New Tor IP: ", new_ip)
}

# Initialize agent without proxy
agent_direct <- asa::initialize_agent(
  backend = "openai",
  model = "gpt-4.1-mini",
  proxy = NULL  # Disable Tor proxy
)
```

### Output Extraction

Extract detailed information from agent traces for analysis:

```r
# Run a task and extract structured data from the trace
result <- asa::run_task(
  prompt = "Research quantum computing applications in drug discovery.",
  agent = agent
)

# Extract search artifacts from raw trace
extracted <- asa::extract_agent_results(result$raw_output)

# Access extracted data
extracted$search_snippets     # List of search result text by source number
extracted$search_urls         # List of URLs from search results
extracted$wikipedia_snippets  # Wikipedia content retrieved
extracted$json_data           # Any JSON data found in response
extracted$search_tiers        # Which search tier was used ("primp", "selenium", "ddgs", "requests")

# Get snippets from specific search call
snippets_from_search_1 <- asa::extract_search_snippets(result$raw_output, source = 1)
urls_from_search_1 <- asa::extract_urls(result$raw_output, source = 1)

# Extract tier info directly
tiers <- asa::extract_search_tiers(result$raw_output)
print(tiers)  # e.g., "primp"

# Batch output processing - add extraction columns to results
processed_df <- asa::process_outputs(
  results_df,
  parallel = TRUE  # Parallel extraction
)
# Adds columns: search_count, wiki_count, plus parsed JSON fields
```

**Search Tiers:** The package uses a 4-tier fallback for DuckDuckGo searches:
- `primp`: Fast HTTP/2 client with browser fingerprint (default, fastest)
- `selenium`: Headless browser for JS-rendered content
- `ddgs`: Standard DDGS Python library
- `requests`: Raw HTTP POST fallback

## Performance

<!-- SPEED_REPORT_START -->
**Last Run:** 2025-12-22 10:53:18 EST | **Status:** PASS

| Benchmark | Current | Baseline | Ratio | Status |
|-----------|---------|----------|-------|--------|
| `build_prompt` | 0.082s | 0.09s | 0.91x | PASS |
| `helper_funcs` | 0.065s | 0.07s | 0.93x | PASS |
| `combined` | 0.089s | 0.09s | 0.98x | PASS |
| `agent_search` | 13.5s | 18s | 0.77x | PASS |

Tests fail if time exceeds 1.25x baseline. 
See [full report](asa/tests/testthat/SPEED_REPORT.md) for details.
<!-- SPEED_REPORT_END -->

## Requirements

- R >= 4.0
- Python >= 3.10 (managed via conda)
- reticulate, jsonlite, rlang

## License

MIT


