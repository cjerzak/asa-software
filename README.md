# asa

[![R-CMD-check](https://github.com/cjerzak/asa-software/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/cjerzak/asa-software/actions/workflows/R-CMD-check.yaml)

**AI Search Agent for Large-Scale Research Automation**

An R package for running LLM-powered research tasks at scale. Unlike search-enabled APIs that charge ~$10 per 1,000 searches with limited control, asa provides full customizability over search behavior, LLM backends, and temporal filtering at minimal cost. Uses a ReAct (Reasoning + Acting) agent pattern with web search capabilities, implemented via LangGraph in Python and orchestrated from R.

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
| `asa_enumerate()` | **Open-ended enumeration** with multi-agent orchestration and temporal filtering |
| `asa_audit()` | **Data quality auditing** for enumeration results |
| `asa_config()` | Create unified configuration object |
| `temporal_options()` | Create temporal filtering options |
| `search_options()` | Create search configuration options |
| `build_prompt()` | Build prompts from templates with variable substitution |
| `configure_search()` | Configure search timing and retry behavior |
| `extract_search_tiers()` | Get which search tier was used from traces |

> **Note:** `run_agent()` has been removed. Use `run_task(..., output_format = "raw")` for full trace access.

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

## Open-Ended Enumeration

For entity enumeration tasks like "find all US senators", use `asa_enumerate()` which orchestrates multi-agent search with intelligent stopping:

```r
# Find all current US senators
senators <- asa::asa_enumerate(
  query = "Find all current United States senators",
  schema = c(name = "character", state = "character", party = "character"),
  sources = list(wikidata = TRUE, web = TRUE, wikipedia = TRUE),
  stop_policy = list(target_items = 100),
  progress = TRUE
)

# Access results
head(senators$data)
#>              name       state      party
#> 1 Tammy Baldwin   Wisconsin   Democratic
#> 2 John Barrasso     Wyoming   Republican
#> ...

# Check coverage
summary(senators)
# Status: complete | Items: 95 | Rounds: 1 | Time: 11.9s
```

**Key features:**
- **Multi-source search**: Wikidata SPARQL, web search, Wikipedia
- **Temporal filtering**: Filter results by date range, use Wayback Machine for historical data
- **Smart stopping**: Target count, novelty plateau, budget limits
- **Schema enforcement**: Structured output with specified columns
- **Checkpointing**: Resume interrupted searches with `resume_from`
- **Provenance tracking**: Optional source attribution per row

**Available Wikidata entity types** (for authoritative enumeration):
- `us_senators`, `us_representatives`, `countries`, `fortune500`, `us_states`

```r
# Full parameter control
result <- asa::asa_enumerate(
  query = "Find Fortune 500 companies with their CEOs",
  schema = c(company = "character", ceo = "character", industry = "character"),
  workers = 4,
  max_rounds = 8,
  budget = list(queries = 50, tokens = 200000, time_sec = 300),
  stop_policy = list(
    target_items = 500,
    plateau_rounds = 2,
    novelty_min = 0.05
  ),
  sources = list(wikidata = TRUE, web = TRUE),
  checkpoint = TRUE,
  include_provenance = TRUE
)
```

## Temporal Filtering

Filter enumeration results by date to find information from specific time periods:

```r
# Results from a specific date range
companies <- asa::asa_enumerate(
  query = "Find tech companies founded recently",
  schema = c(name = "character", founded = "character", industry = "character"),
  temporal = list(
    after = "2020-01-01",      # Only results after this date
    before = "2024-01-01",     # Only results before this date
    strictness = "best_effort" # or "strict" for post-hoc verification
  )
)

# Use DuckDuckGo's native time filter for recent results
recent <- asa::asa_enumerate(
  query = "AI research breakthroughs",
  temporal = list(time_filter = "y")  # "d"=day, "w"=week, "m"=month, "y"=year
)

# Strict historical research with Internet Archive
historical <- asa::asa_enumerate(
  query = "Fortune 500 companies",
  temporal = list(
    before = "2015-01-01",
    strictness = "strict",
    use_wayback = TRUE  # Use Wayback Machine for guaranteed pre-date content
  )
)
```

**Temporal filtering parameters:**

| Parameter | Values | Description |
|-----------|--------|-------------|
| `after` | ISO 8601 date | Only include results valid after this date |
| `before` | ISO 8601 date | Only include results valid before this date |
| `time_filter` | `"d"`, `"w"`, `"m"`, `"y"` | DuckDuckGo native filter (day/week/month/year) |
| `strictness` | `"best_effort"`, `"strict"` | Verification level for web results |
| `use_wayback` | `TRUE`, `FALSE` | Use Wayback Machine for pre-date guarantees |

**How it works by source:**
- **Wikidata**: Uses SPARQL temporal qualifiers (P580/P582) for precise filtering
- **Web search**: Best-effort query-time filtering with optional post-hoc date extraction
- **Wayback Machine**: Searches archived web snapshots for strict historical accuracy

## Data Quality Auditing

Validate enumeration results using `asa_audit()`. Supports **Claude Code CLI** (reasoning-heavy) or **LangGraph** (programmatic) backends.

```r
# Audit enumeration results
audit <- asa_audit(senators, backend = "claude_code")
print(audit)
#> Completeness: 96% | Consistency: 98%
#> Issues: [MEDIUM] Missing states: AK, HI

# With known universe for precise completeness check
audit <- asa_audit(senators, known_universe = state.abb)

# Interactive session or LangGraph backend
asa_audit(senators, interactive = TRUE)
asa_audit(senators, backend = "langgraph", agent = agent)
```

**Checks performed:** completeness (vs. known universe), consistency (types/patterns), gap analysis (systematic missing data), anomaly detection (duplicates/outliers).

| Parameter | Default | Description |
|-----------|---------|-------------|
| `backend` | `"claude_code"` | `"claude_code"` (CLI) or `"langgraph"` |
| `checks` | all | `"completeness"`, `"consistency"`, `"gaps"`, `"anomalies"` |
| `known_universe` | `NULL` | Expected items for completeness check |
| `interactive` | `FALSE` | Spawn interactive Claude Code session |

## Configuration

### LLM Backends

| Backend | Model Examples | API Key Variable |
|---------|----------------|------------------|
| `openai` | `gpt-4.1-mini`, `gpt-4o` | `OPENAI_API_KEY` |
| `groq` | `llama-3.3-70b-versatile` | `GROQ_API_KEY` |
| `xai` | `grok-2-1212`, `grok-3` | `XAI_API_KEY` |
| `gemini` | `gemini-3-flash-preview` | `GOOGLE_API_KEY` (or `GEMINI_API_KEY`) |
| `openrouter` | `google/gemini-2.0-flash-exp:free`, `meta-llama/llama-3.3-70b-instruct:free` | `OPENROUTER_API_KEY` |
| `exo` | Local models | (none) |

### Agent Options

```r
agent <- asa::initialize_agent(
  backend = "openai",
  model = "gpt-4.1-mini",
  proxy = NA,                           # NA=auto from env; NULL=disable; set socks5h://127.0.0.1:9050 for Tor
  timeout = 120,                        # Request timeout in seconds
  rate_limit = 0.1                      # Requests per second (conservative default)
)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `proxy` | `NA` | Proxy URL for search tools (`NA` = auto from env; `NULL` = disable) |
| `timeout` | `120` | Request timeout in seconds |
| `rate_limit` | `0.1` | Max requests per second (conservative default for heavy workloads) |
| `verbose` | `TRUE` | Print initialization status messages |

### Configuration Classes

The package provides typed configuration classes for organized settings management:

```r
# Create temporal filtering options
temporal <- asa::temporal_options(
  time_filter = "y",      # DuckDuckGo time filter
  after = "2023-01-01",   # ISO 8601 date
  before = "2024-12-01"
)

# Create search options
search <- asa::search_options(
  max_retries = 5,
  inter_search_delay = 1.0
)

# Create unified configuration
config <- asa::asa_config(
  backend = "openai",
  model = "gpt-4.1-mini",
  workers = 4,
  temporal = temporal,
  search = search
)

# Use with run_task
result <- asa::run_task(prompt, config = config)

# Use with run_task_batch / asa_enumerate
results <- asa::run_task_batch(prompts, config = config, parallel = TRUE)
enum <- asa::asa_enumerate("Find all current US senators", config = config)
```

### Optional Webpage Reading

When enabled, the agent can open full webpages (not just search snippets) and
extract the most relevant excerpts. This is disabled by default and must be
explicitly turned on per call or via `search_options()`. Relevance selection
uses embeddings when available (local `sentence-transformers` or OpenAI
embeddings via `OPENAI_API_KEY`), otherwise falls back to lexical overlap.
Within a single run, repeated opens of the same URL are cached to avoid
re-fetching.

```r
# Enable optional webpage reading with embedding-based relevance
search <- asa::search_options(
  allow_read_webpages = TRUE,
  webpage_relevance_mode = "embeddings",
  webpage_embedding_provider = "openai",
  webpage_embedding_model = "text-embedding-3-small",
  # Optional: allow larger excerpts (defaults are conservative)
  webpage_max_chars = 30000,
  webpage_max_chunks = 10,
  webpage_chunk_chars = 2000
)

result <- asa::run_task(
  "Find the exact wording of the mission statement and quote it.",
  config = asa::asa_config(search = search)
)
```

**In the agent trace:** Each opened page appears as an `OpenWebpage` tool message in `result$trace_json` (preferred) or `result$raw_output`. The tool output is plain text like:

```text
URL: https://example.com/page
Final URL: https://example.com/page
Title: Example
Bytes read: 12345
Cache: MISS

Relevant excerpts:

[1]
...
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
  memory_keep_recent = 3        # Keep 3 recent exchanges after fold (default: 2)
)

# Monitor folding activity in responses
result <- asa::run_task(
  "Complex multi-step research query...",
  output_format = "raw",  # Returns asa_result with trace and fold_count
  agent = agent
)
print(result$fold_count)  # Number of times memory was folded

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
| `memory_keep_recent` | `2` | Recent exchanges to preserve after each fold |

An exchange is a user turn plus the assistant response, including any tool calls and tool outputs.

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
  inter_search_delay = 3.0   # Delay between searches in seconds (default: 2.0)
)

# Enable debug logging for troubleshooting
asa::configure_search_logging("DEBUG")  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `inter_search_delay` | `2.0` | Seconds between consecutive searches (humanized with jitter) |
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

# Share a Tor exit health registry across workers (avoids reusing tainted exits)
tor_cfg <- asa::tor_options(
  registry_path = "~/asa/tor_exit_registry.sqlite",  # defaults to user cache
  bad_ttl = 3600,    # keep bad exits sidelined for an hour
  overuse_threshold = 8  # rotate if an exit is used too often
)
asa::initialize_agent(
  backend = "openai",
  model = "gpt-4.1-mini",
  tor = tor_cfg
)
# You can also push settings without reinitializing:
asa::configure_tor_registry(registry_path = tor_cfg$registry_path)
```

### Anti-Detection Features

The package includes multiple layers of anti-detection for high-volume workloads (1000+ queries/day):

**Proactive Tor Circuit Rotation:**
- Automatically rotates Tor exit node every 15 requests (not just on errors)
- Gets fresh IP before detection thresholds are reached

**Adaptive Rate Limiting:**
- Dynamically adjusts delays based on success/error patterns
- On CAPTCHA/block: delays increase by 50% (caps at 5x)
- After 10 consecutive successes: delays decrease by 10% (floor at 0.5x)

**Session Reset:**
- Every 50 requests, resets session identity
- Shuffles user-agent pool and clears timing state
- Each batch appears as a different user

**Humanized Timing:**
- Log-normal delay distribution (most quick, occasional long pauses)
- Micro-stutters and fatigue curves simulate human behavior
- 5% chance of "thinking pauses" (0.5-2s hesitation)

All features are enabled by default. For very high-volume work, consider:
```r
# Even more conservative settings for 5000+ queries/day
asa::configure_search(inter_search_delay = 4.0)
agent <- asa::initialize_agent(rate_limit = 0.05)
```

### Output Extraction

Extract detailed information from agent traces for analysis:

```r
# Run a task and extract structured data from the trace
result <- asa::run_task(
  prompt = "Research quantum computing applications in drug discovery.",
  agent = agent
)

# Extract search artifacts from the trace (prefer structured JSON when available)
trace <- if (!is.null(result$trace_json) && nzchar(result$trace_json)) result$trace_json else result$raw_output
extracted <- asa::extract_agent_results(trace)

# Access extracted data
extracted$search_snippets     # List of search result text by source number
extracted$search_urls         # List of URLs from search results
extracted$wikipedia_snippets  # Wikipedia content retrieved
extracted$json_data           # Any JSON data found in response
extracted$search_tiers        # Which search tier was used ("primp", "selenium", "ddgs", "requests")

# Get snippets from specific search call
snippets_from_search_1 <- asa::extract_search_snippets(trace, source = 1)
urls_from_search_1 <- asa::extract_urls(trace, source = 1)

# Extract tier info directly
tiers <- asa::extract_search_tiers(trace)
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

## Troubleshooting

**Rate limited or CAPTCHA blocks?**
- The package now has adaptive rate limiting (enabled by default) that automatically slows down on errors
- For persistent issues, increase delays: `configure_search(inter_search_delay = 4.0)`
- Enable debug logging to see what's happening: `configure_search_logging("DEBUG")`
- Use Tor proxy for IP rotation: `rotate_tor_circuit()`
- Using Tor + stealth Chrome? Rebuild the backend so `undetected-chromedriver` and `stem` are installed (`asa::build_backend(force = TRUE)`), and set `TOR_CONTROL_PORT` per worker (env is read at call time).

**API key not recognized?**
```r
# Check if key is set
Sys.getenv("OPENAI_API_KEY")  # Should not be empty

# Set it for the session
Sys.setenv(OPENAI_API_KEY = "sk-...")
```

**Python environment issues?**
```r
# Rebuild the conda environment
asa::build_backend(conda_env = "asa_env", force = TRUE)
```

**Cost considerations:**
- Web search (DuckDuckGo, Wikipedia): Free
- LLM costs vary by backend: OpenAI ~$0.01-0.10/task, Groq ~$0.001/task

## Performance

<!-- SPEED_REPORT_START -->
**Last Run:** 2026-02-05 21:33:43 CST | **Status:** PASS

| Benchmark | Current | Baseline | Ratio | Status |
|-----------|---------|----------|-------|--------|
| `build_prompt` | 0.087s | 0.09s | 0.97x | PASS |
| `helper_funcs` | 0.046s | 0.07s | 0.66x | PASS |
| `combined` | 0.106s | 0.09s | 1.16x | PASS |
| `agent_search` | 31.9s | 18s | 1.81x | PASS |

Tests fail if time exceeds 4.00x baseline. 
See [full report](asa/tests/testthat/SPEED_REPORT.md) for details.
<!-- SPEED_REPORT_END -->

## Requirements

- R >= 4.0
- Python >= 3.11 (managed via conda)
- reticulate, jsonlite, rlang

**Optional:**
- Claude Code CLI (for `asa_audit()` with `backend = "claude_code"`)
- processx (recommended for robust CLI invocations; base `system2()` fallback is used if unavailable)
- Tor + stealth Chrome support (requires system Tor plus Python packages `undetected-chromedriver` and `stem`, installed by `asa::build_backend()`)

## Reference

### Search Architecture

The agent uses two search tools with multi-tier fallback:

- **Wikipedia**: Encyclopedic information lookup
- **DuckDuckGo**: Web search with 4-tier fallback (PRIMP → Selenium → DDGS → raw requests)

Use `extract_search_tiers()` to identify which tier was used in a trace.

### S3 Classes

| Class | Description |
|-------|-------------|
| `asa_agent` | Initialized agent object |
| `asa_result` | Task result with `$message`, `$parsed`, `$status` |
| `asa_enumerate_result` | Enumeration results with `$data`, `$status`, `$metrics`, `$provenance` |
| `asa_audit_result` | Audit results with `$completeness_score`, `$issues`, `$recommendations` |
| `asa_config` | Unified configuration object |
| `asa_temporal` | Temporal filtering options |
| `asa_search` | Search configuration options |

All classes have `print()` and `summary()` methods. Use `as.data.frame()` for conversion.

## License

MIT

```
╔══════════════════════════════════════════════════════════════════════════════╗
║ [R] ORCHESTRATION: ACTIVE  ::  [PYTHON] BACKEND: CONNECTED  ::  STATUS: OK   ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║           █████╗ ███████╗ █████╗     AI SEARCH AGENT                         ║
║          ██╔══██╗██╔════╝██╔══██╗    Large-Scale Research Automation         ║
║          ███████║███████╗███████║    -------------------------------         ║
║          ██╔══██║╚════██║██╔══██║    > Reasoning: [ReAct Pattern]            ║
║          ██║  ██║███████║██║  ██║    > Memory:    [DeepAgent Folding]        ║
║          ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝    > Search:    [Multi-Tier Fallback]      ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  v0.1.0  ::  DuckDuckGo + Wiki  ::  LangGraph  ::  Parallel Batch Processing ║
╚══════════════════════════════════════════════════════════════════════════════╝
```
