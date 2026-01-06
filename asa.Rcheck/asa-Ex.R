pkgname <- "asa"
source(file.path(R.home("share"), "R", "examples-header.R"))
options(warn = 1)
base::assign(".ExTimings", "asa-Ex.timings", pos = 'CheckExEnv')
base::cat("name\tuser\tsystem\telapsed\n", file=base::get(".ExTimings", pos = 'CheckExEnv'))
base::assign(".format_ptime",
function(x) {
  if(!is.na(x[4L])) x[1L] <- x[1L] + x[4L]
  if(!is.na(x[5L])) x[2L] <- x[2L] + x[5L]
  options(OutDec = '.')
  format(x[1L:3L], digits = 7L)
},
pos = 'CheckExEnv')

### * </HEADER>
library('asa')

base::assign(".oldSearch", base::search(), pos = 'CheckExEnv')
base::assign(".old_wd", base::getwd(), pos = 'CheckExEnv')
cleanEx()
nameEx("asa_audit")
### * asa_audit

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: asa_audit
### Title: Audit Enumeration Results for Completeness and Quality
### Aliases: asa_audit

### ** Examples

## Not run: 
##D # Audit enumeration results with Claude Code
##D senators <- asa_enumerate(
##D   query = "Find all current US senators",
##D   schema = c(name = "character", state = "character", party = "character")
##D )
##D audit <- asa_audit(senators, backend = "claude_code")
##D print(audit)
##D 
##D # Audit with known universe for precise completeness check
##D audit <- asa_audit(senators, known_universe = state.abb)
##D 
##D # Interactive mode for complex audits
##D asa_audit(senators, backend = "claude_code", interactive = TRUE)
##D 
##D # Use LangGraph backend
##D audit <- asa_audit(senators, backend = "langgraph", agent = agent)
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("asa_audit", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("asa_config")
### * asa_config

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: asa_config
### Title: Create ASA Configuration Object
### Aliases: asa_config

### ** Examples

## Not run: 
##D # Create configuration
##D config <- asa_config(
##D   backend = "openai",
##D   model = "gpt-4.1-mini",
##D   workers = 4,
##D   temporal = temporal_options(time_filter = "y")
##D )
##D 
##D # Use with run_task
##D result <- run_task(prompt, config = config)
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("asa_config", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("asa_enumerate")
### * asa_enumerate

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: asa_enumerate
### Title: Multi-Agent Research for Open-Ended Queries
### Aliases: asa_enumerate

### ** Examples

## Not run: 
##D # Find all US senators
##D senators <- asa_enumerate(
##D   query = "Find all current US senators with state, party, and term end date",
##D   schema = c(name = "character", state = "character",
##D              party = "character", term_end = "character"),
##D   stop_policy = list(target_items = 100),
##D   include_provenance = TRUE
##D )
##D head(senators$data)
##D 
##D # Find countries with auto schema
##D countries <- asa_enumerate(
##D   query = "Find all countries with their capitals and populations",
##D   schema = "auto",
##D   output = "csv"
##D )
##D 
##D # Resume from checkpoint
##D result <- asa_enumerate(
##D   query = "Find Fortune 500 CEOs",
##D   resume_from = "/tmp/asa_enumerate_abc123.rds"
##D )
##D 
##D # Temporal filtering: results from specific date range
##D companies_2020s <- asa_enumerate(
##D   query = "Find tech companies founded recently",
##D   temporal = list(
##D     after = "2020-01-01",
##D     before = "2024-01-01",
##D     strictness = "best_effort"
##D   )
##D )
##D 
##D # Temporal filtering: past year with DuckDuckGo time filter
##D recent_news <- asa_enumerate(
##D   query = "Find AI research breakthroughs",
##D   temporal = list(
##D     time_filter = "y"  # past year
##D   )
##D )
##D 
##D # Strict temporal filtering with Wayback Machine
##D historical <- asa_enumerate(
##D   query = "Find Fortune 500 companies",
##D   temporal = list(
##D     before = "2015-01-01",
##D     strictness = "strict",
##D     use_wayback = TRUE
##D   )
##D )
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("asa_enumerate", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("build_backend")
### * build_backend

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: build_backend
### Title: Build the Python Backend Environment
### Aliases: build_backend

### ** Examples

## Not run: 
##D # Create the default environment
##D build_backend()
##D 
##D # Create with a custom name
##D build_backend(conda_env = "my_asa_env")
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("build_backend", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("build_prompt")
### * build_prompt

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: build_prompt
### Title: Build a Task Prompt from Template
### Aliases: build_prompt

### ** Examples

## Not run: 
##D prompt <- build_prompt(
##D   template = "Find information about {{name}} in {{country}} during {{year}}",
##D   name = "Marie Curie",
##D   country = "France",
##D   year = 1903
##D )
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("build_prompt", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("check_backend")
### * check_backend

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: check_backend
### Title: Check Python Environment Availability
### Aliases: check_backend

### ** Examples

## Not run: 
##D status <- check_backend()
##D if (!status$available) {
##D   build_backend()
##D }
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("check_backend", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("configure_search")
### * configure_search

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: configure_search
### Title: Configure Python Search Parameters
### Aliases: configure_search

### ** Examples

## Not run: 
##D # Increase timeout for slow connections
##D configure_search(timeout = 30, max_retries = 5)
##D 
##D # Get more results
##D configure_search(max_results = 20)
##D 
##D # Add delay between searches to avoid rate limiting
##D configure_search(inter_search_delay = 2.0)
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("configure_search", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("configure_search_logging")
### * configure_search_logging

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: configure_search_logging
### Title: Configure Python Search Logging Level
### Aliases: configure_search_logging

### ** Examples

## Not run: 
##D # Enable verbose debugging output
##D configure_search_logging("DEBUG")
##D 
##D # Run a search (will show detailed logs)
##D result <- run_task("What is the population of Tokyo?", agent = agent)
##D 
##D # Disable verbose output
##D configure_search_logging("WARNING")
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("configure_search_logging", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("configure_temporal")
### * configure_temporal

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: configure_temporal
### Title: Configure Temporal Filtering for Search
### Aliases: configure_temporal

### ** Examples

## Not run: 
##D # Restrict to past year
##D configure_temporal("y")
##D result <- run_task("Find recent AI breakthroughs", agent = agent)
##D 
##D # Clear temporal filter
##D configure_temporal(NULL)
##D 
##D # Past week only
##D configure_temporal("w")
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("configure_temporal", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("extract_agent_results")
### * extract_agent_results

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: extract_agent_results
### Title: Extract Structured Data from Agent Traces
### Aliases: extract_agent_results

### ** Examples

## Not run: 
##D response <- run_agent("Who is the president of France?", agent)
##D extracted <- extract_agent_results(response$trace)
##D print(extracted$search_snippets)
##D print(extracted$search_tiers)  # Shows which search tier was used
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("extract_agent_results", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("extract_search_snippets")
### * extract_search_snippets

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: extract_search_snippets
### Title: Extract Search Snippets by Source Number
### Aliases: extract_search_snippets

### ** Examples

## Not run: 
##D snippets <- extract_search_snippets(response$trace)
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("extract_search_snippets", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("extract_search_tiers")
### * extract_search_tiers

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: extract_search_tiers
### Title: Extract Search Tier Information
### Aliases: extract_search_tiers

### ** Examples

## Not run: 
##D tiers <- extract_search_tiers(response$trace)
##D print(tiers)  # e.g., "primp"
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("extract_search_tiers", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("extract_urls")
### * extract_urls

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: extract_urls
### Title: Extract URLs by Source Number
### Aliases: extract_urls

### ** Examples

## Not run: 
##D urls <- extract_urls(response$trace)
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("extract_urls", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("extract_wikipedia_content")
### * extract_wikipedia_content

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: extract_wikipedia_content
### Title: Extract Wikipedia Content
### Aliases: extract_wikipedia_content

### ** Examples

## Not run: 
##D wiki <- extract_wikipedia_content(response$trace)
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("extract_wikipedia_content", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("get_agent")
### * get_agent

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: get_agent
### Title: Get the Current Agent
### Aliases: get_agent

### ** Examples

## Not run: 
##D agent <- get_agent()
##D if (is.null(agent)) {
##D   agent <- initialize_agent()
##D }
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("get_agent", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("get_tor_ip")
### * get_tor_ip

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: get_tor_ip
### Title: Get External IP via Tor
### Aliases: get_tor_ip

### ** Examples

## Not run: 
##D ip <- get_tor_ip()
##D message("Current Tor IP: ", ip)
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("get_tor_ip", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("initialize_agent")
### * initialize_agent

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: initialize_agent
### Title: Initialize the ASA Search Agent
### Aliases: initialize_agent

### ** Examples

## Not run: 
##D # Initialize with OpenAI
##D agent <- initialize_agent(
##D   backend = "openai",
##D   model = "gpt-4.1-mini"
##D )
##D 
##D # Initialize with Groq and custom settings
##D agent <- initialize_agent(
##D   backend = "groq",
##D   model = "llama-3.3-70b-versatile",
##D   use_memory_folding = FALSE,
##D   proxy = NULL  # No Tor proxy
##D )
##D 
##D # Initialize with OpenRouter (access to 100+ models)
##D agent <- initialize_agent(
##D   backend = "openrouter",
##D   model = "anthropic/claude-3-sonnet"  # Note: provider/model format
##D )
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("initialize_agent", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("is_tor_running")
### * is_tor_running

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: is_tor_running
### Title: Check if Tor is Running
### Aliases: is_tor_running

### ** Examples

## Not run: 
##D if (!is_tor_running()) {
##D   message("Start Tor with: brew services start tor")
##D }
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("is_tor_running", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("rotate_tor_circuit")
### * rotate_tor_circuit

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: rotate_tor_circuit
### Title: Rotate Tor Circuit
### Aliases: rotate_tor_circuit

### ** Examples

## Not run: 
##D rotate_tor_circuit()
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("rotate_tor_circuit", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("run_task")
### * run_task

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: run_task
### Title: Run a Structured Task with the Agent
### Aliases: run_task

### ** Examples

## Not run: 
##D # Initialize agent first
##D agent <- initialize_agent(backend = "openai", model = "gpt-4.1-mini")
##D 
##D # Simple text query
##D result <- run_task(
##D   prompt = "What is the capital of France?",
##D   output_format = "text",
##D   agent = agent
##D )
##D print(result$message)
##D 
##D # JSON structured output
##D result <- run_task(
##D   prompt = "Find information about Albert Einstein and return JSON with
##D             fields: birth_year, death_year, nationality, field_of_study",
##D   output_format = "json",
##D   agent = agent
##D )
##D print(result$parsed)
##D 
##D # Raw output for debugging (includes full trace in asa_result)
##D result <- run_task(
##D   prompt = "Search for information",
##D   output_format = "raw",
##D   agent = agent
##D )
##D cat(result$trace)  # View full agent trace
##D 
##D # With temporal filtering (past year only)
##D result <- run_task(
##D   prompt = "Find recent AI research breakthroughs",
##D   temporal = temporal_options(time_filter = "y"),
##D   agent = agent
##D )
##D 
##D # With date range hint
##D result <- run_task(
##D   prompt = "Find tech companies founded recently",
##D   temporal = list(
##D     time_filter = "y",
##D     after = "2020-01-01",
##D     before = "2024-01-01"
##D   ),
##D   agent = agent
##D )
##D 
##D # Using asa_config for unified configuration
##D config <- asa_config(
##D   backend = "openai",
##D   model = "gpt-4.1-mini",
##D   temporal = temporal_options(time_filter = "y")
##D )
##D result <- run_task(prompt, config = config)
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("run_task", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("run_task_batch")
### * run_task_batch

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: run_task_batch
### Title: Run Multiple Tasks in Batch
### Aliases: run_task_batch

### ** Examples

## Not run: 
##D prompts <- c(
##D   "What is the population of Tokyo?",
##D   "What is the population of New York?",
##D   "What is the population of London?"
##D )
##D results <- run_task_batch(prompts, agent = agent)
##D 
##D # With temporal filtering for all tasks
##D results <- run_task_batch(
##D   prompts,
##D   temporal = list(time_filter = "y"),
##D   agent = agent
##D )
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("run_task_batch", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("search_options")
### * search_options

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: search_options
### Title: Create Search Options
### Aliases: search_options

### ** Examples

## Not run: 
##D # Default settings
##D search <- search_options()
##D 
##D # More aggressive settings for faster searches
##D search <- search_options(
##D   max_results = 5,
##D   timeout = 10,
##D   max_retries = 2
##D )
##D 
##D # Conservative settings for rate-limited environments
##D search <- search_options(
##D   inter_search_delay = 2.0,
##D   max_retries = 5,
##D   backoff_multiplier = 2.0
##D )
##D 
##D # Use with asa_config
##D config <- asa_config(
##D   backend = "openai",
##D   search = search_options(max_results = 15)
##D )
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("search_options", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
cleanEx()
nameEx("temporal_options")
### * temporal_options

flush(stderr()); flush(stdout())

base::assign(".ptime", proc.time(), pos = "CheckExEnv")
### Name: temporal_options
### Title: Create Temporal Filtering Options
### Aliases: temporal_options

### ** Examples

## Not run: 
##D # Past year only
##D temporal <- temporal_options(time_filter = "y")
##D 
##D # Specific date range
##D temporal <- temporal_options(
##D   after = "2020-01-01",
##D   before = "2024-01-01"
##D )
##D 
##D # Strict historical verification
##D temporal <- temporal_options(
##D   before = "2015-01-01",
##D   strictness = "strict",
##D   use_wayback = TRUE
##D )
## End(Not run)




base::assign(".dptime", (proc.time() - get(".ptime", pos = "CheckExEnv")), pos = "CheckExEnv")
base::cat("temporal_options", base::get(".format_ptime", pos = 'CheckExEnv')(get(".dptime", pos = "CheckExEnv")), "\n", file=base::get(".ExTimings", pos = 'CheckExEnv'), append=TRUE, sep="\t")
### * <FOOTER>
###
cleanEx()
options(digits = 7L)
base::cat("Time elapsed: ", proc.time() - base::get("ptime", pos = 'CheckExEnv'),"\n")
grDevices::dev.off()
###
### Local variables: ***
### mode: outline-minor ***
### outline-regexp: "\\(> \\)?### [*]+" ***
### End: ***
quit('no')
