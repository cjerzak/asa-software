#' Multi-Agent Research for Open-Ended Queries
#'
#' Performs intelligent open-ended research tasks using multi-agent orchestration.
#' Decomposes complex queries into sub-tasks, executes parallel searches, and
#' aggregates results into structured output (data.frame, CSV, or JSON).
#'
#' @param query Character string describing the research goal. Required for fresh
#'   runs; optional when \code{resume_from} points to an existing checkpoint.
#'   Examples: "Find all current US senators with their state, party, and term end date"
#' @param schema Named character vector defining the output schema.
#'   Names are column names, values are R types ("character", "numeric", "logical").
#'   Use NULL or "auto" for LLM-proposed schema.
#' @param output Output format: "data.frame" (default), "csv", or "json".
#' @param config Optional \code{asa_config} object to supply defaults for agent
#'   initialization and per-run search/temporal settings. Explicit arguments to
#'   \code{asa_enumerate()} take precedence over \code{config}.
#' @param workers Number of parallel search workers. Defaults to value from
#'   \code{config$workers} when provided, otherwise \code{ASA_DEFAULT_WORKERS}
#'   (typically 4).
#' @param max_rounds Maximum research iterations. Defaults to value from
#'   \code{ASA_DEFAULT_MAX_ROUNDS} (typically 8).
#' @param budget Named list with resource limits:
#'   \itemize{
#'     \item queries: Maximum search queries (default: 50)
#'     \item tokens: Maximum LLM tokens (default: 200000)
#'     \item time_sec: Maximum execution time in seconds (default: 300)
#'   }
#' @param stop_policy Named list with stopping criteria:
#'   \itemize{
#'     \item target_items: Stop when this many items found (NULL = unknown)
#'     \item plateau_rounds: Stop after N rounds with no new items (default: 2)
#'     \item novelty_min: Minimum new items ratio per round (default: 0.05)
#'     \item novelty_window: Window size for novelty calculation (default: 20)
#'   }
#' @param sources Named list controlling which sources to use:
#'   \itemize{
#'     \item web: Use DuckDuckGo web search (default: TRUE)
#'     \item wikipedia: Use Wikipedia (default: TRUE)
#'     \item wikidata: Use Wikidata SPARQL for authoritative enumerations (default: TRUE)
#'   }
#' @param allow_read_webpages If TRUE, the agent may open and read full webpages
#'   (in addition to search snippets) when it helps extraction. Use NULL
#'   (default) to inherit from \code{config$search} (when provided); otherwise
#'   defaults to FALSE for safety and to avoid large context usage.
#' @param webpage_relevance_mode Relevance selection for opened webpages.
#'   One of: "auto" (default), "lexical", "embeddings". When "embeddings" or
#'   "auto" with an available provider, the tool uses vector similarity to pick
#'   the most relevant excerpts; otherwise it falls back to lexical overlap.
#' @param webpage_embedding_provider Embedding provider to use for relevance.
#'   One of: "auto" (default), "openai", "sentence_transformers".
#' @param webpage_embedding_model Embedding model identifier. For OpenAI,
#'   defaults to "text-embedding-3-small". For sentence-transformers, use a
#'   local model name (e.g., "all-MiniLM-L6-v2").
#' @param temporal Named list for temporal filtering:
#'   \itemize{
#'     \item after: ISO 8601 date string (e.g., "2020-01-01") - results after this date
#'     \item before: ISO 8601 date string (e.g., "2024-01-01") - results before this date
#'     \item time_filter: DuckDuckGo time filter ("d", "w", "m", "y") for day/week/month/year
#'     \item strictness: "best_effort" (default) or "strict" (verifies dates via metadata)
#'     \item use_wayback: Use Wayback Machine for strict pre-date guarantees (default: FALSE)
#'   }
#' @param pagination Enable pagination for large result sets (default: TRUE).
#' @param progress Show progress bar and status updates (default: TRUE).
#' @param include_provenance Include source URLs and confidence per row (default: FALSE).
#' @param checkpoint Enable auto-save after planning and each completed round
#'   (default: TRUE).
#' @param checkpoint_dir Directory for checkpoint files (default: tempdir()).
#' @param resume_from Path to checkpoint file to resume from (default: NULL).
#'   For v2 checkpoints this continues execution from the last saved round
#'   state. Legacy v1 checkpoints return the saved result without continuation.
#' @param agent An initialized \code{asa_agent} object. If NULL, uses the current
#'   agent or creates a new one with specified backend/model.
#' @param backend LLM backend if creating new agent: "openai", "groq", "xai", "gemini", "exo", "openrouter".
#' @param model Model identifier if creating new agent.
#' @param conda_env Conda environment name. Defaults to the package option
#'   \code{asa.default_conda_env} (or \code{"asa_env"} if unset).
#' @param verbose Print status messages (default: TRUE).
#'
#' @return An object of class \code{asa_enumerate_result} containing:
#'   \itemize{
#'     \item data: data.frame with results matching the schema
#'     \item status: "complete", "partial", or "failed"
#'     \item stop_reason: Why the search stopped
#'     \item metrics: List with rounds, queries_used, novelty_curve, coverage
#'     \item provenance: If include_provenance=TRUE, source info per row
#'     \item checkpoint_file: Path to checkpoint if saved
#'   }
#'
#' @details
#' The function uses an iterative graph architecture:
#' \enumerate{
#'   \item \strong{Planner}: Decomposes the query and proposes a search plan.
#'   \item \strong{Searcher}: Queries Wikidata (when applicable) and falls back to web/Wikipedia.
#'   \item \strong{Deduper}: Removes duplicates using hashing + fuzzy matching.
#'   \item \strong{Stopper}: Evaluates stopping criteria (novelty, budgets, saturation).
#' }
#'
#' Parallelism is limited and backend-dependent. For example, strict temporal
#' filtering may verify publication dates in parallel up to \code{workers}.
#'
#' For known entity types (US senators, countries, Fortune 500), Wikidata provides
#' authoritative enumerations with complete, verified data.
#'
#' @section Checkpointing:
#' With checkpoint=TRUE, state is saved after planning and after each completed
#' round. If interrupted, use resume_from to continue from the last checkpoint:
#' \preformatted{
#' result <- asa_enumerate(resume_from = "/path/to/checkpoint.rds")
#' }
#'
#' @section Schema:
#' The schema defines expected output columns:
#' \preformatted{
#' schema = c(name = "character", state = "character", party = "character")
#' }
#' With schema = "auto", the planner agent proposes a schema based on the query.
#'
#' @examples
#' \dontrun{
#' # Find all US senators
#' senators <- asa_enumerate(
#'   query = "Find all current US senators with state, party, and term end date",
#'   schema = c(name = "character", state = "character",
#'              party = "character", term_end = "character"),
#'   stop_policy = list(target_items = 100),
#'   include_provenance = TRUE
#' )
#' head(senators$data)
#'
#' # Find countries with auto schema
#' countries <- asa_enumerate(
#'   query = "Find all countries with their capitals and populations",
#'   schema = "auto",
#'   output = "csv"
#' )
#'
#' # Resume from checkpoint
#' result <- asa_enumerate(
#'   query = "Find Fortune 500 CEOs",
#'   resume_from = "/tmp/asa_enumerate_abc123.rds"
#' )
#'
#' # Temporal filtering: results from specific date range
#' companies_2020s <- asa_enumerate(
#'   query = "Find tech companies founded recently",
#'   temporal = list(
#'     after = "2020-01-01",
#'     before = "2024-01-01",
#'     strictness = "best_effort"
#'   )
#' )
#'
#' # Temporal filtering: past year with DuckDuckGo time filter
#' recent_news <- asa_enumerate(
#'   query = "Find AI research breakthroughs",
#'   temporal = list(
#'     time_filter = "y"  # past year
#'   )
#' )
#'
#' # Strict temporal filtering with Wayback Machine
#' historical <- asa_enumerate(
#'   query = "Find Fortune 500 companies",
#'   temporal = list(
#'     before = "2015-01-01",
#'     strictness = "strict",
#'     use_wayback = TRUE
#'   )
#' )
#' }
#'
#' @seealso \code{\link{run_task}}, \code{\link{initialize_agent}}
#'
#' @export
asa_enumerate <- function(query = NULL,
                         schema = NULL,
                         output = c("data.frame", "csv", "json"),
                         config = NULL,
                         workers = NULL,
                         max_rounds = NULL,
                         budget = list(queries = 50L, tokens = 200000L, time_sec = 300L),
                         stop_policy = list(target_items = NULL, plateau_rounds = 2L,
                                           novelty_min = 0.05, novelty_window = 20L),
                         sources = list(web = TRUE, wikipedia = TRUE, wikidata = TRUE),
                         allow_read_webpages = NULL,
                         webpage_relevance_mode = NULL,
                         webpage_embedding_provider = NULL,
                         webpage_embedding_model = NULL,
                         temporal = NULL,
                         pagination = TRUE,
                         progress = TRUE,
                         include_provenance = FALSE,
                         checkpoint = TRUE,
                         checkpoint_dir = tempdir(),
                         resume_from = NULL,
                         agent = NULL,
                         backend = NULL,
                         model = NULL,
                         conda_env = NULL,
                         verbose = TRUE) {

  .validate_asa_config(config)

  output <- match.arg(output)

  if (!is.null(resume_from)) {
    if (!file.exists(resume_from)) {
      stop("`resume_from` file not found: ", resume_from, call. = FALSE)
    }
    .warn_resume_research_ignored_args(match.call(expand.dots = FALSE))
    return(.resume_research(
      checkpoint_file = resume_from,
      output = output,
      progress = progress,
      include_provenance = include_provenance,
      verbose = verbose
    ))
  }

  runtime_inputs <- .resolve_runtime_inputs(
    config = config,
    agent = agent,
    temporal = temporal,
    allow_read_webpages = allow_read_webpages,
    webpage_relevance_mode = webpage_relevance_mode,
    webpage_embedding_provider = webpage_embedding_provider,
    webpage_embedding_model = webpage_embedding_model
  )
  runtime <- runtime_inputs$runtime
  temporal <- runtime_inputs$temporal
  allow_rw <- runtime_inputs$allow_rw

  # Apply defaults from constants
  if (is.null(workers)) {
    workers <- if (!is.null(config) && !is.null(config$workers)) config$workers else .get_default_workers()
  }
  max_rounds <- max_rounds %||% ASA_DEFAULT_MAX_ROUNDS
  backend <- backend %||% if (!is.null(config)) config$backend else .get_default_backend()
  model <- model %||% if (!is.null(config)) config$model else .get_default_model_for_backend(backend)
  conda_env <- conda_env %||% if (!is.null(config)) {
    config$conda_env
  } else if (!is.null(agent) && inherits(agent, "asa_agent")) {
    agent$config$conda_env %||% .get_default_conda_env()
  } else {
    .get_default_conda_env()
  }

  # Validate inputs
  .validate_research_inputs(
    query = query,
    schema = schema,
    output = output,
    workers = workers,
    max_rounds = max_rounds,
    budget = budget,
    stop_policy = stop_policy,
    sources = sources,
    allow_read_webpages = allow_read_webpages,
    webpage_relevance_mode = webpage_relevance_mode,
    webpage_embedding_provider = webpage_embedding_provider,
    webpage_embedding_model = webpage_embedding_model,
    checkpoint_dir = checkpoint_dir,
    resume_from = resume_from
  )

  # Normalize schema (no Python required)
  schema_dict <- .normalize_schema(schema, query, verbose)

  # Create research config (validates temporal before any Python initialization)
  config_dict <- .create_research_config(
    workers = workers,
    max_rounds = max_rounds,
    budget = budget,
    stop_policy = stop_policy,
    sources = sources,
    allow_read_webpages = if (identical(allow_rw, "auto")) "auto" else isTRUE(allow_rw),
    temporal = temporal,
    wikidata_template_path = if (is.list(runtime$config_search)) runtime$config_search$wikidata_template_path else NULL
  )

  # Ensure the runtime graph invoke/stream config carries recursion_limit so
  # RemainingSteps-based stop guards work in research graphs.
  recursion_config <- NULL
  if (!is.null(config) && inherits(config, "asa_config")) {
    recursion_config <- config
  } else if (!is.null(agent) && inherits(agent, "asa_agent")) {
    recursion_config <- agent$config
  }
  use_memory_folding <- TRUE
  if (!is.null(recursion_config) && !is.null(recursion_config$memory_folding)) {
    use_memory_folding <- isTRUE(recursion_config$memory_folding)
  }
  config_dict$recursion_limit <- .resolve_effective_recursion_limit(
    config = recursion_config,
    use_memory_folding = use_memory_folding
  )
  if (!is.null(config_dict$recursion_limit) &&
      is.finite(as.numeric(config_dict$recursion_limit)) &&
      as.integer(config_dict$recursion_limit) < 4L) {
    .stop_validation(
      "recursion_limit",
      "be >= 4 for research graphs",
      actual = config_dict$recursion_limit,
      fix = "Use recursion_limit = 4 or higher in research workflows"
    )
  }

  agent_boot <- .ensure_research_agent(
    agent = agent,
    config = config,
    backend = backend,
    model = model,
    conda_env = conda_env,
    verbose = verbose
  )
  agent <- agent_boot$agent
  agent_config <- agent_boot$config

  # Import Python modules
  .import_research_modules()

  # Create checkpoint file path
  checkpoint_file <- .derive_research_checkpoint_file(
    checkpoint = checkpoint,
    checkpoint_dir = checkpoint_dir,
    query = query
  )

  # Create research graph
  if (verbose) message("Creating research graph...")
  graph <- .create_research_graph(agent, config_dict)

  # Execute research
  if (verbose) message("Executing research...")
  start_time <- Sys.time()

  # Enable/disable webpage reading during this call (tool-level gating).
  conda_env_used <- conda_env %||% .get_default_conda_env()
  result <- .with_runtime_wrappers(
    runtime = runtime,
    conda_env = conda_env_used,
    agent = agent,
    fn = function() {
      .run_research_streaming(
        graph = graph,
        query = query,
        schema_dict = schema_dict,
        config_dict = config_dict,
        checkpoint_file = checkpoint_file,
        agent_config = agent_config,
        progress = progress,
        verbose = verbose
      )
    }
  )

  elapsed_total <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))

  research_result <- .materialize_research_result(
    result = result,
    schema_dict = schema_dict,
    include_provenance = include_provenance,
    checkpoint_file = checkpoint_file,
    query = query,
    elapsed_total = elapsed_total,
    extra_metrics = list(
      resumed_from_checkpoint = FALSE,
      checkpoint_version = if (!is.null(checkpoint_file)) 2L else NA_integer_,
      checkpoint_round = .as_scalar_int(result$metrics$round_number %||% NA_integer_),
      thread_id = result$thread_id %||% NA_character_,
      active_elapsed_sec = .checkpoint_active_elapsed_sec(result$metrics %||% list()),
      checkpoint_updated_at = if (!is.null(checkpoint_file)) Sys.time() else NA
    )
  )

  research_result <- .apply_enumerate_output(research_result, output = output, verbose = verbose)

  if (verbose) {
    message("Research complete!")
    message("  Status: ", research_result$status)
    message("  Items found: ", nrow(research_result$data))
    message("  Stop reason: ", research_result$stop_reason %||% "N/A")
  }

  research_result
}


# ============================================================================
# Internal Helper Functions
# ============================================================================

#' Import Research Python Modules
#' @keywords internal
.import_research_modules <- function() {
  .import_python_module(
    module_name = "workflows.research_graph_workflow",
    env_name = "research_graph"
  )
  .import_python_module(
    module_name = "tools.entity_wikidata_tool",
    env_name = "wikidata_tool"
  )
  invisible(NULL)
}


#' Normalize Schema Input
#' @keywords internal
.normalize_schema <- function(schema, query, verbose) {
  if (is.null(schema) || identical(schema, "auto")) {
    if (verbose) message("  Using auto schema detection...")

    schema_out <- list(name = "character")
    q <- tolower(query %||% "")

    add_field <- function(field, type = "character") {
      if (is.null(schema_out[[field]])) {
        schema_out[[field]] <<- type
      }
    }

    # Keyword-based heuristics
    if (grepl("\\bstate\\b", q)) add_field("state")
    if (grepl("\\bparty\\b", q)) add_field("party")
    if (grepl("\\bdistrict\\b", q)) add_field("district")
    if (grepl("term\\s*end", q)) add_field("term_end")
    if (grepl("term\\s*start", q)) add_field("term_start")
    if (grepl("\\bcapital\\b", q)) add_field("capital")
    if (grepl("\\bpopulation\\b", q)) add_field("population", "numeric")
    if (grepl("\\barea\\b", q)) add_field("area", "numeric")
    if (grepl("\\biso\\s*3\\b|\\biso3\\b", q)) add_field("iso3")
    if (grepl("\\bwebsite\\b|\\burl\\b", q)) add_field("website")

    # Parse "with ..." clause (e.g., "with state, party, and term end date")
    with_match <- regmatches(q, regexec("\\bwith\\b\\s+(.*)$", q, perl = TRUE))[[1]]
    if (length(with_match) > 1 && nzchar(with_match[2])) {
      fields_str <- gsub("[\\.;].*$", "", with_match[2])
      fields_str <- gsub("\\band\\b", ",", fields_str)
      candidates <- trimws(strsplit(fields_str, ",", fixed = TRUE)[[1]])
      candidates <- candidates[nzchar(candidates)]

      normalize_name <- function(x) {
        x <- gsub("[^a-z0-9\\s_]+", "", x)
        x <- trimws(x)
        x <- gsub("\\s+", "_", x)
        x
      }

      for (cand in candidates) {
        nm <- normalize_name(cand)
        nm <- switch(
          nm,
          term_end_date = "term_end",
          term_start_date = "term_start",
          nm
        )
        if (!nzchar(nm) || nm %in% names(schema_out)) next
        add_field(nm, "character")
      }
    }

    return(schema_out)
  }

  if (is.character(schema) && !is.null(names(schema))) {
    # Convert named vector to list
    return(as.list(schema))
  }

  if (is.list(schema)) {
    return(schema)
  }

  stop("schema must be a named character vector, list, or 'auto'", call. = FALSE)
}


#' Create Research Configuration
#' @keywords internal
.create_research_config <- function(workers, max_rounds, budget, stop_policy, sources,
                                    allow_read_webpages = FALSE,
                                    max_tool_calls_per_round = NULL,
                                    temporal = NULL,
                                    wikidata_template_path = NULL) {
  config <- list(
    max_workers = as.integer(workers),  # Python side still expects max_workers
    max_rounds = as.integer(max_rounds),
    budget_queries = as.integer(budget$queries %||% 50L),
    budget_tokens = as.integer(budget$tokens %||% 200000L),
    budget_time_sec = as.integer(budget$time_sec %||% 300L),
    target_items = if (!is.null(stop_policy$target_items)) as.integer(stop_policy$target_items) else NULL,
    plateau_rounds = as.integer(stop_policy$plateau_rounds %||% 2L),
    novelty_min = as.numeric(stop_policy$novelty_min %||% 0.05),
    novelty_window = as.integer(stop_policy$novelty_window %||% 20L),
    use_wikidata = isTRUE(sources$wikidata),
    use_web = isTRUE(sources$web),
    use_wikipedia = isTRUE(sources$wikipedia),
    allow_read_webpages = if (identical(allow_read_webpages, "auto")) "auto" else isTRUE(allow_read_webpages),
    max_tool_calls_per_round = if (!is.null(max_tool_calls_per_round)) as.integer(max_tool_calls_per_round) else NULL,
    wikidata_template_path = if (!is.null(wikidata_template_path)) as.character(wikidata_template_path) else NULL
  )

  # Add temporal filtering parameters if provided
  if (!is.null(temporal)) {
    # Use centralized validation from validate.R
    .validate_temporal(temporal)

    # Extract validated parameters
    config$time_filter <- temporal$time_filter
    config$date_after <- temporal$after
    config$date_before <- temporal$before
    config$temporal_strictness <- temporal$strictness %||% "best_effort"
    config$use_wayback <- isTRUE(temporal$use_wayback)
  }

  config
}


#' Create Research Graph
#' @keywords internal
.create_research_graph <- function(agent, config_dict) {
  # Create Python config object with temporal parameters
  py_config <- asa_env$research_graph$ResearchConfig(
    max_workers = config_dict$max_workers,
    max_rounds = config_dict$max_rounds,
    budget_queries = config_dict$budget_queries,
    budget_tokens = config_dict$budget_tokens,
    budget_time_sec = config_dict$budget_time_sec,
    target_items = config_dict$target_items,
    plateau_rounds = config_dict$plateau_rounds,
    novelty_min = config_dict$novelty_min,
    novelty_window = config_dict$novelty_window,
    use_wikidata = config_dict$use_wikidata,
    use_web = config_dict$use_web,
    use_wikipedia = config_dict$use_wikipedia,
    allow_read_webpages = if (identical(config_dict$allow_read_webpages, "auto")) TRUE else (config_dict$allow_read_webpages %||% FALSE),
    max_tool_calls_per_round = config_dict$max_tool_calls_per_round,
    # Temporal filtering parameters
    time_filter = config_dict$time_filter,
    date_after = config_dict$date_after,
    date_before = config_dict$date_before,
    temporal_strictness = config_dict$temporal_strictness %||% "best_effort",
    use_wayback = config_dict$use_wayback %||% FALSE
  )

  # Create Wikidata tool if enabled (with temporal filtering if specified)
  wikidata_tool <- NULL
  if (config_dict$use_wikidata) {
    wikidata_tool <- asa_env$wikidata_tool$create_wikidata_tool(
      date_after = config_dict$date_after,
      date_before = config_dict$date_before,
      template_path = config_dict$wikidata_template_path
    )
  }

  llm <- if (!is.null(agent) && !is.null(agent$llm)) agent$llm else asa_env$llm
  tools <- if (!is.null(agent) && !is.null(agent$tools)) agent$tools else asa_env$tools

  # Create research graph
  asa_env$research_graph$create_research_graph(
    llm = llm,
    tools = tools,
    config = py_config,
    checkpointer = asa_env$MemorySaver(),
    wikidata_tool = wikidata_tool
  )
}

.warn_resume_research_ignored_args <- function(call_expr) {
  arg_names <- names(as.list(call_expr))[-1]
  ignored <- intersect(
    arg_names,
    c(
      "query", "schema", "config", "workers", "max_rounds", "budget",
      "stop_policy", "sources", "temporal", "agent", "backend", "model",
      "conda_env", "allow_read_webpages", "webpage_relevance_mode",
      "webpage_embedding_provider", "webpage_embedding_model", "pagination"
    )
  )
  if (length(ignored) > 0) {
    warning(
      "Ignoring execution-shaping arguments when `resume_from` is provided: ",
      paste(sort(unique(ignored)), collapse = ", "),
      call. = FALSE
    )
  }
}

.derive_research_checkpoint_file <- function(checkpoint, checkpoint_dir, query) {
  if (!isTRUE(checkpoint)) {
    return(NULL)
  }
  query_hash <- substr(digest::digest(query), 1, 12)
  file.path(checkpoint_dir, paste0("asa_enumerate_", query_hash, ".rds"))
}

.agent_config_from_agent <- function(agent) {
  if (is.null(agent) || !inherits(agent, "asa_agent")) {
    return(NULL)
  }

  asa_config(
    backend = agent$backend,
    model = agent$model,
    conda_env = agent$config$conda_env,
    proxy = agent$config$proxy,
    use_browser = agent$config$use_browser %||% ASA_DEFAULT_USE_BROWSER,
    workers = .get_default_workers(),
    timeout = agent$config$timeout,
    rate_limit = agent$config$rate_limit,
    memory_folding = agent$config$use_memory_folding %||% agent$config$memory_folding,
    memory_threshold = agent$config$memory_threshold,
    memory_keep_recent = agent$config$memory_keep_recent,
    use_observational_memory = agent$config$use_observational_memory %||% FALSE,
    om_observation_token_budget = agent$config$om_observation_token_budget %||% ASA_DEFAULT_OM_OBSERVATION_TOKENS,
    om_reflection_token_budget = agent$config$om_reflection_token_budget %||% ASA_DEFAULT_OM_REFLECTION_TOKENS,
    om_buffer_tokens = agent$config$om_buffer_tokens %||% ASA_DEFAULT_OM_BUFFER_TOKENS,
    om_buffer_activation = agent$config$om_buffer_activation %||% ASA_DEFAULT_OM_BUFFER_ACTIVATION,
    om_block_after = agent$config$om_block_after %||% ASA_DEFAULT_OM_BLOCK_AFTER,
    om_async_prebuffer = agent$config$om_async_prebuffer %||% ASA_DEFAULT_OM_ASYNC_PREBUFFER,
    om_cross_thread_memory = agent$config$om_cross_thread_memory %||% FALSE,
    recursion_limit = agent$config$recursion_limit,
    search = agent$config$search,
    tor = agent$config$tor
  )
}

.ensure_research_agent <- function(agent = NULL,
                                   config = NULL,
                                   backend = NULL,
                                   model = NULL,
                                   conda_env = NULL,
                                   verbose = TRUE) {
  if (!is.null(agent) && inherits(agent, "asa_agent")) {
    return(list(
      agent = agent,
      config = config %||% .agent_config_from_agent(agent)
    ))
  }

  config_for_agent <- if (!is.null(config)) config else asa_config(
    backend = backend,
    model = model,
    conda_env = conda_env
  )

  config_for_agent$backend <- backend %||% config_for_agent$backend
  config_for_agent$model <- model %||% config_for_agent$model
  config_for_agent$conda_env <- conda_env %||% config_for_agent$conda_env

  current <- if (.is_initialized()) get_agent() else NULL
  if (!is.null(current) && .agent_matches_config(current, config_for_agent)) {
    return(list(agent = current, config = config_for_agent))
  }

  if (verbose) message("Initializing agent...")
  initialized <- initialize_agent(
    backend = config_for_agent$backend,
    model = config_for_agent$model,
    conda_env = config_for_agent$conda_env,
    proxy = config_for_agent$proxy,
    use_browser = config_for_agent$use_browser %||% ASA_DEFAULT_USE_BROWSER,
    search = config_for_agent$search,
    use_memory_folding = config_for_agent$memory_folding,
    memory_threshold = config_for_agent$memory_threshold,
    memory_keep_recent = config_for_agent$memory_keep_recent,
    use_observational_memory = config_for_agent$use_observational_memory %||% FALSE,
    om_observation_token_budget = config_for_agent$om_observation_token_budget %||% ASA_DEFAULT_OM_OBSERVATION_TOKENS,
    om_reflection_token_budget = config_for_agent$om_reflection_token_budget %||% ASA_DEFAULT_OM_REFLECTION_TOKENS,
    om_buffer_tokens = config_for_agent$om_buffer_tokens %||% ASA_DEFAULT_OM_BUFFER_TOKENS,
    om_buffer_activation = config_for_agent$om_buffer_activation %||% ASA_DEFAULT_OM_BUFFER_ACTIVATION,
    om_block_after = config_for_agent$om_block_after %||% ASA_DEFAULT_OM_BLOCK_AFTER,
    om_async_prebuffer = config_for_agent$om_async_prebuffer %||% ASA_DEFAULT_OM_ASYNC_PREBUFFER,
    om_cross_thread_memory = config_for_agent$om_cross_thread_memory %||% FALSE,
    rate_limit = config_for_agent$rate_limit,
    timeout = config_for_agent$timeout,
    tor = config_for_agent$tor,
    recursion_limit = config_for_agent$recursion_limit,
    verbose = verbose
  )

  list(agent = initialized, config = config_for_agent)
}

.build_research_temporal_from_config <- function(config_dict) {
  if (is.null(config_dict)) {
    return(NULL)
  }
  keys <- c("time_filter", "date_after", "date_before", "temporal_strictness", "use_wayback")
  if (!any(vapply(keys, function(k) !is.null(config_dict[[k]]), logical(1)))) {
    return(NULL)
  }

  temporal_options(
    time_filter = config_dict$time_filter %||% NULL,
    after = config_dict$date_after %||% NULL,
    before = config_dict$date_before %||% NULL,
    strictness = config_dict$temporal_strictness %||% "best_effort",
    use_wayback = isTRUE(config_dict$use_wayback)
  )
}

.checkpoint_active_elapsed_sec <- function(metrics = NULL) {
  metrics <- metrics %||% list()
  active <- suppressWarnings(as.numeric(metrics$active_elapsed_sec %||% NA_real_))
  if (is.finite(active)) {
    return(active)
  }
  fallback <- suppressWarnings(as.numeric(metrics$time_elapsed %||% NA_real_))
  if (is.finite(fallback)) {
    return(fallback)
  }
  NA_real_
}

.build_research_checkpoint_payload <- function(query,
                                               schema_dict,
                                               config_dict,
                                               agent_config,
                                               state_snapshot,
                                               thread_id,
                                               status,
                                               resume_stage,
                                               stop_reason = NULL,
                                               updated_at = Sys.time()) {
  round_number <- .as_scalar_int(state_snapshot$round_number %||% NA_integer_)
  items_found <- length(state_snapshot$results %||% list())
  active_elapsed_sec <- suppressWarnings(as.numeric(state_snapshot$elapsed_carry_sec %||% NA_real_))

  list(
    version = "2.0",
    checkpoint_type = "research_state",
    query = query,
    schema = schema_dict,
    config = config_dict,
    agent_config = agent_config,
    thread_id = thread_id,
    resume_stage = resume_stage,
    status = status,
    stop_reason = stop_reason %||% state_snapshot$stop_reason %||% NULL,
    state_snapshot = state_snapshot,
    metrics = list(
      round_number = round_number,
      items_found = items_found,
      active_elapsed_sec = active_elapsed_sec,
      thread_id = thread_id
    ),
    result_summary = list(
      status = status,
      stop_reason = stop_reason %||% state_snapshot$stop_reason %||% NULL,
      round_number = round_number,
      items_found = items_found
    ),
    updated_at = updated_at
  )
}

.materialize_research_result <- function(result,
                                         schema_dict,
                                         include_provenance,
                                         checkpoint_file,
                                         query,
                                         elapsed_total = NULL,
                                         extra_metrics = list()) {
  if (is.null(result)) {
    result <- list(
      results = list(),
      provenance = list(),
      metrics = list(),
      status = "failed",
      stop_reason = "missing_result",
      errors = list(list(stage = "execution", error = "Missing research result")),
      plan = list(),
      completion_gate = list()
    )
  }

  processed <- .process_research_results(result, schema_dict, include_provenance)
  verification_status <- .try_or(as.character(result$verification_status), character(0))
  verification_status <- if (length(verification_status) > 0 && nzchar(verification_status[[1]])) {
    verification_status[[1]]
  } else {
    NA_character_
  }
  status_out <- result$status %||% "unknown"
  if (!is.na(verification_status) && !verification_status %in% c("searching", "in_progress")) {
    status_out <- verification_status
  }
  completion_gate <- result$completion_gate %||% list()
  metrics_out <- c(
    result$metrics %||% list(),
    list(
      elapsed_total = elapsed_total %||% (.checkpoint_active_elapsed_sec(result$metrics %||% list()) / 60),
      verification_status = verification_status,
      completion_gate = completion_gate
    ),
    extra_metrics %||% list()
  )

  asa_enumerate_result(
    data = processed$data,
    status = status_out,
    stop_reason = result$stop_reason,
    metrics = metrics_out,
    provenance = if (include_provenance) processed$provenance else NULL,
    plan = result$plan,
    checkpoint_file = checkpoint_file,
    query = query,
    schema = schema_dict
  )
}

.apply_enumerate_output <- function(research_result, output = c("data.frame", "csv", "json"), verbose = TRUE) {
  output <- match.arg(output)
  if (output == "csv") {
    csv_file <- tempfile(fileext = ".csv")
    utils::write.csv(research_result$data, csv_file, row.names = FALSE)
    if (verbose) message("  CSV written to: ", csv_file)
    attr(research_result, "csv_file") <- csv_file
  } else if (output == "json") {
    research_result$json <- jsonlite::toJSON(research_result$data, pretty = TRUE, auto_unbox = TRUE)
  }
  research_result
}


#' Run Research (Non-Streaming)
#' @keywords internal
.run_research <- function(graph, query, schema_dict, config_dict) {
  asa_env$research_graph$run_research(
    graph = graph,
    query = query,
    schema = schema_dict,
    config_dict = config_dict
  )
}


#' Run Research via Streaming
#' @keywords internal
.run_research_streaming <- function(graph, query, schema_dict, config_dict,
                                    checkpoint_file = NULL, agent_config = NULL,
                                    progress = TRUE, verbose = TRUE,
                                    thread_id = NULL, state_snapshot = NULL) {
  final_result <- NULL

  tryCatch({
    events <- if (is.null(state_snapshot)) {
      asa_env$research_graph$stream_research(
        graph = graph,
        query = query,
        schema = schema_dict,
        config_dict = config_dict,
        thread_id = thread_id
      )
    } else {
      asa_env$research_graph$stream_research_from_state(
        graph = graph,
        state_snapshot = state_snapshot,
        query = query,
        schema = schema_dict,
        config_dict = config_dict,
        thread_id = thread_id
      )
    }

    event_iter <- reticulate::iterate(events)

    for (event in event_iter) {
      event <- .try_or(reticulate::py_to_r(event), event)
      event_type <- event$event_type

      if (event_type == "node_update") {
        if (isTRUE(progress) && verbose) {
          message(sprintf("  [%s] Items: %d, Elapsed: %.1fs",
                         event$node, event$items_found, event$elapsed))
        }
        if (!is.null(checkpoint_file) && event$node %in% c("planner", "stopper")) {
          checkpoint_payload <- .build_research_checkpoint_payload(
            query = query,
            schema_dict = schema_dict,
            config_dict = config_dict,
            agent_config = agent_config,
            state_snapshot = event$state_snapshot %||% list(),
            thread_id = event$thread_id %||% thread_id %||% NA_character_,
            status = event$state_snapshot$status %||% event$status %||% "searching",
            resume_stage = "searcher",
            stop_reason = event$state_snapshot$stop_reason %||% NULL
          )
          .save_checkpoint(checkpoint_payload = checkpoint_payload, checkpoint_file = checkpoint_file)
        }
      } else if (event_type == "complete") {
        final_result <- event$final_result
        if (!is.null(checkpoint_file)) {
          checkpoint_payload <- .build_research_checkpoint_payload(
            query = query,
            schema_dict = schema_dict,
            config_dict = config_dict,
            agent_config = agent_config,
            state_snapshot = event$state_snapshot %||% list(),
            thread_id = event$thread_id %||% thread_id %||% NA_character_,
            status = final_result$status %||% "complete",
            resume_stage = event$state_snapshot$resume_stage %||% "searcher",
            stop_reason = final_result$stop_reason %||% NULL
          )
          .save_checkpoint(checkpoint_payload = checkpoint_payload, checkpoint_file = checkpoint_file)
          if (verbose) message("  Checkpoint saved: ", checkpoint_file)
        }
        if (verbose) message("  Research complete")
      } else if (event_type == "error") {
        final_result <- event$final_result
        if (!is.null(checkpoint_file)) {
          checkpoint_payload <- .build_research_checkpoint_payload(
            query = query,
            schema_dict = schema_dict,
            config_dict = config_dict,
            agent_config = agent_config,
            state_snapshot = event$state_snapshot %||% list(),
            thread_id = event$thread_id %||% thread_id %||% NA_character_,
            status = final_result$status %||% "failed",
            resume_stage = event$state_snapshot$resume_stage %||% "searcher",
            stop_reason = final_result$stop_reason %||% event$error
          )
          .save_checkpoint(checkpoint_payload = checkpoint_payload, checkpoint_file = checkpoint_file)
          if (verbose) message("  Checkpoint saved: ", checkpoint_file)
        }
        if (verbose) message("  Error: ", event$error)
      }
    }

    if (!is.null(final_result)) {
      return(final_result)
    }

    list(
      results = list(),
      provenance = list(),
      metrics = list(),
      status = "failed",
      stop_reason = "no_result_from_stream",
      errors = list(list(stage = "stream", error = "Stream completed without result")),
      plan = list()
    )

  }, error = function(e) {
    list(
      results = list(),
      provenance = list(),
      metrics = list(),
      status = "failed",
      stop_reason = conditionMessage(e),
      errors = list(list(stage = "execution", error = conditionMessage(e))),
      plan = list()
    )
  })
}


#' Process Research Results
#' @keywords internal
.process_research_results <- function(result, schema_dict, include_provenance) {
  results <- result$results %||% list()

  if (length(results) == 0) {
    # Return empty data.frame with schema columns
    empty_df <- data.frame(matrix(nrow = 0, ncol = length(schema_dict)))
    names(empty_df) <- names(schema_dict)
    return(list(data = empty_df, provenance = NULL))
  }

  # Convert Python results to R data.frame
  rows <- lapply(results, function(r) {
    fields <- r$fields %||% list()
    row <- as.list(fields)

    # Ensure all schema columns exist
    for (col in names(schema_dict)) {
      if (is.null(row[[col]])) {
        row[[col]] <- NA
      }
    }

    row
  })

  # Build data.frame
  df <- do.call(rbind, lapply(rows, function(r) as.data.frame(r, stringsAsFactors = FALSE)))

  # Handle provenance
  provenance_df <- NULL
  if (include_provenance) {
    provenance_items <- .try_or(reticulate::py_to_r(result$provenance), result$provenance)
    if (is.list(provenance_items) && length(provenance_items) > 0) {
      prov_rows <- lapply(seq_along(provenance_items), function(i) {
        p <- provenance_items[[i]]
        data.frame(
          row_id = p$row_id %||% i,
          source_url = p$source_url %||% "",
          confidence = p$confidence %||% 0.5,
          worker_id = p$worker_id %||% "unknown",
          timestamp = p$extraction_timestamp %||% p$timestamp %||% NA,
          stringsAsFactors = FALSE
        )
      })
      provenance_df <- do.call(rbind, prov_rows)
    } else {
      prov_rows <- lapply(seq_along(results), function(i) {
        r <- results[[i]]
        data.frame(
          row_id = r$row_id %||% i,
          source_url = r$source_url %||% "",
          confidence = r$confidence %||% 0.5,
          worker_id = r$worker_id %||% "unknown",
          timestamp = r$extraction_timestamp %||% NA,
          stringsAsFactors = FALSE
        )
      })
      provenance_df <- do.call(rbind, prov_rows)
    }
  }

  list(data = df, provenance = provenance_df)
}


#' Save Checkpoint
#' @keywords internal
.save_checkpoint <- function(result = NULL,
                             query = NULL,
                             schema_dict = NULL,
                             config_dict = NULL,
                             checkpoint_file,
                             checkpoint_payload = NULL) {
  checkpoint_data <- checkpoint_payload %||% list(
    result = result,
    query = query,
    schema = schema_dict,
    config = config_dict,
    timestamp = Sys.time(),
    version = "1.0"
  )
  saveRDS(checkpoint_data, checkpoint_file)
  invisible(checkpoint_file)
}


#' Resume Research from Checkpoint
#' @keywords internal
.resume_research <- function(checkpoint_file,
                             output = c("data.frame", "csv", "json"),
                             progress = TRUE,
                             include_provenance = FALSE,
                             verbose = TRUE) {
  if (!file.exists(checkpoint_file)) {
    stop("`resume_from` file not found: ", checkpoint_file, call. = FALSE)
  }

  output <- match.arg(output)

  if (verbose) message("Resuming from checkpoint: ", checkpoint_file)

  checkpoint <- readRDS(checkpoint_file)
  version <- as.character(checkpoint$version %||% "1.0")

  if (!startsWith(version, "2")) {
    if (is.null(checkpoint$result) || is.null(checkpoint$query)) {
      stop("Invalid checkpoint file structure", call. = FALSE)
    }
    warning(
      "Legacy checkpoint format does not support continuation; returning saved result.",
      call. = FALSE
    )
    legacy_result <- .materialize_research_result(
      result = checkpoint$result,
      schema_dict = checkpoint$schema,
      include_provenance = include_provenance,
      checkpoint_file = checkpoint_file,
      query = checkpoint$query,
      elapsed_total = (.checkpoint_active_elapsed_sec(checkpoint$result$metrics %||% list()) / 60),
      extra_metrics = list(
        resumed_from_checkpoint = TRUE,
        checkpoint_version = 1L,
        checkpoint_round = .as_scalar_int(checkpoint$result$metrics$round_number %||% NA_integer_),
        thread_id = NA_character_,
        active_elapsed_sec = .checkpoint_active_elapsed_sec(checkpoint$result$metrics %||% list()),
        checkpoint_updated_at = checkpoint$timestamp %||% NA
      )
    )
    return(.apply_enumerate_output(legacy_result, output = output, verbose = verbose))
  }

  if (is.null(checkpoint$query) || is.null(checkpoint$schema) || is.null(checkpoint$config)) {
    stop("Invalid v2 checkpoint file structure", call. = FALSE)
  }

  terminal_status <- as.character(checkpoint$status %||% "")
  if (terminal_status %in% c("complete", "failed")) {
    terminal_result <- list(
      results = checkpoint$state_snapshot$results %||% list(),
      provenance = list(),
      metrics = c(
        checkpoint$metrics %||% list(),
        list(
          active_elapsed_sec = checkpoint$metrics$active_elapsed_sec %||%
            checkpoint$state_snapshot$elapsed_carry_sec %||% NA_real_
        )
      ),
      status = terminal_status,
      stop_reason = checkpoint$stop_reason %||% checkpoint$state_snapshot$stop_reason %||% NULL,
      verification_status = checkpoint$state_snapshot$completion_gate$completion_status %||% NULL,
      completion_gate = checkpoint$state_snapshot$completion_gate %||% list(),
      errors = checkpoint$state_snapshot$errors %||% list(),
      plan = checkpoint$state_snapshot$plan %||% list(),
      thread_id = checkpoint$thread_id %||% NA_character_
    )
    terminal_out <- .materialize_research_result(
      result = terminal_result,
      schema_dict = checkpoint$schema,
      include_provenance = include_provenance,
      checkpoint_file = checkpoint_file,
      query = checkpoint$query,
      elapsed_total = (checkpoint$metrics$active_elapsed_sec %||%
        checkpoint$state_snapshot$elapsed_carry_sec %||% NA_real_) / 60,
      extra_metrics = list(
        resumed_from_checkpoint = TRUE,
        checkpoint_version = 2L,
        checkpoint_round = .as_scalar_int(checkpoint$metrics$round_number %||% NA_integer_),
        thread_id = checkpoint$thread_id %||% NA_character_,
        active_elapsed_sec = checkpoint$metrics$active_elapsed_sec %||%
          checkpoint$state_snapshot$elapsed_carry_sec %||% NA_real_,
        checkpoint_updated_at = checkpoint$updated_at %||% NA
      )
    )
    return(.apply_enumerate_output(terminal_out, output = output, verbose = verbose))
  }

  checkpoint_agent_config <- checkpoint$agent_config %||% asa_config(
    backend = .get_default_backend(),
    model = .get_default_model()
  )
  agent_boot <- .ensure_research_agent(
    agent = NULL,
    config = checkpoint_agent_config,
    backend = checkpoint_agent_config$backend %||% NULL,
    model = checkpoint_agent_config$model %||% NULL,
    conda_env = checkpoint_agent_config$conda_env %||% NULL,
    verbose = verbose
  )
  agent <- agent_boot$agent
  agent_config <- agent_boot$config

  .import_research_modules()

  runtime_inputs <- .resolve_runtime_inputs(
    config = agent_config,
    agent = agent,
    temporal = .build_research_temporal_from_config(checkpoint$config),
    allow_read_webpages = checkpoint$config$allow_read_webpages %||% NULL
  )

  if (verbose) message("Creating research graph...")
  graph <- .create_research_graph(agent, checkpoint$config)
  started <- Sys.time()

  result <- .with_runtime_wrappers(
    runtime = runtime_inputs$runtime,
    conda_env = agent_config$conda_env %||% .get_default_conda_env(),
    agent = agent,
    fn = function() {
      .run_research_streaming(
        graph = graph,
        query = checkpoint$query,
        schema_dict = checkpoint$schema,
        config_dict = checkpoint$config,
        checkpoint_file = checkpoint_file,
        agent_config = agent_config,
        progress = progress,
        verbose = verbose,
        thread_id = checkpoint$thread_id %||% NULL,
        state_snapshot = checkpoint$state_snapshot
      )
    }
  )

  resumed <- .materialize_research_result(
    result = result,
    schema_dict = checkpoint$schema,
    include_provenance = include_provenance,
    checkpoint_file = checkpoint_file,
    query = checkpoint$query,
    elapsed_total = as.numeric(difftime(Sys.time(), started, units = "mins")),
    extra_metrics = list(
      resumed_from_checkpoint = TRUE,
      checkpoint_version = 2L,
      checkpoint_round = .as_scalar_int(checkpoint$metrics$round_number %||% NA_integer_),
      thread_id = result$thread_id %||% checkpoint$thread_id %||% NA_character_,
      active_elapsed_sec = .checkpoint_active_elapsed_sec(result$metrics %||% list()),
      checkpoint_updated_at = checkpoint$updated_at %||% NA
    )
  )

  .apply_enumerate_output(resumed, output = output, verbose = verbose)
}


#' Validate Research Inputs
#' @keywords internal
.validate_research_inputs <- function(query, schema, output, workers, max_rounds,
                                      budget, stop_policy, sources,
                                      allow_read_webpages = FALSE,
                                      webpage_relevance_mode = NULL,
                                      webpage_embedding_provider = NULL,
                                      webpage_embedding_model = NULL,
                                      checkpoint_dir, resume_from) {
  # Query validation
  .validate_string(query, "query")

  # Numeric parameters
  if (!is.null(workers) && (!is.numeric(workers) || workers < 1 || workers > 10)) {
    stop("`workers` must be a number between 1 and 10", call. = FALSE)
  }

  if (!is.null(max_rounds) && (!is.numeric(max_rounds) || max_rounds < 1 || max_rounds > 100)) {
    stop("`max_rounds` must be a number between 1 and 100", call. = FALSE)
  }

  # Budget validation
  if (!is.list(budget)) {
    stop("`budget` must be a list with keys: queries, tokens, time_sec", call. = FALSE)
  }

  # Stop policy validation
  if (!is.list(stop_policy)) {
    stop("`stop_policy` must be a list", call. = FALSE)
  }

  # Sources validation
  if (!is.list(sources)) {
    stop("`sources` must be a list with keys: web, wikipedia, wikidata", call. = FALSE)
  }

  # Optional capability flag: TRUE, FALSE, "auto", or NULL
  if (!is.null(allow_read_webpages)) {
    if (!identical(allow_read_webpages, "auto")) {
      .validate_logical(allow_read_webpages, "allow_read_webpages")
    }
  }

  # Webpage reader options (optional)
  if (!is.null(webpage_relevance_mode)) {
    .validate_choice(webpage_relevance_mode, "webpage_relevance_mode",
                     c("auto", "lexical", "embeddings"))
  }
  if (!is.null(webpage_embedding_provider)) {
    .validate_choice(webpage_embedding_provider, "webpage_embedding_provider",
                     c("auto", "openai", "sentence_transformers"))
  }
  if (!is.null(webpage_embedding_model)) {
    .validate_string(webpage_embedding_model, "webpage_embedding_model")
  }

  # Checkpoint directory validation
  if (!is.null(checkpoint_dir) && !dir.exists(checkpoint_dir)) {
    stop("`checkpoint_dir` does not exist: ", checkpoint_dir, call. = FALSE)
  }

  # Resume file validation
  if (!is.null(resume_from) && !file.exists(resume_from)) {
    stop("`resume_from` file not found: ", resume_from, call. = FALSE)
  }

  invisible(TRUE)
}
