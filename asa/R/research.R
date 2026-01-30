#' Multi-Agent Research for Open-Ended Queries
#'
#' Performs intelligent open-ended research tasks using multi-agent orchestration.
#' Decomposes complex queries into sub-tasks, executes parallel searches, and
#' aggregates results into structured output (data.frame, CSV, or JSON).
#'
#' @param query Character string describing the research goal.
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
#' @param checkpoint Enable auto-save after each round (default: TRUE).
#' @param checkpoint_dir Directory for checkpoint files (default: tempdir()).
#' @param resume_from Path to checkpoint file to resume from (default: NULL).
#' @param agent An initialized \code{asa_agent} object. If NULL, uses the current
#'   agent or creates a new one with specified backend/model.
#' @param backend LLM backend if creating new agent: "openai", "groq", "xai", "openrouter".
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
#' With checkpoint=TRUE, state is saved after each round. If interrupted,
#' use resume_from to continue from the last checkpoint:
#' \preformatted{
#' result <- asa_enumerate(query, resume_from = "/path/to/checkpoint.rds")
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
asa_enumerate <- function(query,
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

  if (!is.null(config) && !inherits(config, "asa_config")) {
    stop("`config` must be an asa_config object or NULL", call. = FALSE)
  }

  # Pull defaults from config where appropriate (explicit args win)
  config_search <- NULL
  if (!is.null(config)) {
    config_search <- config$search
  }

  if (is.null(temporal) && !is.null(config) && !is.null(config$temporal)) {
    temporal <- config$temporal
  }

  # Normalize temporal to list for internal validation
  if (inherits(temporal, "asa_temporal")) {
    temporal <- as.list(temporal)
  }

  # Resolve webpage reader settings from config$search when not specified directly
  allow_rw <- allow_read_webpages
  if (is.null(allow_rw) && is.list(config_search) && !is.null(config_search$allow_read_webpages)) {
    allow_rw <- config_search$allow_read_webpages
  }
  relevance_mode <- webpage_relevance_mode
  if (is.null(relevance_mode) && is.list(config_search) && !is.null(config_search$webpage_relevance_mode)) {
    relevance_mode <- config_search$webpage_relevance_mode
  }
  embedding_provider <- webpage_embedding_provider
  if (is.null(embedding_provider) && is.list(config_search) && !is.null(config_search$webpage_embedding_provider)) {
    embedding_provider <- config_search$webpage_embedding_provider
  }
  embedding_model <- webpage_embedding_model
  if (is.null(embedding_model) && is.list(config_search) && !is.null(config_search$webpage_embedding_model)) {
    embedding_model <- config_search$webpage_embedding_model
  }

  # Apply defaults from constants
  if (is.null(workers)) {
    workers <- if (!is.null(config) && !is.null(config$workers)) config$workers else .get_default_workers()
  }
  max_rounds <- max_rounds %||% ASA_DEFAULT_MAX_ROUNDS
  backend <- backend %||% if (!is.null(config)) config$backend else .get_default_backend()
  model <- model %||% if (!is.null(config)) config$model else .get_default_model()
  conda_env <- conda_env %||% if (!is.null(config)) config$conda_env else .get_default_conda_env()

  # Match output format
  output <- match.arg(output)

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

  # Handle resume from checkpoint
  if (!is.null(resume_from)) {
    return(.resume_research(resume_from, verbose))
  }

  # Normalize schema (no Python required)
  schema_dict <- .normalize_schema(schema, query, verbose)

  # Create research config (validates temporal before any Python initialization)
  config_dict <- .create_research_config(
    workers = workers,
    max_rounds = max_rounds,
    budget = budget,
    stop_policy = stop_policy,
    sources = sources,
    allow_read_webpages = isTRUE(allow_rw),
    temporal = temporal
  )

  # Ensure agent is initialized
  if (is.null(agent)) {
    config_for_agent <- if (!is.null(config)) config else asa_config(
      backend = backend,
      model = model,
      conda_env = conda_env
    )

    # Ensure overrides are reflected if config was provided
    config_for_agent$backend <- backend
    config_for_agent$model <- model
    config_for_agent$conda_env <- conda_env

    current <- if (.is_initialized()) get_agent() else NULL
    if (!is.null(current) && .agent_matches_config(current, config_for_agent)) {
      agent <- current
    } else {
      if (verbose) message("Initializing agent...")
      agent <- initialize_agent(
        backend = config_for_agent$backend,
        model = config_for_agent$model,
        conda_env = config_for_agent$conda_env,
        proxy = config_for_agent$proxy,
        use_memory_folding = config_for_agent$memory_folding,
        memory_threshold = config_for_agent$memory_threshold,
        memory_keep_recent = config_for_agent$memory_keep_recent,
        rate_limit = config_for_agent$rate_limit,
        timeout = config_for_agent$timeout,
        tor = config_for_agent$tor,
        verbose = verbose
      )
    }
  }

  # Import Python modules
  .import_research_modules()

  # Create checkpoint file path
  checkpoint_file <- NULL
  if (checkpoint) {
    query_hash <- substr(digest::digest(query), 1, 12)
    checkpoint_file <- file.path(checkpoint_dir, paste0("asa_enumerate_", query_hash, ".rds"))
  }

  # Create research graph
  if (verbose) message("Creating research graph...")
  graph <- .create_research_graph(agent, config_dict)

  # Execute research
  if (verbose) message("Executing research...")
  start_time <- Sys.time()

  # Enable/disable webpage reading during this call (tool-level gating).
  conda_env_used <- conda_env %||% .get_default_conda_env()
  result <- .with_search_config(config_search, conda_env = conda_env_used, function() {
    .with_webpage_reader_config(
      allow_rw,
      relevance_mode = relevance_mode,
      embedding_provider = embedding_provider,
      embedding_model = embedding_model,
      conda_env = conda_env_used,
      fn = function() {
      .with_temporal(temporal, function() {
    if (progress) {
      .run_research_with_progress(graph, query, schema_dict, config_dict,
                                  checkpoint_file, verbose)
    } else {
      .run_research(graph, query, schema_dict, config_dict)
    }
  }, agent = agent)
  })
  })

  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))

  # Process results
  if (verbose) message("Processing results...")
  processed <- .process_research_results(result, schema_dict, include_provenance)

  # Save final checkpoint
  if (checkpoint && !is.null(checkpoint_file)) {
    .save_checkpoint(result, query, schema_dict, config_dict, checkpoint_file)
    if (verbose) message("  Checkpoint saved: ", checkpoint_file)
  }

  # Build result object
  research_result <- asa_enumerate_result(
    data = processed$data,
    status = result$status %||% "unknown",
    stop_reason = result$stop_reason,
    metrics = c(result$metrics, elapsed_total = elapsed),
    provenance = if (include_provenance) processed$provenance else NULL,
    plan = result$plan,
    checkpoint_file = checkpoint_file,
    query = query,
    schema = schema_dict
  )

  # Output format conversion
  if (output == "csv") {
    csv_file <- tempfile(fileext = ".csv")
    utils::write.csv(research_result$data, csv_file, row.names = FALSE)
    if (verbose) message("  CSV written to: ", csv_file)
    attr(research_result, "csv_file") <- csv_file
  } else if (output == "json") {
    research_result$json <- jsonlite::toJSON(research_result$data, pretty = TRUE, auto_unbox = TRUE)
  }

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
  .import_python_module("research_graph")
  .import_python_module("wikidata_tool")
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
                                    temporal = NULL) {
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
    allow_read_webpages = isTRUE(allow_read_webpages)
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
    allow_read_webpages = config_dict$allow_read_webpages %||% FALSE,
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
      date_before = config_dict$date_before
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


#' Run Research with Progress Updates
#' @keywords internal
.run_research_with_progress <- function(graph, query, schema_dict, config_dict,
                                        checkpoint_file, verbose) {
  # Use streaming for progress updates
  # The stream now returns final_result in the complete/error event
  final_result <- NULL

  tryCatch({
    # Stream events
    events <- asa_env$research_graph$stream_research(
      graph = graph,
      query = query,
      schema = schema_dict,
      config_dict = config_dict
    )

    # Process events (Python generator)
    event_iter <- reticulate::iterate(events)

    for (event in event_iter) {
      event_type <- event$event_type

      if (event_type == "node_update") {
        if (verbose) {
          message(sprintf("  [%s] Items: %d, Elapsed: %.1fs",
                         event$node, event$items_found, event$elapsed))
        }
      } else if (event_type == "complete") {
        # Capture final result from streaming (avoids double execution)
        final_result <- event$final_result
        if (verbose) message("  Research complete")
      } else if (event_type == "error") {
        # Capture error result with any partial data
        final_result <- event$final_result
        if (verbose) message("  Error: ", event$error)
      }
    }

    # Return the captured result from streaming
    if (!is.null(final_result)) {
      return(final_result)
    }

    # Fallback: empty result if streaming yielded nothing
    return(list(
      results = list(),
      provenance = list(),
      metrics = list(),
      status = "failed",
      stop_reason = "no_result_from_stream",
      errors = list(list(stage = "stream", error = "Stream completed without result")),
      plan = list()
    ))

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
    prov_rows <- lapply(seq_along(results), function(i) {
      r <- results[[i]]
      data.frame(
        row_id = i,
        source_url = r$source_url %||% "",
        confidence = r$confidence %||% 0.5,
        worker_id = r$worker_id %||% "unknown",
        timestamp = r$extraction_timestamp %||% NA,
        stringsAsFactors = FALSE
      )
    })
    provenance_df <- do.call(rbind, prov_rows)
  }

  list(data = df, provenance = provenance_df)
}


#' Save Checkpoint
#' @keywords internal
.save_checkpoint <- function(result, query, schema_dict, config_dict, checkpoint_file) {
  checkpoint_data <- list(
    result = result,
    query = query,
    schema = schema_dict,
    config = config_dict,
    timestamp = Sys.time(),
    version = "1.0"
  )
  saveRDS(checkpoint_data, checkpoint_file)
}


#' Resume Research from Checkpoint
#' @keywords internal
.resume_research <- function(checkpoint_file, verbose) {
  if (!file.exists(checkpoint_file)) {
    stop("Checkpoint file not found: ", checkpoint_file, call. = FALSE)
  }

  if (verbose) message("Resuming from checkpoint: ", checkpoint_file)

  checkpoint <- readRDS(checkpoint_file)

  # Validate checkpoint structure
  if (is.null(checkpoint$result) || is.null(checkpoint$query)) {
    stop("Invalid checkpoint file structure", call. = FALSE)
  }

  # Process the saved results
  processed <- .process_research_results(
    checkpoint$result,
    checkpoint$schema,
    include_provenance = TRUE
  )

  # Create result object
  asa_enumerate_result(
    data = processed$data,
    status = checkpoint$result$status %||% "resumed",
    stop_reason = checkpoint$result$stop_reason,
    metrics = checkpoint$result$metrics,
    provenance = processed$provenance,
    plan = checkpoint$result$plan,
    checkpoint_file = checkpoint_file,
    query = checkpoint$query,
    schema = checkpoint$schema
  )
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
  if (!is.character(query) || length(query) != 1 || is.na(query) || query == "") {
    stop("`query` must be a non-empty character string", call. = FALSE)
  }

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

  # Optional capability flag
  if (!is.null(allow_read_webpages)) {
    if (!is.logical(allow_read_webpages) || length(allow_read_webpages) != 1 || is.na(allow_read_webpages)) {
      stop("`allow_read_webpages` must be TRUE, FALSE, or NULL", call. = FALSE)
    }
  }

  # Webpage reader options (optional)
  if (!is.null(webpage_relevance_mode)) {
    if (!is.character(webpage_relevance_mode) || length(webpage_relevance_mode) != 1) {
      stop("`webpage_relevance_mode` must be a single string or NULL", call. = FALSE)
    }
    if (!webpage_relevance_mode %in% c("auto", "lexical", "embeddings")) {
      stop("`webpage_relevance_mode` must be one of: auto, lexical, embeddings", call. = FALSE)
    }
  }
  if (!is.null(webpage_embedding_provider)) {
    if (!is.character(webpage_embedding_provider) || length(webpage_embedding_provider) != 1) {
      stop("`webpage_embedding_provider` must be a single string or NULL", call. = FALSE)
    }
    if (!webpage_embedding_provider %in% c("auto", "openai", "sentence_transformers")) {
      stop("`webpage_embedding_provider` must be one of: auto, openai, sentence_transformers", call. = FALSE)
    }
  }
  if (!is.null(webpage_embedding_model)) {
    if (!is.character(webpage_embedding_model) || length(webpage_embedding_model) != 1 || is.na(webpage_embedding_model) || webpage_embedding_model == "") {
      stop("`webpage_embedding_model` must be a non-empty string or NULL", call. = FALSE)
    }
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
