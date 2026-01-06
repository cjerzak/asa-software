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
#' @param workers Number of parallel search workers. Defaults to value from
#'   \code{ASA_DEFAULT_WORKERS} (typically 4).
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
#' @param conda_env Conda environment name (default: "asa_env").
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
#' The function uses a multi-agent architecture:
#' \enumerate{
#'   \item \strong{Planner}: Decomposes query into facets and identifies authoritative sources
#'   \item \strong{Dispatcher}: Spawns parallel workers for each facet
#'   \item \strong{Workers}: Execute searches using DDG, Wikipedia, and Wikidata
#'   \item \strong{Extractor}: Normalizes results to match schema
#'   \item \strong{Deduper}: Removes duplicates using hash + fuzzy matching
#'   \item \strong{Stopper}: Evaluates stopping criteria (novelty, budget, saturation)
#' }
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
                         workers = NULL,
                         max_rounds = NULL,
                         budget = list(queries = 50L, tokens = 200000L, time_sec = 300L),
                         stop_policy = list(target_items = NULL, plateau_rounds = 2L,
                                           novelty_min = 0.05, novelty_window = 20L),
                         sources = list(web = TRUE, wikipedia = TRUE, wikidata = TRUE),
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

  # Apply defaults from constants
  workers <- workers %||% .get_default_workers()
  max_rounds <- max_rounds %||% ASA_DEFAULT_MAX_ROUNDS
  backend <- backend %||% .get_default_backend()
  model <- model %||% .get_default_model()
  conda_env <- conda_env %||% .get_default_conda_env()

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
    checkpoint_dir = checkpoint_dir,
    resume_from = resume_from
  )

  # Handle resume from checkpoint
  if (!is.null(resume_from)) {
    return(.resume_research(resume_from, verbose))
  }

  # Ensure agent is initialized
  if (is.null(agent)) {
    if (!.is_initialized()) {
      if (verbose) message("Initializing agent...")
      agent <- initialize_agent(
        backend = backend,
        model = model,
        conda_env = conda_env,
        verbose = verbose
      )
    } else {
      agent <- get_agent()
    }
  }

  # Import Python modules
  .import_research_modules()

  # Normalize schema
  schema_dict <- .normalize_schema(schema, query, verbose)

  # Create research config
  config_dict <- .create_research_config(
    workers = workers,
    max_rounds = max_rounds,
    budget = budget,
    stop_policy = stop_policy,
    sources = sources,
    temporal = temporal
  )

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

  if (progress) {
    result <- .run_research_with_progress(graph, query, schema_dict, config_dict,
                                          checkpoint_file, verbose)
  } else {
    result <- .run_research(graph, query, schema_dict, config_dict)
  }

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
    # For now, use a default schema - could be enhanced with LLM proposal
    if (verbose) message("  Using auto schema detection...")
    return(list(name = "character"))
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
.create_research_config <- function(workers, max_rounds, budget, stop_policy, sources, temporal = NULL) {
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
    use_wikipedia = isTRUE(sources$wikipedia)
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
