#' Run a Structured Task with the Agent
#'
#' Executes a research task using the AI search agent with a structured prompt
#' and returns parsed results. This is the primary function for running
#' agent tasks.
#'
#' @param prompt The task prompt or question for the agent to research
#' @param output_format Expected output format. One of:
#'   \itemize{
#'     \item "text": Returns response text (default)
#'     \item "json": Parse response as JSON
#'     \item "raw": Include full trace in result for debugging
#'     \item Character vector: Extract specific fields from response
#'   }
#' @param temporal Named list or \code{asa_temporal} object for temporal filtering:
#'   \itemize{
#'     \item time_filter: DuckDuckGo time filter - "d" (day), "w" (week),
#'       "m" (month), "y" (year)
#'     \item after: ISO 8601 date (e.g., "2020-01-01") - hint for results
#'       after this date (added to prompt context)
#'     \item before: ISO 8601 date (e.g., "2024-01-01") - hint for results
#'       before this date (added to prompt context)
#'   }
#' @param config An \code{asa_config} object for unified configuration, or NULL
#'   to use defaults
#' @param agent An asa_agent object from \code{\link{initialize_agent}}, or
#'   NULL to use the currently initialized agent
#' @param expected_fields Optional character vector of field names expected in
#'   JSON output. When provided, validates that all fields are present and
#'   non-null. The result will include a \code{parsing_status} field with
#'   validation details.
#' @param expected_schema Optional JSON schema tree used for best-effort repair
#'   when the agent output is missing required keys (especially when
#'   \code{recursion_limit} is reached). This should be a nested list describing
#'   the required JSON object/array shape, following the conventions used in
#'   \code{asa/tests/testthat/test-langgraph-remainingsteps.R}. When provided,
#'   this bypasses prompt-based schema inference.
#' @param thread_id Optional stable identifier for memory folding sessions.
#'   When provided, the same thread ID is reused so folded summaries persist
#'   across invocations. Defaults to NULL (new thread each call).
#' @param recursion_limit Optional maximum number of agent steps. When NULL
#'   (default), a backend-appropriate default is used.
#' @param verbose Print progress messages (default: FALSE)
#' @param allow_read_webpages If TRUE, allows the agent to open and read full
#'   webpages (HTML/text) via the OpenWebpage tool. Disabled by default.
#' @param webpage_relevance_mode Relevance selection for opened webpages.
#'   One of: "auto" (default), "lexical", "embeddings". When "embeddings" or
#'   "auto" with an available provider, the tool uses vector similarity to pick
#'   the most relevant excerpts; otherwise it falls back to lexical overlap.
#' @param webpage_embedding_provider Embedding provider to use for relevance.
#'   One of: "auto" (default), "openai", "sentence_transformers".
#' @param webpage_embedding_model Embedding model identifier. For OpenAI,
#'   defaults to "text-embedding-3-small". For sentence-transformers, use a
#'   local model name (e.g., "all-MiniLM-L6-v2").
#'
#' @return An \code{asa_result} object with:
#'   \itemize{
#'     \item prompt: The original prompt
#'     \item message: The agent's response text
#'     \item parsed: Parsed output (list for JSON/field extraction, NULL for text/raw)
#'     \item raw_output: Full agent trace (always included, verbose for "raw" format)
#'     \item trace_json: Structured trace JSON (when available)
#'     \item elapsed_time: Execution time in minutes
#'     \item status: "success" or "error"
#'     \item search_tier: Which search tier was used ("primp", "selenium", etc.)
#'     \item parsing_status: Validation result (if expected_fields provided)
#'     \item trace: Full execution trace (for "raw" output_format)
#'     \item fold_count: Number of memory folds (for "raw" output_format)
#'   }
#'
#' @details
#' This function provides the primary interface for running research tasks.
#' For simple text responses, use \code{output_format = "text"}. For structured
#' outputs, use \code{output_format = "json"} or specify field names to extract.
#' For debugging and full trace access, use \code{output_format = "raw"}.
#'
#' When temporal filtering is specified, the search tool's time filter is
#' temporarily set for this task and restored afterward. Date hints (after/before)
#' are appended to the prompt to guide the agent's search behavior.
#'
#' @examples
#' \dontrun{
#' # Initialize agent first
#' agent <- initialize_agent(backend = "openai", model = "gpt-4.1-mini")
#'
#' # Simple text query
#' result <- run_task(
#'   prompt = "What is the capital of France?",
#'   output_format = "text",
#'   agent = agent
#' )
#' print(result$message)
#'
#' # JSON structured output
#' result <- run_task(
#'   prompt = "Find information about Albert Einstein and return JSON with
#'             fields: birth_year, death_year, nationality, field_of_study",
#'   output_format = "json",
#'   agent = agent
#' )
#' print(result$parsed)
#'
#' # Raw output for debugging (includes full trace in asa_result)
#' result <- run_task(
#'   prompt = "Search for information",
#'   output_format = "raw",
#'   agent = agent
#' )
#' cat(result$trace)  # View full agent trace
#'
#' # With temporal filtering (past year only)
#' result <- run_task(
#'   prompt = "Find recent AI research breakthroughs",
#'   temporal = temporal_options(time_filter = "y"),
#'   agent = agent
#' )
#'
#' # With date range hint
#' result <- run_task(
#'   prompt = "Find tech companies founded recently",
#'   temporal = list(
#'     time_filter = "y",
#'     after = "2020-01-01",
#'     before = "2024-01-01"
#'   ),
#'   agent = agent
#' )
#'
#' # Using asa_config for unified configuration
#' config <- asa_config(
#'   backend = "openai",
#'   model = "gpt-4.1-mini",
#'   temporal = temporal_options(time_filter = "y")
#' )
#' result <- run_task(prompt, config = config)
#' }
#'
#' @seealso \code{\link{initialize_agent}}, \code{\link{run_task_batch}},
#'   \code{\link{asa_config}}, \code{\link{temporal_options}}
#'
#' @export
run_task <- function(prompt,
                     output_format = "text",
                     temporal = NULL,
                     config = NULL,
                     agent = NULL,
                     expected_fields = NULL,
                     expected_schema = NULL,
                     thread_id = NULL,
                     verbose = FALSE,
                     allow_read_webpages = NULL,
                     webpage_relevance_mode = NULL,
                     webpage_embedding_provider = NULL,
                     webpage_embedding_model = NULL,
                     recursion_limit = NULL) {

  config_search <- NULL
  config_conda_env <- NULL

  # Extract settings from config if provided
  if (!is.null(config) && inherits(config, "asa_config")) {
    config_search <- config$search
    config_conda_env <- config$conda_env
  }

  temporal <- .resolve_temporal_input(temporal, config)

  webpage_settings <- .resolve_webpage_reader_settings(
    config_search,
    allow_read_webpages,
    webpage_relevance_mode,
    webpage_embedding_provider,
    webpage_embedding_model
  )
  allow_rw <- webpage_settings$allow_read_webpages
  relevance_mode <- webpage_settings$relevance_mode
  embedding_provider <- webpage_settings$embedding_provider
  embedding_model <- webpage_settings$embedding_model

  # Validate inputs
  .validate_run_task(
    prompt = prompt,
    output_format = output_format,
    agent = agent,
    verbose = verbose,
    thread_id = thread_id,
    expected_schema = expected_schema,
    allow_read_webpages = allow_read_webpages,
    webpage_relevance_mode = webpage_relevance_mode,
    webpage_embedding_provider = webpage_embedding_provider,
    webpage_embedding_model = webpage_embedding_model,
    recursion_limit = recursion_limit
  )

  # Initialize agent from config if provided
  if (is.null(agent) && !is.null(config) && inherits(config, "asa_config")) {
    current <- if (.is_initialized()) get_agent() else NULL
    if (!is.null(current)) {
      if (.agent_matches_config(current, config)) {
        agent <- current
      }
    }

    if (is.null(agent)) {
      if (verbose) message("Initializing agent from asa_config...")
      agent <- initialize_agent(
        backend = config$backend,
        model = config$model,
        conda_env = config$conda_env,
        proxy = config$proxy,
        use_browser = config$use_browser %||% ASA_DEFAULT_USE_BROWSER,
        use_memory_folding = config$memory_folding,
        memory_threshold = config$memory_threshold,
        memory_keep_recent = config$memory_keep_recent,
        rate_limit = config$rate_limit,
        timeout = config$timeout,
        tor = config$tor,
        verbose = verbose
      )
    }
  }

  # Validate temporal if provided
  .validate_temporal(temporal)

  # Augment prompt with temporal hints if dates specified
  augmented_prompt <- .augment_prompt_temporal(prompt, temporal, verbose = verbose)
  if (isTRUE(allow_rw)) {
    augmented_prompt <- paste0(
      augmented_prompt,
      "\n\n[Tooling: Webpage reading is enabled. You may use the OpenWebpage tool to open and read full webpages when needed.]"
    )
  }

  start_time <- Sys.time()

  # Acquire rate limit token BEFORE making request (proactive rate limiting)
  .acquire_rate_limit_token(verbose = verbose)

  # Run agent with temporal filtering applied
  conda_env <- config_conda_env %||% .get_default_conda_env()
  response <- .with_search_config(config_search, conda_env = conda_env, function() {
    .with_webpage_reader_config(
      allow_rw,
      relevance_mode = relevance_mode,
      embedding_provider = embedding_provider,
      embedding_model = embedding_model,
      conda_env = conda_env,
      fn = function() {
      .with_temporal(temporal, function() {
        .run_agent(
          augmented_prompt,
          agent = agent,
          recursion_limit = recursion_limit,
          expected_schema = expected_schema,
          thread_id = thread_id,
          verbose = verbose
        )
      }, agent = agent)
    })
  })

  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))

  # Parse output based on format
  parsed <- NULL
  status <- if (response$status_code == ASA_STATUS_SUCCESS) "success" else "error"

  # Detect CAPTCHA/block in response for adaptive rate limiting
  adaptive_status <- status
  if (status == "error" && !is.null(response$message)) {
    msg_lower <- tolower(response$message)
    if (grepl("captcha|robot|unusual traffic", msg_lower)) {
      adaptive_status <- "captcha"
    } else if (grepl("rate.?limit|blocked|forbidden", msg_lower)) {
      adaptive_status <- "blocked"
    }
  }
  # Record result for adaptive rate limiting (adjusts delays dynamically)
  .adaptive_rate_record(adaptive_status, verbose = verbose)

  if (status == "success" && !is.na(response$message)) {
    if (identical(output_format, "json")) {
      parsed <- .parse_json_response(response$message)
    } else if (is.character(output_format) && length(output_format) > 1) {
      # Extract specific fields
      parsed <- .extract_fields(response$message, output_format)
    }
  }

  # Validate parsed JSON against expected schema (if expected_fields provided)
  parsing_status <- .validate_json_schema(parsed, expected_fields)

  # Extract search tier from trace (if search was used)
  search_tier <- .extract_search_tier(response$trace)

  # Build result object - always return asa_result for consistent API
  result <- asa_result(
    prompt = prompt,
    message = response$message,
    parsed = parsed,
    raw_output = response$trace,
    trace_json = response$trace_json %||% "",
    elapsed_time = elapsed,
    status = status,
    search_tier = search_tier,
    parsing_status = parsing_status
  )

  # For "raw" format, add additional fields for debugging
  if (identical(output_format, "raw")) {
    result$trace <- response$trace
    result$fold_count <- response$fold_count
    result$status_code <- response$status_code
    result$raw_response <- response$raw_response
  }

  result
}

#' Build a Task Prompt from Template
#'
#' Creates a formatted prompt by substituting variables into a template.
#'
#' @param template A character string with placeholders in the form \code{{variable_name}}
#' @param ... Named arguments to substitute into the template
#'
#' @return A formatted prompt string
#'
#' @examples
#' \dontrun{
#' prompt <- build_prompt(
#'   template = "Find information about {{name}} in {{country}} during {{year}}",
#'   name = "Marie Curie",
#'   country = "France",
#'   year = 1903
#' )
#' }
#'
#' @export
build_prompt <- function(template, ...) {
  # Validate template
  .validate_build_prompt(template)

  args <- list(...)

  if (length(args) == 0) {
    return(template)
  }

  result <- template
  substitutions_made <- 0L

  for (name in names(args)) {
    pattern <- paste0("\\{\\{", name, "\\}\\}")
    # Check if pattern exists before substitution
    if (grepl(pattern, result)) {
      result <- gsub(pattern, as.character(args[[name]]), result)
      substitutions_made <- substitutions_made + 1L
    } else {
      warning(sprintf("Placeholder '{{%s}}' not found in template", name),
              call. = FALSE)
    }
  }

  # Warn if no substitutions were made when arguments were provided
  if (substitutions_made == 0L && length(args) > 0) {
    warning("No substitutions made: template may not contain expected placeholders",
            call. = FALSE)
  }

  # Check for remaining unsubstituted placeholders

  remaining <- regmatches(result, gregexpr("\\{\\{[^}]+\\}\\}", result))[[1]]
  if (length(remaining) > 0) {
    warning(sprintf("Unsubstituted placeholders remaining: %s",
                    paste(remaining, collapse = ", ")),
            call. = FALSE)
  }

  result
}

#' Augment Prompt with Temporal Context
#'
#' Adds temporal date hints to the prompt when after/before dates are specified.
#' This helps guide the agent to search for time-relevant information.
#'
#' @param prompt Original prompt
#' @param temporal Temporal filtering list (may be NULL)
#' @return Augmented prompt string
#' @keywords internal
.augment_prompt_temporal <- function(prompt, temporal, verbose = FALSE) {
  if (is.null(temporal)) {
    return(prompt)
  }

  # Only augment if date hints are provided
  has_after <- !is.null(temporal$after)
  has_before <- !is.null(temporal$before)

  if (!has_after && !has_before) {
    return(prompt)
  }

  # Inform user about date context being added to prompt
  if (isTRUE(verbose)) {
    if (has_after && has_before) {
      message("Temporal context: focusing on ", temporal$after, " to ", temporal$before)
    } else if (has_after) {
      message("Temporal context: focusing on results after ", temporal$after)
    } else if (has_before) {
      message("Temporal context: focusing on results before ", temporal$before)
    }
  }

  # Build temporal context string
  context_parts <- c()

  if (has_after && has_before) {
    context_parts <- c(context_parts, sprintf(
      "Focus on information from between %s and %s.",
      temporal$after, temporal$before
    ))
  } else if (has_after) {
    context_parts <- c(context_parts, sprintf(
      "Focus on information from after %s.",
      temporal$after
    ))
  } else if (has_before) {
    context_parts <- c(context_parts, sprintf(
      "Focus on information from before %s.",
      temporal$before
    ))
  }

  # Append context to prompt
  temporal_context <- paste(context_parts, collapse = " ")
  paste0(prompt, "\n\n[Temporal context: ", temporal_context, "]")
}


#' Parse JSON Response
#' @param response_text Response text from agent
#' @keywords internal
.parse_json_response <- function(response_text) {
  if (is.na(response_text) || response_text == "") {
    return(NULL)
  }

  # Try to parse full response first
  parsed <- tryCatch(
    jsonlite::fromJSON(response_text),
    error = function(e) NULL
  )
  if (!is.null(parsed)) {
    return(parsed)
  }

  starts <- gregexpr("[\\{\\[]", response_text)[[1]]
  if (starts[1] == -1) {
    return(NULL)
  }

  for (start in starts) {
    json_str <- .extract_json_object(response_text, start = start)
    if (is.null(json_str)) {
      next
    }
    parsed <- tryCatch(
      jsonlite::fromJSON(json_str),
      error = function(e) NULL
    )
    if (!is.null(parsed)) {
      return(parsed)
    }
  }

  NULL
}

#' Extract JSON Object from Text
#' @param text Response text
#' @param start Optional 1-based start index for extraction
#' @keywords internal
.extract_json_object <- function(text, start = NULL) {
  # Try to find JSON object or array in text
  if (is.null(start)) {
    start_obj <- regexpr("\\{", text)
    start_arr <- regexpr("\\[", text)

    starts <- c(start_obj, start_arr)
    starts <- starts[starts > 0]
    if (length(starts) == 0) return(NULL)

    start <- min(starts)
  }
  if (start <= 0) {
    return(NULL)
  }

  # Count braces/brackets to find matching close
  depth <- 0
  in_string <- FALSE
  escape_next <- FALSE

  chars <- strsplit(text, "")[[1]]
  end <- -1

  for (i in start:length(chars)) {
    char <- chars[i]

    if (escape_next) {
      escape_next <- FALSE
      next
    }

    if (char == "\\") {
      escape_next <- TRUE
      next
    }

    if (char == '"' && !escape_next) {
      in_string <- !in_string
      next
    }

    if (!in_string) {
      if (char == "{" || char == "[") depth <- depth + 1
      if (char == "}" || char == "]") {
        depth <- depth - 1
        if (depth == 0) {
          end <- i
          break
        }
      }
    }
  }

  if (end > start) {
    json_str <- paste(chars[start:end], collapse = "")
    return(json_str)
  }

  NULL
}

#' Extract Specific Fields from Response
#' @param text Response text
#' @param fields Character vector of field names to extract
#' @keywords internal
.extract_fields <- function(text, fields) {
  result <- list()

  # First try JSON parsing
  json_data <- .parse_json_response(text)
  if (!is.null(json_data)) {
    for (field in fields) {
      if (field %in% names(json_data)) {
        result[[field]] <- json_data[[field]]
      } else {
        result[[field]] <- NA
      }
    }
    return(result)
  }

  # Fallback to regex extraction
  for (field in fields) {
    pattern <- sprintf('"%s"\\s*:\\s*"([^"]*)"', field)
    match <- regmatches(text, regexec(pattern, text))[[1]]
    if (length(match) > 1) {
      result[[field]] <- match[2]
    } else {
      # Try without quotes for numbers/booleans
      pattern <- sprintf('"%s"\\s*:\\s*([^,}\\s]+)', field)
      match <- regmatches(text, regexec(pattern, text))[[1]]
      if (length(match) > 1) {
        result[[field]] <- match[2]
      } else {
        result[[field]] <- NA
      }
    }
  }

  result
}

#' Run Multiple Tasks in Batch
#'
#' Executes multiple research tasks, optionally in parallel. Includes a circuit
#' breaker that monitors error rates and pauses execution if errors spike,
#' preventing cascading failures.
#'
#' @param prompts Character vector of task prompts, or a data frame with a
#'   'prompt' column
#' @param output_format Expected output format (applies to all tasks)
#' @param temporal Named list for temporal filtering (applies to all tasks).
#'   See \code{\link{run_task}} for details.
#' @param config Optional \code{asa_config} object to apply across the batch.
#'   When provided, \code{config$temporal} is used if \code{temporal} is NULL, and
#'   \code{config$workers} is used if \code{workers} is NULL. In parallel mode,
#'   the config is sent to worker processes so each worker can initialize its
#'   own agent session.
#' @param agent An asa_agent object
#' @param parallel Use parallel processing
#' @param workers Number of parallel workers
#' @param progress Show progress messages
#' @param circuit_breaker Enable circuit breaker for error rate monitoring.
#'   When enabled, tracks recent error rates and pauses if threshold exceeded.
#'   Default TRUE.
#' @param abort_on_trip If TRUE, abort the batch when circuit breaker trips.
#'   If FALSE (default), wait for cooldown and continue.
#' @param allow_read_webpages If TRUE, allows the agent to open and read full
#'   webpages (HTML/text) via the OpenWebpage tool. Disabled by default.
#' @param webpage_relevance_mode Relevance selection for opened webpages.
#'   One of: "auto", "lexical", "embeddings". See \code{\link{run_task}}.
#' @param webpage_embedding_provider Embedding provider for relevance. See
#'   \code{\link{run_task}}.
#' @param webpage_embedding_model Embedding model identifier. See
#'   \code{\link{run_task}}.
#' @param recursion_limit Optional maximum number of agent steps applied to each
#'   task. See \code{\link{run_task}}.
#'
#' @return A list of asa_result objects, or if prompts was a data frame,
#'   the data frame with result columns added. If circuit breaker aborts,
#'   includes attribute "circuit_breaker_aborted" = TRUE.
#'
#' @examples
#' \dontrun{
#' prompts <- c(
#'   "What is the population of Tokyo?",
#'   "What is the population of New York?",
#'   "What is the population of London?"
#' )
#' results <- run_task_batch(prompts, agent = agent)
#'
#' # With temporal filtering for all tasks
#' results <- run_task_batch(
#'   prompts,
#'   temporal = list(time_filter = "y"),
#'   agent = agent
#' )
#'
#' # Disable circuit breaker
#' results <- run_task_batch(prompts, agent = agent, circuit_breaker = FALSE)
#'
#' # Abort on circuit breaker trip
#' results <- run_task_batch(prompts, agent = agent, abort_on_trip = TRUE)
#' }
#'
#' @seealso \code{\link{run_task}}, \code{\link{configure_temporal}}
#'
#' @export
run_task_batch <- function(prompts,
                           output_format = "text",
                           temporal = NULL,
                           config = NULL,
                           agent = NULL,
                           parallel = FALSE,
                           workers = NULL,
                           progress = TRUE,
                           circuit_breaker = TRUE,
                           abort_on_trip = FALSE,
                           allow_read_webpages = NULL,
                           webpage_relevance_mode = NULL,
                           webpage_embedding_provider = NULL,
                           webpage_embedding_model = NULL,
                           recursion_limit = NULL) {

  # Validate config type early (before any work)
  if (!is.null(config) && !inherits(config, "asa_config")) {
    stop("`config` must be an asa_config object or NULL", call. = FALSE)
  }

  # Config temporal overrides direct temporal parameter when temporal is NULL
  temporal <- .resolve_temporal_input(temporal, config)

  # Resolve workers (used for parallel planning, and validated regardless)
  if (is.null(workers)) {
    workers <- if (!is.null(config) && !is.null(config$workers)) config$workers else .get_default_workers()
  }
  workers <- as.integer(workers)

  # Validate inputs
  .validate_run_task_batch(
    prompts = prompts,
    output_format = output_format,
    agent = agent,
    parallel = parallel,
    workers = workers,
    progress = progress
  )

  # Validate temporal if provided
  .validate_temporal(temporal)

  # Initialize circuit breaker if enabled
  if (circuit_breaker) {
    .circuit_breaker_init()
  }
  aborted <- FALSE

  # Handle data frame input
  is_df <- is.data.frame(prompts)
  if (is_df) {
    prompt_vec <- prompts$prompt
  } else {
    prompt_vec <- prompts
  }

  n <- length(prompt_vec)

  # In parallel mode, don't attempt to serialize asa_agent / reticulate objects
  # to workers. Instead, derive a lightweight asa_config template and let each
  # worker initialize its own agent session.
  worker_config <- NULL
  if (parallel) {
    if (!is.null(config)) {
      worker_config <- config
      worker_config$workers <- workers
      agent <- NULL
    } else {
    config_template <- agent
    if (is.null(config_template) && .is_initialized()) {
      config_template <- get_agent()
    }
    if (is.null(config_template)) {
      stop(
        "In parallel mode, provide `config`, provide `agent`, or call initialize_agent() first ",
        "so configuration can be derived for worker processes.",
        call. = FALSE
      )
    }

    worker_config <- asa_config(
      backend = config_template$backend,
      model = config_template$model,
      conda_env = config_template$config$conda_env,
      proxy = config_template$config$proxy,
      workers = workers,
      timeout = config_template$config$timeout,
      rate_limit = config_template$config$rate_limit,
      memory_folding = config_template$config$use_memory_folding,
      memory_threshold = config_template$config$memory_threshold,
      memory_keep_recent = config_template$config$memory_keep_recent,
      tor = config_template$config$tor
    )

    # Drop reference to the (non-serializable) agent object.
    agent <- NULL
    config_template <- NULL
    }
  }

  effective_config <- if (parallel) worker_config else config

  # Function to process one task with circuit breaker integration
  process_one <- function(i) {
    result <- run_task(
      prompt = prompt_vec[i],
      output_format = output_format,
      temporal = temporal,
      config = effective_config,
      agent = agent,
      allow_read_webpages = allow_read_webpages,
      webpage_relevance_mode = webpage_relevance_mode,
      webpage_embedding_provider = webpage_embedding_provider,
      webpage_embedding_model = webpage_embedding_model,
      recursion_limit = recursion_limit,
      verbose = FALSE
    )

    # Record result in circuit breaker (only for sequential, parallel handled separately)
    if (circuit_breaker && !parallel) {
      .circuit_breaker_record(result$status, verbose = progress)
    }

    result
  }

  # Run tasks
  if (parallel) {
    if (!requireNamespace("future", quietly = TRUE) ||
        !requireNamespace("future.apply", quietly = TRUE)) {
      stop("Packages 'future' and 'future.apply' required for parallel processing.",
           call. = FALSE)
    }
    future::plan(future::multisession(workers = workers))
    on.exit(future::plan(future::sequential), add = TRUE)

    # Note: Circuit breaker less effective in parallel mode since workers
    # don't share state. We check after each batch of results.
    results <- future.apply::future_lapply(
      seq_len(n),
      process_one,
      future.packages = "asa",
      future.seed = TRUE
    )

    # Record all results in circuit breaker after parallel execution
    if (circuit_breaker) {
      for (r in results) {
        .circuit_breaker_record(r$status, verbose = progress)
      }
    }
  } else {
    results <- vector("list", n)
    for (i in seq_len(n)) {
      # Check circuit breaker before each task
      if (circuit_breaker && !.circuit_breaker_check(verbose = progress)) {
        if (abort_on_trip) {
          if (progress) {
            message(sprintf("Circuit breaker tripped at task %d/%d - aborting batch", i, n))
          }
          aborted <- TRUE
          break
        } else {
          # Wait for cooldown
          if (progress) {
            message(sprintf("Circuit breaker tripped - waiting %ds cooldown...",
                            ASA_CIRCUIT_BREAKER_COOLDOWN))
          }
          Sys.sleep(ASA_CIRCUIT_BREAKER_COOLDOWN)
          # Reset and continue
          .circuit_breaker_init()
        }
      }

      if (progress) message(sprintf("[%d/%d] Processing...", i, n))
      results[[i]] <- process_one(i)
    }
  }

  # Filter out NULL results if aborted early
  if (aborted) {
    results <- Filter(Negate(is.null), results)
  }

  # Return as data frame if input was data frame
  if (is_df) {
    # Handle partial results if aborted
    n_completed <- length(results)
    if (aborted && n_completed < nrow(prompts)) {
      prompts <- prompts[seq_len(n_completed), , drop = FALSE]
    }

    prompts$response <- vapply(results, function(r) r$message %||% NA_character_, character(1))
    prompts$status <- vapply(results, function(r) r$status %||% NA_character_, character(1))
    prompts$elapsed_time <- vapply(results, function(r) r$elapsed_time %||% NA_real_, numeric(1))
    prompts$search_tier <- vapply(results, function(r) r$search_tier %||% "unknown", character(1))

    # Add parsed fields if JSON output
    if (identical(output_format, "json") || (is.character(output_format) && length(output_format) > 1)) {
      # Try to extract common fields from first successful result
      first_parsed <- NULL
      for (r in results) {
        if (!is.null(r$parsed)) {
          first_parsed <- r$parsed
          break
        }
      }

      if (!is.null(first_parsed) && is.list(first_parsed)) {
        coerce_value <- function(val) {
          if (is.null(val)) return(NA_character_)
          if (is.list(val)) {
            return(as.character(jsonlite::toJSON(val, auto_unbox = TRUE)))
          }
          if (length(val) == 0) return(NA_character_)
          if (length(val) == 1) return(as.character(val))
          as.character(jsonlite::toJSON(val, auto_unbox = TRUE))
        }

        for (field in names(first_parsed)) {
          prompts[[field]] <- vapply(results, function(r) {
            if (!is.null(r$parsed) && field %in% names(r$parsed)) {
              coerce_value(r$parsed[[field]])
            } else {
              NA_character_
            }
          }, character(1))
        }
      }
    }

    # Add circuit breaker metadata
    if (aborted) {
      attr(prompts, "circuit_breaker_aborted") <- TRUE
      attr(prompts, "circuit_breaker_status") <- .circuit_breaker_status()
    }

    return(prompts)
  }

  # Add circuit breaker metadata to list output
  if (aborted) {
    attr(results, "circuit_breaker_aborted") <- TRUE
    attr(results, "circuit_breaker_status") <- .circuit_breaker_status()
  }

  results
}

#' Validate JSON Against Expected Schema
#'
#' Validates that parsed JSON contains all expected fields.
#' Returns a structured validation result indicating success or failure.
#'
#' @param parsed The parsed JSON object (list or NULL)
#' @param expected_fields Character vector of expected field names
#' @return A list with: valid (logical), reason (character), missing (character vector)
#' @keywords internal
.validate_json_schema <- function(parsed, expected_fields) {
  if (is.null(expected_fields) || length(expected_fields) == 0) {
    return(list(valid = TRUE, reason = "no_validation", missing = character(0)))
  }

  if (is.null(parsed)) {
    return(list(valid = FALSE, reason = "parsing_failed", missing = expected_fields))
  }

  if (!is.list(parsed)) {
    return(list(valid = FALSE, reason = "not_object", missing = expected_fields))
  }

  missing <- setdiff(expected_fields, names(parsed))
  if (length(missing) > 0) {
    return(list(valid = FALSE, reason = "missing_fields", missing = missing))
  }

  # Check for NULL/NA values in expected fields
  null_fields <- character(0)
  for (field in expected_fields) {
    val <- parsed[[field]]
    if (is.null(val) || (length(val) == 1 && is.na(val))) {
      null_fields <- c(null_fields, field)
    }
  }

  if (length(null_fields) > 0) {
    return(list(valid = FALSE, reason = "null_values", missing = null_fields))
  }

  return(list(valid = TRUE, reason = "ok", missing = character(0)))
}

#' Extract Search Tier from Response Trace
#'
#' Parses the agent's response trace to determine which search tier
#' was used (PRIMP, Selenium, DDGS, or Requests). This is useful for
#' assessing result quality since higher tiers generally produce
#' more reliable results.
#'
#' @param trace Character string containing the agent's execution trace
#' @return Character string: "primp", "selenium", "ddgs", "requests", or "unknown"
#' @keywords internal
.extract_search_tier <- function(trace) {
  if (is.null(trace) || is.na(trace) || trace == "") {
    return("unknown")
  }


  # Convert to character if needed
  trace_str <- tryCatch(
    as.character(trace),
    error = function(e) ""
  )

  if (trace_str == "") {
    return("unknown")
  }

  # Check for tier markers in order of preference (most specific first)
  # The Python code adds "_tier": "primp"/"selenium"/"ddgs"/"requests" to results
  if (grepl("'_tier':\\s*'primp'|\"_tier\":\\s*\"primp\"", trace_str, ignore.case = TRUE)) {
    return("primp")
  }
  if (grepl("'_tier':\\s*'selenium'|\"_tier\":\\s*\"selenium\"", trace_str, ignore.case = TRUE)) {
    return("selenium")
  }
  if (grepl("'_tier':\\s*'ddgs'|\"_tier\":\\s*\"ddgs\"", trace_str, ignore.case = TRUE)) {
    return("ddgs")
  }
  if (grepl("'_tier':\\s*'requests'|\"_tier\":\\s*\"requests\"", trace_str, ignore.case = TRUE)) {
    return("requests")
  }

  # Fallback: check for tier keywords in log messages
  if (grepl("PRIMP SUCCESS|PRIMP.*returning", trace_str, ignore.case = TRUE)) {
    return("primp")
  }
  if (grepl("Browser search SUCCESS|selenium", trace_str, ignore.case = TRUE)) {
    return("selenium")
  }
  if (grepl("DDGS tier|ddgs\\.text", trace_str, ignore.case = TRUE)) {
    return("ddgs")
  }
  if (grepl("requests scrape|requests_scrape", trace_str, ignore.case = TRUE)) {
    return("requests")
  }

  # No search tier detected (may not have used search tools)
  return("unknown")
}
