# ============================================================================
# CONFIGURATION S3 CLASSES
# ============================================================================

#' Create ASA Configuration Object
#'
#' Creates a configuration object that encapsulates all settings for ASA tasks.
#' This provides a unified way to configure backend, model, search, temporal,
#' and resource settings in a single object.
#'
#' @param backend LLM backend: "openai", "groq", "xai", "exo", "openrouter"
#' @param model Model identifier (e.g., "gpt-4.1-mini")
#' @param conda_env Conda environment name (default: "asa_env")
#' @param proxy SOCKS5 proxy URL or NULL to disable
#' @param workers Number of parallel workers for batch operations
#' @param timeout Request timeout in seconds
#' @param rate_limit Requests per second
#' @param memory_folding Enable DeepAgent-style memory folding
#' @param memory_threshold Messages before folding triggers
#' @param memory_keep_recent Messages to preserve after folding
#' @param temporal Temporal filtering options (use \code{temporal_options()})
#' @param search Search configuration (use \code{search_options()})
#'
#' @return An object of class \code{asa_config}
#'
#' @details
#' The configuration object can be passed to \code{run_task()},
#' \code{run_task_batch()}, \code{asa_enumerate()}, and other functions
#' to provide consistent settings across operations.
#'
#' @examples
#' \dontrun{
#' # Create configuration
#' config <- asa_config(
#'   backend = "openai",
#'   model = "gpt-4.1-mini",
#'   workers = 4,
#'   temporal = temporal_options(time_filter = "y")
#' )
#'
#' # Use with run_task
#' result <- run_task(prompt, config = config)
#' }
#'
#' @seealso \code{\link{temporal_options}}, \code{\link{search_options}}
#'
#' @export
asa_config <- function(backend = NULL,
                       model = NULL,
                       conda_env = NULL,
                       proxy = NULL,
                       workers = NULL,
                       timeout = NULL,
                       rate_limit = NULL,
                       memory_folding = NULL,
                       memory_threshold = NULL,
                       memory_keep_recent = NULL,
                       temporal = NULL,
                       search = NULL) {

  # Use defaults from constants.R if not specified
  backend <- backend %||% .get_default_backend()
  model <- model %||% .get_default_model()
  conda_env <- conda_env %||% .get_default_conda_env()
  workers <- workers %||% .get_default_workers()
  timeout <- timeout %||% ASA_DEFAULT_TIMEOUT
  rate_limit <- rate_limit %||% ASA_DEFAULT_RATE_LIMIT
  memory_folding <- memory_folding %||% ASA_DEFAULT_MEMORY_FOLDING
  memory_threshold <- memory_threshold %||% ASA_DEFAULT_MEMORY_THRESHOLD
  memory_keep_recent <- memory_keep_recent %||% ASA_DEFAULT_MEMORY_KEEP_RECENT

  # Validate backend
  if (!backend %in% ASA_SUPPORTED_BACKENDS) {
    stop(sprintf("`backend` must be one of: %s",
                 paste(ASA_SUPPORTED_BACKENDS, collapse = ", ")),
         call. = FALSE)
  }

  # Validate temporal if provided
  if (!is.null(temporal) && !inherits(temporal, "asa_temporal")) {
    if (is.list(temporal)) {
      # Convert plain list to asa_temporal
      temporal <- do.call(temporal_options, temporal)
    } else {
      stop("`temporal` must be created with temporal_options() or be a list",
           call. = FALSE)
    }
  }

  structure(
    list(
      backend = backend,
      model = model,
      conda_env = conda_env,
      proxy = proxy,
      workers = as.integer(workers),
      timeout = as.integer(timeout),
      rate_limit = rate_limit,
      memory_folding = memory_folding,
      memory_threshold = as.integer(memory_threshold),
      memory_keep_recent = as.integer(memory_keep_recent),
      temporal = temporal,
      search = search
    ),
    class = "asa_config"
  )
}

#' Print Method for asa_config Objects
#'
#' @param x An asa_config object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_config
#' @export
print.asa_config <- function(x, ...) {
  cat("ASA Configuration\n")
  cat("=================\n")
  cat("Backend:         ", x$backend, "\n", sep = "")
  cat("Model:           ", x$model, "\n", sep = "")
  cat("Conda Env:       ", x$conda_env, "\n", sep = "")
  cat("Proxy:           ", x$proxy %||% "None", "\n", sep = "")
  cat("Workers:         ", x$workers, "\n", sep = "")
  cat("Timeout:         ", x$timeout, "s\n", sep = "")
  cat("Rate Limit:      ", x$rate_limit, " req/s\n", sep = "")
  cat("Memory Folding:  ", if (x$memory_folding) "Enabled" else "Disabled", "\n", sep = "")
  if (x$memory_folding) {
    cat("  Threshold:     ", x$memory_threshold, " messages\n", sep = "")
    cat("  Keep Recent:   ", x$memory_keep_recent, " messages\n", sep = "")
  }
  if (!is.null(x$temporal)) {
    cat("\nTemporal Filtering:\n")
    print(x$temporal)
  }
  invisible(x)
}


#' Create Temporal Filtering Options
#'
#' Creates a temporal filtering configuration for constraining search results
#' by date. Supports DuckDuckGo time filters, date ranges, and strict
#' verification modes.
#'
#' @param time_filter DuckDuckGo time filter: "d" (day), "w" (week),
#'   "m" (month), "y" (year), or NULL for no filter
#' @param after ISO 8601 date string (e.g., "2020-01-01") - results after this date
#' @param before ISO 8601 date string (e.g., "2024-01-01") - results before this date
#' @param strictness Verification level: "best_effort" (default) or "strict"
#' @param use_wayback Use Wayback Machine for strict pre-date guarantees
#'
#' @return An object of class \code{asa_temporal}
#'
#' @details
#' Temporal filtering can operate at different levels:
#' \itemize{
#'   \item \strong{time_filter}: DuckDuckGo native filter (fast, approximate)
#'   \item \strong{after/before}: Date hints appended to prompts
#'   \item \strong{strict}: Post-hoc verification of result dates
#'   \item \strong{use_wayback}: Uses Internet Archive for guaranteed historical data
#' }
#'
#' @examples
#' \dontrun{
#' # Past year only
#' temporal <- temporal_options(time_filter = "y")
#'
#' # Specific date range
#' temporal <- temporal_options(
#'   after = "2020-01-01",
#'   before = "2024-01-01"
#' )
#'
#' # Strict historical verification
#' temporal <- temporal_options(
#'   before = "2015-01-01",
#'   strictness = "strict",
#'   use_wayback = TRUE
#' )
#' }
#'
#' @seealso \code{\link{asa_config}}, \code{\link{run_task}}
#'
#' @export
temporal_options <- function(time_filter = NULL,
                             after = NULL,
                             before = NULL,
                             strictness = "best_effort",
                             use_wayback = FALSE) {

  # Validate time_filter
  if (!is.null(time_filter)) {
    if (!time_filter %in% ASA_TIME_FILTERS) {
      stop(sprintf("`time_filter` must be one of: %s",
                   paste0('"', ASA_TIME_FILTERS, '"', collapse = ", ")),
           call. = FALSE)
    }
  }

  # Validate and parse dates
  if (!is.null(after)) {
    after_date <- tryCatch(
      as.Date(after),
      error = function(e) {
        stop("`after` must be a valid ISO 8601 date (YYYY-MM-DD)", call. = FALSE)
      }
    )
  }

  if (!is.null(before)) {
    before_date <- tryCatch(
      as.Date(before),
      error = function(e) {
        stop("`before` must be a valid ISO 8601 date (YYYY-MM-DD)", call. = FALSE)
      }
    )
  }

  # Validate date ordering
  if (!is.null(after) && !is.null(before)) {
    if (as.Date(after) >= as.Date(before)) {
      stop("`after` must be earlier than `before`", call. = FALSE)
    }
  }

  # Validate strictness
  if (!strictness %in% ASA_TEMPORAL_STRICTNESS) {
    stop(sprintf("`strictness` must be one of: %s",
                 paste0('"', ASA_TEMPORAL_STRICTNESS, '"', collapse = ", ")),
         call. = FALSE)
  }

  structure(
    list(
      time_filter = time_filter,
      after = after,
      before = before,
      strictness = strictness,
      use_wayback = use_wayback
    ),
    class = "asa_temporal"
  )
}

#' Print Method for asa_temporal Objects
#'
#' @param x An asa_temporal object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_temporal
#' @export
print.asa_temporal <- function(x, ...) {
  parts <- c()
  if (!is.null(x$time_filter)) {
    filter_desc <- switch(x$time_filter,
                          "d" = "past day",
                          "w" = "past week",
                          "m" = "past month",
                          "y" = "past year")
    parts <- c(parts, sprintf("Time Filter: %s (%s)", x$time_filter, filter_desc))
  }
  if (!is.null(x$after)) {
    parts <- c(parts, sprintf("After: %s", x$after))
  }
  if (!is.null(x$before)) {
    parts <- c(parts, sprintf("Before: %s", x$before))
  }
  parts <- c(parts, sprintf("Strictness: %s", x$strictness))
  if (x$use_wayback) {
    parts <- c(parts, "Wayback: Enabled")
  }
  cat("  ", paste(parts, collapse = ", "), "\n", sep = "")
  invisible(x)
}


#' Create Search Options
#'
#' Creates search configuration for controlling DuckDuckGo search behavior,
#' including rate limiting, retry policies, and result limits. These options
#' are used by the 4-tier search fallback system.
#'
#' @param max_results Maximum number of search results to return per query.
#'   Higher values provide more context but increase latency. Default: 10.
#' @param timeout Timeout in seconds for individual search requests.
#'   Applies to each tier attempt separately. Default: 15.
#' @param max_retries Maximum number of retry attempts when a search tier fails.
#'   After exhausting retries, the system falls back to the next tier. Default: 3.
#' @param retry_delay Initial delay in seconds before the first retry.
#'   Subsequent retries use exponential backoff. Default: 2.
#' @param backoff_multiplier Multiplier for exponential backoff between retries.
#'   E.g., with retry_delay=2 and multiplier=1.5, delays are 2s, 3s, 4.5s. Default: 1.5.
#' @param inter_search_delay Minimum delay in seconds between consecutive searches.
#'   Helps avoid rate limiting from search providers. Default: 0.5.
#'
#' @return An object of class \code{asa_search}
#'
#' @details
#' The search system uses a 4-tier fallback architecture:
#' \enumerate{
#'   \item \strong{PRIMP}: HTTP/2 with browser TLS fingerprint
#'   \item \strong{Selenium}: Headless browser for JS-rendered content
#'   \item \strong{DDGS}: Standard ddgs Python library
#'   \item \strong{Requests}: Raw POST to DuckDuckGo HTML endpoint
#' }
#'
#' The retry/backoff settings apply within each tier. If all retries
#' are exhausted, the system automatically falls back to the next tier.
#'
#' @examples
#' \dontrun{
#' # Default settings
#' search <- search_options()
#'
#' # More aggressive settings for faster searches
#' search <- search_options(
#'   max_results = 5,
#'   timeout = 10,
#'   max_retries = 2
#' )
#'
#' # Conservative settings for rate-limited environments
#' search <- search_options(
#'   inter_search_delay = 2.0,
#'   max_retries = 5,
#'   backoff_multiplier = 2.0
#' )
#'
#' # Use with asa_config
#' config <- asa_config(
#'   backend = "openai",
#'   search = search_options(max_results = 15)
#' )
#' }
#'
#' @seealso \code{\link{asa_config}}, \code{\link{configure_search}}
#'
#' @export
search_options <- function(max_results = NULL,
                           timeout = NULL,
                           max_retries = NULL,
                           retry_delay = NULL,
                           backoff_multiplier = NULL,
                           inter_search_delay = NULL) {

  structure(
    list(
      max_results = max_results %||% ASA_DEFAULT_MAX_RESULTS,
      timeout = timeout %||% 15.0,
      max_retries = max_retries %||% ASA_DEFAULT_MAX_RETRIES,
      retry_delay = retry_delay %||% 2.0,
      backoff_multiplier = backoff_multiplier %||% 1.5,
      inter_search_delay = inter_search_delay %||% ASA_DEFAULT_INTER_SEARCH_DELAY
    ),
    class = "asa_search"
  )
}

#' Print Method for asa_search Objects
#'
#' @param x An asa_search object
#' @param ... Additional arguments (ignored)
#'
#' @method print asa_search
#' @export
print.asa_search <- function(x, ...) {
  cat("Search Options: max_results=", x$max_results,
      ", timeout=", x$timeout, "s",
      ", retries=", x$max_retries,
      ", delay=", x$inter_search_delay, "s\n", sep = "")
  invisible(x)
}


# ============================================================================
# AGENT S3 CLASSES
# ============================================================================

#' Constructor for asa_agent Objects
#'
#' Creates an S3 object representing an initialized ASA search agent.
#'
#' @param python_agent The underlying Python agent object
#' @param backend LLM backend name (e.g., "openai", "groq")
#' @param model Model identifier
#' @param config Agent configuration list
#' @param llm Optional LLM object used by LangGraph
#' @param tools Optional list of tools associated with the agent
#'
#' @return An object of class \code{asa_agent}
#'
#' @export
asa_agent <- function(python_agent, backend, model, config, llm = NULL, tools = NULL) {
  structure(
    list(
      python_agent = python_agent,
      backend = backend,
      model = model,
      config = config,
      llm = llm,
      tools = tools,
      created_at = Sys.time()
    ),
    class = "asa_agent"
  )
}

#' Print Method for asa_agent Objects
#'
#' @param x An asa_agent object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_agent
#' @export
print.asa_agent <- function(x, ...) {
  cat("ASA Search Agent\n")
  cat("================\n")
  cat("Backend:        ", x$backend, "\n", sep = "")
  cat("Model:          ", x$model, "\n", sep = "")

  use_folding <- isTRUE(x$config$use_memory_folding)
  cat("Memory Folding: ", if (use_folding) "Enabled" else "Disabled", "\n", sep = "")
  if (use_folding) {
    cat("  Threshold:    ", x$config$memory_threshold %||% "N/A", " messages\n", sep = "")
    cat("  Keep Recent:  ", x$config$memory_keep_recent %||% "N/A", " messages\n", sep = "")
  }
  cat("Proxy:          ", x$config$proxy %||% "None", "\n", sep = "")
  cat("Created:        ", format(x$created_at, "%Y-%m-%d %H:%M:%S"), "\n", sep = "")

  invisible(x)
}

#' Summary Method for asa_agent Objects
#'
#' @param object An asa_agent object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_agent
#' @export
summary.asa_agent <- function(object, ...) {
  cat("ASA Agent Summary\n")
  cat("-----------------\n")
  cat("Configuration:\n")
  print(object$config)

  invisible(list(
    backend = object$backend,
    model = object$model,
    config = object$config
  ))
}

#' Constructor for asa_response Objects
#'
#' Creates an S3 object representing an agent response.
#'
#' @param message The final response text
#' @param status_code Status code (200 = success, 100 = error)
#' @param raw_response The full Python response object
#' @param trace Full text trace of agent execution
#' @param elapsed_time Execution time in minutes
#' @param fold_count Number of memory folds performed
#' @param prompt The original prompt
#'
#' @return An object of class \code{asa_response}
#'
#' @export
asa_response <- function(message, status_code, raw_response, trace,
                         elapsed_time, fold_count, prompt) {
  structure(
    list(
      message = message,
      status_code = status_code,
      raw_response = raw_response,
      trace = trace,
      elapsed_time = elapsed_time,
      fold_count = fold_count,
      prompt = prompt
    ),
    class = "asa_response"
  )
}

#' Print Method for asa_response Objects
#'
#' @param x An asa_response object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_response
#' @export
print.asa_response <- function(x, ...) {
  cat("ASA Agent Response\n")
  cat("==================\n")
  cat("Status:    ", if (x$status_code == 200) "Success" else "Error", " (", x$status_code, ")\n", sep = "")
  cat("Time:      ", format_duration(x$elapsed_time), "\n", sep = "")
  if (x$fold_count > 0) {
    cat("Folds:     ", x$fold_count, "\n", sep = "")
  }
  cat("\nPrompt:\n")
  cat("  ", truncate_string(x$prompt, 80), "\n", sep = "")
  cat("\nResponse:\n")
  if (!is.na(x$message)) {
    # Word wrap the message
    msg_lines <- strwrap(x$message, width = 76)
    cat(paste("  ", msg_lines, collapse = "\n"), "\n", sep = "")
  } else {
    cat("  [No response]\n")
  }

  invisible(x)
}

#' Summary Method for asa_response Objects
#'
#' @param object An asa_response object
#' @param show_trace Include full trace in output
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_response
#' @export
summary.asa_response <- function(object, show_trace = FALSE, ...) {
  print(object)

  if (show_trace && !is.null(object$trace) && object$trace != "") {
    cat("\nFull Trace:\n")
    cat("----------\n")
    cat(object$trace, "\n")
  }

  invisible(list(
    status_code = object$status_code,
    elapsed_time = object$elapsed_time,
    message_length = nchar(object$message %||% ""),
    trace_length = nchar(object$trace %||% "")
  ))
}

#' Constructor for asa_result Objects
#'
#' Creates an S3 object representing the result of a research task.
#'
#' @param prompt The original prompt
#' @param message The agent's response text
#' @param parsed Parsed output (list or NULL)
#' @param raw_output Full agent trace
#' @param elapsed_time Execution time in minutes
#' @param status Status ("success" or "error")
#' @param search_tier Which search tier was used ("primp", "selenium", "ddgs",
#'   "requests", or "unknown"). Useful for assessing result quality.
#' @param parsing_status List with JSON parsing validation info: valid (logical),
#'   reason ("ok", "parsing_failed", "not_object", "missing_fields", "null_values",
#'   "no_validation"), and missing (character vector of missing/invalid fields).
#'
#' @return An object of class \code{asa_result}
#'
#' @export
asa_result <- function(prompt, message, parsed, raw_output, elapsed_time, status,
                       search_tier = "unknown", parsing_status = NULL) {
  # Default parsing_status if not provided

  if (is.null(parsing_status)) {
    parsing_status <- list(valid = TRUE, reason = "no_validation", missing = character(0))
  }

  structure(
    list(
      prompt = prompt,
      message = message,
      parsed = parsed,
      raw_output = raw_output,
      elapsed_time = elapsed_time,
      status = status,
      search_tier = search_tier,
      parsing_status = parsing_status
    ),
    class = "asa_result"
  )
}

#' Print Method for asa_result Objects
#'
#' @param x An asa_result object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_result
#' @export
print.asa_result <- function(x, ...) {
  cat("ASA Task Result\n")
  cat("===============\n")
  cat("Status:  ", x$status, "\n", sep = "")
  cat("Time:    ", format_duration(x$elapsed_time), "\n", sep = "")
  if (!is.null(x$search_tier) && x$search_tier != "unknown") {
    cat("Search:  ", x$search_tier, "\n", sep = "")
  }
  cat("\n")
  cat("Prompt:\n")
  prompt_lines <- strwrap(x$prompt, width = 74)
  cat(paste("  ", prompt_lines, collapse = "\n"), "\n", sep = "")
  cat("\n")
  cat("Response:\n")
  if (!is.na(x$message) && x$message != "") {
    msg_lines <- strwrap(x$message, width = 74)
    cat(paste("  ", msg_lines, collapse = "\n"), "\n", sep = "")
  } else {
    cat("  [No response]\n")
  }

  if (!is.null(x$parsed)) {
    cat("\nParsed Output:\n")
    for (name in names(x$parsed)) {
      val <- x$parsed[[name]]
      if (length(val) == 1) {
        cat("  ", name, ": ", as.character(val), "\n", sep = "")
      } else {
        cat("  ", name, ": [", length(val), " items]\n", sep = "")
      }
    }
  }

  # Show parsing status if validation failed
  if (!is.null(x$parsing_status) && !isTRUE(x$parsing_status$valid)) {
    cat("\nParsing Status: FAILED\n")
    cat("  Reason: ", x$parsing_status$reason, "\n", sep = "")
    if (length(x$parsing_status$missing) > 0) {
      cat("  Missing/Invalid: ", paste(x$parsing_status$missing, collapse = ", "), "\n", sep = "")
    }
  }

  invisible(x)
}

#' Summary Method for asa_result Objects
#'
#' @param object An asa_result object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_result
#' @export
summary.asa_result <- function(object, ...) {
  cat("Task Result Summary\n")
  cat("-------------------\n")
  cat("Status: ", object$status, "\n", sep = "")
  cat("Time: ", format_duration(object$elapsed_time), "\n", sep = "")
  cat("Response length: ", nchar(object$message %||% ""), " chars\n", sep = "")
  if (!is.null(object$parsed)) {
    cat("Parsed fields: ", paste(names(object$parsed), collapse = ", "), "\n", sep = "")
  }

  invisible(list(
    status = object$status,
    elapsed_time = object$elapsed_time,
    message_length = nchar(object$message %||% ""),
    parsed_fields = names(object$parsed)
  ))
}

#' Convert asa_result to Data Frame
#'
#' @param x An asa_result object
#' @param ... Additional arguments (ignored)
#'
#' @return A single-row data frame
#'
#' @method as.data.frame asa_result
#' @export
as.data.frame.asa_result <- function(x, ...) {
  df <- data.frame(
    prompt = x$prompt,
    message = x$message %||% NA_character_,
    status = x$status %||% NA_character_,
    elapsed_time = x$elapsed_time %||% NA_real_,
    stringsAsFactors = FALSE
  )

  # Add parsed fields as columns
  if (!is.null(x$parsed) && is.list(x$parsed)) {
    for (name in names(x$parsed)) {
      val <- x$parsed[[name]]
      if (length(val) == 1) {
        df[[name]] <- as.character(val)
      }
    }
  }

  df
}


#' Constructor for asa_enumerate_result Objects
#'
#' Creates an S3 object representing the result of an enumeration task.
#'
#' @param data data.frame containing the enumeration results
#' @param status Result status: "complete", "partial", or "failed"
#' @param stop_reason Why the enumeration stopped (e.g., "target_reached", "novelty_plateau")
#' @param metrics List with execution metrics (rounds, queries_used, etc.)
#' @param provenance Optional data.frame with source information per row
#' @param plan The enumeration plan from the planner agent
#' @param checkpoint_file Path to saved checkpoint file
#' @param query The original enumeration query
#' @param schema The schema used for extraction
#'
#' @return An object of class \code{asa_enumerate_result}
#'
#' @export
asa_enumerate_result <- function(data, status, stop_reason, metrics,
                                 provenance = NULL, plan = NULL,
                                 checkpoint_file = NULL, query = NULL,
                                 schema = NULL) {
  structure(
    list(
      data = data,
      status = status,
      stop_reason = stop_reason,
      metrics = metrics,
      provenance = provenance,
      plan = plan,
      checkpoint_file = checkpoint_file,
      query = query,
      schema = schema,
      created_at = Sys.time()
    ),
    class = "asa_enumerate_result"
  )
}

#' Print Method for asa_enumerate_result Objects
#'
#' @param x An asa_enumerate_result object
#' @param n Number of data rows to preview (default: 6)
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_enumerate_result
#' @export
print.asa_enumerate_result <- function(x, n = 6, ...) {
  cat("ASA Enumeration Result\n")
  cat("===================\n")
  cat("Status:      ", x$status, "\n", sep = "")
  cat("Stop Reason: ", x$stop_reason %||% "N/A", "\n", sep = "")
  cat("Items Found: ", nrow(x$data), "\n", sep = "")

  # Metrics summary
  if (!is.null(x$metrics)) {
    cat("\nMetrics:\n")
    if (!is.null(x$metrics$round_number)) {
      cat("  Rounds:        ", x$metrics$round_number, "\n", sep = "")
    }
    if (!is.null(x$metrics$queries_used)) {
      cat("  Queries Used:  ", x$metrics$queries_used, "\n", sep = "")
    }
    if (!is.null(x$metrics$elapsed_total)) {
      cat("  Time Elapsed:  ", format_duration(x$metrics$elapsed_total / 60), "\n", sep = "")
    }
    if (!is.null(x$metrics$novelty_rate)) {
      cat("  Novelty Rate:  ", sprintf("%.1f%%", x$metrics$novelty_rate * 100), "\n", sep = "")
    }
  }

  # Schema summary
  if (!is.null(x$schema)) {
    cat("\nSchema: ", paste(names(x$schema), collapse = ", "), "\n", sep = "")
  }

  # Data preview
  if (nrow(x$data) > 0) {
    cat("\nData Preview (first ", min(n, nrow(x$data)), " rows):\n", sep = "")
    print(utils::head(x$data, n))
  } else {
    cat("\n[No data]\n")
  }

  # Checkpoint info
  if (!is.null(x$checkpoint_file)) {
    cat("\nCheckpoint: ", x$checkpoint_file, "\n", sep = "")
  }

  invisible(x)
}

#' Summary Method for asa_enumerate_result Objects
#'
#' @param object An asa_enumerate_result object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_enumerate_result
#' @export
summary.asa_enumerate_result <- function(object, ...) {
  cat("Enumeration Result Summary\n")
  cat("-----------------------\n")
  cat("Query: ", truncate_string(object$query %||% "[unknown]", 60), "\n", sep = "")
  cat("Status: ", object$status, "\n", sep = "")
  cat("Stop Reason: ", object$stop_reason %||% "N/A", "\n", sep = "")
  cat("Total Items: ", nrow(object$data), "\n", sep = "")

  if (!is.null(object$schema)) {
    cat("Columns: ", paste(names(object$schema), collapse = ", "), "\n", sep = "")
  }

  if (!is.null(object$metrics)) {
    cat("\nExecution Metrics:\n")
    for (name in names(object$metrics)) {
      val <- object$metrics[[name]]
      if (is.numeric(val)) {
        if (grepl("time|elapsed", name, ignore.case = TRUE)) {
          cat("  ", name, ": ", format_duration(val / 60), "\n", sep = "")
        } else if (grepl("rate", name, ignore.case = TRUE)) {
          cat("  ", name, ": ", sprintf("%.2f", val), "\n", sep = "")
        } else {
          cat("  ", name, ": ", val, "\n", sep = "")
        }
      }
    }
  }

  # Provenance summary
  if (!is.null(object$provenance) && nrow(object$provenance) > 0) {
    cat("\nProvenance:\n")
    sources <- unique(object$provenance$source_url)
    cat("  Unique sources: ", length(sources), "\n", sep = "")
    avg_conf <- mean(object$provenance$confidence, na.rm = TRUE)
    cat("  Avg confidence: ", sprintf("%.2f", avg_conf), "\n", sep = "")
  }

  invisible(list(
    status = object$status,
    stop_reason = object$stop_reason,
    n_items = nrow(object$data),
    n_columns = ncol(object$data),
    metrics = object$metrics,
    has_provenance = !is.null(object$provenance)
  ))
}

#' Convert asa_enumerate_result to Data Frame
#'
#' @param x An asa_enumerate_result object
#' @param ... Additional arguments (ignored)
#'
#' @return The data data.frame from the result
#'
#' @method as.data.frame asa_enumerate_result
#' @export
as.data.frame.asa_enumerate_result <- function(x, ...) {
  x$data
}

#' Write asa_enumerate_result to CSV
#'
#' @param x An asa_enumerate_result object
#' @param file Path to output CSV file
#' @param include_provenance Include provenance as additional columns
#' @param ... Additional arguments passed to write.csv
#'
#' @return Invisibly returns the file path
#'
#' @export
write_csv.asa_enumerate_result <- function(x, file, include_provenance = FALSE, ...) {
  data_to_write <- x$data

  if (include_provenance && !is.null(x$provenance)) {
    # Merge provenance columns
    prov <- x$provenance
    names(prov) <- paste0("_prov_", names(prov))
    if (nrow(prov) == nrow(data_to_write)) {
      data_to_write <- cbind(data_to_write, prov)
    }
  }

  utils::write.csv(data_to_write, file, row.names = FALSE, ...)
  message("Written to: ", file)
  invisible(file)
}

#' Constructor for asa_audit_result Objects
#'
#' Creates an S3 object representing the result of a data quality audit.
#'
#' @param data data.frame with original data plus audit columns (_audit_flag, _audit_notes)
#' @param audit_summary Character string with high-level findings
#' @param issues List of identified issues with severity and descriptions
#' @param recommendations Character vector of suggested remediation queries
#' @param completeness_score Numeric 0-1 score for data completeness
#' @param consistency_score Numeric 0-1 score for data consistency
#' @param backend_used Which backend performed the audit ("claude_code" or "langgraph")
#' @param elapsed_time Execution time in seconds
#' @param query The original query (if available)
#' @param checks Character vector of checks that were performed
#'
#' @return An object of class \code{asa_audit_result}
#'
#' @export
asa_audit_result <- function(data, audit_summary, issues, recommendations,
                             completeness_score, consistency_score,
                             backend_used, elapsed_time,
                             query = NULL, checks = NULL) {
  structure(
    list(
      data = data,
      audit_summary = audit_summary,
      issues = issues,
      recommendations = recommendations,
      completeness_score = completeness_score,
      consistency_score = consistency_score,
      backend_used = backend_used,
      elapsed_time = elapsed_time,
      query = query,
      checks = checks,
      created_at = Sys.time()
    ),
    class = "asa_audit_result"
  )
}

#' Print Method for asa_audit_result Objects
#'
#' @param x An asa_audit_result object
#' @param n Number of data rows to preview (default: 6)
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_audit_result
#' @export
print.asa_audit_result <- function(x, n = 6, ...) {
  cat("ASA Audit Result\n")
  cat("================\n")

  # Scores
  cat("Completeness: ", sprintf("%.0f%%", x$completeness_score * 100), "\n", sep = "")
  cat("Consistency:  ", sprintf("%.0f%%", x$consistency_score * 100), "\n", sep = "")
  cat("Backend:      ", x$backend_used, "\n", sep = "")
  cat("Time:         ", sprintf("%.1fs", x$elapsed_time), "\n", sep = "")

  # Summary
  if (!is.null(x$audit_summary) && x$audit_summary != "") {
    cat("\nSummary:\n")
    summary_lines <- strwrap(x$audit_summary, width = 74)
    cat(paste("  ", summary_lines, collapse = "\n"), "\n", sep = "")
  }

  # Issues
  if (length(x$issues) > 0) {
    cat("\nIssues Found: ", length(x$issues), "\n", sep = "")
    for (i in seq_along(x$issues)) {
      issue <- x$issues[[i]]
      severity <- issue$severity %||% "info"
      desc <- issue$description %||% issue$message %||% "[no description]"
      cat("  [", toupper(severity), "] ", truncate_string(desc, 60), "\n", sep = "")
      if (i >= 5 && length(x$issues) > 5) {
        cat("  ... and ", length(x$issues) - 5, " more\n", sep = "")
        break
      }
    }
  } else {
    cat("\nNo issues found.\n")
  }

  # Recommendations
  if (length(x$recommendations) > 0) {
    cat("\nRecommendations:\n")
    for (i in seq_along(x$recommendations)) {
      cat("  - ", truncate_string(x$recommendations[[i]], 70), "\n", sep = "")
      if (i >= 3 && length(x$recommendations) > 3) {
        cat("  ... and ", length(x$recommendations) - 3, " more\n", sep = "")
        break
      }
    }
  }

  # Data preview with audit flags
  if (nrow(x$data) > 0) {
    # Count by flag
    if ("_audit_flag" %in% names(x$data)) {
      flag_counts <- table(x$data[["_audit_flag"]])
      cat("\nFlag Distribution:\n")
      for (flag in names(flag_counts)) {
        cat("  ", flag, ": ", flag_counts[[flag]], "\n", sep = "")
      }
    }

    cat("\nData Preview (first ", min(n, nrow(x$data)), " rows):\n", sep = "")
    # Show key columns including audit flags
    preview_cols <- names(x$data)
    if (length(preview_cols) > 6) {
      # Prioritize audit columns and first few data columns
      audit_cols <- grep("^_audit", preview_cols, value = TRUE)
      data_cols <- setdiff(preview_cols, audit_cols)[1:min(4, length(setdiff(preview_cols, audit_cols)))]
      preview_cols <- c(data_cols, audit_cols)
    }
    print(utils::head(x$data[, preview_cols, drop = FALSE], n))
  }

  invisible(x)
}

#' Summary Method for asa_audit_result Objects
#'
#' @param object An asa_audit_result object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_audit_result
#' @export
summary.asa_audit_result <- function(object, ...) {
  cat("Audit Result Summary\n")
  cat("--------------------\n")
  if (!is.null(object$query)) {
    cat("Query: ", truncate_string(object$query, 60), "\n", sep = "")
  }
  cat("Checks Performed: ", paste(object$checks %||% "all", collapse = ", "), "\n", sep = "")
  cat("Backend: ", object$backend_used, "\n", sep = "")
  cat("Time: ", sprintf("%.1fs", object$elapsed_time), "\n", sep = "")

  cat("\nScores:\n")
  cat("  Completeness: ", sprintf("%.1f%%", object$completeness_score * 100), "\n", sep = "")
  cat("  Consistency:  ", sprintf("%.1f%%", object$consistency_score * 100), "\n", sep = "")

  cat("\nData:\n")
  cat("  Total Rows: ", nrow(object$data), "\n", sep = "")

  if ("_audit_flag" %in% names(object$data)) {
    flag_counts <- table(object$data[["_audit_flag"]])
    cat("  Flags:\n")
    for (flag in names(flag_counts)) {
      pct <- flag_counts[[flag]] / nrow(object$data) * 100
      cat("    ", flag, ": ", flag_counts[[flag]], " (", sprintf("%.1f%%", pct), ")\n", sep = "")
    }
  }

  cat("\nIssues: ", length(object$issues), "\n", sep = "")
  if (length(object$issues) > 0) {
    severities <- sapply(object$issues, function(x) x$severity %||% "info")
    sev_table <- table(severities)
    for (sev in names(sev_table)) {
      cat("  ", sev, ": ", sev_table[[sev]], "\n", sep = "")
    }
  }

  cat("Recommendations: ", length(object$recommendations), "\n", sep = "")

  invisible(list(
    completeness_score = object$completeness_score,
    consistency_score = object$consistency_score,
    n_rows = nrow(object$data),
    n_issues = length(object$issues),
    n_recommendations = length(object$recommendations),
    backend = object$backend_used,
    elapsed_time = object$elapsed_time
  ))
}

#' Convert asa_audit_result to Data Frame
#'
#' @param x An asa_audit_result object
#' @param ... Additional arguments (ignored)
#'
#' @return The audited data.frame with audit columns
#'
#' @method as.data.frame asa_audit_result
#' @export
as.data.frame.asa_audit_result <- function(x, ...) {
  x$data
}
