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
#' @param verbose Print progress messages (default: FALSE)
#'
#' @return An \code{asa_result} object with:
#'   \itemize{
#'     \item prompt: The original prompt
#'     \item message: The agent's response text
#'     \item parsed: Parsed output (list for JSON/field extraction, NULL for text/raw)
#'     \item raw_output: Full agent trace (always included, verbose for "raw" format)
#'     \item elapsed_time: Execution time in minutes
#'     \item status: "success" or "error"
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
                     verbose = FALSE) {

  config_search <- NULL
  config_conda_env <- NULL

  # Extract settings from config if provided
  if (!is.null(config) && inherits(config, "asa_config")) {
    # Config temporal overrides direct temporal parameter
    if (is.null(temporal) && !is.null(config$temporal)) {
      temporal <- config$temporal
    }
    config_search <- config$search
    config_conda_env <- config$conda_env
  }

  # Convert asa_temporal to list for internal functions
  if (inherits(temporal, "asa_temporal")) {
    temporal <- as.list(temporal)
  }

  # Validate inputs
  .validate_run_task(
    prompt = prompt,
    output_format = output_format,
    agent = agent,
    verbose = verbose
  )

  # Initialize agent from config if provided
  if (is.null(agent) && !is.null(config) && inherits(config, "asa_config")) {
    current <- if (.is_initialized()) get_agent() else NULL
    if (!is.null(current)) {
      same_backend <- identical(current$backend, config$backend)
      same_model <- identical(current$model, config$model)
      same_conda <- identical(current$config$conda_env, config$conda_env)
      same_proxy <- identical(current$config$proxy, config$proxy)
      same_folding <- identical(current$config$use_memory_folding, config$memory_folding)
      same_threshold <- identical(current$config$memory_threshold, config$memory_threshold)
      same_keep <- identical(current$config$memory_keep_recent, config$memory_keep_recent)
      same_rate <- identical(current$config$rate_limit, config$rate_limit)
      same_timeout <- identical(current$config$timeout, config$timeout)

      if (same_backend && same_model && same_conda && same_proxy &&
          same_folding && same_threshold && same_keep &&
          same_rate && same_timeout) {
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
        use_memory_folding = config$memory_folding,
        memory_threshold = config$memory_threshold,
        memory_keep_recent = config$memory_keep_recent,
        rate_limit = config$rate_limit,
        timeout = config$timeout,
        verbose = verbose
      )
    }
  }

  # Validate temporal if provided
  .validate_temporal(temporal)

  # Augment prompt with temporal hints if dates specified
  augmented_prompt <- .augment_prompt_temporal(prompt, temporal, verbose = verbose)

  start_time <- Sys.time()

  # Run agent with temporal filtering applied
  conda_env <- config_conda_env %||% .get_default_conda_env()
  response <- .with_search_config(config_search, conda_env = conda_env, function() {
    .with_temporal(temporal, function() {
      .run_agent(augmented_prompt, agent = agent, verbose = verbose)
    })
  })

  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))

  # Parse output based on format
  parsed <- NULL
  status <- if (response$status_code == ASA_STATUS_SUCCESS) "success" else "error"

  if (status == "success" && !is.na(response$message)) {
    if (identical(output_format, "json")) {
      parsed <- .parse_json_response(response$message)
    } else if (is.character(output_format) && length(output_format) > 1) {
      # Extract specific fields
      parsed <- .extract_fields(response$message, output_format)
    }
  }

  # Build result object - always return asa_result for consistent API
  result <- asa_result(
    prompt = prompt,
    message = response$message,
    parsed = parsed,
    raw_output = response$trace,
    elapsed_time = elapsed,
    status = status
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
    # Clean escape sequences
    json_str <- gsub("\\\\n", "\n", json_str)
    json_str <- gsub('\\\\"', '"', json_str)
    json_str <- gsub("\\\\'", "'", json_str)
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
#' Executes multiple research tasks, optionally in parallel.
#'
#' @param prompts Character vector of task prompts, or a data frame with a
#'   'prompt' column
#' @param output_format Expected output format (applies to all tasks)
#' @param temporal Named list for temporal filtering (applies to all tasks).
#'   See \code{\link{run_task}} for details.
#' @param agent An asa_agent object
#' @param parallel Use parallel processing
#' @param workers Number of parallel workers
#' @param progress Show progress messages
#'
#' @return A list of asa_result objects, or if prompts was a data frame,
#'   the data frame with result columns added
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
#' }
#'
#' @seealso \code{\link{run_task}}, \code{\link{configure_temporal}}
#'
#' @export
run_task_batch <- function(prompts,
                           output_format = "text",
                           temporal = NULL,
                           agent = NULL,
                           parallel = FALSE,
                           workers = 4L,
                           progress = TRUE) {

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

  # Handle data frame input
  is_df <- is.data.frame(prompts)
  if (is_df) {
    prompt_vec <- prompts$prompt
  } else {
    prompt_vec <- prompts
  }

  n <- length(prompt_vec)

  # Function to process one task
  process_one <- function(i) {
    run_task(
      prompt = prompt_vec[i],
      output_format = output_format,
      temporal = temporal,
      agent = agent,
      verbose = FALSE
    )
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

    results <- future.apply::future_lapply(
      seq_len(n),
      process_one,
      future.seed = TRUE
    )
  } else {
    results <- vector("list", n)
    for (i in seq_len(n)) {
      if (progress) message(sprintf("[%d/%d] Processing...", i, n))
      results[[i]] <- process_one(i)
    }
  }

  # Return as data frame if input was data frame
  if (is_df) {
    prompts$response <- vapply(results, function(r) r$message %||% NA_character_, character(1))
    prompts$status <- vapply(results, function(r) r$status %||% NA_character_, character(1))
    prompts$elapsed_time <- vapply(results, function(r) r$elapsed_time %||% NA_real_, numeric(1))

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
        for (field in names(first_parsed)) {
          prompts[[field]] <- vapply(results, function(r) {
            if (!is.null(r$parsed) && field %in% names(r$parsed)) {
              as.character(r$parsed[[field]])
            } else {
              NA_character_
            }
          }, character(1))
        }
      }
    }

    return(prompts)
  }

  results
}
