#' Run a Structured Task with the Agent
#'
#' Executes a research task using the AI search agent with a structured prompt
#' and returns parsed results.
#'
#' @param prompt The task prompt or question for the agent to research
#' @param output_format Expected output format. One of: "text" (raw response),
#'   "json" (parse as JSON), or a character vector of field names to extract
#' @param agent An asa_agent object from \code{\link{initialize_agent}}, or
#'   NULL to use the currently initialized agent
#' @param verbose Print progress messages (default: FALSE)
#'
#' @return An object of class \code{asa_result} with components:
#' \itemize{
#'   \item prompt: The original prompt
#'   \item message: The agent's response text
#'   \item parsed: Parsed output (if output_format specified)
#'   \item raw_output: Full agent trace
#'   \item elapsed_time: Execution time in minutes
#'   \item status: "success" or "error"
#' }
#'
#' @details
#' This function provides a high-level interface for running research tasks.
#' For simple text responses, use \code{output_format = "text"}. For structured
#' outputs, use \code{output_format = "json"} or specify field names to extract.
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
#' }
#'
#' @seealso \code{\link{initialize_agent}}, \code{\link{run_agent}},
#'   \code{\link{run_task_batch}}
#'
#' @export
run_task <- function(prompt,
                     output_format = "text",
                     agent = NULL,
                     verbose = FALSE) {

  # Validate inputs

  if (missing(prompt) || !is.character(prompt) || length(prompt) != 1) {
    stop("'prompt' must be a single character string", call. = FALSE)
  }

  start_time <- Sys.time()

  # Run agent
  response <- run_agent(prompt, agent = agent, verbose = verbose)

  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))


  # Parse output based on format
  parsed <- NULL
  status <- if (response$status_code == 200) "success" else "error"

  if (status == "success" && !is.na(response$message)) {
    if (identical(output_format, "json")) {
      parsed <- .parse_json_response(response$message)
    } else if (is.character(output_format) && length(output_format) > 1) {
      # Extract specific fields
      parsed <- .extract_fields(response$message, output_format)
    }
  }

  # Return result object
  asa_result(
    prompt = prompt,
    message = response$message,
    parsed = parsed,
    raw_output = response$trace,
    elapsed_time = elapsed,
    status = status
  )
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
  args <- list(...)

  if (length(args) == 0) {
    return(template)
  }

  # Validate that template is a non-empty string
  if (!is.character(template) || length(template) != 1 || nchar(template) == 0) {
    stop("'template' must be a non-empty character string", call. = FALSE)
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

#' Parse JSON Response
#' @param response_text Response text from agent
#' @keywords internal
.parse_json_response <- function(response_text) {
  if (is.na(response_text) || response_text == "") {
    return(NULL)
  }

  # Try to extract JSON from response
  json_str <- .extract_json_object(response_text)
  if (is.null(json_str)) {
    return(NULL)
  }

  # Parse JSON
  tryCatch({
    jsonlite::fromJSON(json_str)
  }, error = function(e) {
    NULL
  })
}

#' Extract JSON Object from Text
#' @param text Response text
#' @keywords internal
.extract_json_object <- function(text) {
  # Try to find JSON object in text
  # Look for outermost braces
  start <- regexpr("\\{", text)
  if (start == -1) return(NULL)

  # Count braces to find matching close
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
      if (char == "{") depth <- depth + 1
      if (char == "}") {
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
#' }
#'
#' @export
run_task_batch <- function(prompts,
                           output_format = "text",
                           agent = NULL,
                           parallel = FALSE,
                           workers = 4L,
                           progress = TRUE) {

  # Handle data frame input
  is_df <- is.data.frame(prompts)
  if (is_df) {
    if (!"prompt" %in% names(prompts)) {
      stop("Data frame must have a 'prompt' column", call. = FALSE)
    }
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
