#' Constructor for asa_agent Objects
#'
#' Creates an S3 object representing an initialized ASA search agent.
#'
#' @param python_agent The underlying Python agent object
#' @param backend LLM backend name (e.g., "openai", "groq")
#' @param model Model identifier
#' @param config Agent configuration list
#'
#' @return An object of class \code{asa_agent}
#'
#' @export
asa_agent <- function(python_agent, backend, model, config) {
  structure(
    list(
      python_agent = python_agent,
      backend = backend,
      model = model,
      config = config,
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
#'
#' @return An object of class \code{asa_result}
#'
#' @export
asa_result <- function(prompt, message, parsed, raw_output, elapsed_time, status) {
  structure(
    list(
      prompt = prompt,
      message = message,
      parsed = parsed,
      raw_output = raw_output,
      elapsed_time = elapsed_time,
      status = status
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
