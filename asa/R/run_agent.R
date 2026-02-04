#' Run the ASA Agent (Internal)
#'
#' Internal function that invokes the search agent with a prompt.
#' Users should use \code{\link{run_task}} instead.
#'
#' @param prompt The prompt to send to the agent
#' @param agent An asa_agent object
#' @param recursion_limit Maximum number of agent steps
#' @param verbose Print status messages
#'
#' @return An object of class \code{asa_response}
#'
#' @keywords internal
.run_agent <- function(prompt,
                       agent = NULL,
                       recursion_limit = NULL,
                       expected_schema = NULL,
                       thread_id = NULL,
                       verbose = FALSE) {

  # Validate inputs
  .validate_run_agent(
    prompt = prompt,
    agent = agent,
    recursion_limit = recursion_limit,
    expected_schema = expected_schema,
    verbose = verbose,
    thread_id = thread_id
  )

  # Get or initialize agent
  if (is.null(agent)) {
    if (!.is_initialized()) {
      stop("Agent not initialized. Call initialize_agent() first or pass an agent object.",
           call. = FALSE)
    }
    agent <- get_agent()
  }

  # Get config
  config <- agent$config
  use_memory_folding <- (config$use_memory_folding %||% config$memory_folding) %||% TRUE

  # Set recursion limit based on agent type
  if (is.null(recursion_limit)) {
    recursion_limit <- if (use_memory_folding) {
      ASA_RECURSION_LIMIT_FOLDING
    } else {
      ASA_RECURSION_LIMIT_STANDARD
    }
  }

  if (verbose) message("Running agent...")
  t0 <- Sys.time()

  # Build initial state and invoke agent
  raw_response <- tryCatch({
    if (use_memory_folding) {
      .invoke_memory_folding_agent(agent$python_agent, prompt, recursion_limit, expected_schema, thread_id)
    } else {
      .invoke_standard_agent(agent$python_agent, prompt, recursion_limit, expected_schema, thread_id)
    }
  }, error = function(e) {
    structure(list(error = e$message), class = "asa_error")
  })

  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
  if (verbose) message(sprintf("  Agent completed in %.2f minutes", elapsed))

  # Handle errors
  if (inherits(raw_response, "asa_error")) {
    return(asa_response(
      message = NA_character_,
      status_code = ASA_STATUS_ERROR,
      raw_response = raw_response,
      trace = raw_response$error,
      elapsed_time = elapsed,
      fold_count = 0L,
      prompt = prompt
    ))
  }

  # Extract response text
  response_text <- .extract_response_text(raw_response, config$backend)

  # Build trace
  trace <- .build_trace(raw_response)
  trace_json <- .build_trace_json(raw_response)

  # Handle rate limiting and timeouts
  .handle_response_issues(trace, verbose)

  # Extract memory folding stats
  fold_count <- 0L
  if (use_memory_folding) {
    fold_count <- tryCatch(
      as.integer(raw_response$fold_count %||% 0),
      error = function(e) 0L
    )
  }

  # Return response object
  asa_response(
    message = response_text,
    status_code = if (is.na(response_text)) ASA_STATUS_ERROR else ASA_STATUS_SUCCESS,
    raw_response = raw_response,
    trace = trace,
    trace_json = trace_json,
    elapsed_time = elapsed,
    fold_count = fold_count,
    prompt = prompt
  )
}

# NOTE: run_agent() has been removed from public API.
# Use run_task(..., output_format = "raw") instead for full trace access.
# The internal .run_agent() function above is used by run_task() internally.

#' Invoke Memory Folding Agent
#' @keywords internal
.invoke_memory_folding_agent <- function(python_agent, prompt, recursion_limit, expected_schema = NULL, thread_id = NULL) {
  # Import message type
  from_schema <- reticulate::import("langchain_core.messages")
  initial_message <- from_schema$HumanMessage(content = prompt)

  initial_state <- list(messages = list(initial_message))

  if (!is.null(expected_schema)) {
    initial_state$expected_schema <- expected_schema
    initial_state$expected_schema_source <- "explicit"
  }

  # Only seed summary/fold_count when starting a fresh (ephemeral) thread.
  if (is.null(thread_id)) {
    initial_state$summary <- ""
    initial_state$fold_count <- 0L
  }

  python_agent$invoke(
    initial_state,
    config = list(
      configurable = list(thread_id = thread_id %||% rlang::hash(Sys.time())),
      recursion_limit = as.integer(recursion_limit)
    )
  )
}

#' Invoke Standard Agent
#' @keywords internal
.invoke_standard_agent <- function(python_agent, prompt, recursion_limit, expected_schema = NULL, thread_id = NULL) {
  initial_state <- list(messages = list(list(role = "user", content = prompt)))
  if (!is.null(expected_schema)) {
    initial_state$expected_schema <- expected_schema
    initial_state$expected_schema_source <- "explicit"
  }

  python_agent$invoke(
    initial_state,
    config = list(
      configurable = list(thread_id = thread_id %||% rlang::hash(Sys.time())),
      recursion_limit = as.integer(recursion_limit)
    )
  )
}

#' Extract Response Text from Raw Response
#' @keywords internal
.extract_response_text <- function(raw_response, backend) {
  response_text <- tryCatch({
    messages <- raw_response$messages
    if (length(messages) > 0) {

      extract_text_blocks <- function(value) {
        if (is.null(value) || length(value) == 0) {
          return(character(0))
        }
        if (is.character(value)) {
          return(as.character(value))
        }
        if (is.list(value)) {
          value_names <- names(value)
          if (!is.null(value_names) && any(value_names %in% c("text", "content", "value"))) {
            block <- character(0)
            if (!is.null(value$text)) block <- c(block, as.character(value$text))
            if (!is.null(value$content)) block <- c(block, as.character(value$content))
            if (!is.null(value$value)) block <- c(block, as.character(value$value))
            block <- block[!is.na(block) & nzchar(block)]
            return(block)
          }
          parts <- character(0)
          for (item in value) {
            if (is.character(item)) {
              parts <- c(parts, as.character(item))
            } else if (is.list(item)) {
              if (!is.null(item$text)) {
                parts <- c(parts, as.character(item$text))
              } else if (!is.null(item$content)) {
                parts <- c(parts, as.character(item$content))
              } else if (!is.null(item$value)) {
                parts <- c(parts, as.character(item$value))
              }
            }
          }
          parts <- parts[!is.na(parts) & nzchar(parts)]
          return(parts)
        }
        tryCatch(as.character(value), error = function(e) character(0))
      }

      # Helper to check if a message is an AIMessage
      is_ai_message <- function(msg) {
        msg_type <- tryCatch(as.character(msg$`__class__`$`__name__`), error = function(e) "")
        msg_type == "AIMessage"
      }

      # Helper to extract content from a message
      get_message_content <- function(msg) {
        text <- tryCatch(msg$text, error = function(e) NULL)
        if (is.null(text) || length(text) == 0) {
          text <- tryCatch(msg$content, error = function(e) NULL)
        }
        text
      }

      # Try extracting from last message first
      last_message <- messages[[length(messages)]]
      text <- get_message_content(last_message)
      text_parts <- extract_text_blocks(text)

      # If last message has no content, search backwards for last AIMessage with content
      if (length(text_parts) == 0) {
        for (i in rev(seq_along(messages))) {
          msg <- messages[[i]]
          if (is_ai_message(msg)) {
            text <- get_message_content(msg)
            text_parts <- extract_text_blocks(text)
            if (length(text_parts) > 0) break
          }
        }
      }

      # If still no content, return a meaningful message based on stop_reason
      if (length(text_parts) == 0) {
        stop_reason <- tryCatch(raw_response$stop_reason, error = function(e) NULL)
        error_msg <- tryCatch(as.character(raw_response$error), error = function(e) "")

        # Detect recursion limit from stop_reason OR error message
        is_recursion_limit <- (!is.null(stop_reason) && stop_reason == "recursion_limit") ||
                              grepl("recursion", error_msg, ignore.case = TRUE) ||
                              grepl("GraphRecursionError", error_msg, ignore.case = TRUE)

        if (is_recursion_limit) {
          return("[Agent reached step limit before completing task. Increase recursion_limit or simplify the task.]")
        }
        return(NA_character_)
      }

      text <- paste(text_parts, collapse = "\n")
      if (is.na(text) || !nzchar(text)) {
        return(NA_character_)
      }

      # Clean XML tags without stripping inner content
      text <- gsub("</?[^>]+>", "", text)

      # Handle exo backend format
      if (!is.null(backend) && backend == "exo") {
        text <- sub(
          "(?s).*<\\|python_tag\\|>(\\{.*?\\})<\\|eom_id\\|>.*",
          "\\1", text, perl = TRUE
        )
      }
      text
    } else {
      NA_character_
    }
  }, error = function(e) {
    NA_character_
  })

  response_text
}

#' Build Trace from Raw Response
#' @keywords internal
.strip_trace_noise <- function(x) {
  tryCatch({
    if (!is.list(x)) {
      return(x)
    }
    nm <- names(x)
    if (!is.null(nm)) {
      drop <- nm %in% c("__gemini_function_call_thought_signatures__")
      if (any(drop)) {
        x <- x[!drop]
        nm <- names(x)
      }
    }
    x <- lapply(x, .strip_trace_noise)
    if (!is.null(nm)) {
      names(x) <- nm
    }
    x
  }, error = function(e) x)
}

.build_trace <- function(raw_response) {
  tryCatch({
    cleaned <- .strip_trace_noise(raw_response)
    paste(
      lapply(unlist(cleaned), function(l) capture.output(l)),
      collapse = "\n\n"
    )
  }, error = function(e) {
    ""
  })
}

#' Build Structured Trace JSON from Raw Response
#' @keywords internal
.build_trace_json <- function(raw_response) {
  tryCatch({
    messages <- tryCatch(raw_response$messages, error = function(e) NULL)
    if (is.null(messages) || length(messages) == 0) {
      return("")
    }

    msg_type <- function(msg) {
      # Prefer __class__.__name__ when available
      type_name <- tryCatch(
        as.character(msg$`__class__`$`__name__`),
        error = function(e) NA_character_
      )
      if (!is.na(type_name) && nzchar(type_name)) {
        return(type_name)
      }
      # Fallback to "type" field if present (e.g., "ai", "human", "tool")
      tryCatch(as.character(msg$type), error = function(e) NA_character_)
    }

    msg_content <- function(msg) {
      content <- tryCatch(msg$content, error = function(e) NULL)
      if (is.null(content)) return("")
      # Content may be a list of blocks; collapse to a string
      if (is.character(content)) {
        return(paste(content, collapse = "\n"))
      }
      if (is.list(content)) {
        parts <- character(0)
        for (item in content) {
          if (is.character(item)) {
            parts <- c(parts, item)
          } else if (is.list(item)) {
            if (!is.null(item$text)) parts <- c(parts, as.character(item$text))
            if (!is.null(item$content)) parts <- c(parts, as.character(item$content))
            if (!is.null(item$value)) parts <- c(parts, as.character(item$value))
          }
        }
        parts <- parts[!is.na(parts) & nzchar(parts)]
        return(paste(parts, collapse = "\n"))
      }
      tryCatch(as.character(content), error = function(e) "")
    }

    msg_name <- function(msg) {
      name <- tryCatch(msg$name, error = function(e) NULL)
      if (is.null(name)) return(NULL)
      name <- tryCatch(as.character(name), error = function(e) NULL)
      if (is.null(name) || !nzchar(name)) return(NULL)
      name
    }

    msg_tool_calls <- function(msg) {
      tool_calls <- tryCatch(msg$tool_calls, error = function(e) NULL)
      if (is.null(tool_calls)) return(NULL)
      tryCatch(reticulate::py_to_r(tool_calls), error = function(e) NULL)
    }

    out_messages <- lapply(messages, function(msg) {
      list(
        message_type = msg_type(msg),
        name = msg_name(msg),
        content = msg_content(msg),
        tool_calls = msg_tool_calls(msg)
      )
    })

    jsonlite::toJSON(
      list(
        format = "asa_trace_v1",
        messages = out_messages
      ),
      auto_unbox = TRUE,
      null = "null"
    )
  }, error = function(e) {
    ""
  })
}

#' Handle Response Issues (Rate Limiting, Timeouts)
#' @keywords internal
.handle_response_issues <- function(trace, verbose) {
  if (grepl("Ratelimit", trace, ignore.case = TRUE)) {
    if (verbose) warning("Rate limit triggered, waiting ", ASA_RATE_LIMIT_WAIT, " seconds...")
    Sys.sleep(ASA_RATE_LIMIT_WAIT)
  }
  if (grepl("Timeout", trace, ignore.case = TRUE)) {
    if (verbose) warning("Timeout triggered, waiting ", ASA_RATE_LIMIT_WAIT, " seconds...")
    Sys.sleep(ASA_RATE_LIMIT_WAIT)
  }
  invisible(NULL)
}

# NOTE: run_agent_batch() has been removed from public API.
# Use run_task_batch(..., output_format = "raw") instead for full trace access.
