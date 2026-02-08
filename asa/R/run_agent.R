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
                       field_status = NULL,
                       budget_state = NULL,
                       search_budget_limit = NULL,
                       unknown_after_searches = NULL,
                       finalize_on_all_fields_resolved = NULL,
                       verbose = FALSE) {

  # Validate inputs
  .validate_run_agent(
    prompt = prompt,
    agent = agent,
    recursion_limit = recursion_limit,
    expected_schema = expected_schema,
    field_status = field_status,
    budget_state = budget_state,
    search_budget_limit = search_budget_limit,
    unknown_after_searches = unknown_after_searches,
    finalize_on_all_fields_resolved = finalize_on_all_fields_resolved,
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

  # Resolve recursion limit with precedence:
  # run_task arg > agent/config default > mode-specific fallback.
  recursion_limit <- .resolve_effective_recursion_limit(
    recursion_limit = recursion_limit,
    config = config,
    use_memory_folding = use_memory_folding
  )

  if (verbose) message("Running agent...")
  t0 <- Sys.time()
  resolved_thread_id <- .resolve_thread_id(thread_id)
  invoke_max_attempts <- as.integer(config$invoke_max_attempts %||% ASA_DEFAULT_INVOKE_MAX_ATTEMPTS)
  invoke_retry_delay <- as.numeric(config$invoke_retry_delay %||% ASA_DEFAULT_INVOKE_RETRY_DELAY)
  invoke_retry_backoff <- as.numeric(config$invoke_retry_backoff %||% ASA_DEFAULT_INVOKE_RETRY_BACKOFF)
  invoke_retry_jitter <- as.numeric(config$invoke_retry_jitter %||% ASA_DEFAULT_INVOKE_RETRY_JITTER)
  invoke_max_attempts <- max(1L, invoke_max_attempts)

  # Build initial state and invoke agent
  invoke_output <- tryCatch({
    .invoke_agent_with_retry(
      invoke_fn = function() {
        if (use_memory_folding) {
          .invoke_memory_folding_agent(
            python_agent = agent$python_agent,
            prompt = prompt,
            recursion_limit = recursion_limit,
            expected_schema = expected_schema,
            thread_id = resolved_thread_id,
            field_status = field_status,
            budget_state = budget_state,
            search_budget_limit = search_budget_limit,
            unknown_after_searches = unknown_after_searches,
            finalize_on_all_fields_resolved = finalize_on_all_fields_resolved
          )
        } else {
          .invoke_standard_agent(
            python_agent = agent$python_agent,
            prompt = prompt,
            recursion_limit = recursion_limit,
            expected_schema = expected_schema,
            thread_id = resolved_thread_id,
            field_status = field_status,
            budget_state = budget_state,
            search_budget_limit = search_budget_limit,
            unknown_after_searches = unknown_after_searches,
            finalize_on_all_fields_resolved = finalize_on_all_fields_resolved
          )
        }
      },
      max_attempts = invoke_max_attempts,
      retry_delay = invoke_retry_delay,
      retry_backoff = invoke_retry_backoff,
      retry_jitter = invoke_retry_jitter,
      verbose = verbose
    )
  }, error = function(e) {
    structure(list(error = e$message, thread_id = resolved_thread_id), class = "asa_error")
  })
  raw_response <- if (inherits(invoke_output, "asa_error")) invoke_output else invoke_output$response
  if (!inherits(invoke_output, "asa_error")) {
    resolved_thread_id <- invoke_output$thread_id %||% resolved_thread_id
  }

  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
  if (verbose) message(sprintf("  Agent completed in %.2f minutes", elapsed))

  # Handle errors
  if (inherits(raw_response, "asa_error")) {
    error_fold_stats <- .try_or(as.list(raw_response$fold_stats %||% list()), list())
    error_fold_stats$fold_count <- paste0(
      "Error before fold count could be determined: ", raw_response$error
    )
    return(asa_response(
      message = NA_character_,
      status_code = ASA_STATUS_ERROR,
      raw_response = raw_response,
      trace = raw_response$error,
      elapsed_time = elapsed,
      fold_stats = error_fold_stats,
      prompt = prompt,
      thread_id = raw_response$thread_id %||% resolved_thread_id,
      stop_reason = NA_character_,
      budget_state = list(),
      field_status = list(),
      json_repair = list(),
      tokens_used = NA_integer_
    ))
  }

  # Extract response text
  response_text <- .extract_response_text(raw_response, config$backend)
  invoke_exception_fallback <- .has_invoke_exception_fallback(raw_response)

  # Build trace
  trace <- .build_trace(raw_response)
  trace_json <- .build_trace_json(raw_response)

  # Handle rate limiting and timeouts
  .handle_response_issues(trace, verbose)
  stop_reason <- .extract_stop_reason(raw_response)
  budget_state_out <- .coerce_py_list(raw_response$budget_state)
  field_status_out <- .coerce_py_list(raw_response$field_status)
  json_repair <- .coerce_py_list(raw_response$json_repair)

  # Extract memory folding stats (fold_count lives inside fold_stats)
  fold_stats <- list()
  if (use_memory_folding) {
    fold_stats <- .try_or({
      raw_stats <- raw_response$fold_stats
      if (!is.null(raw_stats)) as.list(raw_stats) else list()
    }, list())
    # Ensure fold_count is present as integer
    if (is.null(fold_stats$fold_count)) {
      fold_stats$fold_count <- 0L
    } else {
      fold_stats$fold_count <- as.integer(fold_stats$fold_count)
    }
  } else {
    fold_stats$fold_count <- 0L
  }

  # Extract token usage from Python state
  tokens_used <- .as_scalar_int(.try_or(raw_response$tokens_used, NA_integer_))

  # Return response object
  asa_response(
    message = response_text,
    status_code = if (is.na(response_text) || invoke_exception_fallback) {
      ASA_STATUS_ERROR
    } else {
      ASA_STATUS_SUCCESS
    },
    raw_response = raw_response,
    trace = trace,
    trace_json = trace_json,
    elapsed_time = elapsed,
    fold_stats = fold_stats,
    prompt = prompt,
    thread_id = resolved_thread_id,
    stop_reason = stop_reason,
    budget_state = budget_state_out,
    field_status = field_status_out,
    json_repair = json_repair,
    tokens_used = tokens_used
  )
}

# NOTE: run_agent() has been removed from public API.
# Use run_task(..., output_format = "raw") instead for full trace access.
# The internal .run_agent() function above is used by run_task() internally.

#' Resolve Thread ID for Agent Invocation
#' @importFrom stats runif
#' @keywords internal
.resolve_thread_id <- function(thread_id = NULL) {
  if (!is.null(thread_id)) {
    return(thread_id)
  }
  paste0("asa_", substr(rlang::hash(list(Sys.time(), runif(1))), 1, 16))
}

#' Coerce Python Dict/List to Native R List
#' @keywords internal
.coerce_py_list <- function(value) {
  if (is.null(value)) {
    return(list())
  }
  converted <- .try_or(reticulate::py_to_r(value), value)
  if (is.null(converted)) {
    return(list())
  }
  if (is.list(converted)) {
    return(converted)
  }
  list(value = converted)
}

#' Extract Stop Reason from Raw Response
#' @keywords internal
.extract_stop_reason <- function(raw_response) {
  stop_reason <- .try_or(as.character(raw_response$stop_reason), character(0))
  if (length(stop_reason) == 0 || !nzchar(stop_reason[[1]])) {
    return(NA_character_)
  }
  stop_reason[[1]]
}

#' Determine Whether Agent Invoke Error Is Retryable
#' @keywords internal
.is_retryable_invoke_error <- function(err) {
  msg <- tolower(conditionMessage(err) %||% "")
  if (!nzchar(msg)) {
    return(FALSE)
  }

  # Permanent failures we should not retry.
  if (grepl(
    "invalid api key|authentication|unauthoriz|forbidden|permission|validation|bad request|unsupported|not found",
    msg
  )) {
    return(FALSE)
  }

  grepl(
    "timeout|timed out|rate.?limit|too many requests|\\b429\\b|connection reset|connection aborted|connection error|temporary|service unavailable|\\b502\\b|\\b503\\b|\\b504\\b|gateway",
    msg
  )
}

#' Invoke Agent with Retry for Transient Failures
#' @keywords internal
.invoke_agent_with_retry <- function(invoke_fn,
                                     max_attempts = ASA_DEFAULT_INVOKE_MAX_ATTEMPTS,
                                     retry_delay = ASA_DEFAULT_INVOKE_RETRY_DELAY,
                                     retry_backoff = ASA_DEFAULT_INVOKE_RETRY_BACKOFF,
                                     retry_jitter = ASA_DEFAULT_INVOKE_RETRY_JITTER,
                                     verbose = FALSE) {
  attempts <- max(1L, as.integer(max_attempts %||% 1L))
  delay <- max(0, as.numeric(retry_delay %||% 0))
  backoff <- max(1, as.numeric(retry_backoff %||% 1))
  jitter <- max(0, as.numeric(retry_jitter %||% 0))

  for (attempt in seq_len(attempts)) {
    result <- tryCatch(invoke_fn(), error = function(e) e)
    if (!inherits(result, "error")) {
      return(result)
    }

    retryable <- .is_retryable_invoke_error(result)
    is_last <- attempt >= attempts
    if (!retryable || is_last) {
      stop(result)
    }

    wait_for <- delay * (backoff ^ (attempt - 1L))
    if (jitter > 0 && wait_for > 0) {
      low <- max(0, wait_for * (1 - jitter))
      high <- wait_for * (1 + jitter)
      wait_for <- stats::runif(1, low, high)
    }
    if (verbose) {
      message(
        sprintf(
          "  Invoke attempt %d/%d failed (%s). Retrying in %.2fs...",
          attempt,
          attempts,
          conditionMessage(result),
          wait_for
        )
      )
    }
    if (wait_for > 0) {
      Sys.sleep(wait_for)
    }
  }

  stop("Unexpected invoke retry state reached.", call. = FALSE)
}

#' Detect Model Invoke Exception Fallback Events
#' @keywords internal
.has_invoke_exception_fallback <- function(raw_response) {
  repair_events <- .try_or(raw_response$json_repair, NULL)
  if (is.null(repair_events) || length(repair_events) == 0) {
    return(FALSE)
  }

  repair_events <- .try_or(reticulate::py_to_r(repair_events), repair_events)
  if (!is.list(repair_events) || length(repair_events) == 0) {
    return(FALSE)
  }

  reasons <- vapply(repair_events, function(ev) {
    if (!is.list(ev) || is.null(ev$repair_reason)) {
      return(NA_character_)
    }
    as.character(ev$repair_reason)[1]
  }, character(1))

  any(!is.na(reasons) & reasons == "invoke_exception_fallback")
}

#' Invoke Memory Folding Agent
#' @keywords internal
.invoke_memory_folding_agent <- function(python_agent, prompt, recursion_limit,
                                         expected_schema = NULL, thread_id = NULL,
                                         field_status = NULL, budget_state = NULL,
                                         search_budget_limit = NULL,
                                         unknown_after_searches = NULL,
                                         finalize_on_all_fields_resolved = NULL) {
  # Import message type
  from_schema <- reticulate::import("langchain_core.messages")
  initial_message <- from_schema$HumanMessage(content = prompt)
  resolved_thread_id <- .resolve_thread_id(thread_id)

  initial_state <- list(messages = list(initial_message))
  # Reset terminal markers for this invocation so checkpointed threads do not
  # inherit stale stop signals from prior runs.
  initial_state$stop_reason <- NULL

  if (!is.null(expected_schema)) {
    initial_state$expected_schema <- expected_schema
    initial_state$expected_schema_source <- "explicit"
  }
  if (!is.null(field_status)) {
    initial_state$field_status <- field_status
  }
  if (!is.null(budget_state)) {
    initial_state$budget_state <- budget_state
  }
  if (!is.null(search_budget_limit)) {
    initial_state$search_budget_limit <- as.integer(search_budget_limit)
  }
  if (!is.null(unknown_after_searches)) {
    initial_state$unknown_after_searches <- as.integer(unknown_after_searches)
  }
  if (!is.null(finalize_on_all_fields_resolved)) {
    initial_state$finalize_on_all_fields_resolved <- isTRUE(finalize_on_all_fields_resolved)
  }

  initial_state$tokens_used <- 0L

  # Only seed summary/fold_stats when starting a fresh (ephemeral) thread.
  if (is.null(thread_id)) {
    initial_state$summary <- reticulate::dict()
    initial_state$archive <- list()
    initial_state$fold_stats <- reticulate::dict(fold_count = 0L)
  }

  response <- python_agent$invoke(
    initial_state,
    config = list(
      configurable = list(thread_id = resolved_thread_id),
      recursion_limit = as.integer(recursion_limit)
    )
  )
  list(response = response, thread_id = resolved_thread_id)
}

#' Invoke Standard Agent
#' @keywords internal
.invoke_standard_agent <- function(python_agent, prompt, recursion_limit,
                                   expected_schema = NULL, thread_id = NULL,
                                   field_status = NULL, budget_state = NULL,
                                   search_budget_limit = NULL,
                                   unknown_after_searches = NULL,
                                   finalize_on_all_fields_resolved = NULL) {
  resolved_thread_id <- .resolve_thread_id(thread_id)
  initial_state <- list(
    messages = list(list(role = "user", content = prompt)),
    stop_reason = NULL,
    tokens_used = 0L
  )
  if (!is.null(expected_schema)) {
    initial_state$expected_schema <- expected_schema
    initial_state$expected_schema_source <- "explicit"
  }
  if (!is.null(field_status)) {
    initial_state$field_status <- field_status
  }
  if (!is.null(budget_state)) {
    initial_state$budget_state <- budget_state
  }
  if (!is.null(search_budget_limit)) {
    initial_state$search_budget_limit <- as.integer(search_budget_limit)
  }
  if (!is.null(unknown_after_searches)) {
    initial_state$unknown_after_searches <- as.integer(unknown_after_searches)
  }
  if (!is.null(finalize_on_all_fields_resolved)) {
    initial_state$finalize_on_all_fields_resolved <- isTRUE(finalize_on_all_fields_resolved)
  }

  response <- python_agent$invoke(
    initial_state,
    config = list(
      configurable = list(thread_id = resolved_thread_id),
      recursion_limit = as.integer(recursion_limit)
    )
  )
  list(response = response, thread_id = resolved_thread_id)
}

#' Extract Response Text from Raw Response
#' @keywords internal
.extract_response_text <- function(raw_response, backend) {
  response_text <- tryCatch({
    messages <- raw_response$messages
    stop_reason <- .try_or(as.character(raw_response$stop_reason), character(0))
    stop_reason <- if (length(stop_reason) > 0) stop_reason[[1]] else NA_character_
    error_msg <- .try_or(as.character(raw_response$error), character(0))
    error_msg <- if (length(error_msg) > 0) error_msg[[1]] else ""
    is_recursion_limit <- (
      !is.na(stop_reason) && identical(stop_reason, "recursion_limit")
    ) ||
      (is.character(error_msg) && nzchar(error_msg) &&
         (grepl("recursion", error_msg, ignore.case = TRUE) ||
            grepl("GraphRecursionError", error_msg, ignore.case = TRUE)))
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
        .try_or(as.character(value), character(0))
      }

      # Helper to check if a message is an AIMessage
      is_ai_message <- function(msg) {
        msg_type <- .try_or(as.character(msg$`__class__`$`__name__`), "")
        if (length(msg_type) > 0 && identical(msg_type[[1]], "AIMessage")) {
          return(TRUE)
        }
        role <- tolower(.try_or(as.character(msg$role), ""))
        if (length(role) > 0 && identical(role[[1]], "assistant")) {
          return(TRUE)
        }
        msg_role <- tolower(.try_or(as.character(msg$type), ""))
        isTRUE(length(msg_role) > 0 && msg_role[[1]] %in% c("assistant", "ai"))
      }

      # Helper to check if a message has pending tool calls
      has_tool_calls <- function(msg) {
        tool_calls <- .try_or(msg$tool_calls)
        isTRUE(!is.null(tool_calls) && length(tool_calls) > 0)
      }

      is_tool_message <- function(msg) {
        msg_type <- .try_or(as.character(msg$`__class__`$`__name__`), "")
        if (length(msg_type) > 0 && identical(msg_type[[1]], "ToolMessage")) {
          return(TRUE)
        }
        msg_role <- tolower(.try_or(as.character(msg$type), ""))
        isTRUE(length(msg_role) > 0 && msg_role[[1]] %in% c("tool", "function"))
      }

      # Helper to extract content from a message
      get_message_content <- function(msg) {
        # Prefer canonical `content`; some providers expose `text` as accessor objects.
        text <- .try_or(msg$content)
        if (is.null(text) || length(text) == 0) {
          text <- .try_or(msg$text)
        }
        text
      }

      text_parts <- character(0)
      tool_call_ai_candidates <- list()
      # Prefer the most recent assistant message. Under recursion limits, skip
      # assistant turns that still contain tool calls (non-terminal).
      for (i in rev(seq_along(messages))) {
        msg <- messages[[i]]
        if (!is_ai_message(msg)) {
          next
        }
        if (is_recursion_limit && has_tool_calls(msg)) {
          # Preserve best-effort assistant JSON emitted alongside tool calls.
          text <- get_message_content(msg)
          candidate_parts <- extract_text_blocks(text)
          if (length(candidate_parts) > 0) {
            tool_call_ai_candidates <- c(tool_call_ai_candidates, list(candidate_parts))
          }
          next
        }
        text <- get_message_content(msg)
        text_parts <- extract_text_blocks(text)
        if (length(text_parts) > 0) {
          break
        }
      }

      # Recursion-limited best-effort fallback: if an assistant turn with tool
      # calls already contains valid JSON, prefer that over raw tool payloads.
      if (length(text_parts) == 0 && is_recursion_limit && length(tool_call_ai_candidates) > 0) {
        for (candidate_parts in tool_call_ai_candidates) {
          candidate_text <- paste(candidate_parts, collapse = "\n")
          candidate_json <- .try_or(.parse_json_response(candidate_text))
          if (!is.null(candidate_json)) {
            text_parts <- candidate_parts
            break
          }
        }
      }

      # Non-recursion fallback: keep backward compatibility by allowing content
      # from the last message even when it's not an assistant message.
      if (length(text_parts) == 0 && !is_recursion_limit) {
        last_message <- messages[[length(messages)]]
        text <- get_message_content(last_message)
        text_parts <- extract_text_blocks(text)
      }

      # Recursion-limited fallback: accept last message text only if it's
      # terminal (not a tool call / tool output message).
      if (length(text_parts) == 0 && is_recursion_limit) {
        # Prefer tool output when recursion cut off the final assistant turn.
        for (i in rev(seq_along(messages))) {
          msg <- messages[[i]]
          if (!is_tool_message(msg)) {
            next
          }
          text <- get_message_content(msg)
          text_parts <- extract_text_blocks(text)
          if (length(text_parts) > 0) {
            break
          }
        }
      }

      # Recursion-limited fallback: accept last message text only if it's
      # terminal (not a tool call / tool output message).
      if (length(text_parts) == 0 && is_recursion_limit) {
        last_message <- messages[[length(messages)]]
        if (!has_tool_calls(last_message) && !is_tool_message(last_message)) {
          text <- get_message_content(last_message)
          text_parts <- extract_text_blocks(text)
        }
      }

      # If still no content, return a meaningful message based on stop_reason
      if (length(text_parts) == 0) {
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
    messages <- .try_or(raw_response$messages)
    if (is.null(messages) || length(messages) == 0) {
      return("")
    }

    msg_type <- function(msg) {
      # Prefer __class__.__name__ when available
      type_name <- .try_or(as.character(msg$`__class__`$`__name__`), NA_character_)
      if (!is.na(type_name) && nzchar(type_name)) {
        return(type_name)
      }
      # Fallback to "type" field if present (e.g., "ai", "human", "tool")
      .try_or(as.character(msg$type), NA_character_)
    }

    msg_content <- function(msg) {
      content <- .try_or(msg$content)
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
      .try_or(as.character(content), "")
    }

    msg_name <- function(msg) {
      name <- .try_or(msg$name)
      if (is.null(name)) return(NULL)
      name <- .try_or(as.character(name))
      if (is.null(name) || !nzchar(name)) return(NULL)
      name
    }

    msg_tool_calls <- function(msg) {
      tool_calls <- .try_or(msg$tool_calls)
      if (is.null(tool_calls)) return(NULL)
      .try_or(reticulate::py_to_r(tool_calls))
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
