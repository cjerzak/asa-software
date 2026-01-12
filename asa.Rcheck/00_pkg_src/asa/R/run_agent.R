#' Run the ASA Agent (Internal)
#'
#' Internal function that invokes the search agent with a prompt.
#' Users should use \code{\link{run_task}} instead.
#'
#' @param prompt The prompt to send to the agent
#' @param agent An asa_agent object
#' @param temporal Named list for temporal filtering
#' @param recursion_limit Maximum number of agent steps
#' @param verbose Print status messages
#'
#' @return An object of class \code{asa_response}
#'
#' @keywords internal
.run_agent <- function(prompt,
                       agent = NULL,
                       temporal = NULL,
                       recursion_limit = NULL,
                       thread_id = NULL,
                       verbose = FALSE) {

  # Validate inputs
  .validate_run_agent(
    prompt = prompt,
    agent = agent,
    recursion_limit = recursion_limit,
    verbose = verbose,
    thread_id = thread_id
  )

  # Validate temporal if provided
  .validate_temporal(temporal)

  # Get or initialize agent
  if (is.null(agent)) {
    if (!.is_initialized()) {
      stop("Agent not initialized. Call initialize_agent() first or pass an agent object.",
           call. = FALSE)
    }
    agent <- get_agent()
  }

  # Augment prompt with temporal hints if dates specified
  augmented_prompt <- .augment_prompt_temporal(prompt, temporal, verbose = verbose)

  # Get config
  config <- agent$config
  use_memory_folding <- config$use_memory_folding %||% TRUE

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

  # Build initial state and invoke agent with temporal filtering
  raw_response <- .with_temporal(temporal, function() {
    tryCatch({
      if (use_memory_folding) {
        .invoke_memory_folding_agent(agent$python_agent, augmented_prompt, recursion_limit, thread_id)
      } else {
        .invoke_standard_agent(agent$python_agent, augmented_prompt, recursion_limit, thread_id)
      }
    }, error = function(e) {
      structure(list(error = e$message), class = "asa_error")
    })
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
.invoke_memory_folding_agent <- function(python_agent, prompt, recursion_limit, thread_id = NULL) {
  # Import message type
  from_schema <- reticulate::import("langchain_core.messages")
  initial_message <- from_schema$HumanMessage(content = prompt)

  initial_state <- list(
    messages = list(initial_message)
  )

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
.invoke_standard_agent <- function(python_agent, prompt, recursion_limit, thread_id = NULL) {
  python_agent$invoke(
    list(messages = list(list(role = "user", content = prompt))),
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
      last_message <- messages[[length(messages)]]
      text <- last_message$text
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
.build_trace <- function(raw_response) {
  tryCatch({
    paste(
      lapply(unlist(raw_response), function(l) capture.output(l)),
      collapse = "\n\n"
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
