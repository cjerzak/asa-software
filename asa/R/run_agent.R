#' Run the ASA Agent with a Custom Prompt
#'
#' Invokes the search agent with an arbitrary prompt, returning the full
#' agent trace and response. This is the low-level function for running
#' the agent; for structured task execution, use \code{\link{run_task}}.
#'
#' @param prompt The prompt to send to the agent
#' @param agent An asa_agent object from \code{\link{initialize_agent}}, or
#'   NULL to use/create the default agent
#' @param recursion_limit Maximum number of agent steps (default: 100 for
#'   memory folding, 20 otherwise)
#' @param verbose Print status messages (default: FALSE)
#'
#' @return An object of class \code{asa_response} containing:
#' \itemize{
#'   \item message: The final response text
#'   \item status_code: 200 for success, 100 for error
#'   \item raw_response: The full Python response object
#'   \item trace: Full text trace of agent execution
#'   \item elapsed_time: Execution time in minutes
#'   \item fold_count: Number of memory folds (if memory folding enabled)
#' }
#'
#' @examples
#' \dontrun{
#' # Run with a custom prompt
#' agent <- initialize_agent()
#' result <- run_agent(
#'   prompt = "Who was the 44th president of the United States?",
#'   agent = agent
#' )
#' print(result$message)
#' }
#'
#' @seealso \code{\link{initialize_agent}}, \code{\link{run_task}}
#'
#' @export
run_agent <- function(prompt,
                      agent = NULL,
                      recursion_limit = NULL,
                      verbose = FALSE) {

  # Get or initialize agent
  if (is.null(agent)) {
    if (!.is_initialized()) {
      stop("Agent not initialized. Call initialize_agent() first or pass an agent object.",
           call. = FALSE)
    }
    agent <- get_agent()
  }

  # Validate agent
  if (!inherits(agent, "asa_agent")) {
    stop("'agent' must be an asa_agent object from initialize_agent()",
         call. = FALSE)
  }

  # Get config
  config <- agent$config
  use_memory_folding <- config$use_memory_folding %||% TRUE

  # Set recursion limit based on agent type
  if (is.null(recursion_limit)) {
    recursion_limit <- if (use_memory_folding) 100L else 20L
  }

  if (verbose) message("Running agent...")
  t0 <- Sys.time()

  # Build initial state and invoke agent
  raw_response <- tryCatch({
    if (use_memory_folding) {
      .invoke_memory_folding_agent(agent$python_agent, prompt, recursion_limit)
    } else {
      .invoke_standard_agent(agent$python_agent, prompt, recursion_limit)
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
      status_code = 100L,
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
    status_code = if (is.na(response_text)) 100L else 200L,
    raw_response = raw_response,
    trace = trace,
    elapsed_time = elapsed,
    fold_count = fold_count,
    prompt = prompt
  )
}

#' Invoke Memory Folding Agent
#' @keywords internal
.invoke_memory_folding_agent <- function(python_agent, prompt, recursion_limit) {
  # Import message type
  from_schema <- reticulate::import("langchain_core.messages")
  initial_message <- from_schema$HumanMessage(content = prompt)

  initial_state <- list(
    messages = list(initial_message),
    summary = "",
    fold_count = 0L
  )

  python_agent$invoke(
    initial_state,
    config = list(
      configurable = list(thread_id = rlang::hash(Sys.time())),
      recursion_limit = as.integer(recursion_limit)
    )
  )
}

#' Invoke Standard Agent
#' @keywords internal
.invoke_standard_agent <- function(python_agent, prompt, recursion_limit) {
  python_agent$invoke(
    list(messages = list(list(role = "user", content = prompt))),
    config = list(
      configurable = list(thread_id = rlang::hash(Sys.time())),
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
      text <- last_message$text()
      # Clean XML tags
      text <- gsub("<[^>]+>.*?</[^>]+>\\n?", "", text)
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
    if (verbose) warning("Rate limit triggered, waiting 15 seconds...")
    Sys.sleep(15L)
  }
  if (grepl("Timeout", trace, ignore.case = TRUE)) {
    if (verbose) warning("Timeout triggered, waiting 15 seconds...")
    Sys.sleep(15L)
  }
  invisible(NULL)
}

#' Run Agent in Batch Mode
#'
#' Runs the agent on multiple prompts, optionally in parallel.
#'
#' @param prompts Character vector of prompts
#' @param agent An asa_agent object
#' @param parallel Use parallel processing (requires future.apply package)
#' @param workers Number of parallel workers (default: 4)
#' @param progress Show progress bar (default: TRUE)
#'
#' @return A list of asa_response objects
#'
#' @examples
#' \dontrun{
#' prompts <- c(
#'   "What is the population of Tokyo?",
#'   "What is the population of New York?"
#' )
#' results <- run_agent_batch(prompts, agent)
#' }
#'
#' @export
run_agent_batch <- function(prompts,
                            agent = NULL,
                            parallel = FALSE,
                            workers = 4L,
                            progress = TRUE) {

  if (parallel) {
    if (!requireNamespace("future.apply", quietly = TRUE)) {
      stop("Package 'future.apply' required for parallel processing.",
           call. = FALSE)
    }
    future.apply::plan(future.apply::multisession(workers = workers))
    on.exit(future.apply::plan(future.apply::sequential), add = TRUE)

    results <- future.apply::future_lapply(
      prompts,
      function(p) run_agent(p, agent = agent, verbose = FALSE),
      future.seed = TRUE
    )
  } else {
    n <- length(prompts)
    results <- vector("list", n)
    for (i in seq_len(n)) {
      if (progress) message(sprintf("[%d/%d] Processing...", i, n))
      results[[i]] <- run_agent(prompts[[i]], agent = agent, verbose = FALSE)
    }
  }

  results
}
