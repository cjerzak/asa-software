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
                       auto_openwebpage_policy = NULL,
                       field_rules = NULL,
                       source_policy = NULL,
                       retry_policy = NULL,
                       finalization_policy = NULL,
                       orchestration_options = NULL,
                       query_templates = NULL,
                       use_plan_mode = FALSE,
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
    field_rules = field_rules,
    source_policy = source_policy,
    retry_policy = retry_policy,
    finalization_policy = finalization_policy,
    orchestration_options = orchestration_options,
    query_templates = query_templates,
    use_plan_mode = use_plan_mode,
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
  timing_marks <- list(
    run_agent_started = t0
  )
  resolved_thread_id <- .resolve_thread_id(thread_id)
  invoke_max_attempts <- as.integer(config$invoke_max_attempts %||% ASA_DEFAULT_INVOKE_MAX_ATTEMPTS)
  invoke_retry_delay <- as.numeric(config$invoke_retry_delay %||% ASA_DEFAULT_INVOKE_RETRY_DELAY)
  invoke_retry_backoff <- as.numeric(config$invoke_retry_backoff %||% ASA_DEFAULT_INVOKE_RETRY_BACKOFF)
  invoke_retry_jitter <- as.numeric(config$invoke_retry_jitter %||% ASA_DEFAULT_INVOKE_RETRY_JITTER)
  invoke_max_attempts <- max(1L, invoke_max_attempts)
  if (is.null(auto_openwebpage_policy)) {
    auto_openwebpage_policy <- config$search$auto_openwebpage_policy %||% NULL
  }

  # Build initial state and invoke agent
  timing_marks$invoke_with_retry_started <- Sys.time()
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
            finalize_on_all_fields_resolved = finalize_on_all_fields_resolved,
            auto_openwebpage_policy = auto_openwebpage_policy,
            field_rules = field_rules,
            source_policy = source_policy,
            retry_policy = retry_policy,
            finalization_policy = finalization_policy,
            orchestration_options = orchestration_options,
            query_templates = query_templates,
            use_plan_mode = use_plan_mode,
            model_timeout_s = as.numeric(config$timeout %||% 0),
            om_config = config$om_config %||% list(
              enabled = isTRUE(config$use_observational_memory %||% FALSE),
              scope = if (isTRUE(config$om_cross_thread_memory %||% FALSE)) "resource" else "thread",
              cross_thread_memory = isTRUE(config$om_cross_thread_memory %||% FALSE)
            )
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
            finalize_on_all_fields_resolved = finalize_on_all_fields_resolved,
            auto_openwebpage_policy = auto_openwebpage_policy,
            field_rules = field_rules,
            source_policy = source_policy,
            retry_policy = retry_policy,
            finalization_policy = finalization_policy,
            orchestration_options = orchestration_options,
            query_templates = query_templates,
            use_plan_mode = use_plan_mode,
            model_timeout_s = as.numeric(config$timeout %||% 0)
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
  timing_marks$invoke_with_retry_finished <- Sys.time()
  raw_response <- if (inherits(invoke_output, "asa_error")) invoke_output else invoke_output$response
  backend_phase_timings <- if (!inherits(invoke_output, "asa_error")) {
    invoke_output$phase_timings %||% list()
  } else {
    list()
  }
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
    err_resp <- asa_response(
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
      diagnostics = list(),
      json_repair = list(),
      completion_gate = list(),
      tokens_used = NA_integer_,
      input_tokens = NA_integer_,
      output_tokens = NA_integer_,
      token_trace = list()
    )
    err_resp$phase_timings <- list(
      total_minutes = elapsed,
      invoke_with_retry_minutes = as.numeric(difftime(
        timing_marks$invoke_with_retry_finished,
        timing_marks$invoke_with_retry_started,
        units = "mins"
      )),
      backend = backend_phase_timings
    )
    err_resp$retrieval_metrics <- list()
    err_resp$tool_quality_events <- list()
    err_resp$candidate_resolution <- list()
    err_resp$finalization_status <- list()
    err_resp$orchestration_options <- list()
    err_resp$artifact_status <- list()
    err_resp$policy_version <- NA_character_
    return(err_resp)
  }

  timing_marks$postprocess_started <- Sys.time()
  # Extract canonical payload metadata and response text
  final_payload_info <- .extract_final_payload_details(raw_response)
  response_payload <- .extract_response_text(raw_response, config$backend, with_meta = TRUE)
  response_text <- response_payload$text %||% NA_character_
  response_release_source <- response_payload$released_from %||%
    if (isTRUE(final_payload_info$available)) "final_payload" else "message_text"
  response_sanitized <- isTRUE(response_payload$sanitized)
  invoke_exception_fallback <- .has_invoke_exception_fallback(raw_response)

  # Build trace
  trace <- .build_trace(raw_response)
  trace_json <- .build_trace_json(raw_response)

  # Handle rate limiting and timeouts
  .handle_response_issues(trace, verbose)
  stop_reason <- .extract_stop_reason(raw_response)
  budget_state_out <- .coerce_py_list(raw_response$budget_state)
  field_status_out <- .coerce_py_list(raw_response$field_status)
  diagnostics_out <- .coerce_py_list(raw_response$diagnostics)
  json_repair <- .coerce_py_list(raw_response$json_repair)
  completion_gate <- .coerce_py_list(raw_response$completion_gate)
  retrieval_metrics_out <- .coerce_py_list(raw_response$retrieval_metrics)
  tool_quality_events_out <- .coerce_py_list(raw_response$tool_quality_events)
  candidate_resolution_out <- .coerce_py_list(raw_response$candidate_resolution)
  finalization_status_out <- .coerce_py_list(raw_response$finalization_status)
  orchestration_options_out <- .coerce_py_list(raw_response$orchestration_options)
  artifact_status_out <- .coerce_py_list(raw_response$artifact_status)
  policy_version <- .try_or(as.character(raw_response$policy_version), character(0))
  policy_version <- policy_version[!is.na(policy_version) & nzchar(policy_version)]
  policy_version <- if (length(policy_version) > 0L) policy_version[[1]] else NA_character_
  payload_integrity <- .build_payload_integrity(
    released_text = response_text,
    released_from = response_release_source,
    final_payload_info = final_payload_info,
    trace = trace,
    trace_json = trace_json,
    json_repair = json_repair,
    message_sanitized = response_sanitized
  )

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
  input_tokens <- .as_scalar_int(.try_or(raw_response$input_tokens, NA_integer_))
  output_tokens <- .as_scalar_int(.try_or(raw_response$output_tokens, NA_integer_))
  token_trace <- .try_or(as.list(raw_response$token_trace), list())

  # Extract plan fields from Python state
  plan <- .try_or(reticulate::py_to_r(raw_response$plan), list())
  plan_history <- .try_or(reticulate::py_to_r(raw_response$plan_history), list())
  om_stats <- .try_or(reticulate::py_to_r(raw_response$om_stats), list())
  observations <- .try_or(reticulate::py_to_r(raw_response$observations), list())
  reflections <- .try_or(reticulate::py_to_r(raw_response$reflections), list())
  timing_marks$postprocess_finished <- Sys.time()

  phase_timings <- list(
    total_minutes = elapsed,
    invoke_with_retry_minutes = as.numeric(difftime(
      timing_marks$invoke_with_retry_finished,
      timing_marks$invoke_with_retry_started,
      units = "mins"
    )),
    postprocess_minutes = as.numeric(difftime(
      timing_marks$postprocess_finished,
      timing_marks$postprocess_started,
      units = "mins"
    )),
    backend = backend_phase_timings
  )

  # Return response object
  resp <- asa_response(
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
    diagnostics = diagnostics_out,
    json_repair = json_repair,
    final_payload = final_payload_info$payload,
    terminal_valid = final_payload_info$terminal_valid,
    terminal_payload_hash = final_payload_info$terminal_payload_hash,
    payload_integrity = payload_integrity,
    completion_gate = completion_gate,
    tokens_used = tokens_used,
    input_tokens = input_tokens,
    output_tokens = output_tokens,
    token_trace = token_trace
  )
  resp$phase_timings <- phase_timings
  resp$plan <- plan
  resp$plan_history <- plan_history
  resp$om_stats <- om_stats
  resp$observations <- observations
  resp$reflections <- reflections
  resp$retrieval_metrics <- retrieval_metrics_out
  resp$tool_quality_events <- tool_quality_events_out
  resp$candidate_resolution <- candidate_resolution_out
  resp$finalization_status <- finalization_status_out
  resp$orchestration_options <- orchestration_options_out
  resp$artifact_status <- artifact_status_out
  resp$policy_version <- policy_version
  resp
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

#' Extract terminal final-payload metadata from raw Python response state
#' @keywords internal
.extract_final_payload_details <- function(raw_response) {
  payload_raw <- .try_or(raw_response$final_payload, NULL)
  payload <- .try_or(reticulate::py_to_r(payload_raw), payload_raw)

  payload_json <- NA_character_
  payload_available <- FALSE
  if (!is.null(payload)) {
    payload_json <- .try_or(
      jsonlite::toJSON(payload, auto_unbox = TRUE, null = "null"),
      NA_character_
    )
    if (is.character(payload_json) && length(payload_json) > 1L) {
      payload_json <- paste0(payload_json, collapse = "")
    }
    payload_available <- (
      is.character(payload_json) &&
        length(payload_json) == 1L &&
        !is.na(payload_json) &&
        nzchar(payload_json)
    )
  }

  terminal_valid_raw <- .try_or(raw_response$terminal_valid, NULL)
  terminal_valid <- suppressWarnings(
    as.logical(.try_or(reticulate::py_to_r(terminal_valid_raw), terminal_valid_raw))
  )
  terminal_valid <- if (length(terminal_valid) > 0L && !is.na(terminal_valid[[1]])) {
    isTRUE(terminal_valid[[1]])
  } else {
    FALSE
  }

  payload_hash <- .try_or(as.character(raw_response$terminal_payload_hash), character(0))
  payload_hash <- payload_hash[!is.na(payload_hash) & nzchar(payload_hash)]
  payload_hash <- if (length(payload_hash) > 0L) payload_hash[[1]] else NA_character_
  if ((is.na(payload_hash) || !nzchar(payload_hash)) && isTRUE(payload_available)) {
    payload_hash <- digest::digest(enc2utf8(payload_json), algo = "sha256")
  }

  list(
    payload = if (isTRUE(payload_available)) payload else NULL,
    payload_json = if (isTRUE(payload_available)) payload_json else NA_character_,
    available = isTRUE(payload_available),
    terminal_valid = isTRUE(terminal_valid || payload_available),
    terminal_payload_hash = payload_hash
  )
}

#' Extract JSON repair reason labels from repair events
#' @keywords internal
.json_repair_reasons <- function(json_repair = list()) {
  if (!is.list(json_repair) || length(json_repair) == 0L) {
    return(character(0))
  }
  reasons <- vapply(json_repair, function(ev) {
    if (!is.list(ev) || is.null(ev$repair_reason)) {
      return(NA_character_)
    }
    as.character(ev$repair_reason)[1]
  }, character(1))
  unique(reasons[!is.na(reasons) & nzchar(reasons)])
}

#' Detect explicit truncation markers in release artifacts
#' @keywords internal
.detect_payload_truncation_signals <- function(released_text = "",
                                               trace = "",
                                               trace_json = "") {
  signals <- character(0)
  if (is.character(trace) && any(grepl("[Truncated]", trace, fixed = TRUE))) {
    signals <- c(signals, "trace_truncated_marker")
  }
  if (is.character(trace_json) && any(grepl("[Truncated]", trace_json, fixed = TRUE))) {
    signals <- c(signals, "trace_json_truncated_marker")
  }
  if (is.character(released_text) && any(grepl("[Truncated]", released_text, fixed = TRUE))) {
    signals <- c(signals, "released_text_truncated_marker")
  }
  unique(signals)
}

#' Compute SHA256 hash for a single text blob
#' @keywords internal
.text_sha256 <- function(text) {
  text_chr <- .try_or(as.character(text), character(0))
  text_chr <- text_chr[!is.na(text_chr)]
  if (length(text_chr) == 0L || !nzchar(text_chr[[1]])) {
    return(NA_character_)
  }
  digest::digest(enc2utf8(text_chr[[1]]), algo = "sha256")
}

#' Recursively normalize JSON-like R objects for stable semantic hashing
#' @keywords internal
.normalize_json_semantics <- function(value) {
  if (is.list(value)) {
    if (is.null(names(value))) {
      return(lapply(value, .normalize_json_semantics))
    }
    key_names <- names(value)
    ord <- order(key_names)
    normalized <- lapply(value[ord], .normalize_json_semantics)
    names(normalized) <- key_names[ord]
    return(normalized)
  }
  value
}

#' Compute semantic JSON hash (key-order insensitive for objects)
#' @keywords internal
.semantic_json_sha256 <- function(text) {
  text_chr <- .try_or(as.character(text), character(0))
  text_chr <- text_chr[!is.na(text_chr)]
  if (length(text_chr) == 0L || !nzchar(text_chr[[1]])) {
    return(NA_character_)
  }

  parsed <- .try_or(jsonlite::fromJSON(text_chr[[1]], simplifyVector = FALSE), NULL)
  if (is.null(parsed)) {
    return(NA_character_)
  }
  normalized <- .normalize_json_semantics(parsed)
  normalized_json <- .try_or(
    jsonlite::toJSON(normalized, auto_unbox = TRUE, null = "null"),
    NA_character_
  )
  if (!is.character(normalized_json) || length(normalized_json) == 0L || any(is.na(normalized_json))) {
    return(NA_character_)
  }
  digest::digest(enc2utf8(paste(normalized_json, collapse = "")), algo = "sha256")
}

#' Build payload release integrity summary for diagnostics
#' @keywords internal
.build_payload_integrity <- function(released_text,
                                     released_from = "message_text",
                                     final_payload_info = list(),
                                     trace = "",
                                     trace_json = "",
                                     json_repair = list(),
                                     message_sanitized = FALSE) {
  canonical_available <- isTRUE(final_payload_info$available)
  canonical_text <- final_payload_info$payload_json %||% NA_character_
  canonical_matches <- FALSE
  if (
    canonical_available &&
      is.character(released_text) &&
      length(released_text) == 1L &&
      !is.na(released_text) &&
      is.character(canonical_text) &&
      length(canonical_text) == 1L &&
      !is.na(canonical_text)
  ) {
    canonical_matches <- identical(trimws(released_text), trimws(canonical_text))
  }

  source <- as.character(released_from %||% "message_text")
  if (length(source) == 0L || is.na(source[[1]]) || !nzchar(source[[1]])) {
    source <- "message_text"
  } else {
    source <- source[[1]]
  }

  released_byte_hash <- .text_sha256(released_text)
  canonical_byte_hash <- if (canonical_available) .text_sha256(canonical_text) else NA_character_
  byte_hash_matches <- (
    is.character(released_byte_hash) &&
      length(released_byte_hash) == 1L &&
      !is.na(released_byte_hash) &&
      is.character(canonical_byte_hash) &&
      length(canonical_byte_hash) == 1L &&
      !is.na(canonical_byte_hash) &&
      identical(released_byte_hash, canonical_byte_hash)
  )

  released_semantic_hash <- .semantic_json_sha256(released_text)
  canonical_semantic_hash <- if (canonical_available) .semantic_json_sha256(canonical_text) else NA_character_
  semantic_hash_matches <- (
    is.character(released_semantic_hash) &&
      length(released_semantic_hash) == 1L &&
      !is.na(released_semantic_hash) &&
      is.character(canonical_semantic_hash) &&
      length(canonical_semantic_hash) == 1L &&
      !is.na(canonical_semantic_hash) &&
      identical(released_semantic_hash, canonical_semantic_hash)
  )
  hash_mismatch_type <- NA_character_
  if (
    canonical_available &&
      !is.na(released_byte_hash) &&
      !is.na(canonical_byte_hash) &&
      !is.na(released_semantic_hash) &&
      !is.na(canonical_semantic_hash)
  ) {
    if (!byte_hash_matches && semantic_hash_matches) {
      hash_mismatch_type <- "byte_only"
    } else if (!byte_hash_matches && !semantic_hash_matches) {
      hash_mismatch_type <- "byte_and_semantic"
    } else if (byte_hash_matches && !semantic_hash_matches) {
      hash_mismatch_type <- "semantic_only"
    } else {
      hash_mismatch_type <- "none"
    }
  }

  list(
    released_from = source,
    canonical_available = canonical_available,
    canonical_matches_message = canonical_matches,
    canonical_byte_hash = canonical_byte_hash,
    released_byte_hash = released_byte_hash,
    byte_hash_matches = isTRUE(byte_hash_matches),
    canonical_semantic_hash = canonical_semantic_hash,
    released_semantic_hash = released_semantic_hash,
    semantic_hash_matches = isTRUE(semantic_hash_matches),
    hash_mismatch_type = hash_mismatch_type,
    message_sanitized = isTRUE(message_sanitized),
    truncation_signals = .detect_payload_truncation_signals(
      released_text = released_text,
      trace = trace,
      trace_json = trace_json
    ),
    json_repair_reasons = .json_repair_reasons(json_repair),
    terminal_payload_hash = final_payload_info$terminal_payload_hash %||% NA_character_
  )
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
    "invalid api key|authentication|unauthoriz|forbidden|permission|validation|bad request|unsupported|not found|exceeded your current quota|quota exceeded",
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
                                         finalize_on_all_fields_resolved = NULL,
                                         auto_openwebpage_policy = NULL,
                                         field_rules = NULL,
                                         source_policy = NULL,
                                         retry_policy = NULL,
                                         finalization_policy = NULL,
                                         orchestration_options = NULL,
                                         query_templates = NULL,
                                         use_plan_mode = FALSE,
                                         om_config = NULL,
                                         model_timeout_s = NULL) {
  invoke_phase_started <- Sys.time()
  # Import message type
  from_schema <- reticulate::import("langchain_core.messages")
  initial_message <- from_schema$HumanMessage(content = prompt)
  resolved_thread_id <- .resolve_thread_id(thread_id)

  state_build_started <- Sys.time()
  initial_state <- list(messages = list(initial_message))
  initial_state$thread_id <- resolved_thread_id
  # Reset terminal markers for this invocation so checkpointed threads do not
  # inherit stale stop signals from prior runs.
  initial_state$stop_reason <- NULL
  initial_state$final_emitted <- FALSE
  initial_state$final_payload <- NULL
  initial_state$terminal_valid <- FALSE
  initial_state$terminal_payload_hash <- NULL
  initial_state$finalize_invocations <- 0L
  initial_state$finalize_trigger_reasons <- list()

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
  if (!is.null(auto_openwebpage_policy) && nzchar(as.character(auto_openwebpage_policy))) {
    initial_state$auto_openwebpage_policy <- as.character(auto_openwebpage_policy)
  }
  if (!is.null(field_rules)) {
    initial_state$field_rules <- field_rules
  }
  if (!is.null(source_policy)) {
    initial_state$source_policy <- source_policy
  }
  if (!is.null(retry_policy)) {
    initial_state$retry_policy <- retry_policy
  }
  if (!is.null(finalization_policy)) {
    initial_state$finalization_policy <- finalization_policy
  }
  if (!is.null(orchestration_options)) {
    initial_state$orchestration_options <- orchestration_options
  }
  if (!is.null(query_templates)) {
    initial_state$query_templates <- query_templates
  }

  initial_state$tokens_used <- 0L
  initial_state$input_tokens <- 0L
  initial_state$output_tokens <- 0L
  initial_state$token_trace <- list()

  initial_state$use_plan_mode <- isTRUE(use_plan_mode)
  if (!is.null(model_timeout_s) && is.finite(as.numeric(model_timeout_s))) {
    initial_state$model_timeout_s <- as.numeric(model_timeout_s)
  }
  if (!is.null(om_config)) {
    initial_state$om_config <- om_config
  }

  # Only seed summary/fold_stats when starting a fresh (ephemeral) thread.
  if (is.null(thread_id)) {
    initial_state$summary <- reticulate::dict()
    initial_state$archive <- list()
    initial_state$fold_stats <- reticulate::dict(fold_count = 0L)
    initial_state$observations <- list()
    initial_state$reflections <- list()
    initial_state$om_stats <- list()
    initial_state$om_prebuffer <- list()
  }
  state_build_finished <- Sys.time()

  ddg_module <- asa_env$backend_api %||% .import_backend_api(required = TRUE)
  graph_invoke_started <- Sys.time()
  response <- ddg_module$invoke_graph_safely(
    python_agent,
    initial_state,
    config = list(
      configurable = list(thread_id = resolved_thread_id),
      recursion_limit = as.integer(recursion_limit)
    )
  )
  graph_invoke_finished <- Sys.time()
  invoke_phase_finished <- Sys.time()
  list(
    response = response,
    thread_id = resolved_thread_id,
    phase_timings = list(
      total_minutes = as.numeric(difftime(invoke_phase_finished, invoke_phase_started, units = "mins")),
      state_build_minutes = as.numeric(difftime(state_build_finished, state_build_started, units = "mins")),
      graph_invoke_minutes = as.numeric(difftime(graph_invoke_finished, graph_invoke_started, units = "mins"))
    )
  )
}

#' Invoke Standard Agent
#' @keywords internal
.invoke_standard_agent <- function(python_agent, prompt, recursion_limit,
                                   expected_schema = NULL, thread_id = NULL,
                                   field_status = NULL, budget_state = NULL,
                                   search_budget_limit = NULL,
                                   unknown_after_searches = NULL,
                                   finalize_on_all_fields_resolved = NULL,
                                   auto_openwebpage_policy = NULL,
                                   field_rules = NULL,
                                   source_policy = NULL,
                                   retry_policy = NULL,
                                   finalization_policy = NULL,
                                   orchestration_options = NULL,
                                   query_templates = NULL,
                                   use_plan_mode = FALSE,
                                   model_timeout_s = NULL) {
  invoke_phase_started <- Sys.time()
  resolved_thread_id <- .resolve_thread_id(thread_id)
  state_build_started <- Sys.time()
  initial_state <- list(
    messages = list(list(role = "user", content = prompt)),
    thread_id = resolved_thread_id,
    stop_reason = NULL,
    final_emitted = FALSE,
    final_payload = NULL,
    terminal_valid = FALSE,
    terminal_payload_hash = NULL,
    finalize_invocations = 0L,
    finalize_trigger_reasons = list(),
    tokens_used = 0L,
    input_tokens = 0L,
    output_tokens = 0L,
    token_trace = list()
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
  if (!is.null(auto_openwebpage_policy) && nzchar(as.character(auto_openwebpage_policy))) {
    initial_state$auto_openwebpage_policy <- as.character(auto_openwebpage_policy)
  }
  if (!is.null(field_rules)) {
    initial_state$field_rules <- field_rules
  }
  if (!is.null(source_policy)) {
    initial_state$source_policy <- source_policy
  }
  if (!is.null(retry_policy)) {
    initial_state$retry_policy <- retry_policy
  }
  if (!is.null(finalization_policy)) {
    initial_state$finalization_policy <- finalization_policy
  }
  if (!is.null(orchestration_options)) {
    initial_state$orchestration_options <- orchestration_options
  }
  if (!is.null(query_templates)) {
    initial_state$query_templates <- query_templates
  }
  initial_state$use_plan_mode <- isTRUE(use_plan_mode)
  if (!is.null(model_timeout_s) && is.finite(as.numeric(model_timeout_s))) {
    initial_state$model_timeout_s <- as.numeric(model_timeout_s)
  }
  state_build_finished <- Sys.time()

  ddg_module <- asa_env$backend_api %||% .import_backend_api(required = TRUE)
  graph_invoke_started <- Sys.time()
  response <- ddg_module$invoke_graph_safely(
    python_agent,
    initial_state,
    config = list(
      configurable = list(thread_id = resolved_thread_id),
      recursion_limit = as.integer(recursion_limit)
    )
  )
  graph_invoke_finished <- Sys.time()
  invoke_phase_finished <- Sys.time()
  list(
    response = response,
    thread_id = resolved_thread_id,
    phase_timings = list(
      total_minutes = as.numeric(difftime(invoke_phase_finished, invoke_phase_started, units = "mins")),
      state_build_minutes = as.numeric(difftime(state_build_finished, state_build_started, units = "mins")),
      graph_invoke_minutes = as.numeric(difftime(graph_invoke_finished, graph_invoke_started, units = "mins"))
    )
  )
}

#' Extract Response Text from Raw Response
#' @keywords internal
.extract_response_text <- function(raw_response, backend, with_meta = FALSE) {
  payload_info <- .extract_final_payload_details(raw_response)
  if (
    isTRUE(payload_info$available) &&
      is.character(payload_info$payload_json) &&
      length(payload_info$payload_json) == 1L &&
      !is.na(payload_info$payload_json) &&
      nzchar(payload_info$payload_json)
  ) {
    if (isTRUE(with_meta)) {
      return(list(
        text = payload_info$payload_json,
        sanitized = FALSE,
        released_from = "final_payload"
      ))
    }
    return(payload_info$payload_json)
  }

  sanitized <- FALSE
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

      # Recursion-limited fallback: prefer tool output when recursion cut off
      # the final assistant turn.
      if (length(text_parts) == 0 && is_recursion_limit) {
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

      # Recursion-limited fallback: accept last message text only if terminal.
      if (length(text_parts) == 0 && is_recursion_limit) {
        last_message <- messages[[length(messages)]]
        if (!has_tool_calls(last_message) && !is_tool_message(last_message)) {
          text <- get_message_content(last_message)
          text_parts <- extract_text_blocks(text)
        }
      }

      # If still no content, return a meaningful message based on stop_reason.
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

      # Strip embedded NUL bytes (cause "Embedded NUL in string" from reticulate).
      stripped <- .strip_nul(text)
      if (!identical(stripped, text)) {
        sanitized <- TRUE
      }
      text <- stripped

      # Handle exo backend format.
      if (!is.null(backend) && backend == "exo") {
        exo_text <- sub(
          "(?s).*<\\|python_tag\\|>(\\{.*?\\})<\\|eom_id\\|>.*",
          "\\1", text, perl = TRUE
        )
        if (!identical(exo_text, text)) {
          sanitized <- TRUE
        }
        text <- exo_text
      }
      text
    } else {
      NA_character_
    }
  }, error = function(e) {
    NA_character_
  })

  if (isTRUE(with_meta)) {
    return(list(
      text = response_text,
      sanitized = isTRUE(sanitized),
      released_from = "message_text"
    ))
  }
  response_text
}

#' Strip embedded NUL bytes from a string
#'
#' NUL bytes (\\x00) in web page content survive UTF-8 decoding and cause
#' "Embedded NUL in string" errors when passed from Python to R via reticulate.
#' @keywords internal
.strip_nul <- function(x) {
  if (!is.character(x) || length(x) == 0) return(x)
  vapply(x, function(s) {
    r <- charToRaw(s)
    rawToChar(r[r != as.raw(0L)])
  }, character(1), USE.NAMES = FALSE)
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
    messages <- .try_or(raw_response$messages)
    if (is.null(messages) || length(messages) == 0) {
      # Fallback: print the whole response if no messages list
      cleaned <- .strip_trace_noise(raw_response)
      return(.strip_nul(paste(capture.output(print(cleaned)), collapse = "\n")))
    }
    parts <- character(0)
    for (msg in messages) {
      entry <- tryCatch({
        msg_type <- .try_or(as.character(msg$type), "unknown")
        if (length(msg_type) == 0) msg_type <- "unknown"
        msg_name <- .try_or(as.character(msg$name), "")
        if (length(msg_name) == 0) msg_name <- ""
        header <- if (nzchar(msg_name)) {
          paste0("[", msg_type, ": ", msg_name, "]")
        } else {
          paste0("[", msg_type, "]")
        }
        content <- .try_or(msg$content)
        content_str <- ""
        if (is.character(content)) {
          content_str <- paste(content, collapse = "\n")
        } else if (is.list(content)) {
          text_parts <- character(0)
          for (item in content) {
            if (is.character(item)) {
              text_parts <- c(text_parts, item)
            } else if (is.list(item)) {
              if (!is.null(item$text)) text_parts <- c(text_parts, as.character(item$text))
              if (!is.null(item$content)) text_parts <- c(text_parts, as.character(item$content))
            }
          }
          content_str <- paste(text_parts[nzchar(text_parts)], collapse = "\n")
        } else if (!is.null(content)) {
          content_str <- .try_or(paste(as.character(content), collapse = "\n"), "")
        }
        # Include tool calls if present
        tool_calls <- .try_or(msg$tool_calls)
        tc_str <- ""
        if (!is.null(tool_calls) && length(tool_calls) > 0) {
          tc_parts <- vapply(tool_calls, function(tc) {
            tc_name <- .try_or(as.character(tc$name), "?")
            tc_args <- paste(.try_or(as.character(tc$args), ""), collapse = ", ")
            paste0("  -> ", tc_name, "(", tc_args, ")")
          }, character(1))
          tc_str <- paste(tc_parts, collapse = "\n")
        }
        paste(c(header, content_str, tc_str), collapse = "\n")
      }, error = function(e) {
        paste0("[message format error: ", conditionMessage(e), "]")
      })
      parts <- c(parts, entry)
    }
    .strip_nul(paste(parts, collapse = "\n\n"))
  }, error = function(e) {
    warning("[build_trace] Failed to build trace text: ",
            conditionMessage(e), call. = FALSE)
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

    first_non_empty <- function(value, default = NULL) {
      chars <- .try_or(as.character(value), character(0))
      chars <- chars[!is.na(chars) & nzchar(chars)]
      if (length(chars) == 0) return(default)
      chars[[1]]
    }

    msg_type <- function(msg) {
      # Prefer __class__.__name__ when available
      type_name <- first_non_empty(msg$`__class__`$`__name__`, default = NULL)
      if (!is.null(type_name)) {
        return(type_name)
      }
      # Fallback to "type" field if present (e.g., "ai", "human", "tool")
      first_non_empty(msg$type, default = NA_character_)
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
      first_non_empty(msg$name, default = NULL)
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
    warning("[build_trace_json] Failed to serialise trace: ",
            conditionMessage(e), call. = FALSE)
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
