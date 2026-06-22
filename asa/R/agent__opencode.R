#' Ensure processx is available for OpenCode execution
#' @keywords internal
.opencode_require_processx <- function() {
  if (!requireNamespace("processx", quietly = TRUE)) {
    stop(
      "The `opencode` agent backend requires the `processx` package. ",
      "Install it with install.packages(\"processx\").",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' Resolve the Python executable used for OpenCode helper processes
#' @keywords internal
.opencode_python_binary <- function(conda_env) {
  python <- .try_or(reticulate::conda_python(conda_env), "")
  python <- .try_or(normalizePath(python, winslash = "/", mustWork = TRUE), "")
  if (!nzchar(python) || !file.exists(python)) {
    stop(
      "Could not resolve Python executable for conda env `", conda_env, "`.",
      call. = FALSE
    )
  }
  python
}

#' Resolve the python source path needed by OpenCode helper processes
#' @keywords internal
.opencode_python_path <- function() {
  candidates <- c(
    .get_python_path(),
    file.path(getwd(), "asa", "inst", "python"),
    file.path(getwd(), "inst", "python")
  )
  candidates <- unique(normalizePath(candidates, winslash = "/", mustWork = FALSE))
  required <- c(
    file.path("asa_backend", "free_code", "anthropic_gateway.py"),
    file.path("asa_backend", "free_code", "mcp_search_server.py")
  )
  for (path in candidates) {
    if (dir.exists(path) && all(file.exists(file.path(path, required)))) {
      return(path)
    }
  }
  stop(
    "Could not locate ASA Python sources for OpenCode integration. ",
    "Expected ", paste(required, collapse = " and "),
    " under the package python path.",
    call. = FALSE
  )
}

#' Discover the OpenCode CLI command
#' @keywords internal
.opencode_command_spec <- function() {
  explicit_bin <- Sys.getenv("ASA_OPENCODE_BIN", unset = "")
  if (nzchar(explicit_bin)) {
    resolved <- normalizePath(explicit_bin, winslash = "/", mustWork = FALSE)
    if (!file.exists(resolved)) {
      stop("ASA_OPENCODE_BIN does not exist: ", explicit_bin, call. = FALSE)
    }
    return(list(command = resolved, prefix_args = character(0)))
  }

  path_bin <- Sys.which("opencode")
  if (nzchar(path_bin)) {
    return(list(command = unname(path_bin), prefix_args = character(0)))
  }

  stop(
    "Could not find the `opencode` CLI. Set ASA_OPENCODE_BIN or install ",
    "`opencode` on PATH.",
    call. = FALSE
  )
}

#' Resolve the OpenCode agent name
#' @keywords internal
.opencode_agent_name <- function() {
  value <- trimws(Sys.getenv("ASA_OPENCODE_AGENT", unset = ""))
  if (nzchar(value)) {
    return(value)
  }
  ASA_OPENCODE_DEFAULT_AGENT
}

#' Convert an OpenCode model id into provider/model form
#' @keywords internal
.opencode_model_ref <- function(outer_model = ASA_OPENCODE_OUTER_MODEL) {
  outer_model <- trimws(as.character(outer_model %||% ASA_OPENCODE_OUTER_MODEL)[1])
  if (!nzchar(outer_model)) {
    outer_model <- ASA_OPENCODE_OUTER_MODEL
  }
  if (grepl("/", outer_model, fixed = TRUE)) {
    return(outer_model)
  }
  paste0("anthropic/", outer_model)
}

#' Extract the provider-local model id from an OpenCode model reference
#' @keywords internal
.opencode_provider_model_id <- function(outer_model = ASA_OPENCODE_OUTER_MODEL) {
  sub("^[^/]+/", "", .opencode_model_ref(outer_model))
}

#' Build the local MCP server config for OpenCode
#' @keywords internal
.opencode_mcp_server_config <- function(config,
                                        python,
                                        python_path,
                                        loop_guard = NULL,
                                        allow_read_webpages = NULL,
                                        auto_openwebpage_policy = NULL,
                                        wayback = NULL) {
  environment <- as.list(.free_code_mcp_env(
    config = config,
    python_path = python_path,
    allow_read_webpages = allow_read_webpages,
    auto_openwebpage_policy = auto_openwebpage_policy,
    wayback = wayback
  ))
  if (!is.null(loop_guard)) {
    environment$ASA_FREE_CODE_LOOP_GUARD_JSON <- paste(jsonlite::toJSON(
      loop_guard,
      auto_unbox = TRUE,
      null = "null"
    ), collapse = "")
  }

  list(
    type = "local",
    command = unname(c(python, "-m", "asa_backend.free_code.mcp_search_server")),
    enabled = TRUE,
    environment = environment,
    timeout = as.integer(loop_guard$mcp_timeout_ms %||% 30000L)
  )
}

#' Build OPENCODE_CONFIG_CONTENT for ASA-managed OpenCode runs
#' @keywords internal
.opencode_config_content <- function(config,
                                     gateway_base_url,
                                     python,
                                     python_path,
                                     target_backend,
                                     target_model,
                                     outer_model = ASA_OPENCODE_OUTER_MODEL,
                                     loop_guard = NULL,
                                     allow_read_webpages = NULL,
                                     auto_openwebpage_policy = NULL,
                                     wayback = NULL) {
  model_ref <- .opencode_model_ref(outer_model)
  model_id <- .opencode_provider_model_id(outer_model)

  headers <- list()
  headers[["X-ASA-Target-Backend"]] <- target_backend
  headers[["X-ASA-Target-Model"]] <- target_model

  tools <- list(
    question = FALSE,
    webfetch = FALSE,
    websearch = FALSE,
    write = FALSE,
    edit = FALSE,
    bash = FALSE
  )

  permission <- list(
    question = "deny",
    webfetch = "deny",
    websearch = "deny",
    edit = "deny",
    bash = "deny",
    external_directory = "deny",
    task = "deny"
  )

  provider_models <- list()
  provider_models[[model_id]] <- list(
    name = model_id
  )

  cfg <- list()
  cfg[["$schema"]] <- "https://opencode.ai/config.json"
  cfg$autoupdate <- FALSE
  cfg$share <- "disabled"
  cfg$enabled_providers <- I(c("anthropic"))
  cfg$model <- model_ref
  cfg$small_model <- model_ref
  cfg$provider <- list(
    anthropic = list(
      options = list(
        baseURL = gateway_base_url,
        apiKey = "asa-local-gateway",
        headers = headers,
        timeout = as.integer(as.numeric(config$timeout %||% ASA_DEFAULT_TIMEOUT) * 1000)
      ),
      models = provider_models
    )
  )
  cfg$mcp <- list(
    asa_search = .opencode_mcp_server_config(
      config = config,
      python = python,
      python_path = python_path,
      loop_guard = loop_guard,
      allow_read_webpages = allow_read_webpages,
      auto_openwebpage_policy = auto_openwebpage_policy,
      wayback = wayback
    )
  )
  cfg$tools <- tools
  cfg$permission <- permission

  paste(jsonlite::toJSON(cfg, auto_unbox = TRUE, null = "null"), collapse = "")
}

#' Build OpenCode CLI arguments
#' @keywords internal
.opencode_cli_args <- function(prompt,
                               workdir,
                               outer_model = ASA_OPENCODE_OUTER_MODEL,
                               agent_name = .opencode_agent_name()) {
  c(
    "run",
    "--format", "json",
    "--model", .opencode_model_ref(outer_model),
    "--agent", agent_name,
    "--dir", workdir,
    .opencode_noninteractive_prompt(prompt)
  )
}

#' Build OpenCode CLI environment
#' @keywords internal
.opencode_cli_env <- function(config_content) {
  env <- Sys.getenv()
  env <- c(env, .free_code_proxy_env(NULL))
  env["NO_PROXY"] <- .free_code_join_no_proxy(Sys.getenv("NO_PROXY", unset = ""))
  env["ANTHROPIC_API_KEY"] <- "asa-local-gateway"
  env["OPENCODE_CONFIG_CONTENT"] <- config_content
  env
}

#' Extract a scalar string from OpenCode JSON event fields
#' @keywords internal
.opencode_scalar_text <- function(value) {
  if (is.null(value)) {
    return("")
  }
  value <- as.character(value)
  value <- value[!is.na(value)]
  if (length(value) == 0L) {
    return("")
  }
  value[[1]]
}

#' Add non-interactive batch guardrails to OpenCode prompts
#' @keywords internal
.opencode_noninteractive_prompt <- function(prompt) {
  prompt <- as.character(prompt %||% "")
  if (length(prompt) == 0L || is.na(prompt[[1]])) {
    prompt <- ""
  } else {
    prompt <- prompt[[1]]
  }
  guard <- paste(
    "NON-INTERACTIVE BATCH RUN:",
    "- Do not ask the user follow-up questions or invoke interactive question tools.",
    "- Treat the supplied task metadata as authoritative.",
    "- If evidence is insufficient for a requested field, return Unknown.",
    sep = "\n"
  )
  if (grepl("NON-INTERACTIVE BATCH RUN:", prompt, fixed = TRUE)) {
    return(prompt)
  }
  paste(guard, prompt, sep = "\n\n")
}

#' Render an OpenCode error event into a human-readable string
#' @keywords internal
.opencode_event_error_message <- function(event) {
  error <- event$error %||% list()
  name <- .opencode_scalar_text(error$name %||% error$type %||% event$type %||% "error")
  message <- .opencode_scalar_text(
    error$message %||%
      error$data$message %||%
      error$data$error$message %||%
      event$message %||%
      error$data
  )
  status <- .opencode_scalar_text(error$statusCode %||% error$data$statusCode %||% "")

  parts <- c(name, message)
  parts <- parts[nzchar(parts)]
  out <- paste(parts, collapse = ": ")
  if (nzchar(status)) {
    out <- paste0(out, " (status ", status, ")")
  }
  if (nzchar(out)) out else "OpenCode emitted an error event."
}

#' Aggregate token usage from OpenCode step_finish events
#' @keywords internal
.opencode_usage_from_events <- function(events) {
  int0 <- function(value) {
    value <- .as_scalar_int(value)
    if (is.na(value)) 0L else value
  }

  input_tokens <- 0L
  output_tokens <- 0L
  reasoning_tokens <- 0L
  cache_read_tokens <- 0L
  cache_write_tokens <- 0L

  for (event in events) {
    if (!identical(event$type %||% NULL, "step_finish")) {
      next
    }
    tokens <- event$part$tokens %||% event$tokens %||% list()
    input_tokens <- input_tokens + int0(tokens$input)
    output_tokens <- output_tokens + int0(tokens$output)
    reasoning_tokens <- reasoning_tokens + int0(tokens$reasoning)
    cache <- tokens$cache %||% list()
    cache_read_tokens <- cache_read_tokens + int0(cache$read)
    cache_write_tokens <- cache_write_tokens + int0(cache$write)
  }

  input_total <- input_tokens + cache_read_tokens + cache_write_tokens
  total <- input_total + output_tokens + reasoning_tokens
  if (identical(total, 0L)) {
    total <- NA_integer_
  }

  list(
    input_tokens = if (identical(input_total, 0L)) NA_integer_ else input_total,
    output_tokens = if (identical(output_tokens, 0L)) NA_integer_ else output_tokens,
    reasoning_tokens = if (identical(reasoning_tokens, 0L)) NA_integer_ else reasoning_tokens,
    cache_read_tokens = if (identical(cache_read_tokens, 0L)) NA_integer_ else cache_read_tokens,
    cache_write_tokens = if (identical(cache_write_tokens, 0L)) NA_integer_ else cache_write_tokens,
    total_tokens = total
  )
}

#' Summarize parsed OpenCode JSONL events
#' @keywords internal
.opencode_summarize_events <- function(events, raw_stdout = "") {
  text_parts <- character(0)
  error_messages <- character(0)
  session_ids <- character(0)
  stop_reasons <- character(0)

  for (event in events) {
    session_ids <- c(
      session_ids,
      .opencode_scalar_text(event$sessionID %||% event$session_id %||% event$part$sessionID %||% event$part$session_id %||% "")
    )

    event_type <- .opencode_scalar_text(event$type %||% "")
    if (identical(event_type, "text")) {
      text <- .opencode_scalar_text(event$part$text %||% event$text %||% "")
      if (nzchar(text)) {
        text_parts <- c(text_parts, text)
      }
    } else if (identical(event_type, "error")) {
      error_messages <- c(error_messages, .opencode_event_error_message(event))
    } else if (identical(event_type, "step_finish")) {
      reason <- .opencode_scalar_text(event$part$reason %||% event$reason %||% "")
      if (nzchar(reason)) {
        stop_reasons <- c(stop_reasons, reason)
      }
    }
  }

  session_ids <- session_ids[nzchar(session_ids)]
  stop_reasons <- stop_reasons[nzchar(stop_reasons)]

  list(
    events = events,
    text = paste(text_parts, collapse = "\n"),
    session_id = if (length(session_ids) > 0L) session_ids[[1]] else NULL,
    errors = unique(error_messages[nzchar(error_messages)]),
    stop_reason = if (length(stop_reasons) > 0L) tail(stop_reasons, 1L)[[1]] else NULL,
    usage = .opencode_usage_from_events(events),
    raw_stdout = raw_stdout,
    parse_error = FALSE
  )
}

#' Summarize OpenCode tool-loop signals from JSONL events
#' @keywords internal
.opencode_tool_loop_diagnostics <- function(events, loop_guard = NULL) {
  tool_calls <- 0L
  search_calls <- 0L
  invalid_calls <- 0L
  tool_errors <- 0L
  timeout_errors <- 0L
  guard_errors <- 0L
  cache_hits <- 0L
  internal_tool_calls <- 0L
  question_tool_calls <- 0L
  pending_question_tool_calls <- 0L
  signatures <- character(0)

  for (event in events %||% list()) {
    if (!identical(event$type %||% NULL, "tool_use")) {
      next
    }
    part <- event$part %||% list()
    state <- part$state %||% list()
    tool <- .opencode_scalar_text(part$tool %||% state$tool %||% "")
    input <- state$input %||% list()
    error <- .opencode_scalar_text(state$error %||% "")
    output <- .opencode_scalar_text(state$output %||% "")
    status <- .opencode_scalar_text(state$status %||% "")
    sig <- paste0(tool, ":", .try_or(
      paste(jsonlite::toJSON(input, auto_unbox = TRUE, null = "null"), collapse = ""),
      ""
    ))

    tool_calls <- tool_calls + 1L
    signatures <- c(signatures, sig)
    if (nzchar(tool) && !grepl("^asa_search_", tool)) {
      internal_tool_calls <- internal_tool_calls + 1L
    }
    if (identical(tool, "question")) {
      question_tool_calls <- question_tool_calls + 1L
      if (status %in% c("pending", "running")) {
        pending_question_tool_calls <- pending_question_tool_calls + 1L
      }
    }
    if (grepl("web_search|web_fetch|wayback_search|asa_search", tool, ignore.case = TRUE)) {
      search_calls <- search_calls + 1L
    }
    if (identical(tool, "invalid") || grepl("unavailable tool|invalid tool", paste(error, output), ignore.case = TRUE)) {
      invalid_calls <- invalid_calls + 1L
    }
    if (identical(status, "error") || nzchar(error)) {
      tool_errors <- tool_errors + 1L
    }
    if (grepl("timeout|timed out|-32001", paste(error, output), ignore.case = TRUE)) {
      timeout_errors <- timeout_errors + 1L
    }
    if (grepl("ASA_TOOL_ERROR", output, fixed = TRUE)) {
      guard_errors <- guard_errors + 1L
    }
    if (grepl("ASA_TOOL_CACHE_HIT", output, fixed = TRUE)) {
      cache_hits <- cache_hits + 1L
    }
  }

  signature_counts <- sort(table(signatures), decreasing = TRUE)
  repeated_signature_count <- if (length(signature_counts) > 0L) {
    as.integer(signature_counts[[1]])
  } else {
    0L
  }

  search_limit <- .as_scalar_int(loop_guard$search_budget_limit %||% NA_integer_)
  remaining <- if (!is.na(search_limit)) max(0L, search_limit - search_calls) else NA_integer_

  list(
    config = loop_guard %||% list(),
    tool_calls = tool_calls,
    search_calls = search_calls,
    invalid_tool_calls = invalid_calls,
    tool_errors = tool_errors,
    timeout_errors = timeout_errors,
    guard_errors = guard_errors,
    cache_hits = cache_hits,
    internal_tool_calls = internal_tool_calls,
    question_tool_calls = question_tool_calls,
    pending_question_tool_calls = pending_question_tool_calls,
    repeated_signature_count = repeated_signature_count,
    most_repeated_signature = if (length(signature_counts) > 0L) names(signature_counts)[[1L]] else NA_character_,
    search_calls_limit = search_limit,
    search_calls_remaining = remaining,
    search_budget_exhausted = !is.na(search_limit) && search_calls >= search_limit
  )
}

#' Parse OpenCode JSONL stdout
#' @keywords internal
.opencode_parse_output <- function(stdout) {
  raw_stdout <- as.character(stdout %||% "")
  if (length(raw_stdout) == 0L || is.na(raw_stdout[[1]])) {
    raw_stdout <- ""
  } else {
    raw_stdout <- raw_stdout[[1]]
  }
  lines <- strsplit(raw_stdout, "\r?\n", perl = TRUE)[[1]]
  lines <- lines[nzchar(trimws(lines))]
  if (length(lines) == 0L) {
    stop("OpenCode returned empty stdout.", call. = FALSE)
  }

  events <- vector("list", length(lines))
  for (i in seq_along(lines)) {
    events[[i]] <- tryCatch(
      jsonlite::fromJSON(lines[[i]], simplifyVector = FALSE),
      error = function(e) {
        stop(
          "OpenCode returned malformed JSONL stdout at line ", i, ": ",
          conditionMessage(e),
          call. = FALSE
        )
      }
    )
  }

  .opencode_summarize_events(events, raw_stdout = raw_stdout)
}

#' Extract an OpenCode session id from a parsed event
#' @keywords internal
.opencode_event_session_id <- function(event) {
  .opencode_scalar_text(
    event$sessionID %||%
      event$session_id %||%
      event$part$sessionID %||%
      event$part$session_id %||%
      ""
  )
}

#' Extract an OpenCode session id from parsed output or raw JSONL
#' @keywords internal
.opencode_output_session_id <- function(output) {
  session_id <- .opencode_scalar_text(output$session_id %||% "")
  if (nzchar(session_id)) {
    return(session_id)
  }

  for (event in output$events %||% list()) {
    session_id <- .opencode_event_session_id(event)
    if (nzchar(session_id)) {
      return(session_id)
    }
  }

  raw_stdout <- .opencode_scalar_text(output$raw_stdout %||% "")
  if (!nzchar(raw_stdout)) {
    return("")
  }
  lines <- strsplit(raw_stdout, "\r?\n", perl = TRUE)[[1]]
  lines <- lines[nzchar(trimws(lines))]
  for (line in lines) {
    event <- tryCatch(
      jsonlite::fromJSON(line, simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (is.null(event)) {
      next
    }
    session_id <- .opencode_event_session_id(event)
    if (nzchar(session_id)) {
      return(session_id)
    }
  }

  ""
}

#' Detect whether OpenCode emitted an explicit error event
#' @keywords internal
.opencode_has_error_event <- function(events) {
  any(vapply(events %||% list(), function(event) {
    identical(.opencode_scalar_text(event$type %||% event$part$type %||% ""), "error")
  }, logical(1)))
}

#' Parse stdout from `opencode export`
#' @keywords internal
.opencode_parse_export_stdout <- function(stdout) {
  stdout <- .opencode_scalar_text(stdout %||% "")
  left <- gregexpr("{", stdout, fixed = TRUE)[[1]]
  right <- gregexpr("}", stdout, fixed = TRUE)[[1]]
  if (identical(left[[1]], -1L) || identical(right[[1]], -1L)) {
    stop("OpenCode export stdout did not contain a JSON object.", call. = FALSE)
  }

  json_text <- substr(stdout, left[[1]], tail(right, 1L)[[1]])
  tryCatch(
    jsonlite::fromJSON(json_text, simplifyVector = FALSE),
    error = function(e) {
      stop("OpenCode export stdout did not contain parseable JSON: ", conditionMessage(e), call. = FALSE)
    }
  )
}

#' Extract the last assistant text part from an OpenCode export
#' @keywords internal
.opencode_export_final_assistant_text <- function(exported) {
  messages <- exported$messages %||% list()
  text_parts <- character(0)

  for (message in messages) {
    role <- .opencode_scalar_text(message$info$role %||% "")
    if (!identical(role, "assistant")) {
      next
    }
    for (part in message$parts %||% list()) {
      type <- .opencode_scalar_text(part$type %||% "")
      text <- .opencode_scalar_text(part$text %||% "")
      if (identical(type, "text") && nzchar(text)) {
        text_parts <- c(text_parts, text)
      }
    }
  }

  if (length(text_parts) > 0L) {
    tail(text_parts, 1L)[[1]]
  } else {
    ""
  }
}

#' Extract token usage from an OpenCode export
#' @keywords internal
.opencode_export_token_payload <- function(exported) {
  valid_tokens <- function(tokens) {
    if (!is.list(tokens) || length(tokens) == 0L) {
      return(NULL)
    }
    token_names <- names(tokens) %||% character(0)
    if (any(token_names %in% c("input", "output", "reasoning", "total", "cache"))) {
      return(tokens)
    }
    NULL
  }

  messages <- exported$messages %||% list()
  assistant <- NULL
  for (message in messages) {
    role <- .opencode_scalar_text(message$info$role %||% "")
    if (identical(role, "assistant")) {
      assistant <- message
    }
  }

  if (!is.null(assistant)) {
    parts <- rev(assistant$parts %||% list())
    for (part in parts) {
      type <- .opencode_scalar_text(part$type %||% "")
      if (!(type %in% c("step-finish", "step_finish"))) {
        next
      }
      tokens <- valid_tokens(part$tokens %||% list())
      if (!is.null(tokens)) {
        return(tokens)
      }
    }

    tokens <- valid_tokens(assistant$info$tokens %||% list())
    if (!is.null(tokens)) {
      return(tokens)
    }
  }

  valid_tokens(exported$info$tokens %||% list())
}

#' Determine whether the OpenCode export fallback is applicable
#' @keywords internal
.opencode_should_try_export_fallback <- function(output,
                                                 cli_status = 0L,
                                                 process_timeout = FALSE) {
  result_text <- .opencode_scalar_text(output$text %||% "")
  errors <- as.character(output$errors %||% character(0))
  errors <- errors[!is.na(errors) & nzchar(errors)]
  events <- output$events %||% list()

  identical(as.integer(cli_status %||% 0L), 0L) &&
    !isTRUE(process_timeout) &&
    !isTRUE(output$process_timeout %||% FALSE) &&
    !isTRUE(output$parse_error %||% FALSE) &&
    length(events) > 0L &&
    !nzchar(result_text) &&
    length(errors) == 0L &&
    !.opencode_has_error_event(events)
}

#' Recover missing terminal text from the OpenCode session store
#' @keywords internal
.opencode_apply_export_fallback <- function(output,
                                            cli,
                                            cli_env,
                                            workdir,
                                            cli_status = 0L,
                                            process_timeout = FALSE,
                                            timeout_seconds = NULL) {
  output$opencode_export_fallback_used <- FALSE
  output$opencode_export_fallback_error <- output$opencode_export_fallback_error %||% NULL
  output$opencode_export_session_id <- output$opencode_export_session_id %||% NULL
  output$opencode_export_final_text_chars <- output$opencode_export_final_text_chars %||% NA_integer_

  if (!.opencode_should_try_export_fallback(output, cli_status = cli_status, process_timeout = process_timeout)) {
    return(output)
  }

  session_id <- .opencode_output_session_id(output)
  output$opencode_export_session_id <- if (nzchar(session_id)) session_id else NULL
  if (!nzchar(session_id)) {
    output$opencode_export_fallback_error <- "OpenCode export fallback could not find a session id."
    return(output)
  }

  export_timeout <- suppressWarnings(as.numeric(timeout_seconds %||% 30))
  if (is.na(export_timeout) || export_timeout <= 0) {
    export_timeout <- 30
  }
  export_timeout <- max(5, min(export_timeout, 60))

  export_stdout_file <- tempfile("asa_opencode_export_stdout_")
  export_stderr_file <- tempfile("asa_opencode_export_stderr_")
  on.exit(unlink(c(export_stdout_file, export_stderr_file), force = TRUE), add = TRUE)

  export_result <- tryCatch(
    .run_processx(
      command = cli$command,
      args = c(cli$prefix_args %||% character(0), "export", session_id),
      wd = workdir,
      env = cli_env,
      stdout = export_stdout_file,
      stderr = export_stderr_file,
      error_on_status = FALSE,
      timeout = export_timeout,
      cleanup_tree = TRUE
    ),
    error = function(e) {
      list(status = NA_integer_, stdout = "", stderr = conditionMessage(e), timeout = FALSE)
    }
  )

  if (isTRUE(export_result$timeout %||% FALSE)) {
    output$opencode_export_fallback_error <- paste0(
      "OpenCode export timed out after ", export_timeout, " seconds."
    )
    return(output)
  }

  export_stdout <- .opencode_scalar_text(export_result$stdout %||% "")
  if (file.exists(export_stdout_file) && isTRUE(file.info(export_stdout_file)$size > 0L)) {
    export_stdout <- readChar(
      export_stdout_file,
      nchars = file.info(export_stdout_file)$size,
      useBytes = TRUE
    )
  }
  export_stderr <- .opencode_scalar_text(export_result$stderr %||% "")
  if (file.exists(export_stderr_file) && isTRUE(file.info(export_stderr_file)$size > 0L)) {
    export_stderr <- readChar(
      export_stderr_file,
      nchars = file.info(export_stderr_file)$size,
      useBytes = TRUE
    )
  }

  export_status <- suppressWarnings(as.integer(export_result$status %||% NA_integer_))
  if (is.na(export_status) || !identical(export_status, 0L)) {
    output$opencode_export_fallback_error <- paste(
      c(
        paste0("OpenCode export failed with status ", if (is.na(export_status)) "NA" else export_status, "."),
        if (nzchar(export_stderr)) export_stderr else character(0)
      ),
      collapse = "\n"
    )
    return(output)
  }

  export_parse_error <- NULL
  exported <- tryCatch(
    .opencode_parse_export_stdout(export_stdout),
    error = function(e) {
      export_parse_error <<- conditionMessage(e)
      NULL
    }
  )
  if (is.null(exported)) {
    output$opencode_export_fallback_error <- export_parse_error %||% "OpenCode export stdout could not be parsed."
    return(output)
  }

  final_text <- .opencode_export_final_assistant_text(exported)
  if (!nzchar(final_text)) {
    output$opencode_export_fallback_error <- "OpenCode export contained no non-empty assistant text part."
    return(output)
  }

  export_tokens <- .opencode_export_token_payload(exported)
  step_finish_part <- list(type = "step-finish", reason = "stop")
  if (!is.null(export_tokens)) {
    step_finish_part$tokens <- export_tokens
  }

  output$text <- final_text
  output$session_id <- session_id
  if (!nzchar(.opencode_scalar_text(output$stop_reason %||% ""))) {
    output$stop_reason <- "stop"
  }
  output$events <- c(
    output$events %||% list(),
    list(
      list(
        type = "text",
        sessionID = session_id,
        part = list(type = "text", text = final_text),
        source = "opencode_export_fallback",
        synthetic = TRUE
      ),
      list(
        type = "step_finish",
        sessionID = session_id,
        part = step_finish_part,
        source = "opencode_export_fallback",
        synthetic = TRUE
      )
    )
  )
  output$usage <- .opencode_usage_from_events(output$events)
  output$opencode_export_fallback_used <- TRUE
  output$opencode_export_fallback_error <- NULL
  output$opencode_export_final_text_chars <- nchar(final_text, type = "chars", allowNA = FALSE, keepNA = FALSE)
  output
}

#' Parse the final OpenCode text as strict JSON
#' @keywords internal
.opencode_final_payload <- function(text) {
  text <- .opencode_scalar_text(text)
  text <- trimws(text)
  if (!nzchar(text)) {
    return(list(valid = FALSE, payload = NULL, payload_json = NA_character_))
  }
  payload <- tryCatch(
    jsonlite::fromJSON(text, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (is.null(payload) && startsWith(text, "```")) {
    payload <- .try_or(.parse_json_response(text))
  }
  if (is.null(payload)) {
    return(list(valid = FALSE, payload = NULL, payload_json = NA_character_))
  }
  payload_json <- jsonlite::toJSON(payload, auto_unbox = TRUE, null = "null")
  list(
    valid = TRUE,
    payload = payload,
    payload_json = paste(payload_json, collapse = "")
  )
}

#' Construct an asa_response from OpenCode CLI events
#' @keywords internal
.opencode_response_from_output <- function(output,
                                           prompt,
                                           elapsed_minutes,
                                           cli_status = 0L,
                                           stderr = "",
                                           thread_id = NULL,
                                           target_backend = NULL,
                                           target_model = NULL,
                                           outer_model = ASA_OPENCODE_OUTER_MODEL,
                                           loop_guard = NULL,
                                           process_timeout = FALSE,
                                           timeout_seconds = NULL) {
  result_text <- .opencode_scalar_text(output$text %||% "")
  stderr_text <- .opencode_scalar_text(stderr %||% "")
  errors <- as.character(output$errors %||% character(0))
  errors <- errors[!is.na(errors) & nzchar(errors)]
  parse_error <- isTRUE(output$parse_error %||% FALSE)
  export_fallback_used <- isTRUE(output$opencode_export_fallback_used %||% FALSE)
  export_fallback_error <- .opencode_scalar_text(output$opencode_export_fallback_error %||% "")
  export_session_id <- .opencode_scalar_text(output$opencode_export_session_id %||% "")
  export_final_text_chars <- .as_scalar_int(output$opencode_export_final_text_chars %||% NA_integer_)
  cli_status <- as.integer(cli_status %||% 0L)
  process_timeout <- isTRUE(process_timeout) || isTRUE(output$process_timeout %||% FALSE)
  events <- output$events %||% list()
  event_types <- vapply(events, function(event) {
    .opencode_scalar_text(event$type %||% event$part$type %||% "")
  }, character(1))
  event_types <- event_types[nzchar(event_types)]
  timeout_seconds <- timeout_seconds %||% output$timeout_seconds %||% NULL
  timeout_message <- if (isTRUE(process_timeout)) {
    suffix <- if (!is.null(timeout_seconds) && !is.na(timeout_seconds)) {
      paste0(" after ", timeout_seconds, " seconds")
    } else {
      ""
    }
    paste0("OpenCode process timed out", suffix, ".")
  } else {
    ""
  }
  if (nzchar(timeout_message)) {
    errors <- unique(c(errors, timeout_message))
  }
  empty_terminal_output <- !process_timeout &&
    !parse_error &&
    identical(cli_status, 0L) &&
    !nzchar(result_text) &&
    length(errors) == 0L &&
    length(events) > 0L
  if (empty_terminal_output) {
    last_event_types <- tail(event_types, 8L)
    errors <- unique(c(
      errors,
      paste0(
        "OpenCode returned JSONL events without terminal text. Last event types: ",
        if (length(last_event_types)) paste(last_event_types, collapse = ", ") else "<none>"
      )
    ))
  }
  is_error <- process_timeout || parse_error || length(errors) > 0L || !identical(cli_status, 0L)

  if (is_error && !nzchar(result_text)) {
    message_parts <- c(
      if (isTRUE(process_timeout)) timeout_message else if (parse_error) "OpenCode did not return valid JSONL." else "OpenCode returned an error.",
      errors,
      if (nzchar(stderr_text)) stderr_text else character(0)
    )
    result_text <- paste(message_parts[nzchar(message_parts)], collapse = "\n")
  }

  terminal <- .opencode_final_payload(result_text)
  payload_json <- terminal$payload_json
  terminal_hash <- if (isTRUE(terminal$valid)) {
    digest::digest(enc2utf8(payload_json), algo = "sha256")
  } else {
    NA_character_
  }

  payload_integrity <- list(
    released_from = if (isTRUE(terminal$valid)) "opencode_final_text_json" else "opencode_final_text",
    canonical_available = isTRUE(terminal$valid),
    canonical_matches_message = if (isTRUE(terminal$valid)) identical(result_text, payload_json) else TRUE,
    message_sanitized = FALSE
  )

  trace_json <- jsonlite::toJSON(
    list(
      events = output$events %||% list(),
      cli_status = cli_status,
      process_timeout = process_timeout,
      timeout_seconds = timeout_seconds,
      stderr = stderr_text,
      errors = errors,
      parse_error = parse_error,
      empty_terminal_output = empty_terminal_output,
      opencode_export_fallback_used = export_fallback_used,
      opencode_export_session_id = if (nzchar(export_session_id)) export_session_id else NULL,
      opencode_export_final_text_chars = export_final_text_chars,
      opencode_export_fallback_error = if (nzchar(export_fallback_error)) export_fallback_error else NULL,
      last_event_types = tail(event_types, 8L),
      raw_stdout = output$raw_stdout %||% ""
    ),
    auto_unbox = TRUE,
    null = "null",
    pretty = TRUE
  )

  trace <- result_text
  if (is_error) {
    trace_parts <- c(
      result_text,
      errors,
      if (nzchar(stderr_text)) stderr_text else character(0)
    )
    trace <- paste(unique(trace_parts[nzchar(trace_parts)]), collapse = "\n\n")
  }

  usage <- output$usage %||% list()
  tokens_used <- .as_scalar_int(usage$total_tokens)
  input_tokens <- .as_scalar_int(usage$input_tokens)
  output_tokens <- .as_scalar_int(usage$output_tokens)
  tool_loop_guard <- .opencode_tool_loop_diagnostics(output$events %||% list(), loop_guard = loop_guard)

  resp <- asa_response(
    message = result_text,
    status_code = if (is_error) ASA_STATUS_ERROR else ASA_STATUS_SUCCESS,
    raw_response = output,
    trace = trace,
    trace_json = trace_json,
    elapsed_time = elapsed_minutes,
    fold_stats = list(fold_count = 0L),
    prompt = prompt,
    thread_id = output$session_id %||% thread_id,
    stop_reason = if (isTRUE(process_timeout)) {
      "process_timeout"
    } else {
      output$stop_reason %||% if (isTRUE(terminal$valid)) "structured_output" else "end_turn"
    },
    budget_state = list(
      tool_calls_used = tool_loop_guard$tool_calls %||% NA_integer_,
      tool_calls_limit = loop_guard$recursion_limit %||% NA_integer_,
      tool_calls_remaining = if (!is.null(loop_guard$recursion_limit)) {
        max(0L, as.integer(loop_guard$recursion_limit) - as.integer(tool_loop_guard$tool_calls %||% 0L))
      } else {
        NA_integer_
      },
      search_calls_used = tool_loop_guard$search_calls %||% NA_integer_,
      search_calls_limit = tool_loop_guard$search_calls_limit %||% NA_integer_,
      search_calls_remaining = tool_loop_guard$search_calls_remaining %||% NA_integer_,
      search_budget_exhausted = isTRUE(tool_loop_guard$search_budget_exhausted %||% FALSE)
    ),
    field_status = list(),
    diagnostics = list(
      opencode_cli_status = cli_status,
      opencode_process_timeout = process_timeout,
      opencode_timeout_seconds = timeout_seconds,
      opencode_errors = errors,
      opencode_parse_error = parse_error,
      opencode_empty_terminal_output = empty_terminal_output,
      opencode_export_fallback_used = export_fallback_used,
      opencode_export_session_id = if (nzchar(export_session_id)) export_session_id else NULL,
      opencode_export_final_text_chars = export_final_text_chars,
      opencode_export_fallback_error = if (nzchar(export_fallback_error)) export_fallback_error else NULL,
      opencode_last_event_types = tail(event_types, 8L),
      opencode_stderr = stderr_text,
      tool_loop_guard = tool_loop_guard
    ),
    json_repair = list(),
    final_payload = if (isTRUE(terminal$valid)) terminal$payload else NULL,
    terminal_valid = isTRUE(terminal$valid),
    terminal_payload_hash = terminal_hash,
    payload_integrity = payload_integrity,
    completion_gate = list(),
    tokens_used = tokens_used,
    input_tokens = input_tokens,
    output_tokens = output_tokens,
    token_trace = list()
  )
  resp$phase_timings <- list(
    total_minutes = elapsed_minutes,
    backend_invoke_minutes = elapsed_minutes,
    backend = list(
      agent_backend = "opencode",
      cli_status = cli_status,
      process_timeout = process_timeout,
      timeout_seconds = timeout_seconds
    )
  )
  resp$retrieval_metrics <- list()
  resp$tool_quality_events <- list()
  resp$candidate_resolution <- list()
  resp$finalization_status <- list()
  resp$orchestration_options <- list()
  resp$artifact_status <- list()
  resp$trace_metadata <- list()
  resp$policy_version <- NA_character_
  resp$bridge_schema_version <- NA_character_
  resp$config_snapshot <- list(
    agent_backend = "opencode",
    backend = target_backend %||% NA_character_,
    model = target_model %||% NA_character_,
    outer_model = .opencode_provider_model_id(outer_model)
  )
  resp
}

#' Run the OpenCode backend and map JSONL events back to asa_response
#' @keywords internal
.run_opencode_agent <- function(prompt,
                                agent,
                                recursion_limit = NULL,
                                expected_schema = NULL,
                                thread_id = NULL,
                                search_budget_limit = NULL,
                                unknown_after_searches = NULL,
                                auto_openwebpage_policy = NULL,
                                performance_profile = NULL,
                                webpage_policy = NULL,
                                allow_read_webpages = NULL,
                                wayback = NULL,
                                verbose = FALSE) {
  .opencode_require_processx()
  cli <- .opencode_command_spec()
  python <- .opencode_python_binary(agent$config$conda_env %||% .get_default_conda_env())
  python_path <- .opencode_python_path()
  workdir <- tempfile("asa_opencode_work_")
  dir.create(workdir, recursive = TRUE, showWarnings = FALSE)
  loop_guard <- .free_code_loop_guard_config(
    config = agent$config,
    recursion_limit = recursion_limit,
    search_budget_limit = search_budget_limit,
    unknown_after_searches = unknown_after_searches
  )
  restore_loop_guard <- .free_code_with_loop_guard_env(
    config = agent$config,
    recursion_limit = recursion_limit,
    search_budget_limit = search_budget_limit,
    unknown_after_searches = unknown_after_searches
  )
  on.exit(restore_loop_guard(), add = TRUE)

  gateway <- .free_code_launch_gateway(
    config = agent$config,
    python = python,
    python_path = python_path,
    verbose = verbose
  )
  on.exit({
    .free_code_terminate_process(gateway$process)
    unlink(c(gateway$log_file, gateway$port_file), recursive = FALSE, force = TRUE)
    unlink(workdir, recursive = TRUE, force = TRUE)
  }, add = TRUE)

  outer_model <- ASA_OPENCODE_OUTER_MODEL
  config_content <- .opencode_config_content(
    config = agent$config,
    gateway_base_url = gateway$base_url,
    python = python,
    python_path = python_path,
    target_backend = agent$backend,
    target_model = agent$model,
    outer_model = outer_model,
    loop_guard = loop_guard,
    allow_read_webpages = allow_read_webpages,
    auto_openwebpage_policy = auto_openwebpage_policy,
    wayback = wayback
  )

  cli_env <- .opencode_cli_env(config_content)
  cli_args <- c(
    cli$prefix_args,
    .opencode_cli_args(
      prompt = prompt,
      workdir = workdir,
      outer_model = outer_model,
      agent_name = .opencode_agent_name()
    )
  )

  if (isTRUE(verbose)) {
    message("  Running OpenCode backend...")
  }

  started <- Sys.time()
  timeout_seconds <- as.numeric(agent$config$timeout %||% ASA_DEFAULT_TIMEOUT)
  result <- .run_processx(
    command = cli$command,
    args = cli_args,
    wd = workdir,
    env = cli_env,
    error_on_status = FALSE,
    timeout = timeout_seconds,
    cleanup_tree = TRUE
  )
  elapsed_minutes <- as.numeric(difftime(Sys.time(), started, units = "mins"))
  process_timeout <- isTRUE(result$timeout %||% FALSE)

  parsed <- tryCatch(
    .opencode_parse_output(result$stdout),
    error = function(e) {
      list(
        events = list(),
        text = paste(
          "OpenCode did not return valid JSONL:",
          conditionMessage(e),
          if (nzchar(trimws(result$stderr))) paste0("\n", result$stderr) else ""
        ),
        session_id = NULL,
        errors = conditionMessage(e),
        stop_reason = "opencode_output_parse_error",
        usage = list(),
        raw_stdout = result$stdout,
        parse_error = TRUE
      )
    }
  )
  if (isTRUE(process_timeout)) {
    parsed$process_timeout <- TRUE
    parsed$timeout_seconds <- timeout_seconds
    parsed$stop_reason <- "process_timeout"
    if (!nzchar(.opencode_scalar_text(parsed$text %||% ""))) {
      parsed$text <- paste0("OpenCode process timed out after ", timeout_seconds, " seconds.")
    }
  }
  parsed <- .opencode_apply_export_fallback(
    output = parsed,
    cli = cli,
    cli_env = cli_env,
    workdir = workdir,
    cli_status = result$status,
    process_timeout = process_timeout,
    timeout_seconds = timeout_seconds
  )

  .opencode_response_from_output(
    output = parsed,
    prompt = prompt,
    elapsed_minutes = elapsed_minutes,
    cli_status = result$status,
    stderr = result$stderr,
    thread_id = thread_id,
    target_backend = agent$backend,
    target_model = agent$model,
    outer_model = outer_model,
    loop_guard = loop_guard,
    process_timeout = process_timeout,
    timeout_seconds = timeout_seconds
  )
}
