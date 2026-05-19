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
                                        allow_read_webpages = NULL,
                                        auto_openwebpage_policy = NULL) {
  list(
    type = "local",
    command = unname(c(python, "-m", "asa_backend.free_code.mcp_search_server")),
    enabled = TRUE,
    environment = as.list(.free_code_mcp_env(
      config = config,
      python_path = python_path,
      allow_read_webpages = allow_read_webpages,
      auto_openwebpage_policy = auto_openwebpage_policy
    )),
    timeout = 30000L
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
                                     allow_read_webpages = NULL,
                                     auto_openwebpage_policy = NULL) {
  model_ref <- .opencode_model_ref(outer_model)
  model_id <- .opencode_provider_model_id(outer_model)

  headers <- list()
  headers[["X-ASA-Target-Backend"]] <- target_backend
  headers[["X-ASA-Target-Model"]] <- target_model

  tools <- list(
    webfetch = FALSE,
    websearch = FALSE,
    write = FALSE,
    edit = FALSE,
    bash = FALSE
  )

  permission <- list(
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
      allow_read_webpages = allow_read_webpages,
      auto_openwebpage_policy = auto_openwebpage_policy
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
    prompt
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
      .opencode_scalar_text(event$sessionID %||% event$session_id %||% event$part$sessionID %||% "")
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
                                           outer_model = ASA_OPENCODE_OUTER_MODEL) {
  result_text <- .opencode_scalar_text(output$text %||% "")
  stderr_text <- .opencode_scalar_text(stderr %||% "")
  errors <- as.character(output$errors %||% character(0))
  errors <- errors[!is.na(errors) & nzchar(errors)]
  parse_error <- isTRUE(output$parse_error %||% FALSE)
  cli_status <- as.integer(cli_status %||% 0L)
  is_error <- parse_error || length(errors) > 0L || !identical(cli_status, 0L)

  if (is_error && !nzchar(result_text)) {
    message_parts <- c(
      if (parse_error) "OpenCode did not return valid JSONL." else "OpenCode returned an error.",
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
      stderr = stderr_text,
      errors = errors,
      parse_error = parse_error,
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
    stop_reason = output$stop_reason %||% if (isTRUE(terminal$valid)) "structured_output" else "end_turn",
    budget_state = list(),
    field_status = list(),
    diagnostics = list(
      opencode_cli_status = cli_status,
      opencode_errors = errors,
      opencode_parse_error = parse_error,
      opencode_stderr = stderr_text
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
      cli_status = cli_status
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
                                auto_openwebpage_policy = NULL,
                                performance_profile = NULL,
                                webpage_policy = NULL,
                                allow_read_webpages = NULL,
                                verbose = FALSE) {
  .opencode_require_processx()
  cli <- .opencode_command_spec()
  python <- .opencode_python_binary(agent$config$conda_env %||% .get_default_conda_env())
  python_path <- .opencode_python_path()
  workdir <- tempfile("asa_opencode_work_")
  dir.create(workdir, recursive = TRUE, showWarnings = FALSE)

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
    allow_read_webpages = allow_read_webpages,
    auto_openwebpage_policy = auto_openwebpage_policy
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
  result <- processx::run(
    command = cli$command,
    args = cli_args,
    wd = workdir,
    env = cli_env,
    error_on_status = FALSE,
    timeout = as.numeric(agent$config$timeout %||% ASA_DEFAULT_TIMEOUT) * 1000
  )
  elapsed_minutes <- as.numeric(difftime(Sys.time(), started, units = "mins"))

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

  .opencode_response_from_output(
    output = parsed,
    prompt = prompt,
    elapsed_minutes = elapsed_minutes,
    cli_status = result$status,
    stderr = result$stderr,
    thread_id = thread_id,
    target_backend = agent$backend,
    target_model = agent$model,
    outer_model = outer_model
  )
}
