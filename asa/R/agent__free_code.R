#' Resolve the python source path needed by free-code helper processes
#' @keywords internal
.free_code_python_path <- function() {
  candidates <- c(
    .get_python_path(),
    file.path(getwd(), "asa", "inst", "python"),
    file.path(getwd(), "inst", "python")
  )
  candidates <- unique(normalizePath(candidates, winslash = "/", mustWork = FALSE))
  required <- file.path("asa_backend", "free_code", "anthropic_gateway.py")
  for (path in candidates) {
    if (dir.exists(path) && file.exists(file.path(path, required))) {
      return(path)
    }
  }
  stop(
    "Could not locate ASA Python sources for free-code integration. ",
    "Expected ", required, " under the package python path.",
    call. = FALSE
  )
}

#' Resolve the Python executable used for free-code helper processes
#' @keywords internal
.free_code_python_binary <- function(conda_env) {
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

#' Ensure processx is available for free-code execution
#' @keywords internal
.free_code_require_processx <- function() {
  if (!requireNamespace("processx", quietly = TRUE)) {
    stop(
      "The `free-code` agent backend requires the `processx` package. ",
      "Install it with install.packages(\"processx\").",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' Discover the free-code CLI command
#' @keywords internal
.free_code_command_spec <- function() {
  explicit_bin <- Sys.getenv("ASA_FREE_CODE_BIN", unset = "")
  if (nzchar(explicit_bin)) {
    resolved <- normalizePath(explicit_bin, winslash = "/", mustWork = FALSE)
    if (!file.exists(resolved)) {
      stop("ASA_FREE_CODE_BIN does not exist: ", explicit_bin, call. = FALSE)
    }
    return(list(command = resolved, prefix_args = character(0)))
  }

  repo <- Sys.getenv("ASA_FREE_CODE_REPO", unset = "")
  if (nzchar(repo)) {
    repo <- normalizePath(repo, winslash = "/", mustWork = FALSE)
    entrypoint <- file.path(repo, "src", "entrypoints", "cli.tsx")
    bun <- Sys.which("bun")
    if (!dir.exists(repo) || !file.exists(entrypoint)) {
      stop(
        "ASA_FREE_CODE_REPO does not contain src/entrypoints/cli.tsx: ",
        repo,
        call. = FALSE
      )
    }
    if (!nzchar(bun)) {
      stop("ASA_FREE_CODE_REPO is set, but `bun` was not found on PATH.", call. = FALSE)
    }
    return(list(
      command = unname(bun),
      prefix_args = c("run", normalizePath(entrypoint, winslash = "/", mustWork = TRUE))
    ))
  }

  path_bin <- Sys.which("free-code")
  if (nzchar(path_bin)) {
    return(list(command = unname(path_bin), prefix_args = character(0)))
  }

  stop(
    "Could not find the `free-code` CLI. Set ASA_FREE_CODE_BIN, install `free-code` ",
    "on PATH, or set ASA_FREE_CODE_REPO.",
    call. = FALSE
  )
}

#' Normalize NO_PROXY by appending localhost bypass rules
#' @keywords internal
.free_code_join_no_proxy <- function(existing = NULL) {
  values <- c(existing, "127.0.0.1", "localhost")
  values <- trimws(as.character(values %||% character(0)))
  values <- values[nzchar(values)]
  if (length(values) == 0L) {
    return("127.0.0.1,localhost")
  }
  paste(unique(values), collapse = ",")
}

#' Build proxy env vars for free-code CLI-side traffic
#' @keywords internal
.free_code_proxy_env <- function(proxy = NULL) {
  out <- c(
    HTTP_PROXY = "",
    HTTPS_PROXY = "",
    ALL_PROXY = "",
    http_proxy = "",
    https_proxy = "",
    all_proxy = ""
  )
  if (!is.null(proxy) && length(proxy) == 1L && !is.na(proxy) && nzchar(proxy)) {
    out[c("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")] <- proxy
  }
  out
}

#' Build environment for the gateway process
#' @keywords internal
.free_code_gateway_env <- function(config, python_path, port_file) {
  env <- Sys.getenv()
  env[c("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")] <- ""
  env["NO_PROXY"] <- .free_code_join_no_proxy(Sys.getenv("NO_PROXY", unset = ""))
  env["PYTHONPATH"] <- python_path
  env["PYTHONUNBUFFERED"] <- "1"
  env["ASA_FREE_CODE_PORT_FILE"] <- port_file
  env["ASA_FREE_CODE_GATEWAY_TIMEOUT"] <- as.character(as.numeric(config$timeout %||% ASA_DEFAULT_TIMEOUT))
  env
}

#' Launch the local Anthropic-compatible gateway
#' @keywords internal
.free_code_launch_gateway <- function(config, python, python_path, verbose = FALSE) {
  .free_code_require_processx()
  port_file <- tempfile("asa_free_code_gateway_port_", fileext = ".txt")
  log_file <- tempfile("asa_free_code_gateway_", fileext = ".log")
  env <- .free_code_gateway_env(config = config, python_path = python_path, port_file = port_file)

  proc <- processx::process$new(
    command = python,
    args = c("-m", "asa_backend.free_code.anthropic_gateway", "--host", "127.0.0.1", "--port", "0"),
    stdout = log_file,
    stderr = log_file,
    cleanup = TRUE,
    env = env
  )

  deadline <- Sys.time() + 15
  port <- NA_integer_
  while (Sys.time() < deadline) {
    if (!proc$is_alive()) {
      log_text <- if (file.exists(log_file)) paste(readLines(log_file, warn = FALSE), collapse = "\n") else ""
      stop("free-code gateway exited during startup.\n", log_text, call. = FALSE)
    }
    if (file.exists(port_file)) {
      port_txt <- trimws(paste(readLines(port_file, warn = FALSE), collapse = ""))
      port <- suppressWarnings(as.integer(port_txt))
      if (!is.na(port) && port > 0L) {
        break
      }
    }
    Sys.sleep(0.1)
  }

  if (is.na(port) || port <= 0L) {
    proc$kill()
    log_text <- if (file.exists(log_file)) paste(readLines(log_file, warn = FALSE), collapse = "\n") else ""
    stop("Timed out starting free-code gateway.\n", log_text, call. = FALSE)
  }

  if (isTRUE(verbose)) {
    message("  free-code gateway listening on 127.0.0.1:", port)
  }

  list(
    process = proc,
    base_url = sprintf("http://127.0.0.1:%s", port),
    log_file = log_file,
    port_file = port_file
  )
}

#' Terminate a background process created for free-code integration
#' @keywords internal
.free_code_terminate_process <- function(proc) {
  if (is.null(proc)) {
    return(invisible(NULL))
  }
  alive <- .try_or(proc$is_alive(), FALSE)
  if (isTRUE(alive)) {
    .try_or(proc$kill(), NULL)
    .try_or(proc$wait(timeout = 5000), NULL)
  }
  invisible(NULL)
}

#' Resolve whether webpage fetch should be exposed for this run
#' @keywords internal
.free_code_allow_web_fetch <- function(config,
                                       allow_read_webpages = NULL,
                                       auto_openwebpage_policy = NULL) {
  if (!is.null(allow_read_webpages)) {
    return(isTRUE(allow_read_webpages))
  }

  config_allow <- .try_or(config$search$allow_read_webpages, NULL)
  if (!is.null(config_allow)) {
    return(isTRUE(config_allow))
  }

  policy <- tolower(as.character(auto_openwebpage_policy %||% "")[1])
  identical(policy, "always")
}

#' Build environment for the MCP search server
#' @keywords internal
.free_code_mcp_env <- function(config,
                               python_path,
                               allow_read_webpages = NULL,
                               auto_openwebpage_policy = NULL) {
  search_opts <- .try_or(unclass(config$search), list())
  if (!is.list(search_opts)) {
    search_opts <- list()
  }
  webpage_opts <- .resolve_webpage_reader_settings(
    config_search = config$search %||% NULL,
    allow_read_webpages = .free_code_allow_web_fetch(
      config = config,
      allow_read_webpages = allow_read_webpages,
      auto_openwebpage_policy = auto_openwebpage_policy
    )
  )
  tor_opts <- .try_or(unclass(config$tor), list())
  if (!is.list(tor_opts)) {
    tor_opts <- list()
  }

  env <- Sys.getenv()
  env["PYTHONPATH"] <- python_path
  env["PYTHONUNBUFFERED"] <- "1"
  env["ASA_FREE_CODE_PROXY"] <- as.character(config$proxy %||% "")
  env["ASA_FREE_CODE_USE_BROWSER"] <- if (isTRUE(config$use_browser %||% ASA_DEFAULT_USE_BROWSER)) "true" else "false"
  env["ASA_FREE_CODE_ALLOW_READ_WEBPAGES"] <- if (isTRUE(webpage_opts$allow_read_webpages)) "true" else "false"
  env["ASA_FREE_CODE_SEARCH_OPTIONS_JSON"] <- jsonlite::toJSON(search_opts, auto_unbox = TRUE, null = "null")
  env["ASA_FREE_CODE_TOR_OPTIONS_JSON"] <- jsonlite::toJSON(tor_opts, auto_unbox = TRUE, null = "null")
  env["ASA_FREE_CODE_WEBPAGE_OPTIONS_JSON"] <- jsonlite::toJSON(webpage_opts, auto_unbox = TRUE, null = "null")
  mcp_log_file <- Sys.getenv("ASA_FREE_CODE_MCP_LOG_FILE", unset = "")
  if (nzchar(mcp_log_file)) {
    env["ASA_FREE_CODE_MCP_LOG_FILE"] <- mcp_log_file
  }
  env <- c(env, .free_code_proxy_env(config$proxy %||% NULL))
  env["NO_PROXY"] <- .free_code_join_no_proxy(Sys.getenv("NO_PROXY", unset = ""))
  env
}

#' Write MCP config for free-code stdio server launch
#' @keywords internal
.free_code_write_mcp_config <- function(config,
                                        python,
                                        python_path,
                                        allow_read_webpages = NULL,
                                        auto_openwebpage_policy = NULL) {
  mcp_env <- as.list(.free_code_mcp_env(
    config = config,
    python_path = python_path,
    allow_read_webpages = allow_read_webpages,
    auto_openwebpage_policy = auto_openwebpage_policy
  ))
  cfg <- list(
    mcpServers = list(
      asa_search = list(
        type = "stdio",
        command = python,
        args = c("-m", "asa_backend.free_code.mcp_search_server"),
        env = mcp_env
      )
    )
  )
  path <- tempfile("asa_free_code_mcp_", fileext = ".json")
  writeLines(
    jsonlite::toJSON(cfg, auto_unbox = TRUE, pretty = TRUE, null = "null"),
    con = path,
    useBytes = TRUE
  )
  path
}

#' Convert ASA expected_schema descriptors to JSON Schema for free-code
#' @keywords internal
.free_code_expected_schema_to_json_schema <- function(expected_schema) {
  if (is.null(expected_schema)) {
    return(NULL)
  }
  .free_code_schema_to_json_schema(expected_schema)
}

#' Convert a leaf descriptor string to JSON Schema
#' @keywords internal
.free_code_descriptor_to_json_schema <- function(descriptor) {
  if (!is.character(descriptor) || length(descriptor) != 1L) {
    return(list(type = "string"))
  }

  tokens <- trimws(strsplit(descriptor, "\\|", perl = TRUE)[[1]])
  tokens <- tokens[nzchar(tokens)]
  if (length(tokens) == 0L) {
    return(list(type = "string"))
  }

  type_map <- c(
    string = "string",
    character = "string",
    integer = "integer",
    int = "integer",
    number = "number",
    numeric = "number",
    double = "number",
    boolean = "boolean",
    logical = "boolean",
    object = "object",
    array = "array",
    list = "array",
    null = "null"
  )
  normalized <- tolower(tokens)
  known_types <- unname(type_map[normalized[normalized %in% names(type_map)]])
  enum_values <- tokens[!(normalized %in% names(type_map))]

  branches <- list()
  for (type_name in unique(known_types)) {
    branches[[length(branches) + 1L]] <- list(type = type_name)
  }
  for (enum_value in enum_values) {
    branches[[length(branches) + 1L]] <- list(const = enum_value)
  }

  if (length(branches) == 0L) {
    return(list(type = "string"))
  }
  if (length(branches) == 1L) {
    return(branches[[1L]])
  }
  list(anyOf = branches)
}

#' Recursively convert ASA schema trees to JSON Schema
#' @keywords internal
.free_code_schema_to_json_schema <- function(schema) {
  if (is.character(schema) && length(schema) == 1L) {
    return(.free_code_descriptor_to_json_schema(schema))
  }

  if (is.list(schema)) {
    schema_names <- names(schema)
    has_names <- !is.null(schema_names) && any(nzchar(schema_names))

    if (has_names) {
      properties <- list()
      required <- character(0)
      for (name in schema_names) {
        if (!nzchar(name)) {
          next
        }
        properties[[name]] <- .free_code_schema_to_json_schema(schema[[name]])
        required <- c(required, name)
      }
      return(list(
        type = "object",
        properties = properties,
        required = unique(required),
        additionalProperties = FALSE
      ))
    }

    if (length(schema) >= 1L) {
      return(list(
        type = "array",
        items = .free_code_schema_to_json_schema(schema[[1L]])
      ))
    }
  }

  list(type = "string")
}

#' Build CLI arguments for free-code
#' @keywords internal
.free_code_cli_args <- function(prompt,
                                mcp_config_path,
                                expected_schema = NULL,
                                recursion_limit = NULL) {
  args <- c(
    "--bare",
    "--print",
    "--output-format", "json",
    "--strict-mcp-config",
    "--tools", "",
    "--disallowedTools", "WebSearch,WebFetch",
    "--dangerously-skip-permissions",
    "--model", ASA_FREE_CODE_OUTER_MODEL,
    "--mcp-config", mcp_config_path,
    "--append-system-prompt",
    paste(
      "ASA search mode is active.",
      "Use `mcp__asa_search__web_search` for public web search.",
      "Use `mcp__asa_search__web_fetch` to open public webpages when that tool is available.",
      "Do not assume native WebSearch or WebFetch are available."
    )
  )

  if (!is.null(recursion_limit)) {
    args <- c(args, "--max-turns", as.character(as.integer(recursion_limit)))
  }

  schema_json <- .free_code_expected_schema_to_json_schema(expected_schema)
  if (!is.null(schema_json)) {
    args <- c(
      args,
      "--json-schema",
      jsonlite::toJSON(schema_json, auto_unbox = TRUE, null = "null")
    )
  }

  c(args, prompt)
}

#' Build free-code CLI environment
#' @keywords internal
.free_code_cli_env <- function(config, gateway_base_url, target_backend, target_model) {
  env <- Sys.getenv()
  # Keep provider API traffic direct. free-code's Bun fetch proxy path does not
  # honor NO_PROXY for local gateways, so proxy settings stay scoped to the MCP
  # search server instead of the CLI process.
  env <- c(env, .free_code_proxy_env(NULL))
  env["NO_PROXY"] <- .free_code_join_no_proxy(Sys.getenv("NO_PROXY", unset = ""))
  env["ANTHROPIC_BASE_URL"] <- gateway_base_url
  env["ANTHROPIC_API_KEY"] <- "asa-local-gateway"
  env["ANTHROPIC_CUSTOM_HEADERS"] <- paste(
    c(
      paste0("X-ASA-Target-Backend: ", target_backend),
      paste0("X-ASA-Target-Model: ", target_model)
    ),
    collapse = "\n"
  )
  env
}

#' Parse free-code JSON stdout
#' @keywords internal
.free_code_parse_output <- function(stdout) {
  text <- trimws(as.character(stdout %||% ""))
  if (!nzchar(text)) {
    stop("free-code returned empty stdout.", call. = FALSE)
  }
  jsonlite::fromJSON(text, simplifyVector = FALSE)
}

#' Construct an asa_response from free-code CLI output
#' @keywords internal
.free_code_response_from_output <- function(output,
                                            prompt,
                                            elapsed_minutes,
                                            cli_status = 0L,
                                            stderr = "",
                                            thread_id = NULL,
                                            target_backend = NULL,
                                            target_model = NULL) {
  structured_output <- output$structured_output %||% NULL
  result_text <- output$result %||% ""
  result_text <- as.character(result_text %||% "")
  if (length(result_text) == 0L || is.na(result_text[[1]])) {
    result_text <- ""
  } else {
    result_text <- result_text[[1]]
  }

  payload_json <- NA_character_
  terminal_valid <- FALSE
  terminal_hash <- NA_character_
  payload_integrity <- list(
    released_from = "free_code_result_text",
    canonical_available = FALSE,
    canonical_matches_message = TRUE,
    message_sanitized = FALSE
  )
  if (!is.null(structured_output)) {
    payload_json <- jsonlite::toJSON(structured_output, auto_unbox = TRUE, null = "null")
    terminal_valid <- TRUE
    terminal_hash <- digest::digest(enc2utf8(payload_json), algo = "sha256")
    payload_integrity <- utils::modifyList(
      payload_integrity,
      list(
        released_from = "free_code_structured_output",
        canonical_available = TRUE,
        canonical_matches_message = identical(result_text, payload_json)
      )
    )
    if (!nzchar(result_text)) {
      result_text <- payload_json
    }
  }

  is_error <- isTRUE(output$is_error %||% FALSE) || !identical(as.integer(cli_status), 0L)
  status_code <- if (is_error) ASA_STATUS_ERROR else ASA_STATUS_SUCCESS
  stop_reason <- as.character(output$stop_reason %||% if (terminal_valid) "structured_output" else "end_turn")
  stop_reason <- if (length(stop_reason) > 0L && nzchar(stop_reason[[1]])) stop_reason[[1]] else NA_character_

  usage <- output$usage %||% list()
  input_tokens <- .as_scalar_int(usage$input_tokens %||% NA_integer_)
  output_tokens <- .as_scalar_int(usage$output_tokens %||% NA_integer_)
  tokens_used <- .as_scalar_int(usage$total_tokens %||% NA_integer_)
  if (is.na(tokens_used) && (!is.na(input_tokens) || !is.na(output_tokens))) {
    tokens_used <- sum(c(input_tokens, output_tokens), na.rm = TRUE)
  }

  trace_json <- jsonlite::toJSON(output, auto_unbox = TRUE, null = "null", pretty = TRUE)
  trace <- result_text
  if (identical(status_code, ASA_STATUS_ERROR) && nzchar(trimws(as.character(stderr %||% "")))) {
    trace <- paste(c(result_text, stderr), collapse = "\n\n")
  }

  resp <- asa_response(
    message = result_text,
    status_code = status_code,
    raw_response = output,
    trace = trace,
    trace_json = trace_json,
    elapsed_time = elapsed_minutes,
    fold_stats = list(fold_count = 0L),
    prompt = prompt,
    thread_id = output$session_id %||% thread_id,
    stop_reason = stop_reason,
    budget_state = list(),
    field_status = list(),
    diagnostics = list(
      free_code_cli_status = as.integer(cli_status),
      free_code_permission_denials = output$permission_denials %||% list()
    ),
    json_repair = list(),
    final_payload = structured_output,
    terminal_valid = terminal_valid,
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
      agent_backend = "free-code",
      cli_status = as.integer(cli_status)
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
    agent_backend = "free-code",
    backend = target_backend %||% NA_character_,
    model = target_model %||% NA_character_,
    outer_model = ASA_FREE_CODE_OUTER_MODEL
  )
  resp
}

#' Run the free-code backend and map output back to asa_response
#' @keywords internal
.run_free_code_agent <- function(prompt,
                                 agent,
                                 recursion_limit = NULL,
                                 expected_schema = NULL,
                                 thread_id = NULL,
                                 auto_openwebpage_policy = NULL,
                                 performance_profile = NULL,
                                 webpage_policy = NULL,
                                 allow_read_webpages = NULL,
                                 verbose = FALSE) {
  .free_code_require_processx()
  cli <- .free_code_command_spec()
  python <- .free_code_python_binary(agent$config$conda_env %||% .get_default_conda_env())
  python_path <- .free_code_python_path()
  workdir <- tempfile("asa_free_code_work_")
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

  mcp_config_path <- .free_code_write_mcp_config(
    config = agent$config,
    python = python,
    python_path = python_path,
    allow_read_webpages = allow_read_webpages,
    auto_openwebpage_policy = auto_openwebpage_policy
  )
  on.exit(unlink(mcp_config_path, force = TRUE), add = TRUE)

  cli_env <- .free_code_cli_env(
    config = agent$config,
    gateway_base_url = gateway$base_url,
    target_backend = agent$backend,
    target_model = agent$model
  )
  cli_args <- c(
    cli$prefix_args,
    .free_code_cli_args(
      prompt = prompt,
      mcp_config_path = mcp_config_path,
      expected_schema = expected_schema,
      recursion_limit = recursion_limit
    )
  )

  if (isTRUE(verbose)) {
    message("  Running free-code backend...")
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
    .free_code_parse_output(result$stdout),
    error = function(e) {
      list(
        is_error = TRUE,
        result = paste(
          "free-code did not return valid JSON:",
          conditionMessage(e),
          if (nzchar(trimws(result$stderr))) paste0("\n", result$stderr) else ""
        ),
        stop_reason = "free_code_output_parse_error"
      )
    }
  )

  .free_code_response_from_output(
    output = parsed,
    prompt = prompt,
    elapsed_minutes = elapsed_minutes,
    cli_status = result$status,
    stderr = result$stderr,
    thread_id = thread_id,
    target_backend = agent$backend,
    target_model = agent$model
  )
}
