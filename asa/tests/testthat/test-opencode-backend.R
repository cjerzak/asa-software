make_fake_opencode_bin <- function() {
  path <- tempfile("asa-opencode-bin-", fileext = ".sh")
  writeLines(
    c(
      "#!/bin/sh",
      "if [ -n \"$ASA_OPENCODE_TEST_ARGS_FILE\" ]; then",
      "  printf '%s\\n' \"$@\" > \"$ASA_OPENCODE_TEST_ARGS_FILE\"",
      "fi",
      "if [ -n \"$ASA_OPENCODE_TEST_ENV_FILE\" ]; then",
      "  env | sort > \"$ASA_OPENCODE_TEST_ENV_FILE\"",
      "fi",
      "if [ -n \"$ASA_OPENCODE_TEST_CONFIG_FILE\" ]; then",
      "  printf '%s' \"$OPENCODE_CONFIG_CONTENT\" > \"$ASA_OPENCODE_TEST_CONFIG_FILE\"",
      "fi",
      "printf '%s' \"$ASA_OPENCODE_TEST_STDOUT\""
    ),
    con = path,
    useBytes = TRUE
  )
  Sys.chmod(path, mode = "755")
  path
}

make_fake_opencode_exec <- function(name = "opencode", body = c("#!/bin/sh", "exit 0")) {
  dir <- tempfile("asa-opencode-path-")
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  path <- file.path(dir, name)
  writeLines(body, con = path, useBytes = TRUE)
  Sys.chmod(path, mode = "755")
  list(dir = dir, path = path)
}

opencode_jsonl <- function(...) {
  events <- list(...)
  paste(
    vapply(
      events,
      function(event) paste(jsonlite::toJSON(event, auto_unbox = TRUE, null = "null"), collapse = ""),
      character(1)
    ),
    collapse = "\n"
  )
}

opencode_export_json <- function(messages) {
  paste(jsonlite::toJSON(list(messages = messages), auto_unbox = TRUE, null = "null"), collapse = "")
}

opencode_empty_terminal_output <- function(session_id = "ses_abc", raw_stdout = NULL, errors = character(0), parse_error = FALSE) {
  event <- list(type = "step_start", sessionID = session_id, part = list(type = "step-start"))
  list(
    events = list(event),
    text = "",
    session_id = NULL,
    errors = errors,
    stop_reason = NULL,
    usage = list(),
    raw_stdout = raw_stdout %||% opencode_jsonl(event),
    parse_error = parse_error
  )
}

opencode_gateway_probe <- function(python_path, python = Sys.which("python3")) {
  skip_if_not(nzchar(python), "python3 not found")

  script <- tempfile("asa-opencode-gateway-probe-", fileext = ".py")
  stdout_file <- tempfile("asa-opencode-gateway-probe-stdout-")
  stderr_file <- tempfile("asa-opencode-gateway-probe-stderr-")
  on.exit(unlink(c(script, stdout_file, stderr_file), force = TRUE), add = TRUE)

  writeLines(
    c(
      "import json",
      "import sys",
      "import threading",
      "import types",
      "import urllib.error",
      "import urllib.request",
      "",
      sprintf("sys.path.insert(0, %s)", as.character(jsonlite::toJSON(python_path, auto_unbox = TRUE))),
      "",
      "class DummyChat:",
      "    def __init__(self, *args, **kwargs):",
      "        pass",
      "",
      "class FakeMessage:",
      "    def __init__(self, content='', tool_calls=None, usage_metadata=None, **kwargs):",
      "        self.content = content",
      "        self.tool_calls = tool_calls if tool_calls is not None else kwargs.get('tool_calls', [])",
      "        self.usage_metadata = usage_metadata if usage_metadata is not None else kwargs.get('usage_metadata', {})",
      "",
      "class FakeToolMessage(FakeMessage):",
      "    def __init__(self, content='', tool_call_id='', status=None, **kwargs):",
      "        super().__init__(content=content, **kwargs)",
      "        self.tool_call_id = tool_call_id",
      "        self.status = status",
      "",
      "def install_module(name, **attrs):",
      "    module = types.ModuleType(name)",
      "    for key, value in attrs.items():",
      "        setattr(module, key, value)",
      "    sys.modules[name] = module",
      "    return module",
      "",
      "langchain_core = types.ModuleType('langchain_core')",
      "langchain_core.__path__ = []",
      "sys.modules['langchain_core'] = langchain_core",
      "install_module(",
      "    'langchain_core.messages',",
      "    AIMessage=FakeMessage,",
      "    HumanMessage=FakeMessage,",
      "    SystemMessage=FakeMessage,",
      "    ToolMessage=FakeToolMessage,",
      ")",
      "install_module('langchain_anthropic', ChatAnthropic=DummyChat)",
      "install_module('langchain_aws', ChatBedrockConverse=DummyChat)",
      "install_module('langchain_google_genai', ChatGoogleGenerativeAI=DummyChat)",
      "install_module('langchain_groq', ChatGroq=DummyChat)",
      "install_module('langchain_openai', ChatOpenAI=DummyChat)",
      "",
      "from asa_backend.free_code import anthropic_gateway as gateway",
      "",
      "def fake_invoke(request, backend, model):",
      "    return gateway.AIMessage(",
      "        content='gateway ok',",
      "        usage_metadata={'input_tokens': 2, 'output_tokens': 3},",
      "    )",
      "",
      "gateway._invoke_model = fake_invoke",
      "server = gateway.ThreadingHTTPServer(('127.0.0.1', 0), gateway._GatewayHandler)",
      "thread = threading.Thread(target=server.serve_forever, daemon=True)",
      "thread.start()",
      "base_url = f'http://127.0.0.1:{server.server_port}'",
      "",
      "def post(path, stream=False):",
      "    payload = {",
      "        'model': 'claude-test',",
      "        'max_tokens': 64,",
      "        'stream': stream,",
      "        'messages': [{'role': 'user', 'content': 'ping'}],",
      "    }",
      "    body = json.dumps(payload).encode('utf-8')",
      "    request = urllib.request.Request(",
      "        base_url + path,",
      "        data=body,",
      "        method='POST',",
      "        headers={",
      "            'Content-Type': 'application/json',",
      "            'X-ASA-Target-Backend': 'openai',",
      "            'X-ASA-Target-Model': 'gpt-4.1-mini',",
      "        },",
      "    )",
      "    try:",
      "        with urllib.request.urlopen(request, timeout=5) as response:",
      "            return {",
      "                'status': response.status,",
      "                'content_type': response.headers.get('Content-Type', ''),",
      "                'body': response.read().decode('utf-8'),",
      "            }",
      "    except urllib.error.HTTPError as exc:",
      "        return {",
      "            'status': exc.code,",
      "            'content_type': exc.headers.get('Content-Type', ''),",
      "            'body': exc.read().decode('utf-8'),",
      "        }",
      "",
      "try:",
      "    results = {",
      "        'v1_messages': post('/v1/messages'),",
      "        'messages': post('/messages'),",
      "        'messages_stream': post('/messages', stream=True),",
      "        'bad': post('/bad'),",
      "    }",
      "finally:",
      "    server.shutdown()",
      "    thread.join(timeout=5)",
      "    server.server_close()",
      "",
      "print(json.dumps(results), flush=True)",
      ""
    ),
    con = script,
    useBytes = TRUE
  )

  status <- suppressWarnings(system2(
    command = python,
    args = script,
    stdout = stdout_file,
    stderr = stderr_file,
    timeout = 15
  ))
  status_int <- suppressWarnings(as.integer(status))
  if (is.na(status_int)) {
    status_int <- 0L
  }

  stdout <- paste(readLines(stdout_file, warn = FALSE), collapse = "\n")
  stderr <- paste(readLines(stderr_file, warn = FALSE), collapse = "\n")
  if (status_int != 0L) {
    stop(
      sprintf("OpenCode gateway probe failed (status=%d).\nstdout: %s\nstderr: %s", status_int, stdout, stderr),
      call. = FALSE
    )
  }

  jsonlite::fromJSON(stdout, simplifyVector = FALSE)
}

test_that("asa_config and initialize_agent accept opencode", {
  cfg <- asa::asa_config(
    agent_backend = "opencode",
    backend = "exo",
    model = "local-model",
    proxy = NULL
  )
  expect_s3_class(cfg, "asa_config")
  expect_identical(cfg$agent_backend, "opencode")

  testthat::local_mocked_bindings(
    .opencode_require_processx = function() invisible(TRUE),
    .opencode_command_spec = function() list(command = "/usr/bin/opencode", prefix_args = character(0)),
    .opencode_python_binary = function(conda_env) "/usr/bin/python3",
    .opencode_python_path = function() "/tmp/asa-python",
    .package = "asa"
  )
  on.exit(asa::reset_agent(), add = TRUE)

  agent <- asa::initialize_agent(
    agent_backend = "opencode",
    backend = "exo",
    model = "local-model",
    proxy = NULL,
    verbose = FALSE
  )

  expect_s3_class(agent, "asa_agent")
  expect_null(agent$python_agent)
  expect_identical(agent$config$agent_backend, "opencode")
  expect_identical(agent$backend, "exo")
  expect_identical(agent$model, "local-model")
})

test_that("OpenCode command discovery supports ASA_OPENCODE_BIN and PATH", {
  skip_if_not_installed("withr")

  explicit <- make_fake_opencode_exec("custom-opencode")
  withr::local_envvar(c(
    ASA_OPENCODE_BIN = explicit$path,
    PATH = ""
  ))
  spec <- asa:::.opencode_command_spec()
  expect_identical(
    normalizePath(spec$command, winslash = "/", mustWork = TRUE),
    normalizePath(explicit$path, winslash = "/", mustWork = TRUE)
  )

  path_bin <- make_fake_opencode_exec("opencode")
  withr::local_envvar(c(
    ASA_OPENCODE_BIN = NA_character_,
    PATH = paste(c(path_bin$dir, Sys.getenv("PATH")), collapse = .Platform$path.sep)
  ))
  spec <- asa:::.opencode_command_spec()
  expect_identical(
    normalizePath(spec$command, winslash = "/", mustWork = TRUE),
    normalizePath(path_bin$path, winslash = "/", mustWork = TRUE)
  )
})

test_that("OpenCode config content wires provider, headers, tools, and MCP", {
  cfg <- asa::asa_config(
    agent_backend = "opencode",
    backend = "openai",
    model = "gpt-4.1-mini",
    proxy = "socks5h://127.0.0.1:9050",
    use_browser = FALSE,
    search = asa::search_options(
      max_results = 7L,
      timeout = 12,
      search_doc_content_chars_max = 321L,
      allow_read_webpages = TRUE
    )
  )
  loop_guard <- asa:::.free_code_loop_guard_config(
    config = cfg,
    recursion_limit = 40L,
    search_budget_limit = 8L,
    unknown_after_searches = 3L
  )

  content <- asa:::.opencode_config_content(
    config = cfg,
    gateway_base_url = "http://127.0.0.1:8765",
    python = "/usr/bin/python3",
    python_path = "/tmp/asa-python",
    target_backend = "groq",
    target_model = "llama-3.3-70b-versatile",
    outer_model = "claude-test",
    loop_guard = loop_guard,
    allow_read_webpages = TRUE
  )
  parsed <- jsonlite::fromJSON(content, simplifyVector = FALSE)

  expect_identical(parsed$model, "anthropic/claude-test")
  expect_identical(parsed$small_model, "anthropic/claude-test")
  expect_identical(unlist(parsed$enabled_providers, use.names = FALSE), "anthropic")
  expect_identical(parsed$provider$anthropic$options$baseURL, "http://127.0.0.1:8765")
  expect_identical(parsed$provider$anthropic$options$apiKey, "asa-local-gateway")
  expect_identical(parsed$provider$anthropic$options$headers$`X-ASA-Target-Backend`, "groq")
  expect_identical(parsed$provider$anthropic$options$headers$`X-ASA-Target-Model`, "llama-3.3-70b-versatile")
  expect_true("claude-test" %in% names(parsed$provider$anthropic$models))

  expect_false(parsed$tools$webfetch)
  expect_false(parsed$tools$websearch)
  expect_false(parsed$tools$write)
  expect_false(parsed$tools$edit)
  expect_false(parsed$tools$bash)
  expect_false(parsed$tools$question)
  expect_identical(parsed$permission$question, "deny")
  expect_identical(parsed$permission$webfetch, "deny")
  expect_identical(parsed$permission$edit, "deny")

  server <- parsed$mcp$asa_search
  expect_identical(server$type, "local")
  expect_true(isTRUE(server$enabled))
  expect_identical(server$timeout, 17000L)
  expect_identical(unlist(server$command, use.names = FALSE), c("/usr/bin/python3", "-m", "asa_backend.free_code.mcp_search_server"))
  expect_identical(server$environment$PYTHONPATH, "/tmp/asa-python")
  expect_identical(server$environment$ASA_FREE_CODE_PROXY, "socks5h://127.0.0.1:9050")
  expect_identical(server$environment$ASA_FREE_CODE_USE_BROWSER, "false")
  expect_identical(server$environment$ASA_FREE_CODE_ALLOW_READ_WEBPAGES, "true")
  guard_env <- jsonlite::fromJSON(server$environment$ASA_FREE_CODE_LOOP_GUARD_JSON, simplifyVector = FALSE)
  expect_identical(guard_env$recursion_limit, 40L)
  expect_identical(guard_env$search_budget_limit, 8L)
  expect_identical(guard_env$unknown_after_searches, 3L)

  search_opts <- jsonlite::fromJSON(server$environment$ASA_FREE_CODE_SEARCH_OPTIONS_JSON, simplifyVector = FALSE)
  expect_identical(search_opts$max_results, 7L)
  expect_equal(search_opts$timeout, 12)
  expect_identical(search_opts$search_doc_content_chars_max, 321L)
})

test_that("OpenCode gateway accepts /messages and /v1/messages", {
  python_path <- asa_test_python_path(
    required_files = file.path("asa_backend", "free_code", "anthropic_gateway.py")
  )
  skip_if_not(nzchar(python_path) && dir.exists(python_path), "Python modules not found")

  result <- opencode_gateway_probe(python_path)

  expect_equal(result$v1_messages$status, 200L)
  expect_match(result$v1_messages$body, "gateway ok", fixed = TRUE)
  expect_equal(result$messages$status, 200L)
  expect_match(result$messages$body, "gateway ok", fixed = TRUE)

  expect_equal(result$messages_stream$status, 200L)
  expect_match(result$messages_stream$content_type, "text/event-stream", fixed = TRUE)
  expect_match(result$messages_stream$body, "event: message_start", fixed = TRUE)
  expect_match(result$messages_stream$body, "event: message_stop", fixed = TRUE)

  expect_equal(result$bad$status, 404L)
  expect_match(result$bad$body, "Unsupported path: /bad", fixed = TRUE)
})

test_that("OpenCode JSONL parser handles text, errors, sessions, usage, and malformed stdout", {
  stdout <- opencode_jsonl(
    list(type = "step_start", sessionID = "ses-opencode-1", part = list(type = "step-start")),
    list(type = "text", sessionID = "ses-opencode-1", part = list(type = "text", text = "Hello")),
    list(type = "text", sessionID = "ses-opencode-1", part = list(type = "text", text = "World")),
    list(
      type = "step_finish",
      sessionID = "ses-opencode-1",
      part = list(
        type = "step-finish",
        reason = "stop",
        tokens = list(input = 3L, output = 5L, reasoning = 2L, cache = list(read = 1L, write = 1L))
      )
    )
  )

  parsed <- asa:::.opencode_parse_output(stdout)
  expect_identical(parsed$text, "Hello\nWorld")
  expect_identical(parsed$session_id, "ses-opencode-1")
  expect_identical(parsed$stop_reason, "stop")
  expect_identical(parsed$usage$input_tokens, 5L)
  expect_identical(parsed$usage$output_tokens, 5L)
  expect_identical(parsed$usage$reasoning_tokens, 2L)
  expect_identical(parsed$usage$total_tokens, 12L)

  error_stdout <- opencode_jsonl(
    list(
      type = "error",
      sessionID = "ses-opencode-2",
      error = list(name = "APIError", data = list(message = "Rate limit exceeded", statusCode = 429L))
    )
  )
  error_parsed <- asa:::.opencode_parse_output(error_stdout)
  expect_identical(error_parsed$session_id, "ses-opencode-2")
  expect_match(error_parsed$errors[[1]], "APIError: Rate limit exceeded")
  expect_match(error_parsed$errors[[1]], "status 429")

  expect_error(
    asa:::.opencode_parse_output(paste(stdout, "not-json", sep = "\n")),
    "malformed JSONL stdout at line 5"
  )
  expect_error(asa:::.opencode_parse_output(""), "empty stdout")
})

test_that("OpenCode final payload accepts raw and fenced JSON", {
  raw <- asa:::.opencode_final_payload("{\"status\":\"complete\",\"answer\":42}")
  expect_true(isTRUE(raw$valid))
  expect_identical(raw$payload$status, "complete")
  expect_identical(raw$payload$answer, 42L)

  fenced <- asa:::.opencode_final_payload(
    "```json\n{\"status\":\"complete\",\"answer\":42}\n```"
  )
  expect_true(isTRUE(fenced$valid))
  expect_identical(fenced$payload$status, "complete")
  expect_identical(fenced$payload$answer, 42L)
  expect_identical(fenced$payload_json, raw$payload_json)
})

test_that("OpenCode export fallback recovers empty terminal output", {
  final_text <- "{\"ok\":true,\"confidence\":1}"
  export_stdout <- opencode_export_json(list(
    list(info = list(role = "user"), parts = list(list(type = "text", text = "prompt"))),
    list(info = list(role = "assistant"), parts = list(
      list(type = "text", text = ""),
      list(type = "text", text = final_text)
    ))
  ))
  captured <- NULL
  testthat::local_mocked_bindings(
    .run_processx = function(...) {
      captured <<- list(...)
      list(status = 0L, stdout = export_stdout, stderr = "", timeout = FALSE)
    },
    .package = "asa"
  )

  recovered <- asa:::.opencode_apply_export_fallback(
    output = opencode_empty_terminal_output("ses_abc"),
    cli = list(command = "/fake/opencode", prefix_args = character(0)),
    cli_env = c(TEST = "1"),
    workdir = tempdir(),
    cli_status = 0L,
    process_timeout = FALSE,
    timeout_seconds = 10
  )
  resp <- asa:::.opencode_response_from_output(
    output = recovered,
    prompt = "Return JSON.",
    elapsed_minutes = 0.01,
    cli_status = 0L,
    process_timeout = FALSE,
    timeout_seconds = 10
  )

  expect_identical(captured$command, "/fake/opencode")
  expect_identical(captured$args, c("export", "ses_abc"))
  expect_identical(resp$status_code, asa:::ASA_STATUS_SUCCESS)
  expect_identical(resp$message, final_text)
  expect_false(isTRUE(resp$diagnostics$opencode_empty_terminal_output))
  expect_true(isTRUE(resp$diagnostics$opencode_export_fallback_used))
  expect_identical(resp$diagnostics$opencode_export_session_id, "ses_abc")
  expect_identical(resp$diagnostics$opencode_export_final_text_chars, nchar(final_text))
  expect_true(any(vapply(resp$raw_response$events, function(event) {
    isTRUE(event$synthetic) && identical(event$source, "opencode_export_fallback")
  }, logical(1))))
})

test_that("OpenCode export fallback parses prefixed export stdout", {
  final_text <- "{\"ok\":true}"
  export_stdout <- paste(
    "Exporting session: ses_abc",
    opencode_export_json(list(
      list(info = list(role = "assistant"), parts = list(list(type = "text", text = final_text)))
    )),
    sep = "\n"
  )
  testthat::local_mocked_bindings(
    .run_processx = function(...) {
      list(status = 0L, stdout = export_stdout, stderr = "", timeout = FALSE)
    },
    .package = "asa"
  )

  recovered <- asa:::.opencode_apply_export_fallback(
    output = opencode_empty_terminal_output("ses_abc"),
    cli = list(command = "/fake/opencode", prefix_args = character(0)),
    cli_env = character(0),
    workdir = tempdir(),
    cli_status = 0L,
    process_timeout = FALSE,
    timeout_seconds = 10
  )
  resp <- asa:::.opencode_response_from_output(
    output = recovered,
    prompt = "Return JSON.",
    elapsed_minutes = 0.01,
    cli_status = 0L
  )

  expect_identical(resp$status_code, asa:::ASA_STATUS_SUCCESS)
  expect_identical(resp$message, final_text)
  expect_true(isTRUE(resp$diagnostics$opencode_export_fallback_used))
})

test_that("OpenCode export fallback can recover session id from raw stdout", {
  final_text <- "{\"ok\":true}"
  raw_stdout <- opencode_jsonl(list(type = "step_start", session_id = "ses_raw", part = list(type = "step-start")))
  captured <- NULL
  testthat::local_mocked_bindings(
    .run_processx = function(...) {
      captured <<- list(...)
      list(
        status = 0L,
        stdout = opencode_export_json(list(
          list(info = list(role = "assistant"), parts = list(list(type = "text", text = final_text)))
        )),
        stderr = "",
        timeout = FALSE
      )
    },
    .package = "asa"
  )

  recovered <- asa:::.opencode_apply_export_fallback(
    output = list(
      events = list(list(type = "step_start", part = list(type = "step-start"))),
      text = "",
      session_id = NULL,
      errors = character(0),
      stop_reason = NULL,
      usage = list(),
      raw_stdout = raw_stdout,
      parse_error = FALSE
    ),
    cli = list(command = "/fake/opencode", prefix_args = character(0)),
    cli_env = character(0),
    workdir = tempdir(),
    cli_status = 0L,
    process_timeout = FALSE,
    timeout_seconds = 10
  )

  expect_identical(captured$args, c("export", "ses_raw"))
  expect_identical(recovered$text, final_text)
  expect_true(isTRUE(recovered$opencode_export_fallback_used))
})

test_that("OpenCode export fallback reads redirected export stdout file", {
  final_text <- "{\"ok\":true,\"source\":\"file\"}"
  full_export_stdout <- opencode_export_json(list(
    list(info = list(role = "assistant"), parts = list(list(type = "text", text = final_text)))
  ))
  testthat::local_mocked_bindings(
    .run_processx = function(...) {
      args <- list(...)
      writeLines(full_export_stdout, args$stdout, useBytes = TRUE)
      writeLines("Exporting session: ses_file", args$stderr, useBytes = TRUE)
      list(
        status = 0L,
        stdout = substr(full_export_stdout, 1L, 20L),
        stderr = "",
        timeout = FALSE
      )
    },
    .package = "asa"
  )

  recovered <- asa:::.opencode_apply_export_fallback(
    output = opencode_empty_terminal_output("ses_file"),
    cli = list(command = "/fake/opencode", prefix_args = character(0)),
    cli_env = character(0),
    workdir = tempdir(),
    cli_status = 0L,
    process_timeout = FALSE,
    timeout_seconds = 10
  )

  expect_identical(recovered$text, final_text)
  expect_true(isTRUE(recovered$opencode_export_fallback_used))
})

test_that("OpenCode export fallback preserves empty-terminal failure when export has no assistant text", {
  export_stdout <- opencode_export_json(list(
    list(info = list(role = "user"), parts = list(list(type = "text", text = "prompt"))),
    list(info = list(role = "assistant"), parts = list(list(type = "tool", text = "not terminal text")))
  ))
  testthat::local_mocked_bindings(
    .run_processx = function(...) {
      list(status = 0L, stdout = export_stdout, stderr = "", timeout = FALSE)
    },
    .package = "asa"
  )

  recovered <- asa:::.opencode_apply_export_fallback(
    output = opencode_empty_terminal_output("ses_abc"),
    cli = list(command = "/fake/opencode", prefix_args = character(0)),
    cli_env = character(0),
    workdir = tempdir(),
    cli_status = 0L,
    process_timeout = FALSE,
    timeout_seconds = 10
  )
  resp <- asa:::.opencode_response_from_output(
    output = recovered,
    prompt = "Return JSON.",
    elapsed_minutes = 0.01,
    cli_status = 0L
  )

  expect_identical(resp$status_code, asa:::ASA_STATUS_ERROR)
  expect_true(isTRUE(resp$diagnostics$opencode_empty_terminal_output))
  expect_false(isTRUE(resp$diagnostics$opencode_export_fallback_used))
  expect_match(resp$diagnostics$opencode_export_fallback_error, "no non-empty assistant text")
})

test_that("OpenCode export fallback is skipped when stdout has terminal text", {
  called <- FALSE
  testthat::local_mocked_bindings(
    .run_processx = function(...) {
      called <<- TRUE
      stop("export fallback should not run", call. = FALSE)
    },
    .package = "asa"
  )

  output <- opencode_empty_terminal_output("ses_abc")
  output$text <- "stdout answer"
  recovered <- asa:::.opencode_apply_export_fallback(
    output = output,
    cli = list(command = "/fake/opencode", prefix_args = character(0)),
    cli_env = character(0),
    workdir = tempdir(),
    cli_status = 0L,
    process_timeout = FALSE,
    timeout_seconds = 10
  )
  resp <- asa:::.opencode_response_from_output(
    output = recovered,
    prompt = "Return text.",
    elapsed_minutes = 0.01,
    cli_status = 0L
  )

  expect_false(called)
  expect_identical(resp$status_code, asa:::ASA_STATUS_SUCCESS)
  expect_identical(resp$message, "stdout answer")
  expect_false(isTRUE(resp$diagnostics$opencode_export_fallback_used))
})

test_that("OpenCode export fallback does not mask status, timeout, parse, or error-event failures", {
  called <- FALSE
  testthat::local_mocked_bindings(
    .run_processx = function(...) {
      called <<- TRUE
      stop("export fallback should not run", call. = FALSE)
    },
    .package = "asa"
  )

  error_event <- list(
    type = "error",
    sessionID = "ses_abc",
    error = list(name = "APIError", data = list(message = "bad request", statusCode = 400L))
  )
  cases <- list(
    list(output = opencode_empty_terminal_output("ses_abc"), cli_status = 1L, process_timeout = FALSE),
    list(output = opencode_empty_terminal_output("ses_abc"), cli_status = 0L, process_timeout = TRUE),
    list(output = opencode_empty_terminal_output("ses_abc", parse_error = TRUE), cli_status = 0L, process_timeout = FALSE),
    list(
      output = list(
        events = list(error_event),
        text = "",
        session_id = "ses_abc",
        errors = "APIError: bad request (status 400)",
        stop_reason = NULL,
        usage = list(),
        raw_stdout = opencode_jsonl(error_event),
        parse_error = FALSE
      ),
      cli_status = 0L,
      process_timeout = FALSE
    )
  )

  for (case in cases) {
    recovered <- asa:::.opencode_apply_export_fallback(
      output = case$output,
      cli = list(command = "/fake/opencode", prefix_args = character(0)),
      cli_env = character(0),
      workdir = tempdir(),
      cli_status = case$cli_status,
      process_timeout = case$process_timeout,
      timeout_seconds = 10
    )
    resp <- asa:::.opencode_response_from_output(
      output = recovered,
      prompt = "Return JSON.",
      elapsed_minutes = 0.01,
      cli_status = case$cli_status,
      process_timeout = case$process_timeout,
      timeout_seconds = 10
    )
    expect_identical(resp$status_code, asa:::ASA_STATUS_ERROR)
    expect_false(isTRUE(resp$diagnostics$opencode_export_fallback_used))
  }
  expect_false(called)
})

test_that("OpenCode tool-loop diagnostics count timeout, invalid, and guard events", {
  events <- list(
    list(
      type = "tool_use",
      part = list(
        tool = "asa_search_web_search",
        state = list(
          status = "error",
          input = list(query = "same query"),
          error = "McpError: MCP error -32001: Request timed out"
        )
      )
    ),
    list(
      type = "tool_use",
      part = list(
        tool = "invalid",
        state = list(
          status = "completed",
          input = list(tool = "web_search"),
          output = "Model tried to call unavailable tool 'web_search'."
        )
      )
    ),
    list(
      type = "tool_use",
      part = list(
        tool = "asa_search_web_search",
        state = list(
          status = "completed",
          input = list(query = "same query"),
          output = "ASA_TOOL_ERROR {\"error_type\":\"same_input_timeout_exhausted\"}"
        )
      )
    )
  )

  diag <- asa:::.opencode_tool_loop_diagnostics(
    events,
    loop_guard = list(search_budget_limit = 2L, recursion_limit = 4L)
  )

  expect_identical(diag$tool_calls, 3L)
  expect_identical(diag$search_calls, 2L)
  expect_identical(diag$invalid_tool_calls, 1L)
  expect_identical(diag$timeout_errors, 2L)
  expect_identical(diag$guard_errors, 1L)
  expect_true(isTRUE(diag$search_budget_exhausted))
})

test_that("OpenCode tool-loop diagnostics flag interactive question tools", {
  events <- list(
    list(
      type = "tool_use",
      part = list(
        tool = "question",
        state = list(
          status = "running",
          input = list(
            questions = list(list(question = "Who is the target person?"))
          )
        )
      )
    )
  )

  diag <- asa:::.opencode_tool_loop_diagnostics(events)

  expect_identical(diag$tool_calls, 1L)
  expect_identical(diag$internal_tool_calls, 1L)
  expect_identical(diag$question_tool_calls, 1L)
  expect_identical(diag$pending_question_tool_calls, 1L)
  expect_identical(diag$search_calls, 0L)
})

test_that("OpenCode response maps fenced JSON into final_payload", {
  text <- "```json\n{\"status\":\"complete\",\"answer\":42}\n```"
  resp <- asa:::.opencode_response_from_output(
    output = list(
      events = list(list(type = "text", part = list(type = "text", text = text))),
      text = text,
      session_id = "oc-fenced",
      errors = character(0),
      stop_reason = "stop",
      usage = list(input_tokens = 1L, output_tokens = 2L, reasoning_tokens = 0L, total_tokens = 3L),
      raw_stdout = "",
      parse_error = FALSE
    ),
    prompt = "Return structured output.",
    elapsed_minutes = 0.1
  )

  expect_s3_class(resp, "asa_response")
  expect_true(isTRUE(resp$terminal_valid))
  expect_identical(resp$final_payload$status, "complete")
  expect_identical(resp$final_payload$answer, 42L)
  expect_false(isTRUE(resp$payload_integrity$canonical_matches_message))
})

test_that("run_task marks successful OpenCode non-JSON text as JSON output error", {
  response <- asa:::.opencode_response_from_output(
    output = list(
      events = list(list(type = "text", part = list(type = "text", text = "I cannot access web search."))),
      text = "I cannot access web search.",
      session_id = "oc-non-json",
      errors = character(0),
      stop_reason = "stop",
      usage = list(input_tokens = 1L, output_tokens = 2L, reasoning_tokens = 0L, total_tokens = 3L),
      raw_stdout = "",
      parse_error = FALSE
    ),
    prompt = "Return structured output.",
    elapsed_minutes = 0.1
  )
  agent <- asa_test_mock_agent(
    config = asa::asa_config(
      agent_backend = "opencode",
      backend = "openai",
      model = "gpt-4.1-mini",
      proxy = NULL,
      rate_limit = 1000
    )
  )

  testthat::local_mocked_bindings(
    .run_agent = function(...) response,
    .acquire_rate_limit_token = function(verbose = FALSE) invisible(TRUE),
    .adaptive_rate_record = function(status, verbose = FALSE) invisible(TRUE),
    .package = "asa"
  )

  result <- asa::run_task(
    "Return structured output.",
    agent = agent,
    output_format = "json",
    verbose = FALSE
  )

  expect_identical(result$status, "error")
  expect_true(isTRUE(result$execution$json_output_missing))
  expect_false(isTRUE(result$parsing_status$valid))
})

test_that("run_opencode_agent maps fake CLI output into asa_response and run_task JSON", {
  skip_if_not_installed("withr")

  fake_bin <- make_fake_opencode_bin()
  args_file <- tempfile("asa-opencode-args-", fileext = ".txt")
  env_file <- tempfile("asa-opencode-env-", fileext = ".txt")
  config_file <- tempfile("asa-opencode-config-", fileext = ".json")

  dummy_proc <- new.env(parent = emptyenv())
  dummy_proc$is_alive <- function() FALSE
  dummy_proc$kill <- function() invisible(NULL)
  dummy_proc$wait <- function(timeout = 0) 0L

  stdout <- opencode_jsonl(
    list(type = "step_start", sessionID = "oc-session-1", part = list(type = "step-start")),
    list(type = "text", sessionID = "oc-session-1", part = list(type = "text", text = "{\"status\":\"complete\",\"answer\":42}")),
    list(
      type = "step_finish",
      sessionID = "oc-session-1",
      part = list(type = "step-finish", reason = "stop", tokens = list(input = 11L, output = 7L))
    )
  )

  withr::local_envvar(c(
    ASA_OPENCODE_BIN = fake_bin,
    ASA_OPENCODE_AGENT = "build",
    ASA_OPENCODE_TEST_ARGS_FILE = args_file,
    ASA_OPENCODE_TEST_ENV_FILE = env_file,
    ASA_OPENCODE_TEST_CONFIG_FILE = config_file,
    ASA_OPENCODE_TEST_STDOUT = stdout
  ))

  testthat::local_mocked_bindings(
    .opencode_python_binary = function(conda_env) "/usr/bin/python3",
    .opencode_python_path = function() "/tmp/asa-python",
    .free_code_launch_gateway = function(config, python, python_path, verbose = FALSE) {
      list(
        process = dummy_proc,
        base_url = "http://127.0.0.1:8789",
        log_file = tempfile("asa-opencode-gateway-log-", fileext = ".log"),
        port_file = tempfile("asa-opencode-gateway-port-", fileext = ".txt")
      )
    },
    .acquire_rate_limit_token = function(verbose = FALSE) invisible(TRUE),
    .adaptive_rate_record = function(status, verbose = FALSE) invisible(TRUE),
    .package = "asa"
  )

  agent <- asa::asa_agent(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4.1-mini",
    config = asa::asa_config(
      agent_backend = "opencode",
      backend = "openai",
      model = "gpt-4.1-mini",
      proxy = "socks5h://127.0.0.1:9050",
      timeout = 5L,
      rate_limit = 1000
    )
  )

  resp <- asa:::.run_opencode_agent(
    prompt = "Return structured output.",
    agent = agent,
    allow_read_webpages = TRUE
  )

  expect_s3_class(resp, "asa_response")
  expect_identical(resp$status_code, asa:::ASA_STATUS_SUCCESS)
  expect_true(isTRUE(resp$terminal_valid))
  expect_identical(resp$final_payload$status, "complete")
  expect_identical(resp$final_payload$answer, 42L)
  expect_identical(resp$thread_id, "oc-session-1")
  expect_identical(resp$tokens_used, 18L)
  expect_identical(resp$input_tokens, 11L)
  expect_identical(resp$output_tokens, 7L)
  expect_identical(resp$config_snapshot$agent_backend, "opencode")
  expect_identical(resp$config_snapshot$outer_model, asa:::ASA_OPENCODE_OUTER_MODEL)

  args <- readLines(args_file, warn = FALSE)
  expect_identical(args[[1]], "run")
  expect_true("--format" %in% args)
  expect_true("json" %in% args)
  expect_true("--model" %in% args)
  expect_true(paste0("anthropic/", asa:::ASA_OPENCODE_OUTER_MODEL) %in% args)
  expect_true("--agent" %in% args)
  expect_true("build" %in% args)
  expect_true("--dir" %in% args)
  expect_true(any(grepl("NON-INTERACTIVE BATCH RUN:", args, fixed = TRUE)))
  expect_true(any(grepl("Return structured output.", args, fixed = TRUE)))

  env_lines <- readLines(env_file, warn = FALSE)
  expect_true(any(grepl("^HTTPS_PROXY=$", env_lines)))
  expect_true(any(grepl("^HTTP_PROXY=$", env_lines)))
  expect_true(any(grepl("^ANTHROPIC_API_KEY=asa-local-gateway$", env_lines)))
  expect_true(any(grepl("^OPENCODE_CONFIG_CONTENT=", env_lines)))

  opencode_config <- jsonlite::fromJSON(config_file, simplifyVector = FALSE)
  expect_identical(opencode_config$provider$anthropic$options$baseURL, "http://127.0.0.1:8789")
  expect_identical(opencode_config$provider$anthropic$options$headers$`X-ASA-Target-Backend`, "openai")
  expect_identical(opencode_config$provider$anthropic$options$headers$`X-ASA-Target-Model`, "gpt-4.1-mini")
  expect_false(opencode_config$tools$question)
  expect_identical(opencode_config$permission$question, "deny")

  result <- asa::run_task(
    "Return structured output.",
    agent = agent,
    output_format = "json",
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")
  expect_identical(result$status, "success")
  expect_identical(result$parsed$status, "complete")
  expect_identical(result$parsed$answer, 42L)
  expect_identical(result$execution$config_snapshot$agent_backend, "opencode")
  expect_identical(result$execution$config_snapshot$backend, "openai")
})

test_that("run_opencode_agent enforces configured timeout seconds and maps process timeouts", {
  dummy_proc <- new.env(parent = emptyenv())
  dummy_proc$is_alive <- function() FALSE
  dummy_proc$kill <- function() invisible(NULL)
  dummy_proc$wait <- function(timeout = 0) 0L

  captured_run <- NULL
  testthat::local_mocked_bindings(
    .opencode_require_processx = function() invisible(TRUE),
    .opencode_command_spec = function() list(command = "/usr/bin/opencode", prefix_args = character(0)),
    .opencode_python_binary = function(conda_env) "/usr/bin/python3",
    .opencode_python_path = function() "/tmp/asa-python",
    .free_code_launch_gateway = function(config, python, python_path, verbose = FALSE) {
      list(
        process = dummy_proc,
        base_url = "http://127.0.0.1:8789",
        log_file = tempfile("asa-opencode-gateway-log-", fileext = ".log"),
        port_file = tempfile("asa-opencode-gateway-port-", fileext = ".txt")
      )
    },
    .run_processx = function(...) {
      captured_run <<- list(...)
      list(status = NA_integer_, stdout = "", stderr = "", timeout = TRUE)
    },
    .package = "asa"
  )

  agent <- asa::asa_agent(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4.1-mini",
    config = asa::asa_config(
      agent_backend = "opencode",
      backend = "openai",
      model = "gpt-4.1-mini",
      proxy = NULL,
      timeout = 5L
    )
  )

  resp <- asa:::.run_opencode_agent(
    prompt = "Return structured output.",
    agent = agent
  )

  expect_identical(captured_run$timeout, 5)
  expect_true(isTRUE(captured_run$cleanup_tree))
  expect_identical(resp$status_code, asa:::ASA_STATUS_ERROR)
  expect_identical(resp$stop_reason, "process_timeout")
  expect_true(isTRUE(resp$diagnostics$opencode_process_timeout))
  expect_identical(resp$diagnostics$opencode_timeout_seconds, 5)
})

test_that("resolve_agent_from_config forwards opencode agent_backend", {
  cfg <- asa::asa_config(
    agent_backend = "opencode",
    backend = "openai",
    model = "gpt-4.1-mini"
  )

  captured <- NULL
  testthat::local_mocked_bindings(
    .is_initialized = function() FALSE,
    initialize_agent = function(...) {
      captured <<- list(...)
      asa_test_mock_agent(config = list(agent_backend = "opencode"))
    },
    .package = "asa"
  )

  agent <- asa:::.resolve_agent_from_config(config = cfg, agent = NULL, verbose = FALSE)
  expect_s3_class(agent, "asa_agent")
  expect_identical(captured$agent_backend, "opencode")
})
