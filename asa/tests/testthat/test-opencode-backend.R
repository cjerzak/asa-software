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
      search_doc_content_chars_max = 321L,
      allow_read_webpages = TRUE
    )
  )

  content <- asa:::.opencode_config_content(
    config = cfg,
    gateway_base_url = "http://127.0.0.1:8765",
    python = "/usr/bin/python3",
    python_path = "/tmp/asa-python",
    target_backend = "groq",
    target_model = "llama-3.3-70b-versatile",
    outer_model = "claude-test",
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
  expect_identical(parsed$permission$webfetch, "deny")
  expect_identical(parsed$permission$edit, "deny")

  server <- parsed$mcp$asa_search
  expect_identical(server$type, "local")
  expect_true(isTRUE(server$enabled))
  expect_identical(unlist(server$command, use.names = FALSE), c("/usr/bin/python3", "-m", "asa_backend.free_code.mcp_search_server"))
  expect_identical(server$environment$PYTHONPATH, "/tmp/asa-python")
  expect_identical(server$environment$ASA_FREE_CODE_PROXY, "socks5h://127.0.0.1:9050")
  expect_identical(server$environment$ASA_FREE_CODE_USE_BROWSER, "false")
  expect_identical(server$environment$ASA_FREE_CODE_ALLOW_READ_WEBPAGES, "true")

  search_opts <- jsonlite::fromJSON(server$environment$ASA_FREE_CODE_SEARCH_OPTIONS_JSON, simplifyVector = FALSE)
  expect_identical(search_opts$max_results, 7L)
  expect_identical(search_opts$search_doc_content_chars_max, 321L)
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
  expect_true("Return structured output." %in% args)

  env_lines <- readLines(env_file, warn = FALSE)
  expect_true(any(grepl("^HTTPS_PROXY=$", env_lines)))
  expect_true(any(grepl("^HTTP_PROXY=$", env_lines)))
  expect_true(any(grepl("^ANTHROPIC_API_KEY=asa-local-gateway$", env_lines)))
  expect_true(any(grepl("^OPENCODE_CONFIG_CONTENT=", env_lines)))

  opencode_config <- jsonlite::fromJSON(config_file, simplifyVector = FALSE)
  expect_identical(opencode_config$provider$anthropic$options$baseURL, "http://127.0.0.1:8789")
  expect_identical(opencode_config$provider$anthropic$options$headers$`X-ASA-Target-Backend`, "openai")
  expect_identical(opencode_config$provider$anthropic$options$headers$`X-ASA-Target-Model`, "gpt-4.1-mini")

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
