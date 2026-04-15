make_fake_free_code_bin <- function() {
  path <- tempfile("asa-free-code-bin-", fileext = ".sh")
  writeLines(
    c(
      "#!/bin/sh",
      "if [ -n \"$ASA_FREE_CODE_TEST_ARGS_FILE\" ]; then",
      "  printf '%s\\n' \"$@\" > \"$ASA_FREE_CODE_TEST_ARGS_FILE\"",
      "fi",
      "if [ -n \"$ASA_FREE_CODE_TEST_ENV_FILE\" ]; then",
      "  env | sort > \"$ASA_FREE_CODE_TEST_ENV_FILE\"",
      "fi",
      "printf '%s' \"$ASA_FREE_CODE_TEST_STDOUT\""
    ),
    con = path,
    useBytes = TRUE
  )
  Sys.chmod(path, mode = "755")
  path
}

make_fake_exec <- function(name, body = c("#!/bin/sh", "exit 0")) {
  dir <- tempfile("asa-free-code-path-")
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  path <- file.path(dir, name)
  writeLines(body, con = path, useBytes = TRUE)
  Sys.chmod(path, mode = "755")
  list(dir = dir, path = path)
}

test_that("free-code schema conversion handles nested objects and arrays", {
  schema <- list(
    status = "complete|partial",
    count = "integer|null",
    items = list(list(name = "string", birth_year = "integer|null|Unknown"))
  )

  json_schema <- asa:::.free_code_expected_schema_to_json_schema(schema)

  expect_identical(json_schema$type, "object")
  expect_true(all(c("status", "count", "items") %in% names(json_schema$properties)))
  expect_identical(json_schema$properties$items$type, "array")
  expect_identical(json_schema$properties$items$items$type, "object")
  expect_identical(json_schema$properties$items$items$properties$name$type, "string")
  expect_true(
    any(vapply(
      json_schema$properties$items$items$properties$birth_year$anyOf,
      function(branch) identical(branch$const %||% NULL, "Unknown"),
      logical(1)
    ))
  )
})

test_that("free-code MCP config wires the stdio search server", {
  skip_if_not_installed("withr")

  cfg <- asa::asa_config(
    agent_backend = "free-code",
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

  withr::local_envvar(c(
    ASA_FREE_CODE_MCP_LOG_FILE = "/tmp/asa-free-code-mcp.log"
  ))

  path <- asa:::.free_code_write_mcp_config(
    config = cfg,
    python = "/usr/bin/python3",
    python_path = "/tmp/asa-python",
    allow_read_webpages = TRUE
  )
  on.exit(unlink(path, force = TRUE), add = TRUE)

  parsed <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  server <- parsed$mcpServers$asa_search

  expect_identical(server$command, "/usr/bin/python3")
  expect_identical(unlist(server$args, use.names = FALSE), c("-m", "asa_backend.free_code.mcp_search_server"))
  expect_identical(server$env$PYTHONPATH, "/tmp/asa-python")
  expect_identical(server$env$ASA_FREE_CODE_PROXY, "socks5h://127.0.0.1:9050")
  expect_identical(server$env$HTTPS_PROXY, "socks5h://127.0.0.1:9050")
  expect_identical(server$env$HTTP_PROXY, "socks5h://127.0.0.1:9050")
  expect_identical(server$env$ASA_FREE_CODE_USE_BROWSER, "false")
  expect_identical(server$env$ASA_FREE_CODE_ALLOW_READ_WEBPAGES, "true")
  expect_identical(server$env$ASA_FREE_CODE_MCP_LOG_FILE, "/tmp/asa-free-code-mcp.log")

  search_opts <- jsonlite::fromJSON(server$env$ASA_FREE_CODE_SEARCH_OPTIONS_JSON, simplifyVector = FALSE)
  expect_identical(search_opts$max_results, 7L)
  expect_identical(search_opts$search_doc_content_chars_max, 321L)
})

test_that("free-code CLI env routes API locally without inheriting proxy env", {
  cfg <- asa::asa_config(
    agent_backend = "free-code",
    backend = "groq",
    model = "llama-3.3-70b-versatile",
    proxy = "socks5h://127.0.0.1:9050"
  )

  env <- asa:::.free_code_cli_env(
    config = cfg,
    gateway_base_url = "http://127.0.0.1:8765",
    target_backend = "groq",
    target_model = "llama-3.3-70b-versatile"
  )

  expect_identical(env[["ANTHROPIC_BASE_URL"]], "http://127.0.0.1:8765")
  header_lines <- strsplit(env[["ANTHROPIC_CUSTOM_HEADERS"]], "\n", fixed = TRUE)[[1]]
  expect_true(any(grepl("^X-ASA-Target-Backend: groq$", header_lines)))
  expect_true(any(grepl("^X-ASA-Target-Model: llama-3.3-70b-versatile$", header_lines)))
  expect_identical(env[["HTTPS_PROXY"]], "")
  expect_identical(env[["HTTP_PROXY"]], "")
  expect_identical(env[["ALL_PROXY"]], "")
  expect_true(grepl("127.0.0.1", env[["NO_PROXY"]], fixed = TRUE))
  expect_true(grepl("localhost", env[["NO_PROXY"]], fixed = TRUE))
})

test_that("free-code command discovery prefers explicit repo over PATH binary", {
  skip_if_not_installed("withr")

  fake_free_code <- make_fake_exec("free-code")
  fake_bun <- make_fake_exec("bun")
  repo_dir <- tempfile("asa-free-code-repo-")
  dir.create(file.path(repo_dir, "src", "entrypoints"), recursive = TRUE, showWarnings = FALSE)
  writeLines("// fake entrypoint", file.path(repo_dir, "src", "entrypoints", "cli.tsx"), useBytes = TRUE)

  withr::local_envvar(c(
    PATH = paste(c(fake_bun$dir, fake_free_code$dir, Sys.getenv("PATH")), collapse = .Platform$path.sep),
    ASA_FREE_CODE_REPO = repo_dir,
    ASA_FREE_CODE_BIN = NA_character_
  ))

  spec <- asa:::.free_code_command_spec()
  expect_identical(
    normalizePath(spec$command, winslash = "/", mustWork = TRUE),
    normalizePath(fake_bun$path, winslash = "/", mustWork = TRUE)
  )
  expect_identical(
    spec$prefix_args,
    c("run", normalizePath(file.path(repo_dir, "src", "entrypoints", "cli.tsx"), winslash = "/", mustWork = TRUE))
  )
})

test_that("run_free_code_agent maps structured output back into asa_response", {
  skip_if_not_installed("withr")

  fake_bin <- make_fake_free_code_bin()
  args_file <- tempfile("asa-free-code-args-", fileext = ".txt")
  env_file <- tempfile("asa-free-code-env-", fileext = ".txt")
  mcp_file <- tempfile("asa-free-code-mcp-", fileext = ".json")
  writeLines("{\"mcpServers\":{}}", mcp_file, useBytes = TRUE)

  dummy_proc <- new.env(parent = emptyenv())
  dummy_proc$is_alive <- function() FALSE
  dummy_proc$kill <- function() invisible(NULL)
  dummy_proc$wait <- function(timeout = 0) 0L

  withr::local_envvar(c(
    ASA_FREE_CODE_BIN = fake_bin,
    ASA_FREE_CODE_TEST_ARGS_FILE = args_file,
    ASA_FREE_CODE_TEST_ENV_FILE = env_file,
    ASA_FREE_CODE_TEST_STDOUT = jsonlite::toJSON(
      list(
        subtype = "success",
        is_error = FALSE,
        result = "",
        stop_reason = "end_turn",
        session_id = "fc-session-1",
        usage = list(input_tokens = 11L, output_tokens = 7L, total_tokens = 18L),
        structured_output = list(status = "complete", answer = 42L)
      ),
      auto_unbox = TRUE,
      null = "null"
    )
  ))

  testthat::local_mocked_bindings(
    .free_code_python_binary = function(conda_env) "/usr/bin/python3",
    .free_code_python_path = function() "/tmp/asa-python",
    .free_code_launch_gateway = function(config, python, python_path, verbose = FALSE) {
      list(
        process = dummy_proc,
        base_url = "http://127.0.0.1:8789",
        log_file = tempfile("asa-free-code-gateway-log-", fileext = ".log"),
        port_file = tempfile("asa-free-code-gateway-port-", fileext = ".txt")
      )
    },
    .free_code_write_mcp_config = function(config, python, python_path, allow_read_webpages = NULL,
                                           auto_openwebpage_policy = NULL) {
      mcp_file
    },
    .package = "asa"
  )

  agent <- asa::asa_agent(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4.1-mini",
    config = asa::asa_config(
      agent_backend = "free-code",
      backend = "openai",
      model = "gpt-4.1-mini",
      proxy = "socks5h://127.0.0.1:9050"
    )
  )

  resp <- asa:::.run_free_code_agent(
    prompt = "Return structured output.",
    agent = agent,
    recursion_limit = 9L,
    expected_schema = list(status = "string", answer = "integer"),
    allow_read_webpages = TRUE
  )

  expect_s3_class(resp, "asa_response")
  expect_identical(resp$status_code, asa:::ASA_STATUS_SUCCESS)
  expect_true(isTRUE(resp$terminal_valid))
  expect_identical(resp$final_payload$status, "complete")
  expect_identical(resp$final_payload$answer, 42L)
  expect_identical(resp$thread_id, "fc-session-1")
  expect_identical(resp$tokens_used, 18L)
  expect_identical(resp$input_tokens, 11L)
  expect_identical(resp$output_tokens, 7L)
  expect_identical(resp$config_snapshot$agent_backend, "free-code")
  expect_identical(resp$config_snapshot$outer_model, asa:::ASA_FREE_CODE_OUTER_MODEL)

  args <- readLines(args_file, warn = FALSE)
  expect_true("--tools" %in% args)
  expect_true("--disallowedTools" %in% args)
  expect_true("--json-schema" %in% args)
  expect_true("--max-turns" %in% args)
  expect_true("Return structured output." %in% args)

  env_lines <- readLines(env_file, warn = FALSE)
  env_text <- paste(env_lines, collapse = "\n")
  expect_true(any(grepl("^ANTHROPIC_BASE_URL=http://127.0.0.1:8789$", env_lines)))
  expect_true(any(grepl("^HTTPS_PROXY=$", env_lines)))
  expect_true(any(grepl("^HTTP_PROXY=$", env_lines)))
  expect_true(grepl("ANTHROPIC_CUSTOM_HEADERS=X-ASA-Target-Backend: openai", env_text, fixed = TRUE))
  expect_true(grepl("X-ASA-Target-Model: gpt-4.1-mini", env_text, fixed = TRUE))
})

test_that("run_free_code_agent succeeds against repo entrypoint with live OpenAI backend", {
  skip_if_not_installed("withr")
  asa_test_skip_api_tests()
  asa_test_require_openai_key()
  skip_if_not(nzchar(Sys.which("bun")), "bun not installed")
  skip_if_not(
    dir.exists("/Users/cjerzak/Documents/free-code"),
    "free-code repo not available"
  )

  withr::local_envvar(c(
    ASA_FREE_CODE_REPO = "/Users/cjerzak/Documents/free-code",
    ASA_FREE_CODE_BIN = NA_character_
  ))
  on.exit(asa::reset_agent(), add = TRUE)

  agent <- asa::initialize_agent(
    agent_backend = "free-code",
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  resp <- asa::run_task(
    "Reply with exactly OK and nothing else.",
    agent = agent,
    output_format = "text",
    verbose = FALSE
  )

  expect_s3_class(resp, "asa_result")
  expect_identical(resp$status_code, asa:::ASA_STATUS_SUCCESS)
  expect_identical(trimws(resp$message), "OK")
  expect_identical(resp$config_snapshot$agent_backend, "free-code")
  expect_identical(resp$config_snapshot$backend, "openai")
})

test_that("resolve_agent_from_config forwards agent_backend", {
  cfg <- asa::asa_config(
    agent_backend = "free-code",
    backend = "openai",
    model = "gpt-4.1-mini"
  )

  captured <- NULL
  testthat::local_mocked_bindings(
    .is_initialized = function() FALSE,
    initialize_agent = function(...) {
      captured <<- list(...)
      asa_test_mock_agent(config = list(agent_backend = "free-code"))
    },
    .package = "asa"
  )

  agent <- asa:::.resolve_agent_from_config(config = cfg, agent = NULL, verbose = FALSE)
  expect_s3_class(agent, "asa_agent")
  expect_identical(captured$agent_backend, "free-code")
})
