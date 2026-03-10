mock_direct_agent <- function(invoke_fn, backend = "openai", model = "gpt-4.1-mini") {
  llm <- new.env(parent = emptyenv())
  llm$invoke <- invoke_fn

  asa_agent(
    python_agent = NULL,
    backend = backend,
    model = model,
    config = list(
      timeout = 30,
      invoke_max_attempts = 1,
      invoke_retry_delay = 0,
      invoke_retry_backoff = 1,
      invoke_retry_jitter = 0
    ),
    llm = llm,
    tools = list()
  )
}

mock_direct_runtime <- function() {
  testthat::local_mocked_bindings(
    .resolve_runtime_inputs = function(...) {
      list(runtime = list(), temporal = NULL, allow_rw = FALSE)
    },
    .with_runtime_wrappers = function(runtime, conda_env = NULL, agent = NULL, fn) {
      fn()
    },
    .acquire_rate_limit_token = function(verbose = FALSE) {
      0
    },
    .adaptive_rate_record = function(status, verbose = FALSE) {
      invisible(status)
    },
    .package = "asa"
  )
}

test_that("run_direct_task returns a provider_direct text result", {
  mock_direct_runtime()
  agent <- mock_direct_agent(function(prompt) {
    list(role = "assistant", content = "4")
  })

  result <- asa::run_direct_task(
    prompt = "What is 2+2? Reply with only the number.",
    output_format = "text",
    agent = agent
  )

  expect_s3_class(result, "asa_result")
  expect_identical(result$status, "success")
  expect_identical(result$message, "4")
  expect_null(result$parsed)
  expect_identical(result$trace_json, "")
  expect_identical(result$search_tier, "none")
  expect_identical(result$execution$mode, "provider_direct")
  expect_identical(result$execution$stop_reason, "provider_direct")
  expect_identical(result$execution$status_code, asa:::ASA_STATUS_SUCCESS)
  expect_true(is.character(result$raw_output))
  expect_match(result$raw_output, "4")
  expect_true(all(
    c(
      "total_minutes",
      "rate_limit_wait_minutes",
      "backend_invoke_minutes",
      "output_parse_minutes",
      "execution_assembly_minutes",
      "backend"
    ) %in% names(result$execution$phase_timings)
  ))
})

test_that("run_direct_task is exported and callable via asa namespace", {
  mock_direct_runtime()
  agent <- mock_direct_agent(function(prompt) {
    list(role = "assistant", content = "namespace ok")
  })

  expect_true("run_direct_task" %in% getNamespaceExports("asa"))
  expect_true(is.function(asa::run_direct_task))

  result <- asa::run_direct_task(
    prompt = "Confirm namespace export.",
    output_format = "text",
    agent = agent
  )

  expect_s3_class(result, "asa_result")
  expect_identical(result$message, "namespace ok")
})

test_that("run_direct_task parses JSON, extracts fields, and attaches raw responses", {
  mock_direct_runtime()
  json_agent <- mock_direct_agent(function(prompt) {
    list(role = "assistant", content = '{"answer":4,"source":"provider"}')
  })

  json_result <- asa::run_direct_task(
    prompt = "Return JSON.",
    output_format = "json",
    agent = json_agent
  )

  expect_identical(json_result$status, "success")
  expect_true(is.list(json_result$parsed))
  expect_identical(json_result$parsed$answer, 4L)
  expect_identical(json_result$parsed$source, "provider")
  expect_identical(json_result$execution$mode, "provider_direct")
  expect_identical(json_result$search_tier, "none")

  fields_result <- asa::run_direct_task(
    prompt = "Return specific fields.",
    output_format = c("answer", "missing"),
    agent = json_agent
  )

  expect_identical(fields_result$status, "success")
  expect_true(is.list(fields_result$parsed))
  expect_identical(fields_result$parsed$answer, 4L)
  expect_true(is.na(fields_result$parsed$missing))

  raw_agent <- mock_direct_agent(function(prompt) {
    list(role = "assistant", content = "raw direct output")
  })

  raw_result <- asa::run_direct_task(
    prompt = "Return raw output.",
    output_format = "raw",
    agent = raw_agent
  )

  expect_identical(raw_result$status, "success")
  expect_identical(raw_result$message, "raw direct output")
  expect_true(is.character(raw_result$raw_output))
  expect_match(raw_result$raw_output, "raw direct output")
  expect_false(is.null(raw_result$raw_response))
})

test_that("run_direct_task errors clearly when no agent is available", {
  mock_direct_runtime()
  testthat::local_mocked_bindings(
    .resolve_agent_from_config = function(config = NULL, agent = NULL, verbose = FALSE) {
      NULL
    },
    get_agent = function() {
      NULL
    },
    .package = "asa"
  )

  expect_error(
    asa::run_direct_task(prompt = "Need an agent."),
    "Agent not initialized\\. Call initialize_agent\\(\\) first or pass an asa_config/asa_agent\\."
  )
})

test_that("run_direct_task errors clearly when the resolved agent has no llm", {
  mock_direct_runtime()
  llm_missing_agent <- mock_direct_agent(function(prompt) {
    list(role = "assistant", content = "unused")
  })
  llm_missing_agent$llm <- NULL

  expect_error(
    asa::run_direct_task(prompt = "Need an llm.", agent = llm_missing_agent),
    "Direct provider mode requires an initialized LLM on the agent\\."
  )
})

test_that("run_direct_task returns structured error results for provider failures", {
  mock_direct_runtime()
  agent <- mock_direct_agent(function(prompt) {
    stop("Simulated provider failure", call. = FALSE)
  })

  result <- asa::run_direct_task(
    prompt = "This should fail.",
    output_format = "text",
    agent = agent
  )

  expect_s3_class(result, "asa_result")
  expect_identical(result$status, "error")
  expect_match(result$message, "Simulated provider failure")
  expect_identical(result$execution$mode, "provider_direct")
  expect_identical(result$execution$stop_reason, "invoke_error")
  expect_identical(result$execution$status_code, asa:::ASA_STATUS_ERROR)
  expect_identical(result$execution$invoke_error$error_type, "provider_invoke_error")
  expect_identical(result$execution$invoke_error$error_message, "Simulated provider failure")
  expect_identical(result$execution$invoke_error$retry_attempts, 1L)
  expect_identical(result$execution$invoke_error$retry_max_attempts, 1L)
  expect_identical(result$execution$terminal_payload_source, "invoke_error")
  expect_identical(result$execution$payload_integrity$released_from, "invoke_error")
  expect_false(isTRUE(result$execution$payload_integrity$message_sanitized))
  expect_true(all(
    c(
      "total_minutes",
      "rate_limit_wait_minutes",
      "backend_invoke_minutes",
      "output_parse_minutes",
      "execution_assembly_minutes",
      "backend"
    ) %in% names(result$execution$phase_timings)
  ))
})
