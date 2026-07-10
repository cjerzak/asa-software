# Tests for the consolidated unsupported-parameter warnings on CLI backends
# (opencode/free-code), including the temporal time_filter gate.

.make_cli_agent <- function(agent_backend) {
  asa::asa_agent(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4.1-mini",
    config = asa::asa_config(
      agent_backend = agent_backend,
      backend = "openai",
      model = "gpt-4.1-mini",
      proxy = NULL
    )
  )
}

.reset_unsupported_registry <- function() {
  assign("unsupported_params_warned", character(0), envir = asa:::asa_env)
}

test_that("opencode backend warns once about unsupported run params", {
  .reset_unsupported_registry()
  on.exit(.reset_unsupported_registry(), add = TRUE)

  called <- 0L
  testthat::local_mocked_bindings(
    .run_opencode_agent = function(...) {
      called <<- called + 1L
      "sentinel"
    },
    .package = "asa"
  )

  agent <- .make_cli_agent("opencode")

  warned <- NULL
  result <- withCallingHandlers(
    asa:::.run_agent(
      "p",
      agent = agent,
      expected_schema = list(status = "string"),
      use_plan_mode = TRUE
    ),
    asa_unsupported_params = function(w) {
      warned <<- w
      invokeRestart("muffleWarning")
    }
  )
  expect_identical(result, "sentinel")
  expect_s3_class(warned, "asa_unsupported_params")
  expect_true(all(c("expected_schema", "use_plan_mode") %in% warned$params))
  expect_match(conditionMessage(warned), "opencode")
  expect_match(conditionMessage(warned), "expected_schema")
  expect_match(conditionMessage(warned), "use_plan_mode")

  # Same signature again: deduped, run still executes.
  expect_no_warning(
    result <- asa:::.run_agent(
      "p",
      agent = agent,
      expected_schema = list(status = "string"),
      use_plan_mode = TRUE
    ),
    class = "asa_unsupported_params"
  )
  expect_identical(result, "sentinel")
  expect_identical(called, 2L)
})

test_that("free-code backend supports expected_schema without warning", {
  .reset_unsupported_registry()
  on.exit(.reset_unsupported_registry(), add = TRUE)

  testthat::local_mocked_bindings(
    .run_free_code_agent = function(...) "sentinel",
    .package = "asa"
  )

  agent <- .make_cli_agent("free-code")

  expect_no_warning(
    asa:::.run_agent("p", agent = agent, expected_schema = list(status = "string")),
    class = "asa_unsupported_params"
  )

  # But plan mode is still unsupported on free-code.
  expect_warning(
    asa:::.run_agent("p", agent = agent, use_plan_mode = TRUE),
    class = "asa_unsupported_params"
  )
})

test_that("defaults-only dispatch stays silent on CLI backends", {
  .reset_unsupported_registry()
  on.exit(.reset_unsupported_registry(), add = TRUE)

  testthat::local_mocked_bindings(
    .run_opencode_agent = function(...) "sentinel",
    .package = "asa"
  )

  agent <- .make_cli_agent("opencode")
  expect_no_warning(asa:::.run_agent("p", agent = agent))
})

test_that("temporal time_filter warns and is skipped on CLI backends", {
  .reset_unsupported_registry()
  on.exit(.reset_unsupported_registry(), add = TRUE)

  agent <- .make_cli_agent("opencode")

  warned <- NULL
  result <- withCallingHandlers(
    asa:::.with_temporal(
      list(time_filter = "m"),
      function() "ok",
      agent = agent
    ),
    asa_unsupported_params = function(w) {
      warned <<- w
      invokeRestart("muffleWarning")
    }
  )
  expect_identical(result, "ok")
  expect_s3_class(warned, "asa_unsupported_params")
  expect_match(conditionMessage(warned), "time_filter is not supported")
  expect_no_match(conditionMessage(warned), "Could not set DuckDuckGo")

  # after/before-only temporal hints do not warn (they work via the prompt).
  .reset_unsupported_registry()
  expect_no_warning(
    asa:::.with_temporal(list(after = "2020-01-01"), function() "ok", agent = agent),
    class = "asa_unsupported_params"
  )
})

test_that("inert memory/OM options warn at initialization for CLI backends", {
  .reset_unsupported_registry()
  on.exit(.reset_unsupported_registry(), add = TRUE)

  warned <- NULL
  withCallingHandlers(
    asa:::.warn_inert_memory_options(
      agent_backend = "opencode",
      memory_threshold = asa:::ASA_DEFAULT_MEMORY_THRESHOLD,
      memory_keep_recent = asa:::ASA_DEFAULT_MEMORY_KEEP_RECENT,
      fold_char_budget = asa:::ASA_DEFAULT_FOLD_CHAR_BUDGET,
      use_observational_memory = asa:::ASA_DEFAULT_USE_OBSERVATIONAL_MEMORY,
      om_observation_token_budget = asa:::ASA_DEFAULT_OM_OBSERVATION_TOKENS,
      om_reflection_token_budget = asa:::ASA_DEFAULT_OM_REFLECTION_TOKENS,
      om_buffer_tokens = 12345L,
      om_buffer_activation = asa:::ASA_DEFAULT_OM_BUFFER_ACTIVATION,
      om_block_after = asa:::ASA_DEFAULT_OM_BLOCK_AFTER,
      om_async_prebuffer = asa:::ASA_DEFAULT_OM_ASYNC_PREBUFFER,
      om_cross_thread_memory = asa:::ASA_DEFAULT_OM_CROSS_THREAD_MEMORY
    ),
    asa_unsupported_params = function(w) {
      warned <<- w
      invokeRestart("muffleWarning")
    }
  )
  expect_s3_class(warned, "asa_unsupported_params")
  expect_identical(warned$params, "om_buffer_tokens")

  # All-defaults call stays silent.
  .reset_unsupported_registry()
  expect_no_warning(
    asa:::.warn_inert_memory_options(
      agent_backend = "opencode",
      memory_threshold = asa:::ASA_DEFAULT_MEMORY_THRESHOLD,
      memory_keep_recent = asa:::ASA_DEFAULT_MEMORY_KEEP_RECENT,
      fold_char_budget = asa:::ASA_DEFAULT_FOLD_CHAR_BUDGET,
      use_observational_memory = asa:::ASA_DEFAULT_USE_OBSERVATIONAL_MEMORY,
      om_observation_token_budget = asa:::ASA_DEFAULT_OM_OBSERVATION_TOKENS,
      om_reflection_token_budget = asa:::ASA_DEFAULT_OM_REFLECTION_TOKENS,
      om_buffer_tokens = asa:::ASA_DEFAULT_OM_BUFFER_TOKENS,
      om_buffer_activation = asa:::ASA_DEFAULT_OM_BUFFER_ACTIVATION,
      om_block_after = asa:::ASA_DEFAULT_OM_BLOCK_AFTER,
      om_async_prebuffer = asa:::ASA_DEFAULT_OM_ASYNC_PREBUFFER,
      om_cross_thread_memory = asa:::ASA_DEFAULT_OM_CROSS_THREAD_MEMORY
    ),
    class = "asa_unsupported_params"
  )
})
