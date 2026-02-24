test_that("build_node_trace_entry emits metadata fields", {
  utils <- asa_test_import_module(
    "shared.state_graph_utils",
    required_file = "shared/state_graph_utils.py",
    required_modules = c("pydantic"),
    initialize = TRUE
  )

  entry <- tryCatch(
    reticulate::py_to_r(utils$build_node_trace_entry(
      "tools",
      usage = reticulate::dict(input_tokens = 3L, output_tokens = 1L, total_tokens = 4L),
      elapsed_minutes = 0.25,
      status = "error",
      error_type = "timeout",
      tool_name = "search"
    )),
    error = function(e) NULL
  )

  expect_true(is.list(entry))
  expect_equal(as.character(entry$node), "tools")
  expect_equal(as.character(entry$status), "error")
  expect_equal(as.character(entry$error_type), "timeout")
  expect_equal(as.character(entry$tool_name), "search")
  expect_true(is.character(entry$started_at_utc))
  expect_true(is.character(entry$ended_at_utc))
  expect_true(grepl("Z$", as.character(entry$started_at_utc)))
  expect_true(grepl("Z$", as.character(entry$ended_at_utc)))
})

test_that("field-status diagnostics surface anchor mismatch samples", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  collect_diag <- reticulate::py_get_attr(prod, "_collect_field_status_diagnostics")
  normalize_diag <- reticulate::py_get_attr(prod, "_normalize_diagnostics")

  field_status <- list(
    education_level = list(
      status = "unknown",
      value = "Unknown",
      source_url = NULL,
      evidence = "recovery_blocked_anchor_mismatch",
      evidence_reason = "recovery_blocked_anchor_mismatch",
      evidence_source_url = "https://example.com/profile",
      anchor_mode = "strict",
      anchor_strength = "strong",
      candidate_score = 0.41,
      descriptor = "string|Unknown"
    )
  )

  collected <- reticulate::py_to_r(collect_diag(field_status = field_status))
  expect_true(is.list(collected$anchor_mismatch_samples))
  expect_true(length(collected$anchor_mismatch_samples) >= 1)
  sample <- collected$anchor_mismatch_samples[[1]]
  expect_equal(as.character(sample$field), "education_level")
  expect_equal(as.character(sample$anchor_mode), "strict")
  expect_equal(as.character(sample$anchor_strength), "strong")

  normalized <- reticulate::py_to_r(normalize_diag(list(
    finalization_invariant_failures_by_reason = list(diagnostics_unknown_fields_mismatch = 2L),
    finalization_blocking_fields = list("education_level"),
    diagnostics_recovery_checkpoint_events = 1L,
    diagnostics_recovery_retry_events = 1L,
    finalization_recovery_checkpoints = list(list(
      context = "agent",
      reason = "diagnostics_unknown_count_mismatch",
      reasons = list("diagnostics_unknown_count_mismatch"),
      priority = "anchor_mismatch_recovery",
      action = "replan_and_retry",
      blocking_fields_sample = list("education_level"),
      ts_utc = "2026-02-24T03:43:11Z"
    )),
    empty_tool_results_details = list(list(
      tool_name = "search",
      query_digest = "abcd1234",
      error_type = "timeout",
      round_index = 2L
    )),
    anchor_mismatch_samples = collected$anchor_mismatch_samples
  )))

  expect_equal(
    as.integer(normalized$finalization_invariant_failures_by_reason$diagnostics_unknown_fields_mismatch),
    2L
  )
  expect_true("education_level" %in% as.character(normalized$finalization_blocking_fields))
  expect_equal(as.integer(normalized$diagnostics_recovery_checkpoint_events), 1L)
  expect_equal(as.integer(normalized$diagnostics_recovery_retry_events), 1L)
  expect_true(is.list(normalized$finalization_recovery_checkpoints))
  expect_equal(as.character(normalized$finalization_recovery_checkpoints[[1]]$priority), "anchor_mismatch_recovery")
  expect_true(is.list(normalized$empty_tool_results_details))
  expect_equal(as.character(normalized$empty_tool_results_details[[1]]$tool_name), "search")
  expect_true(length(normalized$anchor_mismatch_samples) >= 1)
})

test_that("schema outcome gate emits auto-recovery checkpoint hints", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  gate_report <- reticulate::py_get_attr(prod, "_schema_outcome_gate_report")
  state <- list(
    expected_schema = list(education_level = "string|Unknown"),
    field_status = list(
      education_level = list(
        status = "unknown",
        value = "Unknown",
        evidence = "recovery_blocked_anchor_mismatch",
        attempts = 1L
      )
    ),
    budget_state = list(
      tool_calls_used = 0L,
      tool_calls_limit = 6L,
      budget_exhausted = FALSE
    ),
    diagnostics = list(
      unknown_fields_count_current = 0L,
      unknown_fields_current = list(),
      recovery_reason_counts = list(recovery_blocked_anchor_mismatch = 1L)
    ),
    finalization_policy = list(
      diagnostics_auto_recovery_enabled = TRUE
    )
  )

  report <- reticulate::py_to_r(gate_report(
    state = state,
    expected_schema = state$expected_schema,
    field_status = state$field_status,
    budget_state = state$budget_state,
    diagnostics = state$diagnostics
  ))

  expect_true(isTRUE(report$finalization_invariant_failed))
  expect_true(isTRUE(report$auto_recovery_checkpoint_needed))
  expect_equal(as.character(report$auto_recovery_action), "replan_and_retry")
  expect_equal(as.character(report$auto_recovery_priority), "anchor_mismatch_recovery")
})

test_that("justification guard rejects mismatched included fields", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  check_justification <- reticulate::py_get_attr(prod, "_justification_counts_match_field_status")
  field_status <- list(
    birth_year = list(status = "found", value = 1980L),
    education_level = list(status = "unknown", value = "Unknown")
  )
  justification <- paste(
    "Source-backed searches resolved 1 core fields",
    "(including education_level), while 1 core fields remain unknown."
  )

  ok <- reticulate::py_to_r(check_justification(
    text = justification,
    normalized_field_status = field_status
  ))

  expect_false(isTRUE(ok))
})
