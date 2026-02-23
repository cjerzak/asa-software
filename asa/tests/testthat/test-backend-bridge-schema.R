test_that(".parse_backend_contract_payload parses schema-versioned payload", {
  response <- list(
    stop_reason = "complete",
    budget_state = list(tool_calls_used = 2L),
    field_status = list(name = "resolved"),
    diagnostics = list(unknown_fields_count = 0L),
    json_repair = list(list(repair_reason = "schema_normalization")),
    completion_gate = list(status = "complete"),
    retrieval_metrics = list(hits = 3L),
    tool_quality_events = list(list(is_off_target = FALSE)),
    candidate_resolution = list(selected = "alpha"),
    finalization_status = list(schema_valid = TRUE),
    orchestration_options = list(mode = "observe"),
    artifact_status = list(trace = list(status = "written")),
    fold_stats = list(fold_count = 1L),
    plan = list(step = "one"),
    plan_history = list("one"),
    om_stats = list(enabled = TRUE),
    observations = list("obs"),
    reflections = list("refl"),
    token_trace = list(list(node = "agent")),
    policy_version = "2026-02-20",
    tokens_used = 11L,
    input_tokens = 5L,
    output_tokens = 6L
  )
  payload <- list(
    schema_version = asa:::.ASA_BACKEND_BRIDGE_SCHEMA_VERSION,
    response = response,
    diagnostics = response$diagnostics,
    phase_timings = list(invoke_graph_minutes = 0.05),
    config_snapshot = list(thread_id = "asa_test_thread"),
    budget_state = response$budget_state,
    field_status = response$field_status
  )

  parsed <- asa:::.parse_backend_contract_payload(
    payload = payload
  )

  expect_equal(parsed$schema_version, asa:::.ASA_BACKEND_BRIDGE_SCHEMA_VERSION)
  expect_equal(parsed$sections$budget_state$tool_calls_used, 2L)
  expect_equal(parsed$sections$field_status$name, "resolved")
  expect_equal(parsed$sections$diagnostics$unknown_fields_count, 0L)
  expect_equal(parsed$stop_reason, "complete")
  expect_equal(parsed$policy_version, "2026-02-20")
  expect_equal(parsed$tokens_used, 11L)
  expect_equal(parsed$config_snapshot$thread_id, "asa_test_thread")
  expect_equal(parsed$phase_timings$invoke_graph_minutes, 0.05)
})

test_that(".parse_backend_contract_payload errors on unsupported schema_version", {
  payload <- list(
    schema_version = "asa_bridge_contract_v99",
    response = list(stop_reason = "complete")
  )

  expect_error(
    asa:::.parse_backend_contract_payload(payload = payload),
    "Unsupported backend bridge schema_version"
  )
})

test_that(".parse_backend_contract_payload errors when schema_version is missing", {
  payload <- list(
    response = list(stop_reason = "legacy_stop")
  )

  expect_error(
    asa:::.parse_backend_contract_payload(payload = payload),
    "missing required field `schema_version`"
  )
})
