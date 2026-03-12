# test-source-policy-and-retrieval-controls.R

test_that("source policy blocks person-aggregator domains", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  schema <- list(
    birth_year = "integer|null|Unknown",
    birth_year_source = "string|null"
  )
  field_status <- list(
    birth_year = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    birth_year_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )
  payload <- list(
    birth_year = 1982L,
    birth_year_source = "https://www.idcrawl.com/ramona-moye"
  )
  extra_payloads <- list(list(
    tool_name = "search_snippet_extract",
    text = "Birth year: 1982. Candidate profile summary.",
    payload = payload,
    source_blocks = list(),
    source_payloads = list(payload),
    has_structured_payload = TRUE,
    urls = list("https://www.idcrawl.com/ramona-moye")
  ))

  updates <- core$`_extract_field_status_updates`(
    existing_field_status = field_status,
    expected_schema = schema,
    tool_messages = list(),
    extra_payloads = extra_payloads,
    tool_calls_delta = 1L,
    unknown_after_searches = 3L,
    evidence_enabled = TRUE
  )
  updates_r <- reticulate::py_to_r(updates)
  updated_fs <- updates_r[[1]]

  expect_false(identical(as.character(updated_fs$birth_year$status), "found"))
  expect_match(
    as.character(updated_fs$birth_year$evidence),
    "source_policy_host_denylist|source_tier_tertiary_disallowed|source_tier_below_policy_threshold",
    perl = TRUE
  )
})

test_that("default source policy promotes secondary-tier synthetic fixture domains", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  schema <- list(
    prior_occupation = "string|Unknown",
    prior_occupation_source = "string|null"
  )
  field_status <- list(
    prior_occupation = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    prior_occupation_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )
  payload <- list(
    prior_occupation = "teacher",
    prior_occupation_source = "https://example.org/profile"
  )
  extra_payloads <- list(list(
    tool_name = "search_snippet_extract",
    text = "Primary occupation before politics: teacher.",
    payload = payload,
    source_blocks = list(),
    source_payloads = list(payload),
    has_structured_payload = TRUE,
    urls = list("https://example.org/profile")
  ))

  updates <- core$`_extract_field_status_updates`(
    existing_field_status = field_status,
    expected_schema = schema,
    tool_messages = list(),
    extra_payloads = extra_payloads,
    tool_calls_delta = 1L,
    unknown_after_searches = 3L,
    evidence_enabled = TRUE
  )
  updates_r <- reticulate::py_to_r(updates)
  updated_fs <- updates_r[[1]]

  expect_identical(as.character(updated_fs$prior_occupation$status), "found")
  expect_identical(as.character(updated_fs$prior_occupation$value), "teacher")
  expect_identical(as.character(updated_fs$prior_occupation_source$status), "found")
  expect_identical(
    as.character(updated_fs$prior_occupation_source$value),
    "https://example.org/profile"
  )
})

test_that("sensitive-field disclosure helper requires explicit disclosure markers", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  blocked <- reticulate::py_to_r(core$`_sensitive_field_disclosure_state`(
    field_key = "lgbtq_status",
    value = "Non-LGBTQ",
    source_text = "Biography and committee memberships are listed.",
    source_policy = list(sensitive_fields_require_explicit_disclosure = TRUE)
  ))
  expect_false(isTRUE(blocked$pass))
  expect_match(as.character(blocked$reason), "sensitive_disclosure", perl = TRUE)

  allowed <- reticulate::py_to_r(core$`_sensitive_field_disclosure_state`(
    field_key = "lgbtq_status",
    value = "Non-LGBTQ",
    source_text = "Public profile states she is non LGBT and heterosexual.",
    source_policy = list(sensitive_fields_require_explicit_disclosure = TRUE)
  ))
  expect_true(isTRUE(allowed$pass))
})

test_that("configured field rules support enum aliases for canonical labels", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  field_status <- list(
    lgbtq_status = list(
      status = "found",
      value = "gay",
      source_url = "https://example.org/profile",
      descriptor = "Non-LGBTQ|Openly LGBTQ|Unknown"
    ),
    lgbtq_status_source = list(
      status = "found",
      value = "https://example.org/profile",
      source_url = "https://example.org/profile",
      descriptor = "string|null"
    )
  )
  field_rules <- list(
    lgbtq_status = list(
      enum_aliases = list(gay = "Openly LGBTQ")
    )
  )

  out <- reticulate::py_to_r(core$`_apply_configured_field_rules`(field_status, field_rules))

  expect_identical(as.character(out$lgbtq_status$status), "found")
  expect_identical(as.character(out$lgbtq_status$value), "Openly LGBTQ")
  expect_identical(as.character(out$lgbtq_status$evidence), "field_rule_enum_alias")
})

test_that("enum coercion matches punctuation variants against schema labels", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  coerced <- reticulate::py_to_r(core$`_coerce_found_value_for_descriptor`(
    "Master's degree",
    "Bachelor's|Master's/Professional|Doctorate|Unknown"
  ))

  expect_identical(as.character(coerced), "Master's/Professional")
})

test_that("configured field rules can reject contaminated values", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  field_status <- list(
    prior_occupation = list(
      status = "found",
      value = "Deputy, Chamber of Deputies",
      source_url = "https://example.org/profile",
      descriptor = "string|Unknown"
    ),
    prior_occupation_source = list(
      status = "found",
      value = "https://example.org/profile",
      source_url = "https://example.org/profile",
      descriptor = "string|null"
    )
  )
  field_rules <- list(
    prior_occupation = list(
      reject_value_patterns = list("(?i)\\bdeputy\\b|\\bcouncilor\\b")
    )
  )

  out <- reticulate::py_to_r(core$`_apply_configured_field_rules`(field_status, field_rules))

  expect_identical(as.character(out$prior_occupation$status), "unknown")
  expect_identical(as.character(out$prior_occupation$value), "Unknown")
  expect_match(as.character(out$prior_occupation$evidence), "field_rule_reject_value_pattern")
  expect_true(is.null(out$prior_occupation$source_url))
  expect_identical(as.character(out$prior_occupation_source$status), "unknown")
})

test_that("finalization policy scrubs terminal fields backed by disallowed sources", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  field_status <- list(
    birth_place = list(
      status = "found",
      value = "La Paz",
      source_url = "https://www.facebook.com/profile.php?id=123",
      descriptor = "string|Unknown"
    ),
    birth_place_source = list(
      status = "found",
      value = "https://www.facebook.com/profile.php?id=123",
      source_url = "https://www.facebook.com/profile.php?id=123",
      descriptor = "string|null"
    )
  )

  scrubbed <- reticulate::py_to_r(core$`_enforce_finalization_policy_on_field_status`(
    field_status,
    list(require_verified_for_non_unknown = TRUE),
    source_policy = list(
      allow_tertiary_sources = FALSE,
      min_source_tier_for_non_unknown = "secondary"
    )
  ))

  expect_identical(as.character(scrubbed$birth_place$status), "unknown")
  expect_identical(as.character(scrubbed$birth_place$value), "Unknown")
  expect_match(
    as.character(scrubbed$birth_place$evidence),
    "^finalization_policy_demotion_source_"
  )
  expect_true(is.null(scrubbed$birth_place$source_url))
  expect_identical(as.character(scrubbed$birth_place_source$status), "unknown")

  diag <- reticulate::py_to_r(core$`_collect_field_status_diagnostics`(scrubbed))
  expect_equal(as.integer(diag$terminal_source_policy_violations), 1L)
  expect_equal(as.integer(diag$terminal_source_scrubbed_fields), 1L)
  expect_true(any(grepl("facebook.com", unlist(diag$terminal_source_policy_violation_urls), fixed = TRUE)))
})

test_that("budget normalization tracks search budgets separately from all tool calls", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  budget <- reticulate::py_to_r(core$`_normalize_budget_state`(
    list(
      search_calls_used = 7L,
      tool_calls_used = 20L,
      all_tool_calls_used = 20L,
      model_calls_used = 1L
    ),
    search_budget_limit = 8L,
    unknown_after_searches = 4L,
    model_budget_limit = 5L
  ))

  expect_equal(as.integer(budget$search_calls_used), 7L)
  expect_equal(as.integer(budget$search_calls_remaining), 1L)
  expect_false(isTRUE(as.logical(budget$search_budget_exhausted)))
  expect_equal(as.integer(budget$tool_calls_used), 7L)
  expect_equal(as.integer(budget$all_tool_calls_used), 20L)
})

test_that("finalization invariant ignores non-core unknown source fields in diagnostics snapshot", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  expected_schema <- list(
    prior_occupation = "string|Unknown",
    prior_occupation_source = "string|null"
  )
  field_status <- list(
    prior_occupation = list(status = "unknown", value = "Unknown", attempts = 1L),
    prior_occupation_source = list(status = "unknown", value = "Unknown", attempts = 1L)
  )
  diagnostics <- list(
    unknown_fields_count_current = 2L,
    unknown_fields_current = list("prior_occupation", "prior_occupation_source")
  )
  state <- list(
    expected_schema = expected_schema,
    field_status = field_status,
    diagnostics = diagnostics
  )

  report <- reticulate::py_to_r(core$`_finalization_invariant_report`(
    state = state,
    expected_schema = expected_schema,
    field_status = field_status,
    diagnostics = diagnostics
  ))

  reasons <- as.character(if (is.null(report$reasons)) character(0) else report$reasons)
  expect_false("diagnostics_unknown_count_mismatch" %in% reasons)
})

test_that("schema outcome gate reconciles stale diagnostics counts with current field_status", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  expected_schema <- list(
    prior_occupation = "string|Unknown"
  )
  field_status <- list(
    prior_occupation = list(status = "unknown", value = "Unknown", attempts = 1L)
  )
  stale_diagnostics <- list(
    unknown_fields_count_current = 0L,
    unknown_fields_current = list()
  )
  state <- list(
    expected_schema = expected_schema,
    field_status = field_status,
    diagnostics = stale_diagnostics,
    budget_state = list(
      tool_calls_used = 0L,
      tool_calls_limit = 4L,
      budget_exhausted = FALSE
    )
  )

  report <- reticulate::py_to_r(core$`_schema_outcome_gate_report`(
    state = state,
    expected_schema = expected_schema,
    field_status = field_status,
    budget_state = state$budget_state,
    diagnostics = stale_diagnostics
  ))

  expect_false(isTRUE(report$finalization_invariant_failed))
  reasons <- as.character(if (is.null(report$finalization_invariant_reasons)) character(0) else report$finalization_invariant_reasons)
  expect_false("diagnostics_unknown_count_mismatch" %in% reasons)
})

test_that("finalization invariant reconciles stale diagnostics before comparing unknown counts", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  expected_schema <- list(
    prior_occupation = "string|Unknown",
    education_level = "string|Unknown"
  )
  field_status <- list(
    prior_occupation = list(
      status = "found",
      value = "Teacher",
      source_url = "https://example.org/profile"
    ),
    education_level = list(
      status = "unknown",
      value = "Unknown",
      attempts = 1L
    )
  )
  stale_diagnostics <- list(
    unknown_fields_count_current = 0L,
    unknown_fields_current = list()
  )
  state <- list(
    expected_schema = expected_schema,
    field_status = field_status,
    diagnostics = stale_diagnostics
  )

  report <- reticulate::py_to_r(core$`_finalization_invariant_report`(
    state = state,
    expected_schema = expected_schema,
    field_status = field_status,
    diagnostics = stale_diagnostics
  ))

  expect_false(isTRUE(report$failed))
  expect_equal(as.integer(report$diagnostics_unknown_fields_count_current), 1L)
  reasons <- as.character(if (is.null(report$reasons)) character(0) else report$reasons)
  expect_false("diagnostics_unknown_count_mismatch" %in% reasons)
  expect_false("diagnostics_unknown_fields_mismatch" %in% reasons)
})

test_that("finalization invariant tracks pending core fields via unresolved diagnostics", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  expected_schema <- list(
    prior_occupation = "string|Unknown",
    education_level = "string|Unknown"
  )
  field_status <- list(
    prior_occupation = list(
      status = "found",
      value = "Teacher",
      source_url = "https://example.org/profile"
    ),
    education_level = list(
      status = "pending",
      value = "",
      attempts = 1L
    )
  )
  stale_diagnostics <- list(
    unknown_fields_count_current = 0L,
    unknown_fields_current = list()
  )
  state <- list(
    expected_schema = expected_schema,
    field_status = field_status,
    diagnostics = stale_diagnostics
  )

  report <- reticulate::py_to_r(core$`_finalization_invariant_report`(
    state = state,
    expected_schema = expected_schema,
    field_status = field_status,
    diagnostics = stale_diagnostics
  ))

  expect_false(isTRUE(report$failed))
  expect_equal(as.integer(report$diagnostics_unresolved_fields_count_current), 1L)
  expect_equal(as.integer(report$diagnostics_explicit_unknown_fields_count_current), 0L)
  expect_true("education_level" %in% as.character(report$diagnostics_unresolved_fields_current_sample))
  reasons <- as.character(if (is.null(report$reasons)) character(0) else report$reasons)
  expect_false("diagnostics_unknown_count_mismatch" %in% reasons)
  expect_false("diagnostics_unknown_fields_mismatch" %in% reasons)
})

test_that("schema outcome gate skips invariant enforcement on intermediate tool-call turns", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  expected_schema <- list(
    prior_occupation = "string|Unknown",
    education_level = "string|Unknown"
  )
  field_status <- list(
    prior_occupation = list(
      status = "found",
      value = "Teacher",
      source_url = "https://example.org/profile"
    ),
    education_level = list(
      status = "pending",
      value = "",
      attempts = 1L
    )
  )
  stale_diagnostics <- list(
    unknown_fields_count_current = 0L,
    unknown_fields_current = list(),
    unresolved_fields_count_current = 0L,
    unresolved_fields_current = list()
  )
  state <- list(
    expected_schema = expected_schema,
    field_status = field_status,
    diagnostics = stale_diagnostics,
    budget_state = list(
      tool_calls_used = 0L,
      tool_calls_limit = 4L,
      budget_exhausted = FALSE
    ),
    messages = list(
      msgs$HumanMessage(content = "Keep searching"),
      msgs$AIMessage(
        content = "",
        tool_calls = list(list(
          name = "Search",
          args = list(query = "Ramona Moye Camaconi education"),
          id = "call_1"
        ))
      )
    ),
    final_emitted = FALSE,
    terminal_valid = FALSE
  )

  report <- reticulate::py_to_r(core$`_schema_outcome_gate_report`(
    state = state,
    expected_schema = expected_schema,
    field_status = field_status,
    budget_state = state$budget_state,
    diagnostics = stale_diagnostics
  ))

  expect_false(isTRUE(report$finalization_invariant_applicable))
  expect_false(isTRUE(report$finalization_invariant_failed))
  expect_false(isTRUE(report$auto_recovery_checkpoint_needed))
  expect_false(isTRUE(report$finalization_invariant$applicable))
  expect_equal(as.character(report$finalization_invariant$skip_reason), "intermediate_turn")
})

test_that("retrieval metrics track no-new-high-quality-evidence streak", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  state <- list(
    retrieval_metrics = list(no_new_high_quality_evidence_streak = 1L),
    orchestration_options = list(
      retrieval_controller = list(enabled = TRUE, mode = "enforce")
    ),
    expected_schema = list(birth_year = "integer|null|Unknown")
  )
  metrics <- reticulate::py_to_r(core$`_build_retrieval_metrics`(
    state = state,
    search_queries = list("Ramona Moye Camaconi birth year"),
    tool_messages = list(),
    prior_field_status = list(),
    field_status = list(),
    prior_evidence_ledger = list(),
    evidence_ledger = list(),
    diagnostics = list(),
    source_policy = list(),
    retry_policy = list(
      no_new_evidence_high_quality_score = 0.70,
      no_new_evidence_min_tier = "secondary"
    )
  ))

  expect_equal(as.integer(metrics$new_high_quality_evidence), 0L)
  expect_equal(as.integer(metrics$no_new_high_quality_evidence_streak), 2L)
})
