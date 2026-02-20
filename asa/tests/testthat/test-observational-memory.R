# Tests for observational-memory behavior and regression safeguards.

test_that("observational memory defaults remain disabled and thread-scoped", {
  cfg <- asa_config()

  expect_false(isTRUE(ASA_DEFAULT_USE_OBSERVATIONAL_MEMORY))
  expect_false(isTRUE(ASA_DEFAULT_OM_CROSS_THREAD_MEMORY))
  expect_false(isTRUE(cfg$use_observational_memory))
  expect_false(isTRUE(cfg$om_cross_thread_memory))
})

test_that("summarize token trace stays 'summarize' when OM is disabled", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_llm(mode = "simple", response_content = "done", var_name = "om_off_llm")
  asa_test_stub_summarizer(var_name = "om_off_summarizer")

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$om_off_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(2),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$om_off_summarizer,
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE),
    debug = FALSE
  )

  summarize_state <- reticulate::dict(
    messages = list(
      msgs$HumanMessage(content = "user 1", id = "h1"),
      msgs$AIMessage(content = "assistant 1", id = "a1"),
      msgs$HumanMessage(content = "user 2", id = "h2"),
      msgs$AIMessage(content = "assistant 2", id = "a2")
    ),
    summary = "",
    archive = list(),
    fold_stats = reticulate::dict(fold_count = 0L),
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE),
    stop_reason = NULL
  )

  summarize_out <- agent$nodes[["summarize"]]$bound$invoke(summarize_state)
  fs <- as.list(summarize_out$fold_stats)
  trace <- reticulate::py_to_r(summarize_out$token_trace)

  expect_equal(as.integer(fs$fold_count), 1L)
  expect_true(length(trace) >= 1L)
  expect_equal(as.character(trace[[1]]$node), "summarize")
})

test_that("observer stage emits observations when OM is enabled", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_llm(mode = "simple", response_content = "done", var_name = "om_observer_llm")

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$om_observer_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(50),
    keep_recent = as.integer(4),
    om_config = list(
      enabled = TRUE,
      cross_thread_memory = FALSE,
      observation_message_tokens = 10L,
      reflection_observation_tokens = 40L,
      buffer_tokens = 20L,
      buffer_activation = 0.70,
      block_after = 0.92,
      async_prebuffer = TRUE
    ),
    debug = FALSE
  )

  observe_state <- reticulate::dict(
    messages = list(
      msgs$HumanMessage(content = paste(rep("alpha", 30), collapse = " "), id = "oh1"),
      msgs$AIMessage(content = paste(rep("beta", 30), collapse = " "), id = "oa1")
    ),
    observations = list(),
    reflections = list(),
    om_stats = list(),
    om_prebuffer = reticulate::dict(ready = FALSE, observations = list(), tokens_estimate = 0L),
    om_config = list(
      enabled = TRUE,
      cross_thread_memory = FALSE,
      observation_message_tokens = 10L,
      reflection_observation_tokens = 40L,
      buffer_tokens = 20L,
      buffer_activation = 0.70,
      block_after = 0.92,
      async_prebuffer = TRUE
    )
  )

  observe_out <- agent$nodes[["observe"]]$bound$invoke(observe_state)
  observations <- reticulate::py_to_r(observe_out$observations)
  om_stats <- reticulate::py_to_r(observe_out$om_stats)

  expect_true(length(observations) >= 1L)
  expect_true(as.integer(om_stats$observer_runs) >= 1L)
  expect_true(as.integer(om_stats$observer_observations_count) >= 1L)
})

test_that("reflect stage appends reflection entries when OM is enabled", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_llm(mode = "simple", response_content = "done", var_name = "om_reflect_llm")
  asa_test_stub_summarizer(var_name = "om_reflect_summarizer")

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$om_reflect_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(2),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$om_reflect_summarizer,
    om_config = list(
      enabled = TRUE,
      cross_thread_memory = FALSE,
      observation_message_tokens = 10L,
      reflection_observation_tokens = 20L,
      buffer_tokens = 80L,
      buffer_activation = 0.70,
      block_after = 0.92,
      async_prebuffer = TRUE
    ),
    debug = FALSE
  )

  reflect_state <- reticulate::dict(
    messages = list(
      msgs$HumanMessage(content = "question", id = "rh1"),
      msgs$AIMessage(content = "draft answer one", id = "ra1"),
      msgs$HumanMessage(content = "follow-up", id = "rh2"),
      msgs$AIMessage(content = "draft answer two", id = "ra2")
    ),
    summary = "",
    archive = list(),
    fold_stats = reticulate::dict(fold_count = 0L),
    observations = list(
      reticulate::dict(text = paste(rep("observation alpha", 25), collapse = " "), kind = "observation"),
      reticulate::dict(text = paste(rep("observation beta", 25), collapse = " "), kind = "observation")
    ),
    reflections = list(),
    om_stats = list(),
    om_config = list(
      enabled = TRUE,
      cross_thread_memory = FALSE,
      observation_message_tokens = 10L,
      reflection_observation_tokens = 20L,
      buffer_tokens = 80L,
      buffer_activation = 0.70,
      block_after = 0.92,
      async_prebuffer = TRUE
    ),
    stop_reason = NULL
  )

  reflect_out <- agent$nodes[["reflect"]]$bound$invoke(reflect_state)
  reflections <- reticulate::py_to_r(reflect_out$reflections)
  om_stats <- reticulate::py_to_r(reflect_out$om_stats)
  trace <- reticulate::py_to_r(reflect_out$token_trace)

  expect_true(length(reflections) >= 1L)
  expect_true(as.integer(om_stats$reflector_runs) >= 1L)
  expect_true(as.integer(om_stats$reflector_reflection_count) >= 1L)
  expect_true(length(trace) >= 1L)
  expect_equal(as.character(trace[[1]]$node), "reflect")
})

test_that("fold summary chars uses delta magnitude when summary shrinks", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_llm(mode = "simple", response_content = "done", var_name = "fold_delta_llm")
  asa_test_stub_summarizer(var_name = "fold_delta_summarizer")

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$fold_delta_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(2),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$fold_delta_summarizer,
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE),
    debug = FALSE
  )

  large_summary <- list(
    version = 1L,
    facts = as.list(paste0("fact_", seq_len(80))),
    decisions = as.list(paste0("decision_", seq_len(25))),
    open_questions = as.list(paste0("question_", seq_len(25))),
    sources = as.list(paste0("https://example.com/", seq_len(20))),
    warnings = as.list(paste0("warning_", seq_len(20)))
  )

  summarize_state <- reticulate::dict(
    messages = list(
      msgs$HumanMessage(content = "user 1", id = "dh1"),
      msgs$AIMessage(content = "assistant 1", id = "da1"),
      msgs$HumanMessage(content = "user 2", id = "dh2"),
      msgs$AIMessage(content = "assistant 2", id = "da2")
    ),
    summary = large_summary,
    archive = list(),
    fold_stats = reticulate::dict(fold_count = 0L),
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE),
    stop_reason = NULL
  )

  summarize_out <- agent$nodes[["summarize"]]$bound$invoke(summarize_state)
  fs <- as.list(summarize_out$fold_stats)

  expect_true(as.integer(fs$fold_summary_delta_chars) < 0L)
  expect_equal(
    as.integer(fs$fold_summary_chars),
    abs(as.integer(fs$fold_summary_delta_chars))
  )
  expect_gt(as.integer(fs$fold_summary_chars), 1L)
})

test_that("canonical terminal payload demotions are reflected in field_status", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  expected_schema <- list(
    birth_place = "string|Unknown",
    birth_place_source = "string|null"
  )
  field_status <- list(
    birth_place = list(
      status = "found",
      value = "Tipnis",
      source_url = NULL,
      descriptor = "string|Unknown"
    ),
    birth_place_source = list(
      status = "pending",
      value = NULL,
      source_url = NULL,
      descriptor = "string|null"
    )
  )
  response <- reticulate::dict(content = "{\"birth_place\":\"Unknown\",\"birth_place_source\":null}")

  sync_fn <- reticulate::py_get_attr(prod, "_sync_field_status_from_terminal_payload")
  synced <- sync_fn(
    response = response,
    field_status = field_status,
    expected_schema = expected_schema
  )
  synced_r <- reticulate::py_to_r(synced)

  expect_equal(as.character(synced_r$birth_place$status), "unknown")
  expect_equal(as.character(synced_r$birth_place$value), "Unknown")
  expect_false(identical(as.character(synced_r$birth_place_source$status), "found"))
})

test_that("field-status diagnostics include demotion counts and reason buckets", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  collect_diag <- reticulate::py_get_attr(prod, "_collect_field_status_diagnostics")
  merge_diag <- reticulate::py_get_attr(prod, "_merge_field_status_diagnostics")

  field_status <- list(
    prior_occupation = list(
      status = "unknown",
      value = "Unknown",
      source_url = NULL,
      evidence = "finalization_policy_demotion_unverified",
      descriptor = "string|Unknown"
    ),
    prior_occupation_source = list(
      status = "unknown",
      value = NULL,
      source_url = NULL,
      evidence = "finalization_policy_demotion_unverified",
      descriptor = "string|null"
    ),
    education_source = list(
      status = "unknown",
      value = NULL,
      source_url = NULL,
      evidence = "source_consistency_demotion",
      descriptor = "string|null"
    )
  )

  collected <- reticulate::py_to_r(collect_diag(field_status = field_status))
  expect_equal(as.integer(collected$field_demotions_count), 3L)
  expect_true("prior_occupation" %in% as.character(collected$field_demotion_fields))
  expect_equal(
    as.integer(collected$field_demotion_reason_counts$finalization_policy_demotion_unverified),
    2L
  )
  expect_equal(
    as.integer(collected$field_demotion_reason_counts$source_consistency_demotion),
    1L
  )

  merged <- reticulate::py_to_r(merge_diag(
    diagnostics = list(
      field_demotions_count = 1L,
      field_demotion_reason_counts = list(finalization_policy_demotion_unverified = 1L),
      field_demotion_fields = list("stale_field")
    ),
    field_status = field_status
  ))
  expect_equal(as.integer(merged$field_demotions_count), 3L)
  expect_equal(
    as.integer(merged$field_demotion_reason_counts$finalization_policy_demotion_unverified),
    2L
  )
  expect_equal(
    as.integer(merged$field_demotion_reason_counts$source_consistency_demotion),
    1L
  )
  expect_true("stale_field" %in% as.character(merged$field_demotion_fields))
  expect_true("prior_occupation_source" %in% as.character(merged$field_demotion_fields))
})

test_that("scratchpad promotions require provenance when schema has sibling source field", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  field_status <- list(
    prior_occupation = list(
      status = "pending",
      value = NULL,
      source_url = NULL,
      descriptor = "string|Unknown"
    ),
    prior_occupation_source = list(
      status = "pending",
      value = NULL,
      source_url = NULL,
      descriptor = "string|null"
    )
  )

  sync_fn <- reticulate::py_get_attr(prod, "_sync_scratchpad_to_field_status")

  # Unsourced finding must not be promoted.
  unsourced <- sync_fn(
    scratchpad = list(list(finding = "prior_occupation = teacher", category = "fact")),
    field_status = field_status
  )
  unsourced_r <- reticulate::py_to_r(unsourced)
  expect_equal(as.character(unsourced_r$prior_occupation$status), "pending")

  # Source-backed finding should promote.
  sourced <- sync_fn(
    scratchpad = list(list(
      finding = "prior_occupation = teacher (source: https://example.com/profile)",
      category = "fact"
    )),
    field_status = field_status
  )
  sourced_r <- reticulate::py_to_r(sourced)
  expect_equal(as.character(sourced_r$prior_occupation$status), "found")
  expect_equal(as.character(sourced_r$prior_occupation$value), "teacher")
  expect_equal(as.character(sourced_r$prior_occupation$source_url), "https://example.com/profile")
})

test_that("summary fact promotions ignore ungrounded entries and require source URLs", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  field_status <- list(
    birth_year = list(
      status = "pending",
      value = NULL,
      source_url = NULL,
      descriptor = "integer|null|Unknown"
    )
  )

  sync_fn <- reticulate::py_get_attr(prod, "_sync_summary_facts_to_field_status")

  blocked <- sync_fn(
    summary = list(facts = list(
      "UNGROUNDED: FIELD_EXTRACT: birth_year = 1982 (source: https://example.com)",
      "FIELD_EXTRACT: birth_year = 1982"
    )),
    field_status = field_status
  )
  blocked_r <- reticulate::py_to_r(blocked)
  expect_equal(as.character(blocked_r$birth_year$status), "pending")

  promoted <- sync_fn(
    summary = list(facts = list(
      "FIELD_EXTRACT: birth_year = 1982 (source: https://example.com/profile)"
    )),
    field_status = field_status
  )
  promoted_r <- reticulate::py_to_r(promoted)
  expect_equal(as.character(promoted_r$birth_year$status), "found")
  expect_equal(as.character(promoted_r$birth_year$value), "1982")
  expect_equal(as.character(promoted_r$birth_year$source_url), "https://example.com/profile")
})

test_that("canonical payload derivations normalize confidence casing", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  deriv_fn <- reticulate::py_get_attr(prod, "_apply_canonical_payload_derivations")
  payload <- list(confidence = "low", justification = "ok")
  normalized <- deriv_fn(payload, list())
  normalized_r <- reticulate::py_to_r(normalized)
  expect_equal(as.character(normalized_r$confidence), "Low")
})

test_that("terminal canonicalization re-syncs derived fields into field_status", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  expected_schema <- list(
    prior_occupation = "string|Unknown",
    confidence = "Low|Medium|High",
    justification = "string"
  )

  field_status <- list(
    prior_occupation = list(
      status = "found",
      value = "teacher",
      source_url = NULL,
      descriptor = "string|Unknown"
    ),
    confidence = list(
      status = "pending",
      value = NULL,
      source_url = NULL,
      descriptor = "Low|Medium|High"
    ),
    justification = list(
      status = "pending",
      value = NULL,
      source_url = NULL,
      descriptor = "string"
    )
  )

  response <- reticulate::dict(
    content = "{\"prior_occupation\":\"teacher\",\"confidence\":\"low\",\"justification\":\"\"}"
  )

  sync_terminal <- reticulate::py_get_attr(prod, "_sync_terminal_response_with_field_status")
  synced <- sync_terminal(
    response = response,
    field_status = field_status,
    expected_schema = expected_schema,
    messages = list(list(role = "user", content = "Return JSON only.")),
    force_canonical = TRUE
  )

  synced_r <- reticulate::py_to_r(synced)
  synced_field_status <- synced_r[[2]]

  expect_equal(as.character(synced_field_status$confidence$status), "found")
  expect_equal(as.character(synced_field_status$confidence$value), "Low")
  expect_equal(as.character(synced_field_status$justification$status), "found")
  expect_true(nzchar(as.character(synced_field_status$justification$value)))
})

test_that("confidence normalization accepts numeric-like values", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  normalize_conf <- reticulate::py_get_attr(prod, "_normalize_confidence_label")

  expect_equal(as.character(normalize_conf("1")), "Low")
  expect_equal(as.character(normalize_conf("2")), "Medium")
  expect_equal(as.character(normalize_conf("3")), "High")
  expect_equal(as.character(normalize_conf("0.85")), "High")
})

test_that("source specificity helper prefers profile-like URLs over listing pages", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  is_specific <- reticulate::py_get_attr(prod, "_is_source_specific_url")
  score_specificity <- reticulate::py_get_attr(prod, "_source_specificity_score")

  expect_false(isTRUE(reticulate::py_to_r(is_specific("https://example.com/search?q=ramona+moye"))))
  expect_true(isTRUE(reticulate::py_to_r(is_specific("https://example.com/people/ramona-moye-camaconi"))))
  expect_true(
    as.numeric(reticulate::py_to_r(score_specificity("https://example.com/people/ramona-moye-camaconi"))) >
      as.numeric(reticulate::py_to_r(score_specificity("https://example.com/search?q=ramona+moye")))
  )
})

test_that("budget normalization tracks model-call budgets", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  normalize_budget <- reticulate::py_get_attr(prod, "_normalize_budget_state")
  budget <- normalize_budget(
    list(model_calls_used = 3L, tool_calls_used = 2L),
    search_budget_limit = 10L,
    unknown_after_searches = 4L,
    model_budget_limit = 5L
  )
  budget_r <- reticulate::py_to_r(budget)

  expect_equal(as.integer(budget_r$model_calls_used), 3L)
  expect_equal(as.integer(budget_r$model_calls_limit), 5L)
  expect_equal(as.integer(budget_r$model_calls_remaining), 2L)
  expect_false(isTRUE(as.logical(budget_r$model_budget_exhausted)))
})

test_that("model-call recorder sets exhaustion reason when budget is hit", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  record_model_call <- reticulate::py_get_attr(prod, "_record_model_call_on_budget")
  budget <- record_model_call(
    list(model_calls_used = 4L, model_calls_limit = 5L, tool_calls_limit = 10L, tool_calls_used = 0L),
    call_delta = 1L
  )
  budget_r <- reticulate::py_to_r(budget)

  expect_true(isTRUE(as.logical(budget_r$model_budget_exhausted)))
  expect_true(isTRUE(as.logical(budget_r$budget_exhausted)))
  expect_equal(as.character(budget_r$limit_trigger_reason), "model_budget")
})
