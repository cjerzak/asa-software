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

test_that("canonical payload derivations auto-add source/confidence siblings", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  deriv_fn <- reticulate::py_get_attr(prod, "_apply_canonical_payload_derivations")
  payload <- list(
    prior_occupation = "teacher"
  )
  field_status <- list(
    prior_occupation = list(
      status = "found",
      value = "teacher",
      source_url = "https://example.com/profile",
      evidence_score = 0.82
    ),
    prior_occupation_source = list(
      status = "found",
      value = "https://example.com/profile",
      source_url = "https://example.com/profile"
    )
  )

  normalized <- deriv_fn(payload, field_status)
  normalized_r <- reticulate::py_to_r(normalized)

  expect_equal(as.character(normalized_r$prior_occupation_source), "https://example.com/profile")
  expect_equal(as.numeric(normalized_r$prior_occupation_confidence), 0.82)
})

test_that("canonical payload derivations emit null sibling metadata for unknown fields", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  deriv_fn <- reticulate::py_get_attr(prod, "_apply_canonical_payload_derivations")
  payload <- list(
    education_level = "Unknown"
  )
  field_status <- list(
    education_level = list(
      status = "unknown",
      value = "Unknown",
      source_url = NULL,
      descriptor = "string|Unknown"
    )
  )

  normalized <- deriv_fn(payload, field_status)
  normalized_r <- reticulate::py_to_r(normalized)

  expect_true("education_level_source" %in% names(normalized_r))
  expect_true("education_level_confidence" %in% names(normalized_r))
  expect_true(is.null(normalized_r$education_level_source))
  expect_true(is.null(normalized_r$education_level_confidence))
})

test_that("canonical array merge preserves derived sibling metadata outside schema", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  merge_fn <- reticulate::py_get_attr(prod, "_merge_canonical_payload_with_model_arrays")
  canonical_payload <- list(
    birth_place = "Beni",
    birth_year = "Unknown",
    birth_place_source = "https://example.com/profile",
    birth_place_confidence = 0.91,
    birth_year_source = NULL,
    birth_year_confidence = NULL,
    extra_debug = "drop_me"
  )
  model_payload <- list(
    birth_place = "Unknown",
    extra_debug = "model_value"
  )
  expected_schema <- list(
    birth_place = "string|Unknown",
    birth_year = "integer|null|Unknown"
  )

  merged <- merge_fn(canonical_payload, model_payload, expected_schema)
  merged_r <- reticulate::py_to_r(merged)

  expect_equal(as.character(merged_r$birth_place), "Beni")
  expect_equal(as.character(merged_r$birth_place_source), "https://example.com/profile")
  expect_equal(as.numeric(merged_r$birth_place_confidence), 0.91)
  expect_true("birth_year_source" %in% names(merged_r))
  expect_true("birth_year_confidence" %in% names(merged_r))
  expect_true(is.null(merged_r$birth_year_source))
  expect_true(is.null(merged_r$birth_year_confidence))
  expect_false("extra_debug" %in% names(merged_r))
  merged_names <- names(merged_r)
  expect_equal(which(merged_names == "birth_place_source"), which(merged_names == "birth_place") + 1L)
  expect_equal(which(merged_names == "birth_place_confidence"), which(merged_names == "birth_place") + 2L)
  expect_equal(which(merged_names == "birth_year_source"), which(merged_names == "birth_year") + 1L)
  expect_equal(which(merged_names == "birth_year_confidence"), which(merged_names == "birth_year") + 2L)
})

test_that("terminal guard emits sibling metadata when no fields are resolved", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  guard_fn <- reticulate::py_get_attr(prod, "_apply_field_status_terminal_guard")
  expected_schema <- list(
    prior_occupation = "string|Unknown",
    education_level = "string|Unknown",
    confidence = "Low|Medium|High",
    justification = "string"
  )
  field_status <- list(
    prior_occupation = list(
      status = "unknown",
      value = "Unknown",
      source_url = NULL,
      descriptor = "string|Unknown"
    ),
    education_level = list(
      status = "unknown",
      value = "Unknown",
      source_url = NULL,
      descriptor = "string|Unknown"
    )
  )
  response <- reticulate::dict(
    content = "{\"prior_occupation\":\"Unknown\",\"education_level\":\"Unknown\",\"confidence\":\"low\",\"justification\":\"none\"}"
  )

  guarded <- guard_fn(
    response = response,
    expected_schema = expected_schema,
    field_status = field_status,
    schema_source = "explicit",
    context = "finalize",
    debug = FALSE
  )
  guarded_r <- reticulate::py_to_r(guarded)
  guarded_response <- guarded_r[[1]]
  guarded_text <- if (!is.null(guarded_response$content)) {
    as.character(guarded_response$content)
  } else {
    as.character(guarded_response[["content"]])
  }
  parsed <- jsonlite::fromJSON(guarded_text, simplifyVector = FALSE)

  expect_true("prior_occupation_source" %in% names(parsed))
  expect_true("prior_occupation_confidence" %in% names(parsed))
  expect_true(is.null(parsed$prior_occupation_source))
  expect_true(is.null(parsed$prior_occupation_confidence))
  expect_true("education_level_source" %in% names(parsed))
  expect_true("education_level_confidence" %in% names(parsed))
  expect_true(is.null(parsed$education_level_source))
  expect_true(is.null(parsed$education_level_confidence))
  expect_equal(as.character(parsed$confidence), "Low")
  parsed_names <- names(parsed)
  expect_equal(which(parsed_names == "prior_occupation_source"), which(parsed_names == "prior_occupation") + 1L)
  expect_equal(which(parsed_names == "prior_occupation_confidence"), which(parsed_names == "prior_occupation") + 2L)
  expect_equal(which(parsed_names == "education_level_source"), which(parsed_names == "education_level") + 1L)
  expect_equal(which(parsed_names == "education_level_confidence"), which(parsed_names == "education_level") + 2L)
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

test_that("field recovery promotes source-backed unresolved integer fields", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  recover_fn <- reticulate::py_get_attr(prod, "_recover_unknown_fields_from_tool_evidence")
  profile_url <- "https://www.vicepresidencia.gob.bo/spip.php?page=parlamentario&id_parlamentario=609"

  recovered <- recover_fn(
    field_status = list(
      birth_year = list(
        status = "unknown",
        value = "Unknown",
        source_url = NULL,
        descriptor = "integer|null|Unknown"
      ),
      birth_year_source = list(
        status = "unknown",
        value = NULL,
        source_url = NULL,
        descriptor = "string|null"
      )
    ),
    expected_schema = list(
      birth_year = "integer|null|Unknown",
      birth_year_source = "string|null"
    ),
    finalization_policy = list(
      field_recovery_enabled = TRUE,
      field_recovery_mode = "balanced"
    ),
    allowed_source_urls = list(profile_url),
    source_text_index = list(
      "https://www.vicepresidencia.gob.bo/spip.php?page=parlamentario&id_parlamentario=609" =
        "RAMONA MOYE CAMACONI Fecha de nacimiento: 1982-01-07 Diputado uninominal - Circunscripción 6"
    ),
    entity_name_tokens = list("Ramona", "Moye", "Camaconi")
  )
  out <- reticulate::py_to_r(recovered)

  expect_equal(as.character(out$birth_year$status), "found")
  expect_equal(as.integer(out$birth_year$value), 1982L)
  expect_equal(as.character(out$birth_year_source$status), "found")
  expect_equal(as.character(out$birth_year_source$value), profile_url)
  expect_equal(as.character(out$birth_year$evidence), "recovery_source_backed")
})

test_that("field recovery can be disabled via finalization policy", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  recover_fn <- reticulate::py_get_attr(prod, "_recover_unknown_fields_from_tool_evidence")
  profile_url <- "https://example.gov/profile"

  recovered <- recover_fn(
    field_status = list(
      birth_year = list(
        status = "unknown",
        value = "Unknown",
        source_url = NULL,
        descriptor = "integer|null|Unknown"
      ),
      birth_year_source = list(
        status = "unknown",
        value = NULL,
        source_url = NULL,
        descriptor = "string|null"
      )
    ),
    expected_schema = list(
      birth_year = "integer|null|Unknown",
      birth_year_source = "string|null"
    ),
    finalization_policy = list(field_recovery_enabled = FALSE),
    allowed_source_urls = list(profile_url),
    source_text_index = list(
      "https://example.gov/profile" = "PROFILE CARD Date of birth: 1982-01-07"
    )
  )
  out <- reticulate::py_to_r(recovered)

  expect_equal(as.character(out$birth_year$status), "unknown")
  expect_true(is.null(out$birth_year_source$value) || is.na(out$birth_year_source$value))
})

test_that("field-status diagnostics include recovery promotion and rejection counters", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  collect_diag <- reticulate::py_get_attr(prod, "_collect_field_status_diagnostics")
  merge_diag <- reticulate::py_get_attr(prod, "_merge_field_status_diagnostics")

  field_status <- list(
    birth_year = list(
      status = "found",
      value = 1982L,
      source_url = "https://example.gov/profile",
      evidence = "recovery_source_backed",
      descriptor = "integer|null|Unknown"
    ),
    birth_year_source = list(
      status = "found",
      value = "https://example.gov/profile",
      source_url = "https://example.gov/profile",
      evidence = "recovery_source_backed",
      descriptor = "string|null"
    ),
    education_level = list(
      status = "unknown",
      value = "Unknown",
      source_url = NULL,
      evidence = "recovery_blocked_entity_mismatch",
      descriptor = "string|Unknown"
    )
  )

  collected <- reticulate::py_to_r(collect_diag(field_status = field_status))
  expect_equal(as.integer(collected$recovery_promotions_count), 2L)
  expect_equal(as.integer(collected$recovery_rejections_count), 1L)
  expect_true("birth_year" %in% as.character(collected$recovery_promoted_fields))
  expect_true("education_level" %in% as.character(collected$recovery_rejected_fields))
  expect_equal(
    as.integer(collected$recovery_reason_counts$recovery_blocked_entity_mismatch),
    1L
  )

  merged <- reticulate::py_to_r(merge_diag(
    diagnostics = list(
      recovery_promotions_count = 1L,
      recovery_rejections_count = 0L,
      recovery_promoted_fields = list("stale_recovery"),
      recovery_reason_counts = list(recovery_blocked_non_specific_source = 1L)
    ),
    field_status = field_status
  ))
  expect_equal(as.integer(merged$recovery_promotions_count), 2L)
  expect_equal(as.integer(merged$recovery_rejections_count), 1L)
  expect_true("stale_recovery" %in% as.character(merged$recovery_promoted_fields))
  expect_equal(
    as.integer(merged$recovery_reason_counts$recovery_blocked_non_specific_source),
    1L
  )
})

test_that("field-status diagnostics include unknown reason buckets", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  collect_diag <- reticulate::py_get_attr(prod, "_collect_field_status_diagnostics")
  merge_diag <- reticulate::py_get_attr(prod, "_merge_field_status_diagnostics")

  field_status <- list(
    education_level = list(
      status = "unknown",
      value = "Unknown",
      evidence = "recovery_blocked_entity_mismatch",
      attempts = 2L,
      descriptor = "string|Unknown"
    ),
    birth_place = list(
      status = "unknown",
      value = "Unknown",
      attempts = 1L,
      descriptor = "string|Unknown"
    ),
    prior_occupation = list(
      status = "found",
      value = "Teacher",
      source_url = "https://example.gov/profile",
      evidence = "recovery_source_backed",
      descriptor = "string|Unknown"
    )
  )

  collected <- reticulate::py_to_r(collect_diag(field_status = field_status))
  expect_equal(as.integer(collected$unknown_fields_count), 2L)
  expect_true("education_level" %in% as.character(collected$unknown_fields))
  expect_equal(
    as.integer(collected$unknown_reason_counts$recovery_blocked_entity_mismatch),
    1L
  )
  expect_equal(
    as.integer(collected$unknown_reason_counts$attempted_no_verified_evidence),
    1L
  )
  expect_true(
    "education_level" %in% as.character(
      collected$unknown_fields_by_reason$recovery_blocked_entity_mismatch
    )
  )

  merged <- reticulate::py_to_r(merge_diag(
    diagnostics = list(
      unknown_fields_count = 1L,
      unknown_reason_counts = list(not_attempted = 3L),
      unknown_fields_by_reason = list(not_attempted = list("stale_field"))
    ),
    field_status = field_status
  ))
  expect_equal(as.integer(merged$unknown_fields_count), 2L)
  expect_equal(as.integer(merged$unknown_reason_counts$not_attempted), 3L)
  expect_true("stale_field" %in% as.character(merged$unknown_fields_by_reason$not_attempted))
})

test_that("schema outcome quality gate blocks completion when unknown ratio is high", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  gate_fn <- reticulate::py_get_attr(prod, "_schema_outcome_gate_report")
  state <- list(
    expected_schema = list(
      education_level = "string|Unknown",
      prior_occupation = "string|Unknown",
      birth_year = "integer|null|Unknown"
    ),
    field_status = list(
      education_level = list(status = "unknown", value = "Unknown", attempts = 2L),
      prior_occupation = list(status = "unknown", value = "Unknown", attempts = 2L),
      birth_year = list(status = "found", value = 1982L, attempts = 1L)
    ),
    budget_state = list(budget_exhausted = FALSE),
    finalization_policy = list(
      quality_gate_enforce = TRUE,
      quality_gate_unknown_ratio_max = 0.50,
      quality_gate_min_resolvable_fields = 2L
    ),
    use_plan_mode = FALSE,
    json_repair = list()
  )

  report <- reticulate::py_to_r(gate_fn(
    state = state,
    expected_schema = state$expected_schema,
    field_status = state$field_status,
    budget_state = state$budget_state
  ))

  expect_true(isTRUE(report$quality_gate_failed))
  expect_equal(as.character(report$quality_gate_reason), "unknown_ratio_exceeds_limit")
  expect_equal(as.character(report$completion_status), "in_progress")
  expect_false(isTRUE(report$done))
  expect_equal(as.integer(report$missing_required_field_count), 2L)
  expect_true("plan_mode_disabled" %in% as.character(report$artifact_markers))
  expect_true("invoke_error_absent" %in% as.character(report$artifact_markers))
})

test_that("summarize fallback records parse retry history", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_llm(mode = "simple", response_content = "done", var_name = "fold_retry_llm")
  reticulate::py_run_string(paste0(
    "class _ASABadSummarizer:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        class _Resp:\n",
    "            def __init__(self, content):\n",
    "                self.content = content\n",
    "        return _Resp('not valid json output')\n",
    "_asa_bad_summarizer = _ASABadSummarizer()\n"
  ))

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$fold_retry_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(2),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$`_asa_bad_summarizer`,
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE),
    debug = FALSE
  )

  summarize_state <- reticulate::dict(
    messages = list(
      msgs$HumanMessage(content = "user 1", id = "frh1"),
      msgs$AIMessage(content = "assistant 1", id = "fra1"),
      msgs$HumanMessage(content = "user 2", id = "frh2"),
      msgs$AIMessage(content = "assistant 2", id = "fra2")
    ),
    summary = "",
    archive = list(),
    fold_stats = reticulate::dict(fold_count = 0L),
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE),
    stop_reason = NULL
  )

  summarize_out <- agent$nodes[["summarize"]]$bound$invoke(summarize_state)
  fs <- reticulate::py_to_r(summarize_out$fold_stats)
  parse_retry <- fs$fold_parse_retry
  retry_stages <- vapply(
    X = parse_retry,
    FUN = function(x) {
      if (!is.list(x) || is.null(x$stage)) return("")
      as.character(x$stage)
    },
    FUN.VALUE = character(1)
  )

  expect_true(length(parse_retry) >= 2L)
  expect_true("initial_parse" %in% retry_stages)
  expect_true("schema_repair_strict" %in% retry_stages)
  expect_true(any(retry_stages != "initial_parse"))
})

test_that("retrieval metrics track duplicate URLs within a round", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  build_metrics <- reticulate::py_get_attr(prod, "_build_retrieval_metrics")
  search_tool_text <- paste0(
    "__START_OF_SOURCE 1__ <URL> https://example.gov/profile/1 </URL> <CONTENT> A </CONTENT> __END_OF_SOURCE 1__ ",
    "__START_OF_SOURCE 2__ <URL> https://example.gov/profile/1 </URL> <CONTENT> B </CONTENT> __END_OF_SOURCE 2__"
  )
  metrics <- reticulate::py_to_r(build_metrics(
    state = list(retrieval_metrics = list(), expected_schema = list()),
    search_queries = list("example query"),
    tool_messages = list(
      list(role = "tool", name = "Search", content = search_tool_text),
      list(role = "tool", name = "Search", content = search_tool_text)
    ),
    prior_field_status = list(),
    field_status = list(),
    prior_evidence_ledger = list(),
    evidence_ledger = list(),
    diagnostics = list()
  ))

  expect_equal(as.integer(metrics$last_round_url_dedupe_hits), 1L)
  expect_equal(as.integer(metrics$round_url_dedupe_hits), 1L)
})

test_that("retrieval metrics expose diminishing-returns signals", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  build_metrics <- reticulate::py_get_attr(prod, "_build_retrieval_metrics")
  metrics <- reticulate::py_to_r(build_metrics(
    state = list(
      retrieval_metrics = list(
        diminishing_returns_streak = 1L,
        early_stop_min_gain = 0.10
      ),
      expected_schema = list(name = "string"),
      orchestration_options = list(
        retrieval_controller = list(
          enabled = TRUE,
          mode = "enforce",
          adaptive_budget_enabled = TRUE,
          adaptive_low_value_threshold = 0.20,
          adaptive_patience_steps = 2L
        )
      )
    ),
    search_queries = list("example query"),
    tool_messages = list(
      list(role = "tool", name = "Search", content = "__START_OF_SOURCE 1__ <URL> https://example.gov/profile/1 </URL> <CONTENT> A </CONTENT> __END_OF_SOURCE 1__")
    ),
    prior_field_status = list(),
    field_status = list(),
    prior_evidence_ledger = list(),
    evidence_ledger = list(),
    diagnostics = list()
  ))

  expect_equal(as.numeric(metrics$value_per_search_call), 0, tolerance = 1e-8)
  expect_equal(as.integer(metrics$diminishing_returns_streak), 2L)
})

test_that("retrieval metrics track duplicate-query and empty-round guardrails", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  build_metrics <- reticulate::py_get_attr(prod, "_build_retrieval_metrics")
  metrics <- reticulate::py_to_r(build_metrics(
    state = list(
      retrieval_metrics = list(
        duplicate_query_rounds = 0L,
        empty_round_streak = 0L
      ),
      expected_schema = list(name = "string"),
      orchestration_options = list(
        retrieval_controller = list(
          enabled = TRUE,
          mode = "enforce",
          dedupe_queries = TRUE,
          max_empty_round_streak = 2L
        )
      )
    ),
    search_queries = list("duplicate query", "duplicate query"),
    tool_messages = list(
      list(role = "tool", name = "Search", content = "No good DuckDuckGo search result was found")
    ),
    prior_field_status = list(),
    field_status = list(),
    prior_evidence_ledger = list(),
    evidence_ledger = list(),
    diagnostics = list()
  ))

  expect_equal(as.integer(metrics$last_round_query_dedupe_hits), 1L)
  expect_equal(as.integer(metrics$duplicate_query_rounds), 1L)
  expect_equal(as.integer(metrics$empty_rounds), 1L)
  expect_equal(as.integer(metrics$empty_round_streak), 1L)
  expect_true(isTRUE(metrics$last_round_empty))
})

test_that("diagnostics normalization keeps performance telemetry bucket", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  normalize_diag <- reticulate::py_get_attr(prod, "_normalize_diagnostics")
  diagnostics <- reticulate::py_to_r(normalize_diag(list(
    performance = list(
      tool_round_count = 2L,
      tool_round_elapsed_ms_total = 450L,
      provider_error_count = 1L,
      provider_error_last_message = "TimeoutError: provider timeout"
    )
  )))

  expect_true(is.list(diagnostics$performance))
  expect_equal(as.integer(diagnostics$performance$tool_round_count), 2L)
  expect_equal(as.integer(diagnostics$performance$tool_round_elapsed_ms_total), 450L)
  expect_equal(as.integer(diagnostics$performance$provider_error_count), 1L)
  expect_match(as.character(diagnostics$performance$provider_error_last_message), "TimeoutError")
})

test_that("quality gate blocks terminal short-circuit when budget remains", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  should_finalize <- reticulate::py_get_attr(prod, "_should_finalize_after_terminal")
  state <- list(
    messages = list(list(
      role = "assistant",
      content = "{\"education_level\":\"Unknown\",\"prior_occupation\":\"Unknown\",\"birth_year\":1982}"
    )),
    expected_schema = list(
      education_level = "string|Unknown",
      prior_occupation = "string|Unknown",
      birth_year = "integer|null|Unknown"
    ),
    field_status = list(
      education_level = list(status = "unknown", value = "Unknown", attempts = 2L),
      prior_occupation = list(status = "unknown", value = "Unknown", attempts = 2L),
      birth_year = list(status = "found", value = 1982L, attempts = 1L)
    ),
    budget_state = list(budget_exhausted = FALSE),
    finalization_policy = list(
      skip_finalize_if_terminal_valid = TRUE,
      quality_gate_enforce = TRUE,
      quality_gate_unknown_ratio_max = 0.50,
      quality_gate_min_resolvable_fields = 2L
    )
  )

  expect_true(isTRUE(reticulate::py_to_r(should_finalize(state))))
})

test_that("invoke_graph_safely materializes terminal payload metadata", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  reticulate::py_run_string(paste0(
    "class _ASAInvokeGraphStub:\n",
    "    def stream(self, initial_state, config=None, stream_mode='values'):\n",
    "        yield {\n",
    "            'messages': [{'role': 'assistant', 'content': '{\"birth_year\":1982}'}],\n",
    "            'expected_schema': {'birth_year': 'integer|null|Unknown'},\n",
    "            'finalization_policy': {'terminal_dedupe_mode': 'hash'}\n",
    "        }\n",
    "_asa_invoke_graph_stub = _ASAInvokeGraphStub()\n"
  ))

  invoke_safely <- reticulate::py_get_attr(core, "invoke_graph_safely")
  out <- reticulate::py_to_r(invoke_safely(
    graph = reticulate::py$`_asa_invoke_graph_stub`,
    initial_state = list()
  ))

  expect_true(is.list(out$final_payload))
  expect_equal(as.integer(out$final_payload$birth_year), 1982L)
  expect_true(isTRUE(out$terminal_valid))
  expect_true(nzchar(as.character(out$terminal_payload_hash)))
})
