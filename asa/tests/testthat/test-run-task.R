# Tests for run_task functions

test_that("build_prompt substitutes variables correctly", {
  template <- "Find information about {{name}} in {{country}}"
  result <- build_prompt(template, name = "Einstein", country = "Germany")

  expect_equal(result, "Find information about Einstein in Germany")
})

test_that("build_prompt handles missing variables gracefully", {
  template <- "Hello {{name}}, you are {{age}} years old"

  # Suppress expected warning about unsubstituted placeholder
  result <- suppressWarnings(build_prompt(template, name = "Alice"))

  expect_match(result, "Hello Alice")
  expect_match(result, "\\{\\{age\\}\\}")

  # Verify the warning is actually produced
  expect_warning(
    build_prompt(template, name = "Alice"),
    "Unsubstituted placeholders"
  )
})

test_that("build_prompt handles numeric values", {
  template <- "The year is {{year}}"
  result <- build_prompt(template, year = 2024)

  expect_equal(result, "The year is 2024")
})

test_that("build_prompt returns unchanged template with no args", {
  template <- "No variables here"
  result <- build_prompt(template)

  expect_equal(result, template)
})

test_that("build_prompt handles empty strings", {
  template <- "Name: {{name}}"
  result <- build_prompt(template, name = "")

  expect_equal(result, "Name: ")
})

# ============================================================================
# Prompt Temporal Augmentation Tests
# ============================================================================

test_that(".augment_prompt_temporal returns unchanged prompt for NULL temporal", {
  prompt <- "Find some information"
  result <- asa:::.augment_prompt_temporal(prompt, NULL)
  expect_equal(result, prompt)
})

test_that(".augment_prompt_temporal returns unchanged prompt when no dates specified", {
  prompt <- "Find some information"
  # Only time_filter, no date hints
  result <- asa:::.augment_prompt_temporal(prompt, list(time_filter = "y"))
  expect_equal(result, prompt)
})

test_that(".augment_prompt_temporal adds context for after date only", {
  prompt <- "Find companies"
  temporal <- list(after = "2020-01-01")
  result <- asa:::.augment_prompt_temporal(prompt, temporal, verbose = FALSE)

  expect_match(result, "Find companies")
  expect_match(result, "\\[Temporal context:")
  expect_match(result, "after 2020-01-01")
})

test_that(".augment_prompt_temporal shows message for date context", {
  prompt <- "Find companies"

  # Test after-only message
  expect_message(
    asa:::.augment_prompt_temporal(prompt, list(after = "2020-01-01"), verbose = TRUE),
    "Temporal context: focusing on results after 2020-01-01"
  )

  # Test before-only message
  expect_message(
    asa:::.augment_prompt_temporal(prompt, list(before = "2024-01-01"), verbose = TRUE),
    "Temporal context: focusing on results before 2024-01-01"
  )

  # Test date range message
  expect_message(
    asa:::.augment_prompt_temporal(
      prompt,
      list(after = "2020-01-01", before = "2024-01-01"),
      verbose = TRUE
    ),
    "Temporal context: focusing on 2020-01-01 to 2024-01-01"
  )
})

test_that(".augment_prompt_temporal adds context for before date only", {
  prompt <- "Find companies"
  temporal <- list(before = "2024-01-01")
  result <- asa:::.augment_prompt_temporal(prompt, temporal, verbose = FALSE)

  expect_match(result, "Find companies")
  expect_match(result, "\\[Temporal context:")
  expect_match(result, "before 2024-01-01")
})

test_that(".augment_prompt_temporal adds context for date range", {
  prompt <- "Find companies"
  temporal <- list(after = "2020-01-01", before = "2024-01-01")
  result <- asa:::.augment_prompt_temporal(prompt, temporal, verbose = FALSE)

  expect_match(result, "Find companies")
  expect_match(result, "\\[Temporal context:")
  expect_match(result, "between 2020-01-01 and 2024-01-01")
})

test_that(".augment_prompt_temporal preserves original prompt structure", {
  prompt <- "Line 1\nLine 2\nLine 3"
  temporal <- list(after = "2020-01-01")
  result <- asa:::.augment_prompt_temporal(prompt, temporal, verbose = FALSE)

  # Original content preserved
  expect_match(result, "Line 1\nLine 2\nLine 3")
  # Temporal context appended (not prepended)
  expect_true(grepl("Line 3.*\\[Temporal context:", result))
})

# ============================================================================
# run_task() output_format = "raw" Tests
# ============================================================================

test_that(".parse_json_response parses JSON arrays", {
  json_text <- '[{"name":"Ada","age":36},{"name":"Alan","age":41}]'
  parsed <- asa:::.parse_json_response(json_text)

  expect_true(is.data.frame(parsed))
  expect_equal(parsed$name[1], "Ada")
  expect_equal(parsed$age[2], 41)
})

test_that(".parse_json_response extracts JSON arrays from mixed output", {
  mixed <- 'Here is the data: [{"name":"Grace","age":85}] Thanks!'
  parsed <- asa:::.parse_json_response(mixed)

  expect_true(is.data.frame(parsed))
  expect_equal(parsed$name[1], "Grace")
})

test_that(".parse_json_response skips non-JSON brackets", {
  mixed <- "Note [bracketed text] then {\"name\":\"Ada\"}."
  parsed <- asa:::.parse_json_response(mixed)

  expect_equal(parsed$name, "Ada")
})

test_that(".parse_json_response preserves escaped quotes inside strings", {
  mixed <- "Preamble {\"note\":\"The \\\"Great\\\" Wall\"} epilogue"
  parsed <- asa:::.parse_json_response(mixed)

  expect_true(is.list(parsed))
  expect_equal(parsed$note, 'The "Great" Wall')
})

test_that("run_task accepts output_format = 'raw'", {
  # Test that "raw" is a valid output_format (validation passes)
  # The validation function should not throw for "raw"
  expect_silent(asa:::.validate_run_task("test", "raw", NULL, FALSE))
})

test_that(".has_invoke_exception_fallback detects model invoke fallback repairs", {
  expect_true(
    asa:::.has_invoke_exception_fallback(
      list(json_repair = list(list(repair_reason = "invoke_exception_fallback")))
    )
  )

  expect_false(
    asa:::.has_invoke_exception_fallback(
      list(json_repair = list(list(repair_reason = "schema_normalization")))
    )
  )
})

test_that("run_task validation accepts all output_format options", {
  # Mock validation should pass for all valid formats
  expect_silent(asa:::.validate_run_task("prompt", "text", NULL, FALSE))
  expect_silent(asa:::.validate_run_task("prompt", "json", NULL, FALSE))
  expect_silent(asa:::.validate_run_task("prompt", "raw", NULL, FALSE))
  expect_silent(asa:::.validate_run_task("prompt", c("field1", "field2"), NULL, FALSE))
})

test_that("run_task validation accepts allow_read_webpages", {
  expect_silent(
    asa:::.validate_run_task("prompt", "text", NULL, FALSE, allow_read_webpages = TRUE)
  )
  expect_error(
    asa:::.validate_run_task("prompt", "text", NULL, FALSE, allow_read_webpages = "yes"),
    "allow_read_webpages"
  )
})

test_that("run_task validation accepts webpage embedding options", {
  expect_silent(
    asa:::.validate_run_task(
      "prompt", "text", NULL, FALSE,
      webpage_relevance_mode = "auto",
      webpage_embedding_provider = "auto",
      webpage_embedding_model = "text-embedding-3-small"
    )
  )
  expect_error(
    asa:::.validate_run_task("prompt", "text", NULL, FALSE, webpage_relevance_mode = "bad"),
    "webpage_relevance_mode"
  )
  expect_error(
    asa:::.validate_run_task("prompt", "text", NULL, FALSE, webpage_embedding_provider = "bad"),
    "webpage_embedding_provider"
  )
})

test_that("run_task validation accepts recursion_limit", {
  expect_silent(asa:::.validate_run_task("prompt", "text", NULL, FALSE, recursion_limit = 50L))
  expect_silent(asa:::.validate_run_task("prompt", "text", NULL, FALSE, recursion_limit = 4L))
  expect_error(asa:::.validate_run_task("prompt", "text", NULL, FALSE, recursion_limit = 0L), "recursion_limit")
  expect_error(asa:::.validate_run_task("prompt", "text", NULL, FALSE, recursion_limit = 1L), "recursion_limit")
  expect_error(asa:::.validate_run_task("prompt", "text", NULL, FALSE, recursion_limit = 2L), "recursion_limit")
  expect_error(asa:::.validate_run_task("prompt", "text", NULL, FALSE, recursion_limit = 3L), "recursion_limit")
  expect_error(asa:::.validate_run_task("prompt", "text", NULL, FALSE, recursion_limit = 999L), "recursion_limit")
})

test_that("run_task validation accepts budget and field-status controls", {
  expect_silent(
    asa:::.validate_run_task(
      "prompt",
      "text",
      NULL,
      FALSE,
      field_status = list(name = list(status = "unknown")),
      budget_state = list(tool_calls_used = 1L, tool_calls_limit = 4L),
      search_budget_limit = 5L,
      unknown_after_searches = 2L,
      finalize_on_all_fields_resolved = TRUE
    )
  )
  expect_error(
    asa:::.validate_run_task("prompt", "text", NULL, FALSE, search_budget_limit = -1L),
    "search_budget_limit"
  )
  expect_error(
    asa:::.validate_run_task("prompt", "text", NULL, FALSE, unknown_after_searches = 0L),
    "unknown_after_searches"
  )
  expect_error(
    asa:::.validate_run_task("prompt", "text", NULL, FALSE, finalize_on_all_fields_resolved = "yes"),
    "finalize_on_all_fields_resolved"
  )
})

test_that("run_task validation accepts orchestration_options", {
  expect_silent(
    asa:::.validate_run_task(
      "prompt",
      "json",
      NULL,
      FALSE,
      orchestration_options = list(
        policy_version = "2026-02-20",
        retrieval_controller = list(enabled = TRUE, mode = "observe")
      )
    )
  )
  expect_error(
    asa:::.validate_run_task(
      "prompt",
      "json",
      NULL,
      FALSE,
      orchestration_options = "invalid"
    ),
    "orchestration_options"
  )
})

test_that("search_options stores orchestration override knobs", {
  search <- asa::search_options(
    finalize_when_all_unresolved_exhausted = FALSE,
    field_attempt_budget_mode = "soft_cap"
  )

  expect_false(isTRUE(search$finalize_when_all_unresolved_exhausted))
  expect_equal(as.character(search$field_attempt_budget_mode), "soft_cap")
})

test_that(".resolve_orchestration_options_for_run_task merges search overrides", {
  search <- asa::search_options(
    finalize_when_all_unresolved_exhausted = FALSE,
    field_attempt_budget_mode = "soft_cap"
  )

  merged <- asa:::.resolve_orchestration_options_for_run_task(
    orchestration_options = list(
      retrieval_controller = list(mode = "observe"),
      finalizer = list(strict_schema_finalize = TRUE)
    ),
    config_search = search
  )

  expect_true(is.list(merged))
  expect_equal(as.character(merged$retrieval_controller$mode), "observe")
  expect_true(isTRUE(merged$finalizer$strict_schema_finalize))
  expect_false(isTRUE(merged$finalizer$finalize_when_all_unresolved_exhausted))
  expect_equal(as.character(merged$field_resolver$field_attempt_budget_mode), "soft_cap")
})

test_that("run_task rejects non-asa_config config", {
  expect_error(
    run_task("test prompt", config = list(foo = "bar")),
    "asa_config object or NULL"
  )
})

test_that("effective recursion_limit precedence is explicit > config > defaults", {
  expect_equal(
    asa:::.resolve_effective_recursion_limit(
      recursion_limit = 12L,
      config = list(recursion_limit = 77L),
      use_memory_folding = TRUE
    ),
    12L
  )

  expect_equal(
    asa:::.resolve_effective_recursion_limit(
      recursion_limit = NULL,
      config = list(recursion_limit = 77L),
      use_memory_folding = FALSE
    ),
    77L
  )

  expect_equal(
    asa:::.resolve_effective_recursion_limit(
      recursion_limit = NULL,
      config = list(),
      use_memory_folding = TRUE
    ),
    ASA_RECURSION_LIMIT_FOLDING
  )

  expect_equal(
    asa:::.resolve_effective_recursion_limit(
      recursion_limit = NULL,
      config = list(),
      use_memory_folding = FALSE
    ),
    ASA_RECURSION_LIMIT_STANDARD
  )

  expect_error(
    asa:::.resolve_effective_recursion_limit(
      recursion_limit = NULL,
      config = list(recursion_limit = 0L),
      use_memory_folding = TRUE
    ),
    "agent\\$config\\$recursion_limit"
  )

  expect_error(
    asa:::.resolve_effective_recursion_limit(
      recursion_limit = NULL,
      config = list(recusion_limit = 17L),
      use_memory_folding = TRUE
    ),
    "recusion_limit"
  )
})

test_that("run_task_batch validation accepts 'raw' output_format", {
  expect_silent(
    asa:::.validate_run_task_batch(
      c("prompt1", "prompt2"), "raw", NULL, FALSE, 4L, TRUE
    )
  )
})

# ============================================================================
# run_task() Temporal Validation Tests
# ============================================================================

test_that("run_task rejects invalid temporal$time_filter", {
  expect_error(
    run_task("test prompt", temporal = list(time_filter = "invalid")),
    "time_filter"
  )
})

test_that("run_task rejects invalid temporal$after", {
  expect_error(
    run_task("test prompt", temporal = list(after = "not-a-date")),
    "after.*valid"
  )
})

test_that("run_task rejects invalid temporal$before", {
  expect_error(
    run_task("test prompt", temporal = list(before = "not-a-date")),
    "before.*valid"
  )
})

test_that("run_task rejects after >= before", {
  expect_error(
    run_task("test prompt", temporal = list(after = "2024-06-01", before = "2024-01-01")),
    "after < before"
  )
})

test_that("run_task warns when time_filter conflicts with date range", {
  # time_filter="y" (past year) conflicts with after="1990-01-01" (30+ years ago)
  expect_warning(
    asa:::.validate_temporal(
      list(time_filter = "y", after = "1990-01-01"),
      "temporal"
    ),
    "Temporal conflict"
  )

  # No warning when date range is within time_filter window
  expect_silent(
    asa:::.validate_temporal(
      list(time_filter = "y", after = as.character(Sys.Date() - 100)),
      "temporal"
    )
  )
})

# ============================================================================
# run_task_batch() Temporal Validation Tests
# ============================================================================

test_that("run_task_batch rejects invalid temporal", {
  expect_error(
    run_task_batch(c("a", "b"), temporal = list(time_filter = "invalid")),
    "time_filter"
  )
  expect_error(
    run_task_batch(c("a", "b"), temporal = list(after = "bad-date")),
    "after.*valid"
  )
})

test_that(".attach_result_aliases keeps top-level aliases synchronized with execution", {
  base <- asa_result(
    prompt = "p",
    message = "m",
    parsed = NULL,
    raw_output = "trace",
    trace_json = "{}",
    elapsed_time = 0.1,
    status = "success",
    execution = list(
      fold_stats = list(fold_count = 2L),
      status_code = 200L,
      final_payload = list(status = "ok"),
      terminal_valid = TRUE,
      terminal_payload_hash = "abc123",
      terminal_payload_source = "final_payload",
      payload_integrity = list(
        released_from = "final_payload",
        canonical_available = TRUE,
        canonical_matches_message = TRUE
      ),
      retrieval_metrics = list(dedupe_hits = 2L),
      tool_quality_events = list(list(is_off_target = TRUE)),
      candidate_resolution = list(selected_candidate = "alpha"),
      finalization_status = list(schema_valid = TRUE),
      orchestration_options = list(retrieval_controller = list(mode = "observe")),
      policy_version = "2026-02-20",
      artifact_status = list(trace = list(status = "written")),
      token_stats = list(tokens_used = 12L),
      plan = list(step = "x"),
      plan_history = list(),
      om_stats = list(enabled = TRUE),
      observations = list("obs"),
      reflections = list("refl"),
      action_steps = list(list(type = "ai")),
      action_ascii = "ascii",
      action_investigator_summary = c("summary"),
      action_overall = c("overall"),
      langgraph_step_timings = list(list(node = "agent")),
      phase_timings = list(
        total_minutes = 0.1,
        backend_invoke_minutes = 0.05
      )
    )
  )

  aliased <- asa:::.attach_result_aliases(base, include_raw_response = FALSE)
  expect_equal(aliased$fold_stats$fold_count, 2L)
  expect_equal(aliased$status_code, 200L)
  expect_true(is.list(aliased$final_payload))
  expect_true(isTRUE(aliased$terminal_valid))
  expect_equal(aliased$terminal_payload_hash, "abc123")
  expect_equal(aliased$terminal_payload_source, "final_payload")
  expect_true(is.list(aliased$payload_integrity))
  expect_equal(aliased$policy_version, "2026-02-20")
  expect_true(is.list(aliased$retrieval_metrics))
  expect_true(is.list(aliased$tool_quality_events))
  expect_true(is.list(aliased$candidate_resolution))
  expect_true(is.list(aliased$finalization_status))
  expect_true(is.list(aliased$orchestration_options))
  expect_true(is.list(aliased$artifact_status))
  expect_equal(aliased$token_stats$tokens_used, 12L)
  expect_equal(aliased$action_ascii, "ascii")
  expect_true(is.list(aliased$phase_timings))
  expect_equal(as.numeric(aliased$phase_timings$total_minutes), 0.1, tolerance = 1e-8)
  expect_false("raw_response" %in% names(aliased))
  expect_false("trace" %in% names(aliased))

  aliased_raw <- asa:::.attach_result_aliases(
    base,
    include_raw_response = TRUE,
    raw_response = list(ok = TRUE)
  )
  expect_true("raw_response" %in% names(aliased_raw))
  expect_true(is.list(aliased_raw$raw_response))
  expect_true(isTRUE(aliased_raw$raw_response$ok))
})

test_that(".build_result_artifact_status returns deterministic status map", {
  response <- list(
    message = "{\"ok\":true}",
    trace = "trace text",
    trace_json = "{\"trace\":1}",
    final_payload = list(ok = TRUE)
  )
  status <- asa:::.build_result_artifact_status(response)
  expect_true(is.list(status))
  expect_equal(status$message$status, "written")
  expect_equal(status$trace$status, "written")
  expect_equal(status$trace_json$status, "written")
  expect_equal(status$final_payload$status, "written")
  expect_true(is.character(status$final_payload$byte_hash))
  expect_equal(nchar(status$final_payload$byte_hash), 64L)
})

test_that(".build_payload_integrity reports byte-vs-semantic mismatch classes", {
  integrity <- asa:::.build_payload_integrity(
    released_text = "{\"b\":2,\"a\":1}",
    released_from = "message_text",
    final_payload_info = list(
      available = TRUE,
      payload_json = "{\"a\":1,\"b\":2}",
      terminal_payload_hash = "x"
    ),
    trace = "",
    trace_json = "",
    json_repair = list(),
    message_sanitized = FALSE
  )

  expect_true(is.list(integrity))
  expect_false(isTRUE(integrity$canonical_matches_message))
  expect_false(isTRUE(integrity$byte_hash_matches))
  expect_true(isTRUE(integrity$semantic_hash_matches))
  expect_equal(integrity$hash_mismatch_type, "byte_only")
})

test_that("run_task_batch data frame output preserves metadata and unions parsed fields", {
  result1 <- asa_result(
    prompt = "p1",
    message = "m1",
    parsed = list(a = 1L, b = "x"),
    raw_output = "trace-1",
    trace_json = "{\"run\":1}",
    elapsed_time = 0.25,
    status = "success",
    search_tier = "primp",
    execution = list(
      thread_id = "t1",
      stop_reason = "done",
      status_code = 200L,
      tool_calls_used = 1L,
      tool_calls_limit = 5L,
      tool_calls_remaining = 4L,
      fold_count = 0L,
      token_stats = list(
        tokens_used = 10L,
        input_tokens = 7L,
        output_tokens = 3L,
        fold_tokens = 0L,
        token_trace = list()
      )
    )
  )
  result1 <- asa:::.attach_result_aliases(result1)

  result2 <- asa_result(
    prompt = "p2",
    message = "m2",
    parsed = list(b = "y", c = 2L),
    raw_output = "trace-2",
    trace_json = "{\"run\":2}",
    elapsed_time = 0.5,
    status = "error",
    search_tier = "unknown",
    execution = list(
      thread_id = "t2",
      stop_reason = "recursion_limit",
      status_code = 100L,
      tool_calls_used = 5L,
      tool_calls_limit = 5L,
      tool_calls_remaining = 0L,
      fold_count = 1L,
      token_stats = list(
        tokens_used = 22L,
        input_tokens = 14L,
        output_tokens = 8L,
        fold_tokens = 0L,
        token_trace = list()
      )
    )
  )
  result2 <- asa:::.attach_result_aliases(result2)

  idx <- 0L
  testthat::local_mocked_bindings(
    run_task = function(...) {
      idx <<- idx + 1L
      list(result1, result2)[[idx]]
    },
    .package = "asa"
  )

  prompts <- data.frame(
    prompt = c("Prompt 1", "Prompt 2"),
    stringsAsFactors = FALSE
  )
  out <- run_task_batch(
    prompts = prompts,
    output_format = "json",
    parallel = FALSE,
    progress = FALSE,
    circuit_breaker = FALSE
  )

  expect_s3_class(out, "data.frame")
  expect_equal(out$response, c("m1", "m2"))
  expect_equal(out$status, c("success", "error"))
  expect_equal(out$thread_id, c("t1", "t2"))
  expect_equal(out$stop_reason, c("done", "recursion_limit"))
  expect_equal(out$status_code, c(200L, 100L))
  expect_equal(out$tool_calls_used, c(1L, 5L))
  expect_equal(out$tool_calls_limit, c(5L, 5L))
  expect_equal(out$tool_calls_remaining, c(4L, 0L))
  expect_equal(out$fold_count, c(0L, 1L))
  expect_equal(out$tokens_used, c(10L, 22L))

  # Union of parsed fields across all results (not first parsed row only)
  expect_true(all(c("a", "b", "c") %in% names(out)))
  expect_equal(out$a, c("1", NA_character_))
  expect_equal(out$b, c("x", "y"))
  expect_equal(out$c, c(NA_character_, "2"))

  # Rich per-row metadata and full result object are preserved.
  expect_true(all(c("parsed", "parsing_status", "execution", "token_stats", "asa_result") %in% names(out)))
  expect_true(is.list(out$parsed))
  expect_true(is.list(out$execution))
  expect_true(is.list(out$asa_result))
  expect_s3_class(out$asa_result[[1]], "asa_result")
  expect_equal(out$execution[[2]]$thread_id, "t2")

  results_attr <- attr(out, "asa_results")
  expect_true(is.list(results_attr))
  expect_length(results_attr, 2L)
})

# ============================================================================
# Integration Tests (Skip if no agent)
# ============================================================================

.skip_if_no_agent <- function() {
  if (!asa:::.is_initialized()) {
    skip("Agent not initialized - skipping integration test")
  }
}

test_that("configure_temporal errors when agent not initialized", {
  # This test only runs when agent is NOT initialized
  skip_if(asa:::.is_initialized(), "Agent is initialized")
  expect_error(configure_temporal("y"), "Agent not initialized")
})

test_that("configure_temporal sets and clears time filter", {
  .skip_if_no_agent()

  # Store original value
  # Note: tools[[2]] is the search tool by convention (matches helpers.R:789)
  search_tool <- asa:::asa_env$tools[[2]]
  original_filter <- tryCatch(
    search_tool$api_wrapper$time,
    error = function(e) "none"
  )

  # Set filter
  old_filter <- suppressMessages(configure_temporal("y"))
  expect_equal(search_tool$api_wrapper$time, "y")

  # Clear filter
  suppressMessages(configure_temporal(NULL))
  expect_equal(search_tool$api_wrapper$time, "none")

  # Restore original
  suppressMessages(configure_temporal(original_filter))
})

test_that("configure_temporal accepts all valid time filters", {
  .skip_if_no_agent()

  for (filter in c("d", "w", "m", "y")) {
    expect_silent(suppressMessages(configure_temporal(filter)))
  }
  # Clean up
  suppressMessages(configure_temporal(NULL))
})

test_that(".with_temporal restores original filter after execution", {
  .skip_if_no_agent()

  # Set a known initial state
  suppressMessages(configure_temporal("m"))

  # Note: tools[[2]] is the search tool by convention (matches helpers.R:789)
  search_tool <- asa:::asa_env$tools[[2]]
  original <- search_tool$api_wrapper$time

  # Run with different temporal filter
  result <- asa:::.with_temporal(list(time_filter = "y"), function() {
    # During execution, filter should be "y"
    expect_equal(search_tool$api_wrapper$time, "y")
    "done"
  })

  # After execution, filter should be restored
  expect_equal(search_tool$api_wrapper$time, original)
  expect_equal(result, "done")

  # Clean up
  suppressMessages(configure_temporal(NULL))
})

test_that(".with_temporal handles NULL temporal gracefully", {
  .skip_if_no_agent()

  result <- asa:::.with_temporal(NULL, function() {
    "executed"
  })
  expect_equal(result, "executed")
})

# ============================================================================
# Note: run_agent() and run_agent_batch() have been removed from the public API.
# Use run_task(..., output_format = "raw") instead for full trace access.
# ============================================================================
