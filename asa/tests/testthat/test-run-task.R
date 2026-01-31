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
  expect_error(asa:::.validate_run_task("prompt", "text", NULL, FALSE, recursion_limit = 0L), "recursion_limit")
  expect_error(asa:::.validate_run_task("prompt", "text", NULL, FALSE, recursion_limit = 999L), "recursion_limit")
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
