# Tests for error handling throughout the package

# Tests for initialize_agent error handling
test_that("initialize_agent validates backend parameter", {
  skip_on_ci()
  skip("Requires conda environment")

  expect_error(
    initialize_agent(backend = "invalid_backend"),
    "should be one of"
  )
})

test_that("initialize_agent warns about OpenRouter model format", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # OpenRouter models should be in provider/model format
  expect_warning(
    suppressMessages(initialize_agent(
      backend = "openrouter",
      model = "gpt-4",  # Missing provider/
      verbose = FALSE
    )),
    "provider/model-name"
  )

  reset_agent()
})

test_that("initialize_agent handles missing conda environment", {
  skip_on_ci()

  expect_error(
    initialize_agent(conda_env = "nonexistent_env_xyz123"),
    "not find"
  )
})

test_that("initialize_agent requires reticulate package", {
  # This would require mocking package availability
  skip("Requires package mocking")
})

# Tests for run_agent error handling
test_that("run_agent validates agent parameter", {
  fake_agent <- list(not_an_agent = TRUE)
  class(fake_agent) <- "wrong_class"

  expect_error(
    run_agent("test", agent = fake_agent),
    "asa_agent"
  )
})

test_that("run_agent errors when no agent initialized", {
  reset_agent()

  expect_error(
    run_agent("test prompt", agent = NULL),
    "not initialized"
  )
})

test_that("run_agent handles Python errors gracefully", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  # Would test with intentionally problematic input
  # Should return asa_response with status_code = 100
})

test_that("run_agent returns error response on failure", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  # Mock a failing agent call
  # Response should have status_code = 100, message = NA
})

# Tests for run_task error handling
test_that("run_task validates prompt parameter", {
  expect_error(
    run_task(prompt = NULL),
    "must be a single character string"
  )

  expect_error(
    run_task(prompt = c("multiple", "prompts")),
    "single character string"
  )

  expect_error(
    run_task(prompt = 123),
    "character"
  )

  expect_error(
    run_task(prompt = ""),  # Empty string might be allowed
    NA  # Or might error - depends on implementation
  )
})

test_that("run_task handles missing agent", {
  reset_agent()

  expect_error(
    run_task("test", agent = NULL),
    "not initialized"
  )
})

# Tests for build_prompt error handling
test_that("build_prompt validates template parameter", {
  expect_error(
    build_prompt(template = NULL, var = "value"),
    "non-empty character string"
  )

  expect_error(
    build_prompt(template = c("multiple", "templates")),
    "non-empty character string"
  )

  expect_error(
    build_prompt(template = ""),
    "non-empty"
  )
})

test_that("build_prompt warns about missing placeholders", {
  expect_warning(
    build_prompt(template = "No placeholders here", var = "value"),
    "not found"
  )
})

test_that("build_prompt warns about unsubstituted placeholders", {
  expect_warning(
    build_prompt(template = "Hello {{name}}, you are {{age}}"),
    "Unsubstituted"
  )

  expect_warning(
    build_prompt(template = "Hello {{name}}", age = 30),
    "not found"
  )
})

# Tests for batch operation error handling
test_that("run_task_batch requires future packages for parallel", {
  skip_if(requireNamespace("future", quietly = TRUE) &&
          requireNamespace("future.apply", quietly = TRUE),
          "future packages available")

  expect_error(
    run_task_batch(c("test"), parallel = TRUE),
    "future"
  )
})

test_that("run_agent_batch requires future packages for parallel", {
  skip_if(requireNamespace("future", quietly = TRUE) &&
          requireNamespace("future.apply", quietly = TRUE),
          "future packages available")

  expect_error(
    run_agent_batch(c("test"), parallel = TRUE),
    "future"
  )
})

# Tests for missing API keys
test_that("initialize_agent handles missing API keys", {
  skip_on_ci()
  skip("Manual test - requires removing API keys")

  # This would test behavior when OPENAI_API_KEY is not set
  # Might error during initialization or during first API call
})

# Tests for network failures
test_that("run_agent handles network timeouts", {
  skip_on_ci()
  skip("Requires live agent and network manipulation")

  # Would test with very short timeout
  # Should return error response, not crash
})

test_that("run_agent handles rate limiting", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  # Function should handle rate limit responses
  # .handle_response_issues should detect "Ratelimit" in trace
})

# Tests for invalid inputs
test_that("extract functions handle malformed input", {
  # These should not error, just return empty results
  expect_error(extract_search_snippets("garbage input"), NA)
  expect_error(extract_wikipedia_content("not valid trace"), NA)
  expect_error(extract_urls("random text"), NA)
  expect_error(extract_search_tiers("no tier info"), NA)
  expect_error(extract_agent_results("malformed"), NA)
})

test_that("process_outputs validates data frame structure", {
  df_wrong <- data.frame(wrong_column = 1:3)

  expect_error(
    process_outputs(df_wrong),
    "raw_output"
  )
})

test_that("process_outputs handles NA values", {
  df <- data.frame(
    raw_output = c(NA_character_, "valid", NA_character_),
    stringsAsFactors = FALSE
  )

  expect_error(process_outputs(df), NA)
  result <- process_outputs(df)
  expect_equal(nrow(result), 3)
})

# Tests for JSON parsing errors
test_that("safe_json_parse handles invalid JSON", {
  expect_null(safe_json_parse("not json"))
  expect_null(safe_json_parse("{invalid"))
  expect_null(safe_json_parse(""))
  expect_null(safe_json_parse(NULL))
  expect_null(safe_json_parse(NA))
})

test_that("safe_json_parse handles valid JSON", {
  result <- safe_json_parse('{"key": "value"}')
  expect_type(result, "list")
  expect_equal(result$key, "value")
})

# Tests for S3 method errors
test_that("S3 constructors handle missing parameters", {
  # asa_agent with NULL values
  agent <- asa_agent(NULL, "openai", "gpt-4", list())
  expect_s3_class(agent, "asa_agent")

  # asa_response with minimal params
  response <- asa_response(NA_character_, 100L, NULL, "", 0, 0L, "")
  expect_s3_class(response, "asa_response")

  # asa_result with NULL parsed
  result <- asa_result("", "", NULL, "", 0, "error")
  expect_s3_class(result, "asa_result")
})

test_that("as.data.frame.asa_result handles NULL parsed", {
  result <- asa_result("prompt", "message", NULL, "", 1.0, "success")
  df <- as.data.frame(result)

  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 1)
})

test_that("as.data.frame.asa_result handles list parsed with non-scalar values", {
  result <- asa_result(
    "prompt",
    "message",
    parsed = list(scalar = "value", vector = c(1, 2, 3)),
    "",
    1.0,
    "success"
  )

  df <- as.data.frame(result)

  # Vector values should be excluded or handled specially
  expect_s3_class(df, "data.frame")
  expect_true("scalar" %in% names(df))
})

# Tests for internal helper errors
test_that(".extract_json_from_trace handles malformed JSON", {
  # Test via extract_agent_results
  trace <- "AIMessage(content='{invalid json}', additional_kwargs={})"
  result <- extract_agent_results(trace)

  # Should return NULL for json_data, not error
  expect_null(result$json_data)
})

test_that(".extract_json_object handles edge cases", {
  # Test via .parse_json_response (internal)
  # These are tested indirectly through run_task
})

# Tests for memory folding errors
test_that("initialize_agent accepts memory folding parameters", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  expect_error(
    initialize_agent(
      backend = "openai",
      model = "gpt-4.1-mini",
      use_memory_folding = TRUE,
      memory_threshold = 10L,
      memory_keep_recent = 5L,
      verbose = FALSE
    ),
    NA
  )

  reset_agent()
})

# Tests for timeout handling
test_that("run_agent handles timeout in trace", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  # .handle_response_issues should detect "Timeout" in trace
  # and wait before continuing
})

# Tests for empty/NULL handling
test_that("functions handle empty strings gracefully", {
  expect_equal(json_escape(""), "")
  expect_equal(clean_whitespace(""), "")
  expect_equal(truncate_string("", 10), "")
})

test_that("functions handle NULL values gracefully", {
  expect_equal(json_escape(NULL), "")
  expect_null(decode_html(NULL))
  expect_null(clean_whitespace(NULL))
  expect_null(truncate_string(NULL, 10))
  expect_equal(format_duration(NULL), "N/A")
})

# Tests for resource cleanup
test_that("reset_agent cleans up resources", {
  # reset_agent should close HTTP clients
  # Tested indirectly - should not leak resources
  reset_agent()
  expect_null(get_agent())
})

# Edge cases
test_that("run_task handles very long prompts", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  long_prompt <- paste(rep("word", 1000), collapse = " ")

  # Should handle without error (might hit token limits)
  # but should return a response, not crash
})

test_that("batch operations handle empty input", {
  expect_error(run_task_batch(character(0)), NA)
  expect_error(run_agent_batch(character(0)), NA)
})

test_that("process_outputs handles empty dataframe", {
  df <- data.frame(raw_output = character(0), stringsAsFactors = FALSE)
  result <- process_outputs(df)

  expect_equal(nrow(result), 0)
  expect_true("search_count" %in% names(result))
})
