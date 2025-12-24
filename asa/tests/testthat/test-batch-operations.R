# Tests for batch processing operations

# Mock agent for testing
create_mock_agent <- function() {
  asa_agent(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4.1-mini",
    config = list(
      backend = "openai",
      model = "gpt-4.1-mini",
      use_memory_folding = TRUE,
      memory_threshold = 4L,
      memory_keep_recent = 2L
    )
  )
}

# Tests for run_task_batch
test_that("run_task_batch validates inputs", {
  skip_on_ci()  # Requires initialized agent

  # Should error on data frame without 'prompt' column
  df_no_prompt <- data.frame(query = c("test1", "test2"))
  expect_error(
    run_task_batch(df_no_prompt, agent = create_mock_agent()),
    "prompt"
  )
})

test_that("run_task_batch handles character vector input", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  prompts <- c("What is 2+2?", "What is 3+3?")
  results <- run_task_batch(prompts, agent = create_mock_agent(), progress = FALSE)

  expect_type(results, "list")
  expect_length(results, 2)
  expect_s3_class(results[[1]], "asa_result")
  expect_s3_class(results[[2]], "asa_result")
})

test_that("run_task_batch handles data frame input", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  df <- data.frame(
    id = 1:2,
    prompt = c("What is 2+2?", "What is 3+3?"),
    stringsAsFactors = FALSE
  )

  result_df <- run_task_batch(df, agent = create_mock_agent(), progress = FALSE)

  expect_s3_class(result_df, "data.frame")
  expect_equal(nrow(result_df), 2)
  expect_true("response" %in% names(result_df))
  expect_true("status" %in% names(result_df))
  expect_true("elapsed_time" %in% names(result_df))
  expect_true("id" %in% names(result_df))  # Original column preserved
})

test_that("run_task_batch respects output_format parameter", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  prompts <- c("Simple query")
  results <- run_task_batch(
    prompts,
    output_format = "text",
    agent = create_mock_agent(),
    progress = FALSE
  )

  expect_type(results, "list")
  expect_length(results, 1)
})

test_that("run_task_batch parallel parameter requires packages", {
  skip_on_ci()

  # If future/future.apply not available, should error
  skip_if(requireNamespace("future", quietly = TRUE) &&
          requireNamespace("future.apply", quietly = TRUE),
          "future packages are available")

  prompts <- c("test")
  expect_error(
    run_task_batch(prompts, parallel = TRUE, agent = create_mock_agent()),
    "future"
  )
})

test_that("run_task_batch processes empty vector", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  prompts <- character(0)
  results <- run_task_batch(prompts, agent = create_mock_agent(), progress = FALSE)

  expect_type(results, "list")
  expect_length(results, 0)
})

test_that("run_task_batch adds parsed fields to data frame", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  df <- data.frame(
    prompt = "Return JSON with field 'answer': '42'",
    stringsAsFactors = FALSE
  )

  result_df <- run_task_batch(
    df,
    output_format = "json",
    agent = create_mock_agent(),
    progress = FALSE
  )

  expect_s3_class(result_df, "data.frame")
  # Would have parsed fields if JSON parsing succeeded
})

# Tests for run_agent_batch
test_that("run_agent_batch validates inputs", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  prompts <- c("Test prompt 1", "Test prompt 2")
  results <- run_agent_batch(prompts, agent = create_mock_agent(), progress = FALSE)

  expect_type(results, "list")
  expect_length(results, 2)
  expect_s3_class(results[[1]], "asa_response")
  expect_s3_class(results[[2]], "asa_response")
})

test_that("run_agent_batch handles single prompt", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  prompts <- c("Single test prompt")
  results <- run_agent_batch(prompts, agent = create_mock_agent(), progress = FALSE)

  expect_type(results, "list")
  expect_length(results, 1)
  expect_s3_class(results[[1]], "asa_response")
})

test_that("run_agent_batch parallel execution works", {
  skip_on_ci()
  skip("Requires live agent - integration test")
  skip_if_not(requireNamespace("future", quietly = TRUE),
              "future package not available")
  skip_if_not(requireNamespace("future.apply", quietly = TRUE),
              "future.apply package not available")

  prompts <- c("Query 1", "Query 2", "Query 3")
  results <- run_agent_batch(
    prompts,
    agent = create_mock_agent(),
    parallel = TRUE,
    workers = 2,
    progress = FALSE
  )

  expect_type(results, "list")
  expect_length(results, 3)
  expect_true(all(sapply(results, function(r) inherits(r, "asa_response"))))
})

test_that("run_agent_batch sequential execution works", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  prompts <- c("Query 1", "Query 2")
  results <- run_agent_batch(
    prompts,
    agent = create_mock_agent(),
    parallel = FALSE,
    progress = FALSE
  )

  expect_type(results, "list")
  expect_length(results, 2)
})

test_that("run_agent_batch handles errors gracefully", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  # Even with potentially problematic prompts, should return results
  prompts <- c("", "valid prompt", "")
  results <- run_agent_batch(prompts, agent = create_mock_agent(), progress = FALSE)

  expect_type(results, "list")
  expect_length(results, 3)
})

test_that("run_agent_batch respects workers parameter", {
  skip_on_ci()
  skip("Requires live agent - integration test")
  skip_if_not(requireNamespace("future", quietly = TRUE),
              "future package not available")
  skip_if_not(requireNamespace("future.apply", quietly = TRUE),
              "future.apply package not available")

  prompts <- rep("test", 5)

  # Should not error with different worker counts
  expect_error(
    run_agent_batch(prompts, parallel = TRUE, workers = 1, agent = create_mock_agent(), progress = FALSE),
    NA
  )

  expect_error(
    run_agent_batch(prompts, parallel = TRUE, workers = 4, agent = create_mock_agent(), progress = FALSE),
    NA
  )
})

test_that("run_task_batch preserves column order in data frame", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  df <- data.frame(
    id = 1:2,
    category = c("A", "B"),
    prompt = c("Test 1", "Test 2"),
    stringsAsFactors = FALSE
  )

  original_cols <- names(df)
  result_df <- run_task_batch(df, agent = create_mock_agent(), progress = FALSE)

  # Original columns should still be present
  expect_true(all(original_cols %in% names(result_df)))
})

test_that("batch operations handle empty results", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  prompts <- character(0)

  results_agent <- run_agent_batch(prompts, agent = create_mock_agent(), progress = FALSE)
  expect_length(results_agent, 0)

  results_task <- run_task_batch(prompts, agent = create_mock_agent(), progress = FALSE)
  expect_length(results_task, 0)
})

# Performance and progress tests
test_that("run_task_batch progress messages work", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  prompts <- c("Test 1", "Test 2")

  # With progress = TRUE, should see output
  expect_output(
    run_task_batch(prompts, agent = create_mock_agent(), progress = TRUE),
    "\\[1/2\\]"
  )
})

test_that("run_agent_batch progress messages work", {
  skip_on_ci()
  skip("Requires live agent - integration test")

  prompts <- c("Test 1", "Test 2")

  # With progress = TRUE, should see output
  expect_output(
    run_agent_batch(prompts, agent = create_mock_agent(), progress = TRUE),
    "\\[1/2\\]"
  )
})

# Parallel execution cleanup test
test_that("batch operations clean up parallel workers", {
  skip_on_ci()
  skip("Requires live agent - integration test")
  skip_if_not(requireNamespace("future", quietly = TRUE),
              "future package not available")

  prompts <- c("Test")

  # Run with parallel
  run_task_batch(prompts, parallel = TRUE, workers = 2, agent = create_mock_agent(), progress = FALSE)

  # Future plan should be reset to sequential after execution
  # This is handled by on.exit in the functions
})
