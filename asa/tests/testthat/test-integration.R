# Integration tests for end-to-end workflows
# These tests require a working conda environment and API keys

test_that("complete workflow: initialize -> run_task -> extract results", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Initialize agent
  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    use_memory_folding = FALSE,
    verbose = FALSE
  )

  expect_s3_class(agent, "asa_agent")

  # Run a simple task
  result <- run_task(
    prompt = "What is 2+2? Return just the number.",
    output_format = "text",
    agent = agent,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")
  expect_equal(result$status, "success")
  expect_true(!is.na(result$message))

  # Extract results from trace
  extracted <- extract_agent_results(result$raw_output)
  expect_type(extracted, "list")

  reset_agent()
})

test_that("workflow with JSON output format", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  result <- run_task(
    prompt = 'Return JSON: {"answer": "42", "question": "life"}',
    output_format = "json",
    agent = agent,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")

  # Should have parsed JSON
  if (!is.null(result$parsed)) {
    expect_type(result$parsed, "list")
  }

  reset_agent()
})

test_that("workflow with field extraction", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  result <- run_task(
    prompt = 'Return JSON with fields: {"name": "Test", "value": "123"}',
    output_format = c("name", "value"),
    agent = agent,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")

  if (!is.null(result$parsed)) {
    expect_type(result$parsed, "list")
    expect_true("name" %in% names(result$parsed) ||
                "value" %in% names(result$parsed))
  }

  reset_agent()
})

test_that("multi-step workflow with conversation", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    use_memory_folding = TRUE,
    memory_threshold = 4L,
    verbose = FALSE
  )

  # Run multiple queries
  result1 <- run_agent("What is the capital of France?", agent, verbose = FALSE)
  expect_s3_class(result1, "asa_response")

  result2 <- run_agent("What is 5 + 7?", agent, verbose = FALSE)
  expect_s3_class(result2, "asa_response")

  # Both should succeed
  expect_equal(result1$status_code, 200L)
  expect_equal(result2$status_code, 200L)

  reset_agent()
})

test_that("workflow with search results extraction", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  # Ask a question that requires web search
  result <- run_task(
    prompt = "What is the current population of Tokyo?",
    agent = agent,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")

  # Extract search results from trace
  extracted <- extract_agent_results(result$raw_output)

  # Should have used search
  expect_true(length(extracted$search_snippets) > 0 ||
              length(extracted$wikipedia_snippets) > 0)

  # Should have search tiers
  if (length(extracted$search_snippets) > 0) {
    expect_true(length(extracted$search_tiers) > 0)
  }

  reset_agent()
})

test_that("batch workflow with multiple queries", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  prompts <- c(
    "What is 3+4?",
    "What is 8-2?",
    "What is 5*2?"
  )

  results <- run_task_batch(
    prompts,
    agent = agent,
    progress = FALSE
  )

  expect_type(results, "list")
  expect_length(results, 3)
  expect_true(all(sapply(results, function(r) inherits(r, "asa_result"))))

  # All should be successful
  statuses <- sapply(results, function(r) r$status)
  expect_true(all(statuses == "success"))

  reset_agent()
})

test_that("workflow with different backends", {
  skip_on_ci()
  skip("Requires conda environment and multiple API keys")

  backends <- c("openai", "groq")

  for (backend in backends) {
    model <- if (backend == "openai") "gpt-4.1-mini" else "llama-3.3-70b-versatile"

    agent <- initialize_agent(
      backend = backend,
      model = model,
      verbose = FALSE
    )

    result <- run_task(
      prompt = "What is 2+2?",
      agent = agent,
      verbose = FALSE
    )

    expect_s3_class(result, "asa_result")
    expect_equal(result$status, "success")

    reset_agent()
  }
})

test_that("workflow with template prompts", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  template <- "What is {{num1}} + {{num2}}? Return just the number."

  prompt <- build_prompt(template, num1 = 5, num2 = 7)

  result <- run_task(
    prompt = prompt,
    agent = agent,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")
  expect_equal(result$status, "success")

  reset_agent()
})

test_that("workflow with memory folding triggers", {
  skip_on_ci()
  skip("Requires conda environment and API keys - long running")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    use_memory_folding = TRUE,
    memory_threshold = 3L,  # Low threshold to trigger folding
    memory_keep_recent = 1L,
    verbose = FALSE
  )

  # Send enough messages to trigger folding
  for (i in 1:5) {
    result <- run_agent(
      paste("Question", i, ": What is", i, "+ 1?"),
      agent = agent,
      verbose = FALSE
    )

    expect_s3_class(result, "asa_response")
  }

  # At least one should have triggered folding
  # (This is hard to test directly without inspecting fold_count)

  reset_agent()
})

test_that("workflow with Tor proxy", {
  skip_on_ci()
  skip_if_not(is_tor_running(), "Tor not running")
  skip("Requires conda environment, API keys, and Tor")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    proxy = "socks5h://127.0.0.1:9050",
    verbose = FALSE
  )

  result <- run_task(
    prompt = "What is the capital of Germany?",
    agent = agent,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")
  expect_equal(result$status, "success")

  reset_agent()
})

test_that("workflow: configure -> initialize -> run", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Configure search parameters
  suppressMessages(configure_search(max_results = 15L, timeout = 30L))

  # Configure logging
  suppressMessages(configure_search_logging("WARNING"))

  # Initialize agent
  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  # Run task
  result <- run_task(
    prompt = "What is AI?",
    agent = agent,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")

  reset_agent()
})

test_that("workflow with process_outputs on batch results", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  df <- data.frame(
    id = 1:3,
    prompt = c("What is AI?", "What is ML?", "What is NLP?"),
    stringsAsFactors = FALSE
  )

  # Run batch
  result_df <- run_task_batch(df, agent = agent, progress = FALSE)

  # Process outputs (if raw_output available)
  # Note: run_task_batch returns parsed results, not raw_output by default
  # This is more of a conceptual test

  expect_s3_class(result_df, "data.frame")
  expect_equal(nrow(result_df), 3)
  expect_true("response" %in% names(result_df))

  reset_agent()
})

test_that("workflow: save and reload agent", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Initialize
  agent1 <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  # Get the same agent
  agent2 <- get_agent()

  expect_equal(agent1$backend, agent2$backend)
  expect_equal(agent1$model, agent2$model)

  # Both should work
  result1 <- run_task("Test 1", agent = agent1, verbose = FALSE)
  result2 <- run_task("Test 2", agent = agent2, verbose = FALSE)

  expect_s3_class(result1, "asa_result")
  expect_s3_class(result2, "asa_result")

  reset_agent()
})

test_that("workflow with result conversion to data frame", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  result <- run_task(
    prompt = 'Return JSON: {"name": "Test", "value": 42}',
    output_format = "json",
    agent = agent,
    verbose = FALSE
  )

  # Convert to data frame
  df <- as.data.frame(result)

  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 1)
  expect_true("prompt" %in% names(df))
  expect_true("message" %in% names(df))
  expect_true("status" %in% names(df))

  reset_agent()
})

test_that("complete pipeline with all extraction functions", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  # Query that should use both search and wikipedia
  result <- run_task(
    prompt = "Tell me about Albert Einstein's contributions to physics",
    agent = agent,
    verbose = FALSE
  )

  trace <- result$raw_output

  # Extract all types of information
  snippets <- extract_search_snippets(trace)
  urls <- extract_urls(trace)
  wiki <- extract_wikipedia_content(trace)
  tiers <- extract_search_tiers(trace)
  all_results <- extract_agent_results(trace)

  # Should have gotten some information
  expect_true(
    length(snippets) > 0 ||
    length(wiki) > 0 ||
    length(urls) > 0
  )

  # Combined extraction should work
  expect_type(all_results, "list")
  expect_named(all_results, c("search_snippets", "search_urls",
                              "wikipedia_snippets", "json_data", "search_tiers"))

  reset_agent()
})

test_that("error recovery workflow", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Initialize
  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  # Run a potentially problematic query
  result1 <- run_task("", agent = agent, verbose = FALSE)

  # Agent should still work after error
  result2 <- run_task("What is 2+2?", agent = agent, verbose = FALSE)

  expect_s3_class(result2, "asa_result")

  reset_agent()
})
