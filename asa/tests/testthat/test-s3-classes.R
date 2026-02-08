# Tests for S3 class constructors

test_that("asa_agent constructor creates correct object", {
  agent <- asa_test_mock_agent()

  expect_s3_class(agent, "asa_agent")
  expect_equal(agent$backend, "openai")
  expect_equal(agent$model, "gpt-4")
  expect_true(agent$config$use_memory_folding)
  expect_true(!is.null(agent$created_at))
})

test_that("asa_response constructor creates correct object", {
  response <- asa_response(
    message = "Test response",
    status_code = 200L,
    raw_response = NULL,
    trace = "trace text",
    elapsed_time = 1.5,
    fold_stats = list(fold_count = 2L),
    prompt = "Test prompt"
  )

  expect_s3_class(response, "asa_response")
  expect_equal(response$message, "Test response")
  expect_equal(response$status_code, 200L)
  expect_equal(response$elapsed_time, 1.5)
  expect_equal(response$fold_stats$fold_count, 2L)
  expect_true(inherits(response$created_at, "POSIXct"))
  expect_true(is.na(response$tokens_used))
})

test_that("asa_response stores tokens_used when provided", {
  response <- asa_response(
    message = "Test response",
    status_code = 200L,
    raw_response = NULL,
    trace = "trace text",
    elapsed_time = 1.5,
    prompt = "Test prompt",
    tokens_used = 1500L
  )

  expect_equal(response$tokens_used, 1500L)
})

test_that("asa_response stores fold_stats correctly", {
  stats <- list(
    fold_count = 2L,
    fold_messages_removed = 5L,
    fold_total_messages_removed = 8L,
    fold_chars_input = 1200L,
    fold_summary_chars = 350L,
    fold_trigger_reason = "char_budget",
    fold_safe_boundary_idx = 6L,
    fold_compression_ratio = 0.29,
    fold_parse_success = TRUE,
    fold_summarizer_latency_m = 0.002
  )
  response <- asa_response(
    message = "Test response",
    status_code = 200L,
    raw_response = NULL,
    trace = "trace text",
    elapsed_time = 1.5,
    fold_stats = stats,
    prompt = "Test prompt"
  )

  expect_equal(response$fold_stats, stats)
  expect_equal(response$fold_stats$fold_count, 2L)
  expect_equal(response$fold_stats$fold_messages_removed, 5L)
  expect_equal(response$fold_stats$fold_total_messages_removed, 8L)
  expect_equal(response$fold_stats$fold_chars_input, 1200L)
  expect_equal(response$fold_stats$fold_summary_chars, 350L)
  expect_equal(response$fold_stats$fold_trigger_reason, "char_budget")
  expect_equal(response$fold_stats$fold_safe_boundary_idx, 6L)
  expect_equal(response$fold_stats$fold_compression_ratio, 0.29)
  expect_equal(response$fold_stats$fold_parse_success, TRUE)
  expect_equal(response$fold_stats$fold_summarizer_latency_m, 0.002)
})

test_that("asa_response defaults fold_stats to empty list", {
  response <- asa_response(
    message = "Test response",
    status_code = 200L,
    raw_response = NULL,
    trace = "trace text",
    elapsed_time = 1.5,
    prompt = "Test prompt"
  )

  expect_equal(response$fold_stats, list())
})

test_that("asa_result constructor creates correct object", {
  result <- asa_result(
    prompt = "Test prompt",
    message = "Test response message",
    parsed = list(field1 = "value1", field2 = 42),
    raw_output = "raw trace",
    elapsed_time = 2.0,
    status = "success"
  )

  expect_s3_class(result, "asa_result")
  expect_equal(result$prompt, "Test prompt")
  expect_equal(result$message, "Test response message")
  expect_equal(result$status, "success")
  expect_equal(result$parsed$field1, "value1")
  expect_equal(result$parsed$field2, 42)
  expect_true(inherits(result$created_at, "POSIXct"))
})

test_that("as.data.frame.asa_result works", {
  result <- asa_result(
    prompt = "Test query",
    message = "Test answer",
    parsed = list(name = "John", age = "30"),
    raw_output = "trace",
    elapsed_time = 1.0,
    status = "success"
  )

  df <- as.data.frame(result)

  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 1)
  expect_equal(df$prompt, "Test query")
  expect_equal(df$status, "success")
  expect_equal(df$name, "John")
  expect_equal(df$age, "30")
})

test_that("asa_result stores execution metadata", {
  result <- asa_result(
    prompt = "Test query",
    message = "Test answer",
    parsed = NULL,
    raw_output = "trace",
    elapsed_time = 1.0,
    status = "success",
    execution = list(
      thread_id = "thread-123",
      stop_reason = "recursion_limit",
      status_code = 200L,
      tool_calls_used = 2L,
      tool_calls_limit = 5L,
      tool_calls_remaining = 3L,
      fold_count = 1L,
      tokens_used = 2500L
    )
  )

  expect_equal(result$execution$thread_id, "thread-123")
  expect_equal(result$execution$tool_calls_used, 2L)
  expect_equal(result$execution$tokens_used, 2500L)

  df <- as.data.frame(result)
  expect_equal(df$thread_id, "thread-123")
  expect_equal(df$stop_reason, "recursion_limit")
  expect_equal(df$tool_calls_limit, 5L)
  expect_equal(df$fold_count, 1L)
  expect_equal(df$tokens_used, 2500L)
})

test_that("asa_config defaults and printing are consistent", {
  cfg <- asa_config()
  expect_s3_class(cfg, "asa_config")
  expect_true(isTRUE(is.na(cfg$proxy)))
  expect_null(cfg$recursion_limit)
  output <- capture.output(print(cfg))
  expect_true(any(grepl("^Proxy:", output)))
  expect_true(any(grepl("Auto", output)))
  expect_true(any(grepl("^Recursion Limit:", output)))

  cfg_off <- asa_config(proxy = NULL)
  output_off <- capture.output(print(cfg_off))
  expect_true(any(grepl("^Proxy:.*None", output_off)))

  cfg_rl <- asa_config(recursion_limit = 42L)
  expect_equal(cfg_rl$recursion_limit, 42L)
  output_rl <- capture.output(print(cfg_rl))
  expect_true(any(grepl("^Recursion Limit:.*42", output_rl)))
})

test_that("print methods produce expected headers", {
  agent <- asa_test_mock_agent(config = list())
  expect_output(print(agent), "ASA Search Agent")

  response <- asa_response("msg", 200L, NULL, "", 1.0, "prompt")
  expect_output(print(response), "ASA Agent Response")

  result <- asa_result("prompt", "message", NULL, "", 1.0, "success")
  expect_output(print(result), "ASA Task Result")
})

test_that("print.asa_response shows tokens when present", {
  response <- asa_response(
    message = "Test", status_code = 200L, raw_response = NULL,
    trace = "", elapsed_time = 1.0, prompt = "p", tokens_used = 1234L
  )
  output <- capture.output(print(response))
  expect_true(any(grepl("Tokens:", output)))
  expect_true(any(grepl("1234", output)))
})

test_that("print.asa_result shows tokens when present", {
  result <- asa_result(
    prompt = "p", message = "m", parsed = NULL, raw_output = "",
    elapsed_time = 1.0, status = "success",
    execution = list(tokens_used = 5678L)
  )
  output <- capture.output(print(result))
  expect_true(any(grepl("Tokens:", output)))
  expect_true(any(grepl("5678", output)))
})
