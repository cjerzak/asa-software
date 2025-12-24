# Tests for S3 class constructors

test_that("asa_agent constructor creates correct object", {
  agent <- asa_agent(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4",
    config = list(use_memory_folding = TRUE)
  )

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
    fold_count = 2L,
    prompt = "Test prompt"
  )

  expect_s3_class(response, "asa_response")
  expect_equal(response$message, "Test response")
  expect_equal(response$status_code, 200L)
  expect_equal(response$elapsed_time, 1.5)
  expect_equal(response$fold_count, 2L)
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

test_that("print methods don't error", {
  agent <- asa_agent(NULL, "openai", "gpt-4", list())
  expect_output(print(agent), "ASA Search Agent")

  response <- asa_response("msg", 200L, NULL, "", 1.0, 0L, "prompt")
  expect_output(print(response), "ASA Agent Response")

  result <- asa_result("prompt", "message", NULL, "", 1.0, "success")
  expect_output(print(result), "ASA Task Result")
})

# Tests for summary methods
test_that("summary.asa_agent works correctly", {
  agent <- asa_agent(
    python_agent = NULL,
    backend = "groq",
    model = "llama-3.3-70b-versatile",
    config = list(
      backend = "groq",
      model = "llama-3.3-70b-versatile",
      use_memory_folding = TRUE,
      memory_threshold = 5L,
      proxy = NULL
    )
  )

  expect_output(summary(agent), "ASA Agent Summary")

  result <- summary(agent)
  expect_type(result, "list")
  expect_equal(result$backend, "groq")
  expect_equal(result$model, "llama-3.3-70b-versatile")
  expect_type(result$config, "list")
})

test_that("summary.asa_response works correctly", {
  response <- asa_response(
    message = "Test response message",
    status_code = 200L,
    raw_response = NULL,
    trace = "Full trace text here",
    elapsed_time = 2.5,
    fold_count = 3L,
    prompt = "Test prompt"
  )

  expect_output(summary(response), "ASA Agent Response")

  result <- summary(response)
  expect_type(result, "list")
  expect_equal(result$status_code, 200L)
  expect_equal(result$elapsed_time, 2.5)
  expect_type(result$message_length, "integer")
  expect_type(result$trace_length, "integer")
})

test_that("summary.asa_response with show_trace parameter", {
  response <- asa_response(
    message = "Message",
    status_code = 200L,
    raw_response = NULL,
    trace = "Trace content",
    elapsed_time = 1.0,
    fold_count = 0L,
    prompt = "Prompt"
  )

  # Without trace
  expect_output(summary(response, show_trace = FALSE), "ASA Agent Response")
  expect_output(summary(response, show_trace = FALSE), "Trace", negate = TRUE)

  # With trace
  expect_output(summary(response, show_trace = TRUE), "Full Trace")
  expect_output(summary(response, show_trace = TRUE), "Trace content")
})

test_that("summary.asa_result works correctly", {
  result <- asa_result(
    prompt = "Test query",
    message = "Test answer",
    parsed = list(field1 = "value1", field2 = "value2"),
    raw_output = "raw trace",
    elapsed_time = 1.5,
    status = "success"
  )

  expect_output(summary(result), "Task Result Summary")

  summary_result <- summary(result)
  expect_type(summary_result, "list")
  expect_equal(summary_result$status, "success")
  expect_equal(summary_result$elapsed_time, 1.5)
  expect_type(summary_result$message_length, "integer")
  expect_equal(summary_result$parsed_fields, c("field1", "field2"))
})

test_that("summary methods return invisible results", {
  agent <- asa_agent(NULL, "openai", "gpt-4", list())
  result <- summary(agent)
  expect_type(result, "list")

  response <- asa_response("msg", 200L, NULL, "", 1.0, 0L, "prompt")
  result <- summary(response)
  expect_type(result, "list")

  task_result <- asa_result("p", "m", NULL, "", 1.0, "success")
  result <- summary(task_result)
  expect_type(result, "list")
})

test_that("summary.asa_response handles empty trace", {
  response <- asa_response(
    message = "Message",
    status_code = 200L,
    raw_response = NULL,
    trace = "",
    elapsed_time = 1.0,
    fold_count = 0L,
    prompt = "Prompt"
  )

  result <- summary(response)
  expect_equal(result$trace_length, 0)
})

test_that("summary.asa_result handles NULL parsed", {
  result_obj <- asa_result(
    prompt = "Test",
    message = "Answer",
    parsed = NULL,
    raw_output = "trace",
    elapsed_time = 1.0,
    status = "success"
  )

  summary_result <- summary(result_obj)
  expect_null(summary_result$parsed_fields)
})

test_that("summary.asa_agent includes config details", {
  agent <- asa_agent(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4.1-mini",
    config = list(
      backend = "openai",
      model = "gpt-4.1-mini",
      use_memory_folding = FALSE,
      proxy = "socks5h://127.0.0.1:9050"
    )
  )

  result <- summary(agent)
  expect_equal(result$config$use_memory_folding, FALSE)
  expect_equal(result$config$proxy, "socks5h://127.0.0.1:9050")
})

test_that("print.asa_agent displays memory folding info", {
  agent <- asa_agent(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4",
    config = list(
      use_memory_folding = TRUE,
      memory_threshold = 10L,
      memory_keep_recent = 5L
    )
  )

  expect_output(print(agent), "Memory Folding:.*Enabled")
  expect_output(print(agent), "Threshold:.*10")
  expect_output(print(agent), "Keep Recent:.*5")
})

test_that("print.asa_agent handles disabled memory folding", {
  agent <- asa_agent(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4",
    config = list(use_memory_folding = FALSE)
  )

  expect_output(print(agent), "Memory Folding:.*Disabled")
})

test_that("print.asa_response displays fold count when present", {
  response <- asa_response(
    message = "Message",
    status_code = 200L,
    raw_response = NULL,
    trace = "",
    elapsed_time = 1.0,
    fold_count = 5L,
    prompt = "Prompt"
  )

  expect_output(print(response), "Folds:.*5")
})

test_that("print.asa_response handles no folds", {
  response <- asa_response(
    message = "Message",
    status_code = 200L,
    raw_response = NULL,
    trace = "",
    elapsed_time = 1.0,
    fold_count = 0L,
    prompt = "Prompt"
  )

  # Should not display fold count when 0
  output <- capture.output(print(response))
  expect_false(any(grepl("Folds:", output)))
})

test_that("print.asa_result displays parsed output", {
  result <- asa_result(
    prompt = "Test",
    message = "Answer",
    parsed = list(name = "John", age = "30", city = "NYC"),
    raw_output = "",
    elapsed_time = 1.0,
    status = "success"
  )

  expect_output(print(result), "Parsed Output:")
  expect_output(print(result), "name: John")
  expect_output(print(result), "age: 30")
  expect_output(print(result), "city: NYC")
})

test_that("print.asa_result handles vector values in parsed", {
  result <- asa_result(
    prompt = "Test",
    message = "Answer",
    parsed = list(scalar = "value", vector = c(1, 2, 3, 4)),
    raw_output = "",
    elapsed_time = 1.0,
    status = "success"
  )

  expect_output(print(result), "scalar: value")
  expect_output(print(result), "vector: \\[4 items\\]")
})

test_that("as.data.frame preserves all parsed fields", {
  result <- asa_result(
    prompt = "Query",
    message = "Answer",
    parsed = list(a = "1", b = "2", c = "3"),
    raw_output = "",
    elapsed_time = 1.0,
    status = "success"
  )

  df <- as.data.frame(result)

  expect_true("a" %in% names(df))
  expect_true("b" %in% names(df))
  expect_true("c" %in% names(df))
  expect_equal(df$a, "1")
  expect_equal(df$b, "2")
  expect_equal(df$c, "3")
})
