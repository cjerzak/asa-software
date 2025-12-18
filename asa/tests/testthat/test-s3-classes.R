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
