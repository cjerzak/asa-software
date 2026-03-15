asa_test_mock_eval_task_result <- function(message = "1993",
                                           parsed = NULL,
                                           status = "success",
                                           elapsed_time = 0.1,
                                           input_tokens = 7L,
                                           output_tokens = 3L,
                                           tokens_used = 10L) {
  out <- asa_result(
    prompt = "prompt",
    message = message,
    parsed = parsed,
    raw_output = "",
    elapsed_time = elapsed_time,
    status = status,
    execution = list(
      input_tokens = input_tokens,
      output_tokens = output_tokens,
      tokens_used = tokens_used
    )
  )
  out$token_stats <- list(
    input_tokens = input_tokens,
    output_tokens = output_tokens,
    tokens_used = tokens_used
  )
  out
}

test_that("evaluate_agent aggregates text benchmark runs", {
  benchmark <- create_benchmark(
    name = "TextBench",
    tasks = list(
      list(
        id = "r_release",
        prompt = "In what year was R first released? Return the year only.",
        expected = "1993",
        match = "exact",
        output_format = "text",
        category = "history"
      ),
      list(
        id = "pearl_book",
        prompt = "Who wrote 'Causal Inference in Statistics: A Primer'? Return the surname only.",
        expected = "Pearl",
        match = "exact",
        output_format = "text",
        category = "attribution"
      )
    )
  )

  mock_agent <- structure(list(config = asa_config()), class = "asa_agent")
  testthat::local_mocked_bindings(
    .ensure_research_agent = function(...) list(agent = mock_agent, config = asa_config()),
    run_task = function(prompt, ...) {
      if (grepl("R first released", prompt, fixed = TRUE)) {
        return(asa_test_mock_eval_task_result(message = "1993"))
      }
      asa_test_mock_eval_task_result(message = "Pearl")
    }
  )

  result <- evaluate_agent(benchmark, runs = 2L, verbose = FALSE)

  expect_s3_class(result, "asa_evaluation_result")
  expect_equal(nrow(result$task_results), 4L)
  expect_equal(result$metrics$runs, 2L)
  expect_equal(result$metrics$evaluations, 4L)
  expect_equal(result$metrics$pass_rate, 1)
  expect_equal(result$metrics$tokens_used_total, 40L)
  expect_true(all(result$task_results$pass))

  printed <- capture.output(print(result))
  expect_true(any(grepl("ASA Evaluation Result", printed)))
  expect_equal(nrow(as.data.frame(result)), 4L)
})

test_that("evaluate_agent assigns partial credit for structured field scoring", {
  benchmark <- create_benchmark(
    name = "StructuredBench",
    tasks = list(
      list(
        id = "ada_profile",
        prompt = "Return birth_year and nationality for Ada Lovelace.",
        expected = list(birth_year = "1815", nationality = "British"),
        output_format = c("birth_year", "nationality"),
        match = "fields",
        field_matchers = list(
          birth_year = "exact",
          nationality = "contains"
        ),
        category = "biography"
      )
    )
  )

  mock_agent <- structure(list(config = asa_config()), class = "asa_agent")
  testthat::local_mocked_bindings(
    .ensure_research_agent = function(...) list(agent = mock_agent, config = asa_config()),
    run_task = function(...) {
      asa_test_mock_eval_task_result(
        message = "{\"birth_year\":\"1815\",\"nationality\":\"French\"}",
        parsed = list(birth_year = "1815", nationality = "French")
      )
    }
  )

  result <- evaluate_agent(benchmark, verbose = FALSE)

  expect_equal(nrow(result$task_results), 1L)
  expect_equal(result$task_results$score[[1]], 0.5)
  expect_false(result$task_results$pass[[1]])
  expect_equal(result$metrics$mean_task_score, 0.5)
})

test_that("evaluate_agent supports numeric tolerance field matchers", {
  benchmark <- create_benchmark(
    name = "NumericBench",
    tasks = list(
      list(
        id = "radius",
        prompt = "Return only radius_km for Earth.",
        expected = list(radius_km = 6371),
        output_format = c("radius_km"),
        match = "fields",
        field_matchers = list(radius_km = list(type = "numeric_tolerance", tol = 5))
      )
    )
  )

  mock_agent <- structure(list(config = asa_config()), class = "asa_agent")
  testthat::local_mocked_bindings(
    .ensure_research_agent = function(...) list(agent = mock_agent, config = asa_config()),
    run_task = function(...) {
      asa_test_mock_eval_task_result(
        message = "{\"radius_km\":6374}",
        parsed = list(radius_km = 6374)
      )
    }
  )

  result <- evaluate_agent(benchmark, verbose = FALSE)

  expect_true(result$task_results$pass[[1]])
  expect_equal(result$task_results$score[[1]], 1)
})

test_that("evaluate_agent can load a built-in benchmark name", {
  mock_agent <- structure(list(config = asa_config()), class = "asa_agent")
  testthat::local_mocked_bindings(
    .ensure_research_agent = function(...) list(agent = mock_agent, config = asa_config()),
    run_task = function(prompt, ...) {
      if (grepl("R first released", prompt, fixed = TRUE)) {
        return(asa_test_mock_eval_task_result(message = "1993"))
      }
      if (grepl("Causal Inference in Statistics", prompt, fixed = TRUE)) {
        return(asa_test_mock_eval_task_result(message = "Pearl"))
      }
      asa_test_mock_eval_task_result(message = "Germany")
    }
  )

  result <- evaluate_agent("FactRetrieval2024", verbose = FALSE)

  expect_s3_class(result, "asa_evaluation_result")
  expect_equal(result$benchmark$name, "FactRetrieval2024")
  expect_true(result$metrics$evaluations >= 1L)
})

test_that("evaluate_agent respects fail_fast on execution failure", {
  benchmark <- create_benchmark(
    name = "FailFastBench",
    tasks = list(
      list(
        id = "first_task",
        prompt = "Will fail",
        expected = "x",
        match = "exact",
        output_format = "text"
      ),
      list(
        id = "second_task",
        prompt = "Should not run",
        expected = "y",
        match = "exact",
        output_format = "text"
      )
    )
  )

  mock_agent <- structure(list(config = asa_config()), class = "asa_agent")
  calls <- 0L
  testthat::local_mocked_bindings(
    .ensure_research_agent = function(...) list(agent = mock_agent, config = asa_config()),
    run_task = function(...) {
      calls <<- calls + 1L
      stop("provider failed")
    }
  )

  expect_error(
    evaluate_agent(benchmark, fail_fast = TRUE, verbose = FALSE),
    "fail_fast = TRUE"
  )
  expect_equal(calls, 1L)
})
