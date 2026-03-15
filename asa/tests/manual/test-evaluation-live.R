test_that("evaluate_agent smoke test works with a live OpenAI provider", {
  asa_test_skip_api_tests()
  skip_on_cran()

  benchmark <- create_benchmark(
    name = "LiveEvalSmoke",
    tasks = list(
      list(
        id = "r_release",
        prompt = "In what year was R first released? Return the year only.",
        expected = "1993",
        match = "exact",
        output_format = "text"
      )
    )
  )

  agent <- initialize_agent(
    backend = "openai",
    model = Sys.getenv("ASA_TEST_OPENAI_MODEL", unset = "gpt-4.1-mini"),
    verbose = FALSE
  )

  result <- evaluate_agent(benchmark = benchmark, agent = agent, verbose = FALSE)

  expect_s3_class(result, "asa_evaluation_result")
  expect_equal(result$metrics$evaluations, 1L)
})
