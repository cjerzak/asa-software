test_that("create_benchmark normalizes tasks and benchmark summaries work", {
  benchmark <- create_benchmark(
    name = "MiniBench",
    description = "A tiny benchmark",
    citation = "Doe (2026). MiniBench.",
    tasks = list(
      list(
        id = "r_release",
        prompt = "In what year was R first released? Return the year only.",
        expected = "1993",
        output_format = "text",
        match = "exact",
        category = "history"
      ),
      list(
        id = "ada_profile",
        prompt = "Return only birth_year for Ada Lovelace.",
        expected = list(birth_year = "1815"),
        output_format = c("birth_year"),
        match = "fields"
      )
    )
  )

  expect_s3_class(benchmark, "asa_benchmark")
  expect_equal(length(benchmark$tasks), 2L)
  expect_equal(benchmark$tasks[[2]]$match, "fields")
  expect_equal(benchmark$tasks[[2]]$output_format, "birth_year")

  printed <- capture.output(print(benchmark))
  expect_true(any(grepl("ASA Benchmark", printed)))

  summary_out <- summary(benchmark)
  expect_equal(summary_out$task_count, 2L)
  expect_true("fields" %in% summary_out$matchers)
})

test_that("create_benchmark rejects duplicate task ids", {
  expect_error(
    create_benchmark(
      name = "BadBench",
      tasks = list(
        list(id = "dup", prompt = "A", expected = "1", match = "exact"),
        list(id = "dup", prompt = "B", expected = "2", match = "exact")
      )
    ),
    "unique task ids"
  )
})

test_that("benchmark_from_df supports structured expected values", {
  df <- data.frame(
    id = c("r_release", "ada_profile"),
    prompt = c(
      "In what year was R first released? Return the year only.",
      "Return only birth_year for Ada Lovelace."
    ),
    expected = I(list(
      "1993",
      list(birth_year = "1815")
    )),
    category = c("history", "biography"),
    output_format = I(list("text", c("birth_year"))),
    match = c("exact", "fields"),
    field_matchers = I(list(NULL, list(birth_year = "exact"))),
    stringsAsFactors = FALSE
  )

  benchmark <- benchmark_from_df(
    df = df,
    prompt_col = "prompt",
    expected_col = "expected",
    id_col = "id",
    category_col = "category",
    output_format_col = "output_format",
    match_col = "match",
    field_matchers_col = "field_matchers",
    name = "FromDfBench"
  )

  expect_s3_class(benchmark, "asa_benchmark")
  expect_equal(benchmark$tasks[[2]]$expected$birth_year, "1815")
  expect_equal(benchmark$tasks[[2]]$field_matchers$birth_year, "exact")
})

test_that("benchmark_from_yaml reads YAML definitions and built-ins are discoverable", {
  yaml_file <- tempfile(fileext = ".yml")
  writeLines(
    c(
      'name: YamlBench',
      'version: "1.0"',
      'description: YAML benchmark',
      'tasks:',
      '  - id: yaml_task',
      '    prompt: "In what year was R first released? Return the year only."',
      '    output_format: text',
      '    expected: "1993"',
      '    match: exact'
    ),
    yaml_file
  )

  benchmark <- benchmark_from_yaml(yaml_file)
  expect_s3_class(benchmark, "asa_benchmark")
  expect_equal(benchmark$name, "YamlBench")
  expect_equal(benchmark$tasks[[1]]$id, "yaml_task")

  available <- list_benchmarks()
  expect_true(nrow(available) >= 2L)
  expect_true("FactRetrieval2024" %in% available$name)

  built_in <- load_benchmark("FactRetrieval2024")
  expect_s3_class(built_in, "asa_benchmark")
  expect_true(length(built_in$tasks) >= 1L)
})
