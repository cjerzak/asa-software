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
