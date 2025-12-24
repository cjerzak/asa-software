# Tests for backend setup and environment management

test_that("check_backend returns correct structure", {
  # Mock a non-existent environment
  result <- check_backend(conda_env = "nonexistent_env_xyz")

  expect_type(result, "list")
  expect_named(result, c("available", "conda_env", "python_version", "missing_packages"))
  expect_equal(result$conda_env, "nonexistent_env_xyz")
  expect_false(result$available)
})

test_that("check_backend handles missing environment gracefully", {
  result <- check_backend(conda_env = "fake_env_12345")

  expect_false(result$available)
  expect_true(length(result$missing_packages) > 0)
})

test_that("check_backend validates required packages", {
  skip_if_not(requireNamespace("reticulate", quietly = TRUE),
              "reticulate package not available")

  # This test requires the environment to actually exist
  skip_if_not(any(grepl("asa_env", reticulate::conda_list()$name, fixed = TRUE)),
              "asa_env not found - run build_backend() first")

  result <- check_backend(conda_env = "asa_env")

  expect_type(result, "list")
  expect_true(is.logical(result$available))

  # If environment exists, check package validation
  if (result$available) {
    expect_equal(length(result$missing_packages), 0)
    expect_false(is.null(result$python_version))
  }
})

test_that("build_backend validates inputs", {
  skip_on_ci()  # Don't create environments in CI
  skip("Manual test - creates conda environment")

  # This is a manual test that would create a real environment
  # Uncomment to test manually
  # expect_error(build_backend(conda_env = ""), NA)
})

test_that("build_backend requires reticulate", {
  # Mock missing reticulate by temporarily hiding it
  skip("Requires mocking package availability")

  # This would require more sophisticated mocking
  # to test the requireNamespace check
})

test_that("check_backend returns python_version when available", {
  skip_if_not(requireNamespace("reticulate", quietly = TRUE),
              "reticulate package not available")
  skip_if_not(any(grepl("asa_env", reticulate::conda_list()$name, fixed = TRUE)),
              "asa_env not found")

  result <- check_backend(conda_env = "asa_env")

  if (result$available) {
    expect_type(result$python_version, "character")
    expect_true(nchar(result$python_version) > 0)
    expect_match(result$python_version, "^[0-9]+\\.[0-9]+")
  }
})

test_that("check_backend handles errors gracefully", {
  # Test with completely invalid input
  result <- suppressWarnings(check_backend(conda_env = ""))

  expect_type(result, "list")
  expect_false(result$available)
})
