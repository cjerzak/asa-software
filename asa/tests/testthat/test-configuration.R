# Tests for configuration functions

test_that("configure_search_logging validates log levels", {
  skip_on_ci()
  skip("Requires conda environment")

  valid_levels <- c("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

  for (level in valid_levels) {
    expect_error(
      suppressMessages(configure_search_logging(level, conda_env = "asa_env")),
      NA
    )
  }
})

test_that("configure_search_logging rejects invalid levels", {
  skip_on_ci()
  skip("Requires conda environment")

  expect_error(
    configure_search_logging("INVALID_LEVEL"),
    "Invalid log level"
  )

  expect_error(
    configure_search_logging("debug"),  # lowercase might work if uppercased
    NA  # Function converts to uppercase
  )
})

test_that("configure_search_logging accepts case-insensitive input", {
  skip_on_ci()
  skip("Requires conda environment")

  # Function should convert to uppercase
  expect_error(
    suppressMessages(configure_search_logging("debug")),
    NA
  )

  expect_error(
    suppressMessages(configure_search_logging("WaRnInG")),
    NA
  )
})

test_that("configure_search_logging returns level invisibly", {
  skip_on_ci()
  skip("Requires conda environment")

  result <- suppressMessages(configure_search_logging("WARNING"))

  expect_equal(result, "WARNING")
})

test_that("configure_search_logging prints confirmation message", {
  skip_on_ci()
  skip("Requires conda environment")

  expect_message(
    configure_search_logging("INFO"),
    "Search logging level set to: INFO"
  )
})

test_that("configure_search handles all parameters", {
  skip_on_ci()
  skip("Requires conda environment")

  expect_error(
    suppressMessages(configure_search(
      max_results = 20L,
      timeout = 30L,
      max_retries = 5L
    )),
    NA
  )
})

test_that("configure_search accepts NULL parameters", {
  skip_on_ci()
  skip("Requires conda environment")

  # NULL parameters should be skipped
  expect_error(
    suppressMessages(configure_search(
      max_results = NULL,
      timeout = NULL
    )),
    NA
  )
})

test_that("configure_search with numeric parameters", {
  skip_on_ci()
  skip("Requires conda environment")

  expect_error(
    suppressMessages(configure_search(
      max_results = 15L,
      timeout = 25L,
      max_retries = 4L,
      retry_delay = 3.0,
      backoff_multiplier = 2.0
    )),
    NA
  )
})

test_that("configure_search returns configuration invisibly", {
  skip_on_ci()
  skip("Requires conda environment")

  result <- suppressMessages(configure_search(max_results = 10L))

  # Should return something (config object from Python)
  # Could be NULL if module not available
  expect_true(is.null(result) || is.list(result))
})

test_that("configure_search shows confirmation messages", {
  skip_on_ci()
  skip("Requires conda environment")

  expect_message(
    configure_search(max_results = 10L, timeout = 20L),
    "max_results=10"
  )

  expect_message(
    configure_search(timeout = 30L),
    "timeout=30"
  )
})

test_that("configure_search handles partial parameter sets", {
  skip_on_ci()
  skip("Requires conda environment")

  # Only set max_results
  expect_error(
    suppressMessages(configure_search(max_results = 25L)),
    NA
  )

  # Only set timeout and retries
  expect_error(
    suppressMessages(configure_search(timeout = 40L, max_retries = 6L)),
    NA
  )
})

test_that("configure_search_logging handles missing module gracefully", {
  skip_on_ci()

  # If conda_env doesn't exist, should warn not error
  expect_warning(
    configure_search_logging("DEBUG", conda_env = "nonexistent_env_xyz"),
    "Could not configure"
  )
})

test_that("configure_search handles missing module gracefully", {
  skip_on_ci()

  # If conda_env doesn't exist, should warn not error
  expect_warning(
    configure_search(max_results = 10L, conda_env = "nonexistent_env_xyz"),
    "Could not configure"
  )
})

test_that("configuration functions use correct conda environment", {
  skip_on_ci()
  skip("Requires conda environment")

  # Test with explicit conda_env parameter
  expect_error(
    suppressMessages(configure_search_logging("WARNING", conda_env = "asa_env")),
    NA
  )

  expect_error(
    suppressMessages(configure_search(max_results = 10L, conda_env = "asa_env")),
    NA
  )
})

test_that("configure_search accepts delay parameters", {
  skip_on_ci()
  skip("Requires conda environment")

  expect_error(
    suppressMessages(configure_search(
      inter_search_delay = 1.5,
      page_load_wait = 3.0
    )),
    NA
  )
})

test_that("configure_search accepts backoff parameters", {
  skip_on_ci()
  skip("Requires conda environment")

  expect_error(
    suppressMessages(configure_search(
      backoff_multiplier = 2.5,
      captcha_backoff_base = 4L
    )),
    NA
  )
})

test_that("configure_search message includes all set parameters", {
  skip_on_ci()
  skip("Requires conda environment")

  msg <- capture_messages(
    configure_search(
      max_results = 15L,
      timeout = 25L,
      inter_search_delay = 2.0
    )
  )

  expect_true(any(grepl("max_results=15", msg)))
  expect_true(any(grepl("timeout=25", msg)))
  expect_true(any(grepl("inter_search_delay=2", msg)))
})

test_that("configuration persists for agent", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Configure search parameters
  suppressMessages(configure_search(max_results = 20L))

  # Initialize agent
  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  # Configuration should have been applied to Python module
  # (This is more of an integration test)

  reset_agent()
})

test_that("logging configuration affects Python module", {
  skip_on_ci()
  skip("Requires conda environment")

  # Set to DEBUG level
  suppressMessages(configure_search_logging("DEBUG"))

  # Set back to WARNING
  suppressMessages(configure_search_logging("WARNING"))

  # Should not error and should actually call Python module
})

test_that("configure_search validates numeric inputs", {
  skip_on_ci()
  skip("Requires conda environment")

  # Positive numbers should work
  expect_error(
    suppressMessages(configure_search(max_results = 10L)),
    NA
  )

  # Could add validation tests if function validates inputs
})

test_that("configuration functions are exported", {
  # Verify these functions are available in the package
  expect_true(exists("configure_search_logging", envir = asNamespace("asa")))
  expect_true(exists("configure_search", envir = asNamespace("asa")))
})

test_that("configure_search handles all timing parameters", {
  skip_on_ci()
  skip("Requires conda environment")

  expect_error(
    suppressMessages(configure_search(
      timeout = 30L,
      retry_delay = 2.5,
      page_load_wait = 3.0,
      inter_search_delay = 1.0
    )),
    NA
  )
})

test_that("configure_search with extreme values", {
  skip_on_ci()
  skip("Requires conda environment")

  # Test with very high values (should be accepted by Python)
  expect_error(
    suppressMessages(configure_search(
      max_results = 100L,
      timeout = 300L,
      max_retries = 20L
    )),
    NA
  )

  # Test with very low values
  expect_error(
    suppressMessages(configure_search(
      max_results = 1L,
      timeout = 5L,
      max_retries = 1L
    )),
    NA
  )
})
