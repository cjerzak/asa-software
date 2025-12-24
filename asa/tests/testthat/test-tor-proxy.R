# Tests for Tor proxy functionality

test_that("is_tor_running returns logical", {
  result <- is_tor_running()
  expect_type(result, "logical")
  expect_length(result, 1)
})

test_that("is_tor_running handles custom port", {
  result <- is_tor_running(port = 9050L)
  expect_type(result, "logical")

  # Test with unlikely port (should be FALSE)
  result_bad <- is_tor_running(port = 99999L)
  expect_false(result_bad)
})

test_that("is_tor_running handles connection errors gracefully", {
  # Port that definitely won't have Tor
  expect_error(is_tor_running(port = 1L), NA)
  expect_false(is_tor_running(port = 1L))
})

test_that("is_tor_running with default port", {
  result <- is_tor_running()

  # Should return TRUE or FALSE, not error
  expect_true(is.logical(result))
})

test_that("get_tor_ip returns character or NA", {
  skip_if_not(is_tor_running(), "Tor not running")

  ip <- get_tor_ip()

  expect_true(is.character(ip) || is.na(ip))

  if (!is.na(ip)) {
    # Should look like an IP address
    expect_match(ip, "^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$")
  }
})

test_that("get_tor_ip handles missing Tor gracefully", {
  # If Tor is not running, should return NA, not error
  if (!is_tor_running()) {
    ip <- get_tor_ip()
    expect_true(is.na(ip) || is.character(ip))
  } else {
    skip("Tor is running - test needs Tor to be off")
  }
})

test_that("get_tor_ip uses provided proxy", {
  skip_if_not(is_tor_running(), "Tor not running")

  ip <- get_tor_ip(proxy = "socks5h://127.0.0.1:9050")

  expect_true(is.character(ip) || is.na(ip))
})

test_that("get_tor_ip handles invalid proxy", {
  # Invalid proxy should return NA, not error
  ip <- get_tor_ip(proxy = "invalid://proxy")

  expect_true(is.na(ip))
})

test_that("rotate_tor_circuit accepts valid methods", {
  skip_on_ci()
  skip_if_not(is_tor_running(), "Tor not running")
  skip("Manual test - requires Tor control")

  # This test requires Tor to be running and properly configured
  # Only run manually

  # expect_error(rotate_tor_circuit(method = "brew"), NA)
  # expect_error(rotate_tor_circuit(method = "systemctl"), NA)
  # expect_error(rotate_tor_circuit(method = "signal"), NA)
})

test_that("rotate_tor_circuit validates method argument", {
  # Should error on invalid method
  expect_error(rotate_tor_circuit(method = "invalid_method"))
})

test_that("rotate_tor_circuit returns NULL invisibly", {
  skip_on_ci()
  skip("Manual test - requires Tor")

  # Would test this if Tor is running and properly configured
  # result <- rotate_tor_circuit(method = "signal", wait = 0L)
  # expect_null(result)
})

test_that("rotate_tor_circuit respects wait parameter", {
  skip_on_ci()
  skip("Manual test - requires Tor")

  # This would test timing
  # start <- Sys.time()
  # rotate_tor_circuit(method = "signal", wait = 2L)
  # elapsed <- as.numeric(difftime(Sys.time(), start, units = "secs"))
  # expect_true(elapsed >= 2)
})

test_that("agent initialization with proxy parameter", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Test that proxy parameter is accepted and stored
  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    proxy = "socks5h://127.0.0.1:9050",
    verbose = FALSE
  )

  expect_equal(agent$config$proxy, "socks5h://127.0.0.1:9050")

  reset_agent()
})

test_that("agent initialization with NULL proxy", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Test that NULL proxy is handled
  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    proxy = NULL,
    verbose = FALSE
  )

  # Proxy should be NULL or from environment
  expect_true(is.null(agent$config$proxy) || is.character(agent$config$proxy))

  reset_agent()
})

test_that("is_tor_running handles timeout correctly", {
  # The function uses timeout = 2 internally
  # Testing with a bad port should return quickly, not hang
  start <- Sys.time()
  result <- is_tor_running(port = 99999L)
  elapsed <- as.numeric(difftime(Sys.time(), start, units = "secs"))

  expect_false(result)
  expect_true(elapsed < 5)  # Should fail within timeout
})

test_that("get_tor_ip handles curl errors", {
  # Test with a proxy that won't work
  ip <- get_tor_ip(proxy = "socks5h://255.255.255.255:99999")

  # Should return NA, not error
  expect_true(is.na(ip))
})

test_that("Tor functions are exported", {
  # Verify these functions are available in the package
  expect_true(exists("is_tor_running", envir = asNamespace("asa")))
  expect_true(exists("get_tor_ip", envir = asNamespace("asa")))
  expect_true(exists("rotate_tor_circuit", envir = asNamespace("asa")))
})

test_that("is_tor_running with different connection states", {
  # Test various port scenarios
  ports_to_test <- c(9050L, 9051L, 8080L, 3128L)

  for (port in ports_to_test) {
    result <- is_tor_running(port = port)
    expect_type(result, "logical")
    expect_length(result, 1)
  }
})

# Integration test with agent
test_that("agent can be initialized with Tor when running", {
  skip_on_ci()
  skip_if_not(is_tor_running(), "Tor not running")
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    proxy = "socks5h://127.0.0.1:9050",
    verbose = FALSE
  )

  expect_s3_class(agent, "asa_agent")
  expect_equal(agent$config$proxy, "socks5h://127.0.0.1:9050")

  reset_agent()
})

test_that("get_tor_ip JSON parsing works", {
  skip_if_not(is_tor_running(), "Tor not running")

  # The function should handle JSON from ipify
  ip <- get_tor_ip()

  if (!is.na(ip)) {
    # Should be a valid-looking IP
    expect_type(ip, "character")
    expect_true(grepl("^[0-9.]+$", ip))
  }
})

test_that("rotate_tor_circuit with minimal wait", {
  skip_on_ci()
  skip("Manual test - modifies Tor")

  # Test that wait = 0 works (no wait)
  # expect_error(rotate_tor_circuit(method = "signal", wait = 0L), NA)
})

test_that("proxy parameter propagates to search tools", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # When agent is initialized with proxy, tools should use it
  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    proxy = "socks5h://127.0.0.1:9050",
    verbose = FALSE
  )

  # Proxy should be in config
  expect_equal(agent$config$proxy, "socks5h://127.0.0.1:9050")

  reset_agent()
})
