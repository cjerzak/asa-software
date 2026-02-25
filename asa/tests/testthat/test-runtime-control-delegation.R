test_that(".runtime_control_call returns default when backend API is unavailable", {
  testthat::local_mocked_bindings(
    .runtime_control_api = function(required = FALSE) NULL,
    .package = "asa"
  )

  out <- asa:::.runtime_control_call("runtime_rate_acquire", default = 123)
  expect_identical(out, 123)
})


test_that("rate limiter wrapper delegates to runtime control API", {
  calls <- list()

  testthat::local_mocked_bindings(
    .runtime_control_call = function(method, ..., default = NULL, required = FALSE, quiet = TRUE) {
      calls[[length(calls) + 1L]] <<- list(method = method, args = list(...))
      if (identical(method, "runtime_rate_acquire")) {
        return(0.25)
      }
      default
    },
    .package = "asa"
  )

  wait <- asa:::.acquire_rate_limit_token(verbose = TRUE)
  expect_equal(wait, 0.25)
  expect_length(calls, 1L)
  expect_identical(calls[[1L]]$method, "runtime_rate_acquire")
  expect_true(isTRUE(calls[[1L]]$args$verbose))
})


test_that("adaptive wrappers delegate and normalize status", {
  calls <- character(0)

  testthat::local_mocked_bindings(
    .runtime_control_call = function(method, ..., default = NULL, required = FALSE, quiet = TRUE) {
      calls <<- c(calls, method)
      if (identical(method, "runtime_adaptive_record")) {
        return(list(multiplier = 1.75, enabled = TRUE, success_streak = 3L, recent_count = 9L))
      }
      if (identical(method, "runtime_adaptive_status")) {
        return(list(multiplier = 1.5, enabled = TRUE, success_streak = 4L, recent_count = 10L))
      }
      if (identical(method, "runtime_adaptive_reset")) {
        return(list(multiplier = 1.0, enabled = TRUE, success_streak = 0L, recent_count = 0L))
      }
      default
    },
    .package = "asa"
  )

  rec <- asa:::.adaptive_rate_record("captcha", verbose = TRUE)
  expect_equal(rec, 1.75)
  expect_equal(asa:::.adaptive_rate_get_multiplier(), 1.5)

  st <- asa:::.adaptive_rate_status()
  expect_true(isTRUE(st$enabled))
  expect_equal(st$multiplier, 1.5)
  expect_equal(st$success_streak, 4L)
  expect_equal(st$recent_count, 10L)

  expect_invisible(asa:::.adaptive_rate_reset())

  expect_true("runtime_adaptive_record" %in% calls)
  expect_true("runtime_adaptive_status" %in% calls)
  expect_true("runtime_adaptive_reset" %in% calls)
})


test_that("circuit breaker wrappers preserve expected return shapes", {
  calls <- character(0)

  testthat::local_mocked_bindings(
    .runtime_control_call = function(method, ..., default = NULL, required = FALSE, quiet = TRUE) {
      calls <<- c(calls, method)
      if (identical(method, "runtime_circuit_record")) {
        return(TRUE)
      }
      if (identical(method, "runtime_circuit_check")) {
        return(FALSE)
      }
      if (identical(method, "runtime_circuit_status")) {
        return(list(tripped = TRUE, error_rate = 0.4, recent_count = 8L, trip_count = 2L, enabled = TRUE))
      }
      default
    },
    .package = "asa"
  )

  expect_invisible(asa:::.circuit_breaker_init())
  expect_true(isTRUE(asa:::.circuit_breaker_record("error", verbose = TRUE)))
  expect_false(asa:::.circuit_breaker_check(verbose = TRUE))

  st <- asa:::.circuit_breaker_status()
  expect_identical(names(st), c("tripped", "error_rate", "recent_count", "trip_count"))
  expect_true(isTRUE(st$tripped))
  expect_equal(st$error_rate, 0.4)
  expect_equal(st$recent_count, 8L)
  expect_equal(st$trip_count, 2L)

  expect_true("runtime_circuit_init" %in% calls)
  expect_true("runtime_circuit_record" %in% calls)
  expect_true("runtime_circuit_check" %in% calls)
  expect_true("runtime_circuit_status" %in% calls)
})


test_that(".close_http_clients delegates to runtime and clears legacy clients", {
  asa_ns_env <- get("asa_env", envir = asNamespace("asa"))
  old_clients <- asa_ns_env$http_clients
  on.exit({
    asa_ns_env$http_clients <- old_clients
  }, add = TRUE)

  direct_closed <- 0L
  proxied_closed <- 0L
  runtime_calls <- character(0)

  asa_ns_env$http_clients <- list(
    direct = list(close = function() {
      direct_closed <<- direct_closed + 1L
      invisible(NULL)
    }),
    proxied = list(close = function() {
      proxied_closed <<- proxied_closed + 1L
      invisible(NULL)
    })
  )

  testthat::local_mocked_bindings(
    .runtime_control_call = function(method, ..., default = NULL, required = FALSE, quiet = TRUE) {
      runtime_calls <<- c(runtime_calls, method)
      default
    },
    .package = "asa"
  )

  expect_invisible(asa:::.close_http_clients())
  expect_true("runtime_clients_close" %in% runtime_calls)
  expect_equal(direct_closed, 1L)
  expect_equal(proxied_closed, 1L)
  expect_null(asa_ns_env$http_clients)
})
