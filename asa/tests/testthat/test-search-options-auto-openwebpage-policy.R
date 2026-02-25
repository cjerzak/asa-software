test_that("search_options defaults auto_openwebpage_policy by stability profile", {
  stealth <- asa::search_options(stability_profile = "stealth_first")
  balanced <- asa::search_options(stability_profile = "balanced")
  deterministic <- asa::search_options(stability_profile = "deterministic")

  expect_identical(as.character(stealth$auto_openwebpage_policy), "auto")
  expect_identical(as.character(balanced$auto_openwebpage_policy), "auto")
  expect_identical(as.character(deterministic$auto_openwebpage_policy), "off")
})

test_that("search_options treats any explicit non-off token as auto", {
  legacy_enabled_values <- c(
    "conservative", "safe", "budgeted", "aggressive", "on", "enabled", "true", "1", "banana"
  )

  for (value in legacy_enabled_values) {
    opt <- asa::search_options(auto_openwebpage_policy = value)
    expect_identical(as.character(opt$auto_openwebpage_policy), "auto")
  }

  off_opt <- asa::search_options(auto_openwebpage_policy = "off")
  expect_identical(as.character(off_opt$auto_openwebpage_policy), "off")
})

test_that("print.asa_search suppresses default auto policy and shows explicit off", {
  default_opt <- asa::search_options(stability_profile = "stealth_first")
  default_text <- paste(capture.output(print(default_opt)), collapse = "\n")
  expect_false(grepl("auto_openwebpage_policy", default_text, fixed = TRUE))

  off_opt <- asa::search_options(auto_openwebpage_policy = "off")
  off_text <- paste(capture.output(print(off_opt)), collapse = "\n")
  expect_true(grepl("auto_openwebpage_policy=off", off_text, fixed = TRUE))
})

test_that("search_options stores normalized performance profile and webpage policy", {
  latency <- asa::search_options(performance_profile = "latency")
  expect_identical(as.character(latency$performance_profile), "latency")
  expect_true(is.list(latency$webpage_policy))
  expect_equal(as.integer(latency$webpage_policy$max_open_calls), 2L)
  expect_equal(as.integer(latency$webpage_policy$host_cooldown_seconds), 60L)
  expect_equal(as.integer(latency$webpage_policy$blocked_host_ttl_seconds), 900L)

  expect_error(
    asa::search_options(performance_profile = "fast"),
    "performance_profile"
  )

  expect_error(
    asa::search_options(
      webpage_policy = list(max_open_calls = 2L)
    ),
    "webpage_policy"
  )
})
