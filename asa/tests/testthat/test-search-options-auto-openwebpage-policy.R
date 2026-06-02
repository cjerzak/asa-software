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

test_that("search_options validates numeric and logical controls", {
  valid <- asa::search_options(
    max_results = 10L,
    timeout = 15,
    max_retries = 0L,
    retry_delay = 0,
    backoff_multiplier = 1.5,
    inter_search_delay = 0,
    humanize_timing = FALSE,
    jitter_factor = 0,
    allow_direct_fallback = FALSE,
    langgraph_node_retries = TRUE,
    langgraph_cache_enabled = FALSE,
    finalize_when_all_unresolved_exhausted = TRUE,
    allow_read_webpages = FALSE,
    webpage_use_mmr = TRUE,
    webpage_mmr_lambda = 0.5,
    webpage_cache_enabled = FALSE,
    webpage_blocked_detect_on_200 = TRUE,
    webpage_pdf_enabled = FALSE
  )
  expect_s3_class(valid, "asa_search")
  expect_identical(valid$max_results, 10L)
  expect_identical(valid$max_retries, 0L)
  expect_false(valid$humanize_timing)
  expect_true(valid$finalize_when_all_unresolved_exhausted)

  invalid_cases <- list(
    list(args = list(max_results = -5L), pattern = "max_results"),
    list(args = list(max_results = 200L), pattern = "max_results"),
    list(args = list(timeout = -1), pattern = "timeout"),
    list(args = list(max_retries = -1L), pattern = "max_retries"),
    list(args = list(backoff_multiplier = 0.5), pattern = "backoff_multiplier"),
    list(args = list(humanize_timing = "yes"), pattern = "humanize_timing"),
    list(args = list(allow_direct_fallback = "yes"), pattern = "allow_direct_fallback"),
    list(args = list(webpage_use_mmr = "yes"), pattern = "webpage_use_mmr"),
    list(args = list(webpage_mmr_lambda = 2), pattern = "webpage_mmr_lambda"),
    list(args = list(webpage_cache_enabled = "yes"), pattern = "webpage_cache_enabled"),
    list(args = list(webpage_relevance_mode = "semantic"), pattern = "webpage_relevance_mode"),
    list(args = list(webpage_embedding_provider = "bad"), pattern = "webpage_embedding_provider")
  )

  for (case in invalid_cases) {
    expect_error(
      do.call(asa::search_options, case$args),
      case$pattern
    )
  }
})
