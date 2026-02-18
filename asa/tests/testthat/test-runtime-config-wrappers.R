# Tests for runtime configuration wrappers

.make_search_fixture <- function() {
  list(
    max_results = 10L,
    timeout = 30,
    max_retries = 3L,
    retry_delay = 2,
    backoff_multiplier = 1.5,
    captcha_backoff_base = 5,
    page_load_wait = 2,
    inter_search_delay = 1.5
  )
}

.mock_configure_search_factory <- function(initial_cfg) {
  state <- new.env(parent = emptyenv())
  state$current <- initial_cfg
  state$calls <- list()

  fn <- function(max_results = NULL,
                 timeout = NULL,
                 max_retries = NULL,
                 retry_delay = NULL,
                 backoff_multiplier = NULL,
                 captcha_backoff_base = NULL,
                 page_load_wait = NULL,
                 inter_search_delay = NULL,
                 conda_env = NULL) {
    call <- list(
      max_results = max_results,
      timeout = timeout,
      max_retries = max_retries,
      retry_delay = retry_delay,
      backoff_multiplier = backoff_multiplier,
      captcha_backoff_base = captcha_backoff_base,
      page_load_wait = page_load_wait,
      inter_search_delay = inter_search_delay,
      conda_env = conda_env
    )
    state$calls[[length(state$calls) + 1L]] <- call

    updates <- call[names(state$current)]
    for (nm in names(updates)) {
      if (!is.null(updates[[nm]])) {
        state$current[[nm]] <- updates[[nm]]
      }
    }

    state$current
  }

  list(fn = fn, state = state)
}

.make_webpage_fixture <- function(allow_read_webpages = TRUE, max_chars = 40000L) {
  list(
    allow_read_webpages = allow_read_webpages,
    relevance_mode = "lexical",
    heuristic_profile = "generic",
    embedding_provider = "auto",
    embedding_model = "text-embedding-3-small",
    timeout = 30,
    max_bytes = 1000000,
    max_chars = max_chars,
    max_chunks = 2L,
    chunk_chars = 2000L,
    embedding_api_base = NULL,
    prefilter_k = 20L,
    use_mmr = TRUE,
    mmr_lambda = 0.5,
    cache_enabled = TRUE,
    cache_max_entries = 64L,
    cache_max_text_chars = 50000L,
    blocked_cache_ttl_sec = 600L,
    blocked_cache_max_entries = 256L,
    blocked_probe_bytes = 65536L,
    blocked_detect_on_200 = TRUE,
    blocked_body_scan_bytes = 32768L,
    pdf_enabled = TRUE,
    pdf_timeout = 20,
    pdf_max_bytes = 8000000L,
    pdf_max_pages = 8L,
    pdf_max_text_chars = 40000L,
    user_agent = "asa-test"
  )
}

.mock_webpage_tool_factory <- function(initial_cfg) {
  state <- new.env(parent = emptyenv())
  state$current <- initial_cfg
  state$configure_calls <- list()
  state$clear_calls <- 0L

  tool <- list(
    configure_webpage_reader = function(allow_read_webpages = NULL,
                                        relevance_mode = NULL,
                                        heuristic_profile = NULL,
                                        embedding_provider = NULL,
                                        embedding_model = NULL,
                                        timeout = NULL,
                                        max_bytes = NULL,
                                        max_chars = NULL,
                                        max_chunks = NULL,
                                        chunk_chars = NULL,
                                        embedding_api_base = NULL,
                                        prefilter_k = NULL,
                                        use_mmr = NULL,
                                        mmr_lambda = NULL,
                                        cache_enabled = NULL,
                                        cache_max_entries = NULL,
                                        cache_max_text_chars = NULL,
                                        blocked_cache_ttl_sec = NULL,
                                        blocked_cache_max_entries = NULL,
                                        blocked_probe_bytes = NULL,
                                        blocked_detect_on_200 = NULL,
                                        blocked_body_scan_bytes = NULL,
                                        pdf_enabled = NULL,
                                        pdf_timeout = NULL,
                                        pdf_max_bytes = NULL,
                                        pdf_max_pages = NULL,
                                        pdf_max_text_chars = NULL,
                                        user_agent = NULL) {
      updates <- list(
        allow_read_webpages = allow_read_webpages,
        relevance_mode = relevance_mode,
        heuristic_profile = heuristic_profile,
        embedding_provider = embedding_provider,
        embedding_model = embedding_model,
        timeout = timeout,
        max_bytes = max_bytes,
        max_chars = max_chars,
        max_chunks = max_chunks,
        chunk_chars = chunk_chars,
        embedding_api_base = embedding_api_base,
        prefilter_k = prefilter_k,
        use_mmr = use_mmr,
        mmr_lambda = mmr_lambda,
        cache_enabled = cache_enabled,
        cache_max_entries = cache_max_entries,
        cache_max_text_chars = cache_max_text_chars,
        blocked_cache_ttl_sec = blocked_cache_ttl_sec,
        blocked_cache_max_entries = blocked_cache_max_entries,
        blocked_probe_bytes = blocked_probe_bytes,
        blocked_detect_on_200 = blocked_detect_on_200,
        blocked_body_scan_bytes = blocked_body_scan_bytes,
        pdf_enabled = pdf_enabled,
        pdf_timeout = pdf_timeout,
        pdf_max_bytes = pdf_max_bytes,
        pdf_max_pages = pdf_max_pages,
        pdf_max_text_chars = pdf_max_text_chars,
        user_agent = user_agent
      )
      state$configure_calls[[length(state$configure_calls) + 1L]] <- updates

      for (nm in names(updates)) {
        if (!is.null(updates[[nm]])) {
          state$current[[nm]] <- updates[[nm]]
        }
      }

      state$current
    },
    clear_webpage_reader_cache = function() {
      state$clear_calls <- state$clear_calls + 1L
      invisible(NULL)
    }
  )

  list(tool = tool, state = state)
}

test_that(".with_search_config skips configure/restore when requested settings already match", {
  fixture <- .make_search_fixture()
  mock <- .mock_configure_search_factory(fixture)

  testthat::local_mocked_bindings(
    configure_search = mock$fn,
    .get_default_conda_env = function() "asa_env",
    .package = "asa"
  )

  search <- asa::search_options(
    max_results = fixture$max_results,
    timeout = fixture$timeout,
    max_retries = fixture$max_retries,
    retry_delay = fixture$retry_delay,
    backoff_multiplier = fixture$backoff_multiplier,
    inter_search_delay = fixture$inter_search_delay
  )

  out <- asa:::.with_search_config(
    search = search,
    conda_env = "asa_env",
    fn = function() "ok"
  )

  expect_identical(out, "ok")
  expect_length(mock$state$calls, 1L)
  expect_equal(mock$state$current, fixture)
})

test_that(".with_search_config applies and restores when requested settings differ", {
  fixture <- .make_search_fixture()
  mock <- .mock_configure_search_factory(fixture)

  testthat::local_mocked_bindings(
    configure_search = mock$fn,
    .get_default_conda_env = function() "asa_env",
    .package = "asa"
  )

  changed <- fixture
  changed$max_results <- 25L

  search <- asa::search_options(
    max_results = changed$max_results,
    timeout = changed$timeout,
    max_retries = changed$max_retries,
    retry_delay = changed$retry_delay,
    backoff_multiplier = changed$backoff_multiplier,
    inter_search_delay = changed$inter_search_delay
  )

  inside <- NULL
  out <- asa:::.with_search_config(
    search = search,
    conda_env = "asa_env",
    fn = function() {
      inside <<- mock$state$current
      "ok"
    }
  )

  expect_identical(out, "ok")
  expect_equal(inside$max_results, changed$max_results)
  expect_length(mock$state$calls, 3L)
  expect_equal(mock$state$current, fixture)
})

test_that(".with_webpage_reader_config skips configure/restore when requested settings already match", {
  asa_ns_env <- get("asa_env", envir = asNamespace("asa"))
  old_tool <- asa_ns_env$webpage_tool
  on.exit({
    asa_ns_env$webpage_tool <- old_tool
  }, add = TRUE)

  fixture <- .make_webpage_fixture(allow_read_webpages = TRUE, max_chars = 40000L)
  mock <- .mock_webpage_tool_factory(fixture)
  asa_ns_env$webpage_tool <- mock$tool

  testthat::local_mocked_bindings(
    use_condaenv = function(conda_env, required = TRUE) invisible(NULL),
    .package = "reticulate"
  )

  out <- asa:::.with_webpage_reader_config(
    allow_read_webpages = TRUE,
    relevance_mode = "lexical",
    max_chars = 40000L,
    max_chunks = 2L,
    chunk_chars = 2000L,
    cache_enabled = TRUE,
    cache_max_entries = 64L,
    cache_max_text_chars = 50000L,
    user_agent = "asa-test",
    conda_env = "asa_env",
    fn = function() "ok"
  )

  expect_identical(out, "ok")
  expect_length(mock$state$configure_calls, 1L)
  expect_equal(mock$state$clear_calls, 2L)
  expect_equal(mock$state$current, fixture)
})

test_that(".with_webpage_reader_config applies and restores when requested settings differ", {
  asa_ns_env <- get("asa_env", envir = asNamespace("asa"))
  old_tool <- asa_ns_env$webpage_tool
  on.exit({
    asa_ns_env$webpage_tool <- old_tool
  }, add = TRUE)

  fixture <- .make_webpage_fixture(allow_read_webpages = FALSE, max_chars = 30000L)
  mock <- .mock_webpage_tool_factory(fixture)
  asa_ns_env$webpage_tool <- mock$tool

  testthat::local_mocked_bindings(
    use_condaenv = function(conda_env, required = TRUE) invisible(NULL),
    .package = "reticulate"
  )

  inside <- NULL
  out <- asa:::.with_webpage_reader_config(
    allow_read_webpages = TRUE,
    relevance_mode = "lexical",
    max_chars = 12345L,
    max_chunks = 2L,
    chunk_chars = 2000L,
    cache_enabled = TRUE,
    cache_max_entries = 64L,
    cache_max_text_chars = 50000L,
    blocked_detect_on_200 = FALSE,
    blocked_body_scan_bytes = 12345L,
    user_agent = "asa-test",
    conda_env = "asa_env",
    fn = function() {
      inside <<- mock$state$current
      "ok"
    }
  )

  expect_identical(out, "ok")
  expect_true(isTRUE(inside$allow_read_webpages))
  expect_equal(inside$max_chars, 12345L)
  expect_false(isTRUE(inside$blocked_detect_on_200))
  expect_equal(inside$blocked_body_scan_bytes, 12345L)
  expect_length(mock$state$configure_calls, 3L)
  expect_equal(mock$state$clear_calls, 2L)
  expect_equal(mock$state$current, fixture)
})
