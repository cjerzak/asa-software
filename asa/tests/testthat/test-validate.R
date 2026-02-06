# ============================================================================
# Tests for validation functions
# ============================================================================

# ============================================================================
# Core Primitives Tests
# ============================================================================

test_that(".validate_string rejects non-strings", {
  expect_error(.validate_string(123, "param"), "`param` must be a single character string")
  expect_error(.validate_string(c("a", "b"), "param"), "`param` must be a single character string")
  expect_error(.validate_string(NULL, "param"), "`param` must be a single character string")
  expect_error(.validate_string(list(), "param"), "`param` must be a single character string")
})

test_that(".validate_string rejects empty strings by default", {
  expect_error(.validate_string("", "param"), "not be empty")
  expect_error(.validate_string("   ", "param"), "not be empty")
  expect_error(.validate_string("\t\n", "param"), "not be empty")
  expect_silent(.validate_string("", "param", allow_empty = TRUE))
  expect_silent(.validate_string("   ", "param", allow_empty = TRUE))
})

test_that(".validate_string rejects NA by default", {
  expect_error(.validate_string(NA_character_, "param"), "not be NA")
  expect_silent(.validate_string(NA_character_, "param", allow_na = TRUE, allow_empty = TRUE))
})

test_that(".validate_string accepts valid strings", {
  expect_silent(.validate_string("hello", "param"))
  expect_silent(.validate_string("a", "param"))
  expect_silent(.validate_string("hello world", "param"))
})

test_that(".validate_positive rejects negative numbers", {
  expect_error(.validate_positive(-1, "param"), "be positive")
  expect_error(.validate_positive(-0.5, "param"), "be positive")
  expect_error(.validate_positive(0, "param"), "be positive")
  expect_silent(.validate_positive(0, "param", allow_zero = TRUE))
  expect_silent(.validate_positive(1, "param"))
  expect_silent(.validate_positive(0.5, "param"))
})

test_that(".validate_positive rejects non-numeric values", {
  expect_error(.validate_positive("5", "param"), "be a single")
  expect_error(.validate_positive(NA_real_, "param"), "be a single")
  expect_error(.validate_positive(NULL, "param"), "be a single")
  expect_error(.validate_positive(c(1, 2), "param"), "be a single")
})

test_that(".validate_positive enforces integer_only", {
  expect_error(.validate_positive(1.5, "param", integer_only = TRUE), "be an integer")
  expect_error(.validate_positive(2.1, "param", integer_only = TRUE), "be an integer")
  expect_silent(.validate_positive(1.5, "param", integer_only = FALSE))
  expect_silent(.validate_positive(2L, "param", integer_only = TRUE))
  expect_silent(.validate_positive(2.0, "param", integer_only = TRUE))  # 2.0 == 2L
})

test_that(".validate_logical rejects non-booleans", {
  expect_error(.validate_logical("TRUE", "param"), "be TRUE or FALSE")
  expect_error(.validate_logical(1, "param"), "be TRUE or FALSE")
  expect_error(.validate_logical(0, "param"), "be TRUE or FALSE")
  expect_error(.validate_logical(NA, "param"), "be TRUE or FALSE")
  expect_error(.validate_logical(NULL, "param"), "be TRUE or FALSE")
  expect_error(.validate_logical(c(TRUE, FALSE), "param"), "be TRUE or FALSE")
})

test_that(".validate_logical accepts TRUE and FALSE", {
  expect_silent(.validate_logical(TRUE, "param"))
  expect_silent(.validate_logical(FALSE, "param"))
})

test_that(".validate_choice rejects invalid choices", {
  expect_error(.validate_choice("invalid", "param", c("a", "b", "c")), 'be one of: "a", "b", "c"')
  expect_error(.validate_choice("A", "param", c("a", "b", "c")), 'be one of:')  # case sensitive
  expect_error(.validate_choice(1, "param", c("a", "b")), 'be one of:')
})

test_that(".validate_choice accepts valid choices", {
  expect_silent(.validate_choice("b", "param", c("a", "b", "c")))
  expect_silent(.validate_choice("a", "param", c("a", "b", "c")))
  expect_silent(.validate_choice("c", "param", c("a", "b", "c")))
})

test_that(".validate_proxy_url accepts valid formats", {
  expect_silent(.validate_proxy_url(NULL, "proxy"))
  expect_silent(.validate_proxy_url(NA, "proxy"))
  expect_silent(.validate_proxy_url("socks5h://127.0.0.1:9050", "proxy"))
  expect_silent(.validate_proxy_url("socks5://localhost:1080", "proxy"))
  expect_silent(.validate_proxy_url("socks5h://192.168.1.1:9999", "proxy"))
  expect_silent(.validate_proxy_url("http://proxy.com:8080", "proxy"))
  expect_silent(.validate_proxy_url("https://proxy.com:8443", "proxy"))
})

test_that(".validate_proxy_url rejects invalid formats", {
  expect_error(.validate_proxy_url("https://proxy.com", "proxy"), "proxy URL")  # missing port
  expect_error(.validate_proxy_url("127.0.0.1:9050", "proxy"), "proxy URL")     # missing scheme
  expect_error(.validate_proxy_url("socks5://localhost", "proxy"), "proxy URL") # missing port
  expect_error(.validate_proxy_url("ftp://localhost:21", "proxy"), "proxy URL")
})

test_that(".validate_conda_env accepts valid names", {
  expect_silent(.validate_conda_env("asa_env", "conda_env"))
  expect_silent(.validate_conda_env("my-env-123", "conda_env"))
  expect_silent(.validate_conda_env("TestEnv", "conda_env"))
  expect_silent(.validate_conda_env("a", "conda_env"))
  expect_silent(.validate_conda_env("myEnv2024", "conda_env"))
})

test_that(".validate_conda_env rejects invalid names", {
  expect_error(.validate_conda_env("123env", "conda_env"), "start with letter")
  expect_error(.validate_conda_env("my env", "conda_env"), "valid conda environment name")
  expect_error(.validate_conda_env("env@name", "conda_env"), "valid conda environment name")
  expect_error(.validate_conda_env("env.name", "conda_env"), "valid conda environment name")
  expect_error(.validate_conda_env("_env", "conda_env"), "start with letter")
  expect_error(.validate_conda_env("-env", "conda_env"), "start with letter")
})

test_that(".validate_string_vector accepts valid vectors", {
  expect_silent(.validate_string_vector("single", "param"))
  expect_silent(.validate_string_vector(c("a", "b", "c"), "param"))
  expect_silent(.validate_string_vector(c("hello", "world"), "param", min_length = 2L))
})

test_that(".validate_string_vector rejects invalid vectors", {
  expect_error(.validate_string_vector(c(), "param"), "at least 1 element")
  expect_error(.validate_string_vector(1:3, "param"), "character vector")
  expect_error(.validate_string_vector(list("a", "b"), "param"), "character vector")
  expect_error(.validate_string_vector(c("a"), "param", min_length = 2L), "at least 2")
})

test_that(".validate_string_vector rejects vectors with NA", {
  expect_error(.validate_string_vector(c("a", NA, "b"), "param"), "NA values")
  expect_error(.validate_string_vector(NA_character_, "param"), "NA values")
})

test_that(".validate_string_vector rejects vectors with empty strings", {
  expect_error(.validate_string_vector(c("a", "", "b"), "param"), "empty or whitespace")
  expect_error(.validate_string_vector(c("a", "   ", "b"), "param"), "empty or whitespace")
})

test_that(".validate_dataframe rejects non-dataframes", {
  expect_error(.validate_dataframe(list(a = 1), "df"), "be a data frame")
  expect_error(.validate_dataframe(matrix(1:4, 2, 2), "df"), "be a data frame")
  expect_error(.validate_dataframe(c(1, 2, 3), "df"), "be a data frame")
  expect_error(.validate_dataframe(NULL, "df"), "be a data frame")
})

test_that(".validate_dataframe accepts valid dataframes", {
  df <- data.frame(a = 1, b = 2)
  expect_silent(.validate_dataframe(df, "df"))
  expect_silent(.validate_dataframe(df, "df", required_cols = "a"))
  expect_silent(.validate_dataframe(df, "df", required_cols = c("a", "b")))
})

test_that(".validate_dataframe checks required columns", {
  df <- data.frame(a = 1, b = 2)
  expect_error(.validate_dataframe(df, "df", required_cols = c("a", "c")), "missing: c")
  expect_error(.validate_dataframe(df, "df", required_cols = "x"), "missing: x")
})

test_that(".validate_range checks bounds", {
  expect_silent(.validate_range(5, "param", min = 1, max = 10))
  expect_silent(.validate_range(1, "param", min = 1))
  expect_silent(.validate_range(10, "param", max = 10))
  expect_error(.validate_range(0, "param", min = 1), "be >= 1")
  expect_error(.validate_range(15, "param", max = 10), "be <= 10")
  expect_error(.validate_range(-5, "param", min = 0), "be >= 0")
})

test_that(".validate_consistency stops on false condition", {
  expect_silent(.validate_consistency(TRUE, "msg", "fix"))
  expect_silent(.validate_consistency(1 > 0, "msg", "fix"))
  expect_error(.validate_consistency(FALSE, "Error message", "How to fix"), "Error message")
  expect_error(.validate_consistency(FALSE, "Error message", "How to fix"), "Fix: How to fix")
})

# ============================================================================
# Function-Level Validator Tests
# ============================================================================

test_that(".validate_initialize_agent allows memory params regardless of relative sizes", {
  expect_silent(
    .validate_initialize_agent(
      backend = "openai", model = "gpt-4", conda_env = "asa_env",
      proxy = NULL, use_browser = TRUE, use_memory_folding = TRUE,
      memory_threshold = 2L, memory_keep_recent = 4L,
      rate_limit = 0.2, timeout = 120L, verbose = TRUE
    )
  )
})

test_that(".validate_initialize_agent allows valid memory params when folding disabled", {
  # Folding disabled should still accept memory params without cross-checks
  expect_silent(
    .validate_initialize_agent(
      backend = "openai", model = "gpt-4", conda_env = "asa_env",
      proxy = NULL, use_browser = TRUE, use_memory_folding = FALSE,
      memory_threshold = 2L, memory_keep_recent = 4L,
      rate_limit = 0.2, timeout = 120L, verbose = TRUE
    )
  )
})

test_that(".validate_initialize_agent rejects invalid rate_limit", {
  expect_error(
    .validate_initialize_agent(
      backend = "openai", model = "gpt-4", conda_env = "asa_env",
      proxy = NULL, use_browser = TRUE, use_memory_folding = TRUE,
      memory_threshold = 4L, memory_keep_recent = 2L,
      rate_limit = -0.1, timeout = 120L, verbose = TRUE
    ),
    "rate_limit.*must.*be positive"
  )
})

test_that(".validate_run_task accepts valid output_format values", {
  expect_silent(.validate_run_task("prompt", "text", NULL, FALSE))
  expect_silent(.validate_run_task("prompt", "json", NULL, FALSE))
  expect_silent(.validate_run_task("prompt", c("field1", "field2"), NULL, FALSE))
  expect_silent(.validate_run_task("prompt", c("a", "b", "c"), NULL, FALSE))
})

test_that(".validate_run_task rejects invalid output_format", {
  expect_error(.validate_run_task("prompt", "xml", NULL, FALSE), 'be one of: "text", "json", "raw"')
  expect_error(.validate_run_task("prompt", 123, NULL, FALSE), 'be "text", "json"')
  expect_error(.validate_run_task("prompt", list("a"), NULL, FALSE), 'be "text", "json"')
})

test_that(".validate_run_task rejects empty prompts", {
  expect_error(.validate_run_task("", "text", NULL, FALSE), "not be empty")
  expect_error(.validate_run_task("   ", "text", NULL, FALSE), "not be empty")
  expect_error(.validate_run_task("\t", "text", NULL, FALSE), "not be empty")
})

test_that(".validate_run_task_batch validates prompts vector", {
  expect_silent(.validate_run_task_batch(c("a", "b"), "text", NULL, FALSE, 4L, TRUE))
  expect_error(.validate_run_task_batch(c(), "text", NULL, FALSE, 4L, TRUE), "at least 1")
  expect_error(.validate_run_task_batch(c("a", NA), "text", NULL, FALSE, 4L, TRUE), "NA values")
})

test_that(".validate_run_task_batch validates prompts dataframe", {
  df_good <- data.frame(prompt = c("a", "b"), stringsAsFactors = FALSE)
  df_bad_col <- data.frame(question = c("a", "b"), stringsAsFactors = FALSE)
  df_na <- data.frame(prompt = c("a", NA), stringsAsFactors = FALSE)

  expect_silent(.validate_run_task_batch(df_good, "text", NULL, FALSE, 4L, TRUE))
  expect_error(.validate_run_task_batch(df_bad_col, "text", NULL, FALSE, 4L, TRUE), "missing: prompt")
  expect_error(.validate_run_task_batch(df_na, "text", NULL, FALSE, 4L, TRUE), "NA")
})

test_that(".validate_run_task_batch rejects invalid workers", {
  expect_error(.validate_run_task_batch(c("a"), "text", NULL, FALSE, -1L, TRUE), "be positive")
  expect_error(.validate_run_task_batch(c("a"), "text", NULL, FALSE, 0L, TRUE), "be positive")
  expect_error(.validate_run_task_batch(c("a"), "text", NULL, FALSE, 1.5, TRUE), "be an integer")
})

test_that(".validate_run_agent validates recursion_limit", {
  expect_silent(.validate_run_agent("prompt", NULL, NULL, FALSE))  # NULL recursion_limit is OK
  expect_silent(.validate_run_agent("prompt", NULL, 50L, FALSE))
  expect_error(.validate_run_agent("prompt", NULL, 0L, FALSE), "be positive")
  expect_error(.validate_run_agent("prompt", NULL, 600L, FALSE), "be <= 500")
  expect_error(.validate_run_agent("prompt", NULL, -5L, FALSE), "be positive")
})

test_that(".validate_build_backend validates python_version format", {
  expect_silent(.validate_build_backend("asa_env", "auto", "3.13"))
  expect_silent(.validate_build_backend("asa_env", "auto", "3.9"))
  expect_silent(.validate_build_backend("my_env", "/usr/bin/conda", "3.11"))

  expect_error(.validate_build_backend("asa_env", "auto", "python3.13"), 'format "X.Y"')
  expect_error(.validate_build_backend("asa_env", "auto", "3"), 'format "X.Y"')
  expect_error(.validate_build_backend("asa_env", "auto", "3.11.5"), 'format "X.Y"')
  expect_error(.validate_build_backend("asa_env", "auto", "three.eleven"), 'format "X.Y"')
})

test_that(".validate_configure_search validates numeric parameters", {
  # All NULL is valid (use defaults)
  expect_silent(.validate_configure_search(
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "asa_env"
  ))

  # Valid values
  expect_silent(.validate_configure_search(
    max_results = 10L, timeout = 15, max_retries = 3L,
    retry_delay = 2, backoff_multiplier = 1.5,
    captcha_backoff_base = 3, page_load_wait = 2,
    inter_search_delay = 0.5, conda_env = "asa_env"
  ))

  # Invalid max_results
  expect_error(.validate_configure_search(
    max_results = -5L, timeout = NULL, max_retries = NULL,
    retry_delay = NULL, backoff_multiplier = NULL,
    captcha_backoff_base = NULL, page_load_wait = NULL,
    inter_search_delay = NULL, conda_env = "asa_env"
  ), "max_results.*be positive")

  # max_results out of range
  expect_error(.validate_configure_search(
    max_results = 200L, timeout = NULL, max_retries = NULL,
    retry_delay = NULL, backoff_multiplier = NULL,
    captcha_backoff_base = NULL, page_load_wait = NULL,
    inter_search_delay = NULL, conda_env = "asa_env"
  ), "max_results.*be <= 100")

  # Invalid backoff_multiplier
  expect_error(.validate_configure_search(
    max_results = NULL, timeout = NULL, max_retries = NULL,
    retry_delay = NULL, backoff_multiplier = 10,
    captcha_backoff_base = NULL, page_load_wait = NULL,
    inter_search_delay = NULL, conda_env = "asa_env"
  ), "backoff_multiplier.*be <= 5")
})

test_that(".validate_process_outputs validates inputs", {
  df <- data.frame(raw_output = c("a", "b"), stringsAsFactors = FALSE)
  expect_silent(.validate_process_outputs(df, FALSE, 10L))

  df_bad <- data.frame(output = c("a", "b"), stringsAsFactors = FALSE)
  expect_error(.validate_process_outputs(df_bad, FALSE, 10L), "raw_output")

  expect_error(.validate_process_outputs(df, "no", 10L), "be TRUE or FALSE")
  expect_error(.validate_process_outputs(df, FALSE, -1L), "be positive")
})

# ============================================================================
# Error Message Format Tests
# ============================================================================

test_that("error messages include Got and Fix sections", {
  tryCatch(
    .validate_positive(-5, "workers"),
    error = function(e) {
      msg <- conditionMessage(e)
      expect_match(msg, "Got:")
      expect_match(msg, "Fix:")
      expect_match(msg, "-5")  # actual value shown
    }
  )
})

test_that("error messages are actionable", {
  tryCatch(
    .validate_proxy_url("http://bad", "proxy"),
    error = function(e) {
      msg <- conditionMessage(e)
      expect_match(msg, "socks5h://127.0.0.1:9050")  # Example of correct format
    }
  )

  tryCatch(
    .validate_conda_env("123bad", "conda_env"),
    error = function(e) {
      msg <- conditionMessage(e)
      expect_match(msg, "asa_env")  # Example of correct format
    }
  )
})

test_that("error messages handle different value types gracefully", {
  # Long vector
  tryCatch(
    .validate_string(1:100, "param"),
    error = function(e) {
      msg <- conditionMessage(e)
      expect_match(msg, "integer of length 100")
    }
  )

  # Empty string
  tryCatch(
    .validate_string("", "param"),
    error = function(e) {
      msg <- conditionMessage(e)
      expect_match(msg, '""')
    }
  )

  # NULL - validation stops before showing value because it's not numeric
  tryCatch(
    .validate_positive(NULL, "param"),
    error = function(e) {
      msg <- conditionMessage(e)
      expect_match(msg, "must be a single")  # Error stops at type check
    }
  )
})

# ============================================================================
# Temporal Filtering Validation Tests
# ============================================================================

test_that(".validate_temporal returns NULL for NULL input", {
  expect_null(.validate_temporal(NULL))
})

test_that(".validate_temporal rejects non-list input", {
  expect_error(.validate_temporal("string"), "be a list or NULL")
  expect_error(.validate_temporal(123), "be a list or NULL")
  expect_error(.validate_temporal(c("a", "b")), "be a list or NULL")
})

test_that(".validate_temporal accepts valid empty list", {
  expect_silent(.validate_temporal(list()))
})

test_that(".validate_temporal rejects invalid time_filter values", {
  expect_error(.validate_temporal(list(time_filter = "x")), "time_filter")
  expect_error(.validate_temporal(list(time_filter = "day")), "time_filter")
  expect_error(.validate_temporal(list(time_filter = "year")), "time_filter")
  expect_error(.validate_temporal(list(time_filter = 123)), "time_filter")
  expect_error(.validate_temporal(list(time_filter = TRUE)), "time_filter")
})

test_that(".validate_temporal accepts valid time_filter values", {
  expect_silent(.validate_temporal(list(time_filter = "d")))
  expect_silent(.validate_temporal(list(time_filter = "w")))
  expect_silent(.validate_temporal(list(time_filter = "m")))
  expect_silent(.validate_temporal(list(time_filter = "y")))
})

test_that(".validate_temporal rejects invalid after date format", {
  expect_error(.validate_temporal(list(after = "not-a-date")), "after.*valid")
  expect_error(.validate_temporal(list(after = "January 1, 2024")), "after.*valid")
  expect_error(.validate_temporal(list(after = 12345)), "after.*character string")
  expect_error(.validate_temporal(list(after = c("2024-01-01", "2024-02-01"))), "after.*single character")
})

test_that(".validate_temporal rejects invalid after dates", {
  expect_error(.validate_temporal(list(after = "2024-13-01")), "after.*valid")  # Invalid month
  expect_error(.validate_temporal(list(after = "2024-00-15")), "after.*valid")  # Invalid month
})

test_that(".validate_temporal accepts valid after dates", {
  expect_silent(.validate_temporal(list(after = "2024-01-01")))
  expect_silent(.validate_temporal(list(after = "2020-12-31")))
  expect_silent(.validate_temporal(list(after = "1999-06-15")))
})

test_that(".validate_temporal rejects invalid before date format", {
  expect_error(.validate_temporal(list(before = "not-a-date")), "before.*valid")
  expect_error(.validate_temporal(list(before = "Dec 25, 2024")), "before.*valid")
  expect_error(.validate_temporal(list(before = 20240101)), "before.*character string")
})

test_that(".validate_temporal rejects invalid before dates", {
  expect_error(.validate_temporal(list(before = "2024-02-30")), "before.*valid")  # Invalid day
  expect_error(.validate_temporal(list(before = "2024-00-15")), "before.*valid")  # Invalid month
})

test_that(".validate_temporal accepts valid before dates", {
  expect_silent(.validate_temporal(list(before = "2024-12-31")))
  expect_silent(.validate_temporal(list(before = "2025-01-01")))
})

test_that(".validate_temporal rejects after >= before", {
  # after == before
  expect_error(
    .validate_temporal(list(after = "2024-01-01", before = "2024-01-01")),
    "after < before"
  )
  # after > before
  expect_error(
    .validate_temporal(list(after = "2024-06-01", before = "2024-01-01")),
    "after < before"
  )
})

test_that(".validate_temporal accepts valid date ranges", {
  expect_silent(.validate_temporal(list(after = "2020-01-01", before = "2024-01-01")))
  expect_silent(.validate_temporal(list(after = "2024-01-01", before = "2024-01-02")))
})

test_that(".validate_temporal rejects invalid strictness values", {
  expect_error(.validate_temporal(list(strictness = "very_strict")), "strictness")
  expect_error(.validate_temporal(list(strictness = "none")), "strictness")
  expect_error(.validate_temporal(list(strictness = TRUE)), "strictness")
})

test_that(".validate_temporal accepts valid strictness values", {
  expect_silent(.validate_temporal(list(strictness = "best_effort")))
  expect_silent(.validate_temporal(list(strictness = "strict")))
})

test_that(".validate_temporal rejects invalid use_wayback values", {
  expect_error(.validate_temporal(list(use_wayback = "yes")), "use_wayback.*TRUE or FALSE")
  expect_error(.validate_temporal(list(use_wayback = 1)), "use_wayback.*TRUE or FALSE")
  expect_error(.validate_temporal(list(use_wayback = c(TRUE, FALSE))), "use_wayback.*TRUE or FALSE")
})

test_that(".validate_temporal accepts valid use_wayback values", {
  expect_silent(.validate_temporal(list(use_wayback = TRUE)))
  expect_silent(.validate_temporal(list(use_wayback = FALSE)))
})

test_that(".validate_temporal accepts complex valid temporal lists with conflict warning", {
  # This combination triggers a warning about time_filter conflicting
  # with the date range (after = 2020 is outside past year).
  # Verify that validation passes but warns about the conflict.

  expect_warning(
    .validate_temporal(list(
      time_filter = "y",
      after = "2020-01-01",
      before = "2024-12-31",
      strictness = "best_effort",
      use_wayback = FALSE
    )),
    "Temporal conflict"
  )
})

test_that(".validate_temporal accepts complex valid temporal lists without conflict", {
  # Use a date range compatible with time_filter to avoid warnings
  today <- Sys.Date()
  expect_silent(.validate_temporal(list(
    time_filter = "y",
    after = as.character(today - 100),
    before = as.character(today - 1),
    strictness = "best_effort",
    use_wayback = FALSE
  )))
})

# ============================================================================
# API Key Validation Tests
# ============================================================================

test_that(".validate_api_key passes for exo backend (no key needed)", {
  expect_silent(.validate_api_key("exo"))
})

test_that(".validate_api_key errors for unknown backend", {
  expect_error(.validate_api_key("unknown_backend"), "Unknown backend")
})

test_that(".validate_api_key errors when env var is not set", {
  withr::with_envvar(c(OPENAI_API_KEY = ""), {
    expect_error(.validate_api_key("openai"), "OPENAI_API_KEY")
  })
})

test_that(".validate_api_key errors for groq when key missing", {
  withr::with_envvar(c(GROQ_API_KEY = ""), {
    expect_error(.validate_api_key("groq"), "GROQ_API_KEY")
  })
})

test_that(".validate_api_key errors for xai when key missing", {
  withr::with_envvar(c(XAI_API_KEY = ""), {
    expect_error(.validate_api_key("xai"), "XAI_API_KEY")
  })
})

test_that(".validate_api_key errors for gemini when both keys missing", {
  withr::with_envvar(c(GOOGLE_API_KEY = "", GEMINI_API_KEY = "", GOOGLE_GENAI_USE_VERTEXAI = ""), {
    expect_error(.validate_api_key("gemini"), "GOOGLE_API_KEY.*GEMINI_API_KEY|GEMINI_API_KEY.*GOOGLE_API_KEY")
  })
})

test_that(".validate_api_key passes for gemini with GOOGLE_API_KEY set", {
  withr::with_envvar(c(GOOGLE_API_KEY = "valid-api-key-1234567890"), {
    expect_silent(.validate_api_key("gemini"))
  })
})

test_that(".validate_api_key passes for gemini with GEMINI_API_KEY set", {
  withr::with_envvar(c(GOOGLE_API_KEY = "", GEMINI_API_KEY = "valid-api-key-1234567890"), {
    expect_silent(.validate_api_key("gemini"))
  })
})

test_that(".validate_api_key warns for gemini vertex mode without key", {
  withr::with_envvar(c(GOOGLE_API_KEY = "", GEMINI_API_KEY = "", GOOGLE_GENAI_USE_VERTEXAI = "true"), {
    expect_warning(.validate_api_key("gemini"), "Vertex AI")
  })
})

test_that(".validate_api_key errors for openrouter when key missing", {
  withr::with_envvar(c(OPENROUTER_API_KEY = ""), {
    expect_error(.validate_api_key("openrouter"), "OPENROUTER_API_KEY")
  })
})

test_that(".validate_api_key warns for short keys", {
  withr::with_envvar(c(OPENAI_API_KEY = "short"), {
    expect_warning(.validate_api_key("openai"), "too short")
  })
})

test_that(".validate_api_key warns for placeholder keys", {
  withr::with_envvar(c(OPENAI_API_KEY = "your-api-key-here-placeholder"), {
    expect_warning(.validate_api_key("openai"), "placeholder")
  })

  withr::with_envvar(c(OPENAI_API_KEY = "test_key_example_1234567890"), {
    expect_warning(.validate_api_key("openai"), "placeholder")
  })
})

test_that(".validate_api_key passes for valid-length keys", {
  withr::with_envvar(c(OPENAI_API_KEY = "sk-abcdefghij1234567890abcdefghij"), {
    expect_silent(.validate_api_key("openai"))
  })
})

# ============================================================================
# S3 Constructor Validator Tests
# ============================================================================

test_that(".validate_asa_agent rejects invalid backend", {
  expect_error(.validate_asa_agent(NULL, 123, "gpt-4", list()), "backend")
  expect_error(.validate_asa_agent(NULL, "", "gpt-4", list()), "not be empty")
})

test_that(".validate_asa_agent rejects invalid model", {
  expect_error(.validate_asa_agent(NULL, "openai", 123, list()), "model")
  expect_error(.validate_asa_agent(NULL, "openai", "", list()), "not be empty")
})

test_that(".validate_asa_agent rejects non-list config", {
  expect_error(.validate_asa_agent(NULL, "openai", "gpt-4", "not-a-list"), "config.*be a list")
})

test_that(".validate_asa_agent accepts valid inputs", {
  expect_silent(.validate_asa_agent(NULL, "openai", "gpt-4", list()))
  expect_silent(.validate_asa_agent(NULL, "groq", "llama-3", list(use_memory_folding = TRUE)))
})

test_that(".validate_asa_response rejects invalid status_code", {
  expect_error(.validate_asa_response("msg", -1L, NULL, "", 1.0, "prompt"), "be positive")
  expect_error(.validate_asa_response("msg", "200", NULL, "", 1.0, "prompt"), "be a single")
})

test_that(".validate_asa_response rejects invalid elapsed_time", {
  expect_error(.validate_asa_response("msg", 200L, NULL, "", -1.0, "prompt"), "non-negative")
})

test_that(".validate_asa_response rejects non-string trace", {
  expect_error(.validate_asa_response("msg", 200L, NULL, 123, 1.0, "prompt"), "character string")
})

test_that(".validate_asa_response accepts valid inputs", {
  expect_silent(.validate_asa_response("msg", 200L, NULL, "", 1.0, "prompt"))
  expect_silent(.validate_asa_response("msg", 200L, NULL, "trace data", 0.0, "test prompt"))
})

test_that(".validate_asa_response accepts valid fold_stats", {
  expect_silent(.validate_asa_response("msg", 200L, NULL, "", 1.0, "prompt", fold_stats = list()))
  expect_silent(.validate_asa_response("msg", 200L, NULL, "", 1.0, "prompt", fold_stats = list(
    fold_count = 1L,
    fold_messages_removed = 3L,
    fold_total_messages_removed = 5L,
    fold_chars_input = 800L,
    fold_summary_chars = 200L
  )))
})

test_that(".validate_asa_response accepts fold_count as error message in fold_stats", {
  expect_silent(.validate_asa_response("msg", 200L, NULL, "", 1.0, "prompt", fold_stats = list(
    fold_count = "Error before fold count could be determined: connection timeout"
  )))
})

test_that(".validate_asa_response rejects non-list fold_stats", {
  expect_error(.validate_asa_response("msg", 200L, NULL, "", 1.0, "prompt", fold_stats = "not-a-list"), "be a list")
  expect_error(.validate_asa_response("msg", 200L, NULL, "", 1.0, "prompt", fold_stats = 42), "be a list")
})

test_that(".validate_asa_result rejects invalid status", {
  expect_error(.validate_asa_result("prompt", "msg", NULL, "", 1.0, "invalid"), "be one of")
})

test_that(".validate_asa_result rejects non-list parsed", {
  expect_error(.validate_asa_result("prompt", "msg", "not-a-list", "", 1.0, "success"), "be NULL or a list")
})

test_that(".validate_asa_result rejects non-string raw_output", {
  expect_error(.validate_asa_result("prompt", "msg", NULL, 123, 1.0, "success"), "character string")
})

test_that(".validate_asa_result accepts valid inputs", {
  expect_silent(.validate_asa_result("prompt", "msg", NULL, "", 1.0, "success"))
  expect_silent(.validate_asa_result("prompt", "msg", list(a = 1), "raw", 0.0, "error"))
})

# ============================================================================
# Proxy URL Regex Edge Case Tests
# ============================================================================

test_that(".validate_proxy_url accepts valid auth formats", {
  expect_silent(.validate_proxy_url("socks5h://user:pass@127.0.0.1:9050", "proxy"))
  expect_silent(.validate_proxy_url("http://admin:secret@proxy.com:8080", "proxy"))
})

test_that(".validate_proxy_url rejects malformed formats", {
  # Missing port after auth
  expect_error(.validate_proxy_url("socks5h://user:pass@127.0.0.1", "proxy"), "proxy URL")
  # No scheme
  expect_error(.validate_proxy_url("user:pass@127.0.0.1:9050", "proxy"), "proxy URL")
  # Invalid scheme
  expect_error(.validate_proxy_url("ftp://user:pass@127.0.0.1:9050", "proxy"), "proxy URL")
})
