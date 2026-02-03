# test-selenium-tier.R
# Deterministic tests that exercise the Selenium browser tier against local HTML fixtures.

.get_python_path_selenium <- function() {
  python_path <- system.file("python", package = "asa")
  if (python_path == "" || !dir.exists(python_path)) {
    # Development path fallback
    python_path <- file.path(getwd(), "inst/python")
    if (!dir.exists(python_path)) {
      python_path <- file.path(dirname(getwd()), "asa/inst/python")
    }
  }
  python_path
}

.skip_if_no_python_selenium <- function() {
  python_path <- .get_python_path_selenium()
  if (python_path == "" || !dir.exists(python_path)) {
    skip("Python modules not found")
  }
  conda_env <- tryCatch(asa:::.get_default_conda_env(), error = function(e) NULL)
  if (!is.null(conda_env) && is.character(conda_env) && nzchar(conda_env)) {
    try(reticulate::use_condaenv(conda_env, required = FALSE), silent = TRUE)
  }
  if (!reticulate::py_available(initialize = TRUE)) {
    skip("Python not available")
  }
  python_path
}

.skip_if_missing_python_modules_selenium <- function(modules) {
  if (is.null(modules) || length(modules) == 0) {
    return(invisible(TRUE))
  }
  for (module in modules) {
    if (!reticulate::py_module_available(module)) {
      skip(paste0("Python module not available: ", module))
    }
  }
  invisible(TRUE)
}

.import_custom_ddg_selenium <- function(python_path) {
  tryCatch(
    reticulate::import_from_path("custom_ddg_production", path = python_path),
    error = function(e) {
      skip(paste0("Failed to import custom_ddg_production: ", conditionMessage(e)))
    }
  )
}

.file_url <- function(path) {
  paste0("file://", normalizePath(path, winslash = "/", mustWork = TRUE))
}

.configure_fast_search <- function(ddg) {
  ddg$configure_search(
    max_results = 3L,
    timeout = 0.2,
    max_retries = 1L,
    retry_delay = 0.0,
    backoff_multiplier = 1.0,
    captcha_backoff_base = 0.0,
    page_load_wait = 0.0,
    inter_search_delay = 0.0,
    humanize_timing = FALSE,
    jitter_factor = 0.0
  )
}

.install_primp_stub <- function(fixture_path) {
  Sys.setenv(
    ASA_PRIMP_FIXTURE = normalizePath(fixture_path, winslash = "/", mustWork = TRUE)
  )
  reticulate::py_run_string(
    "
import os
import sys
import types

_fixture = os.environ.get('ASA_PRIMP_FIXTURE')

class _StubResponse:
    def __init__(self, text):
        self.status_code = 200
        self.text = text

class _StubClient:
    def __init__(self, *args, **kwargs):
        pass
    def headers_update(self, headers):
        return None
    def get(self, url, params=None, timeout=None):
        with open(_fixture, 'r', encoding='utf-8') as handle:
            text = handle.read()
        return _StubResponse(text)
    def close(self):
        return None

sys.modules['primp'] = types.SimpleNamespace(Client=_StubClient)
"
  )
}

.install_ddgs_stub <- function(ddg) {
  original_ddgs <- reticulate::py_get_attr(ddg, "DDGS", convert = FALSE)
  reticulate::py_run_string(
    "
class _StubDDGS:
    def __init__(self, *args, **kwargs):
        pass
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False
    def text(self, query, max_results=None, region='__region_default__', safesearch='__safesearch_default__', timelimit='__timelimit_default__'):
        return [{
            'title': 'Example',
            'href': 'https://example.com',
            'body': 'Stub snippet',
            'region': region,
            'safesearch': safesearch,
            'timelimit': timelimit
        }]
"
  )
  stub_ddgs <- reticulate::py_eval("_StubDDGS", convert = FALSE)
  reticulate::py_set_attr(ddg, "DDGS", stub_ddgs)
  function() {
    reticulate::py_set_attr(ddg, "DDGS", original_ddgs)
  }
}

test_that("Selenium tier can load and parse a local result fixture", {
  skip_on_cran()
  run_selenium <- tolower(Sys.getenv("ASA_RUN_SELENIUM_TESTS")) %in% c("true", "1", "yes")
  skip_if(!run_selenium, "Set ASA_RUN_SELENIUM_TESTS=true to run Selenium tier tests")

  python_path <- .skip_if_no_python_selenium()
  .skip_if_missing_python_modules_selenium(c("selenium", "bs4"))

  ddg <- .import_custom_ddg_selenium(python_path)

  setTimeLimit(elapsed = 60, transient = TRUE)
  on.exit(setTimeLimit(cpu = Inf, elapsed = Inf, transient = FALSE), add = TRUE)

  fixture <- tempfile(fileext = ".html")
  writeLines(
    c(
      "<!doctype html>",
      "<html><head><meta charset='utf-8'></head><body>",
      "<a class='result__a' href='https://example.com'>Example</a>",
      "<div class='result__snippet'>Snippet text</div>",
      "</body></html>"
    ),
    fixture
  )

  cfg <- ddg$SearchConfig(
    max_results = 3L,
    timeout = 0.5,
    max_retries = 1L,
    retry_delay = 0.0,
    backoff_multiplier = 1.0,
    captcha_backoff_base = 0.0,
    page_load_wait = 0.0,
    inter_search_delay = 0.0,
    humanize_timing = FALSE,
    jitter_factor = 0.0,
    allow_direct_fallback = FALSE
  )

  res <- ddg$`_browser_search`(
    query = "irrelevant",
    max_results = 3L,
    proxy = NULL,
    headers = NULL,
    headless = TRUE,
    bypass_proxy_for_driver = TRUE,
    timeout = 0.5,
    max_retries = 1L,
    url_override = .file_url(fixture),
    config = cfg
  )

  expect_true(is.list(res) && length(res) >= 1)
  expect_equal(res[[1]][["_tier"]], "selenium")
  expect_match(res[[1]][["body"]], "__START_OF_SOURCE", fixed = TRUE)
})

test_that("Selenium tier raises a timeout on a local empty fixture", {
  skip_on_cran()
  run_selenium <- tolower(Sys.getenv("ASA_RUN_SELENIUM_TESTS")) %in% c("true", "1", "yes")
  skip_if(!run_selenium, "Set ASA_RUN_SELENIUM_TESTS=true to run Selenium tier tests")

  python_path <- .skip_if_no_python_selenium()
  .skip_if_missing_python_modules_selenium(c("selenium", "bs4"))

  ddg <- .import_custom_ddg_selenium(python_path)

  setTimeLimit(elapsed = 60, transient = TRUE)
  on.exit(setTimeLimit(cpu = Inf, elapsed = Inf, transient = FALSE), add = TRUE)

  fixture <- tempfile(fileext = ".html")
  writeLines(
    c(
      "<!doctype html>",
      "<html><head><meta charset='utf-8'></head><body>",
      "<p>No results here</p>",
      "</body></html>"
    ),
    fixture
  )

  cfg <- ddg$SearchConfig(
    max_results = 3L,
    timeout = 0.2,
    max_retries = 1L,
    retry_delay = 0.0,
    backoff_multiplier = 1.0,
    captcha_backoff_base = 0.0,
    page_load_wait = 0.0,
    inter_search_delay = 0.0,
    humanize_timing = FALSE,
    jitter_factor = 0.0,
    allow_direct_fallback = FALSE
  )

  expect_error(
    ddg$`_browser_search`(
      query = "irrelevant",
      max_results = 3L,
      proxy = NULL,
      headers = NULL,
      headless = TRUE,
      bypass_proxy_for_driver = TRUE,
      timeout = 0.2,
      max_retries = 1L,
      url_override = .file_url(fixture),
      config = cfg
    ),
    regexp = "Timeout|timeout"
  )
})

test_that("PRIMP tier parses a local fixture without network access", {
  skip_on_cran()
  run_selenium <- tolower(Sys.getenv("ASA_RUN_SELENIUM_TESTS")) %in% c("true", "1", "yes")
  skip_if(!run_selenium, "Set ASA_RUN_SELENIUM_TESTS=true to run Selenium tier tests")

  python_path <- .skip_if_no_python_selenium()
  .skip_if_missing_python_modules_selenium(c("selenium", "bs4", "ddgs", "primp"))

  ddg <- .import_custom_ddg_selenium(python_path)
  .configure_fast_search(ddg)

  fixture <- tempfile(fileext = ".html")
  writeLines(
    c(
      "<!doctype html>",
      "<html><head><meta charset='utf-8'></head><body>",
      "<a class='result__a' href='https://example.com'>Example</a>",
      "<div class='result__snippet'>Stub snippet</div>",
      "</body></html>"
    ),
    fixture
  )

  .install_primp_stub(fixture)

  wrapper <- ddg$PatchedDuckDuckGoSearchAPIWrapper(
    max_results = 3L,
    use_browser = FALSE,
    proxy = NULL,
    headers = NULL
  )

  res <- wrapper$`_search_text`("irrelevant", 3L)

  expect_true(is.list(res) && length(res) >= 1)
  expect_equal(res[[1]][["_tier"]], "primp")
  expect_match(res[[1]][["body"]], "__START_OF_SOURCE", fixed = TRUE)
})

test_that("DDGS tier returns stubbed results when PRIMP is empty", {
  skip_on_cran()
  run_selenium <- tolower(Sys.getenv("ASA_RUN_SELENIUM_TESTS")) %in% c("true", "1", "yes")
  skip_if(!run_selenium, "Set ASA_RUN_SELENIUM_TESTS=true to run Selenium tier tests")

  python_path <- .skip_if_no_python_selenium()
  .skip_if_missing_python_modules_selenium(c("selenium", "bs4", "ddgs", "primp"))

  ddg <- .import_custom_ddg_selenium(python_path)
  .configure_fast_search(ddg)

  fixture <- tempfile(fileext = ".html")
  writeLines(
    c(
      "<!doctype html>",
      "<html><head><meta charset='utf-8'></head><body>",
      "<p>No results here</p>",
      "</body></html>"
    ),
    fixture
  )

  .install_primp_stub(fixture)
  cleanup_ddgs <- .install_ddgs_stub(ddg)
  on.exit(cleanup_ddgs(), add = TRUE)

  wrapper <- ddg$PatchedDuckDuckGoSearchAPIWrapper(
    max_results = 1L,
    use_browser = FALSE,
    proxy = NULL,
    headers = NULL
  )

  res <- wrapper$`_search_text`("irrelevant", 1L)

  expect_true(is.list(res) && length(res) >= 1)
  expect_equal(res[[1]][["_tier"]], "ddgs")
  expect_equal(res[[1]][["title"]], "Example")
  expect_equal(res[[1]][["region"]], "__region_default__")
  expect_equal(res[[1]][["safesearch"]], "__safesearch_default__")
  expect_equal(res[[1]][["timelimit"]], "__timelimit_default__")
})
