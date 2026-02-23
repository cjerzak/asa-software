# test-selenium-tier.R
# Deterministic tests that exercise the Selenium browser tier against local HTML fixtures.

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

.install_curl_cffi_fail_stub <- function() {
  reticulate::py_run_string(
    "
import sys
import types

class _FailModule:
    class requests:
        @staticmethod
        def get(*args, **kwargs):
            raise ConnectionError('curl_cffi stub: forced failure for testing')

sys.modules['curl_cffi'] = _FailModule()
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
  asa_test_skip_if_no_selenium()

  python_path <- asa_test_skip_if_no_python(required_files = "asa_backend/api/agent_api.py")
  asa_test_skip_if_missing_python_modules(c("selenium", "bs4"))

  ddg <- asa_test_import_from_path_or_skip("asa_backend.api.agent_api", python_path)

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

  cfg <- asa_test_fast_search_config(ddg, timeout = 0.5)

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
  asa_test_skip_if_no_selenium()

  python_path <- asa_test_skip_if_no_python(required_files = "asa_backend/api/agent_api.py")
  asa_test_skip_if_missing_python_modules(c("selenium", "bs4"))

  ddg <- asa_test_import_from_path_or_skip("asa_backend.api.agent_api", python_path)

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

  cfg <- asa_test_fast_search_config(ddg)

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
  asa_test_skip_if_no_selenium()

  python_path <- asa_test_skip_if_no_python(required_files = "asa_backend/api/agent_api.py")
  asa_test_skip_if_missing_python_modules(c("selenium", "bs4", "ddgs", "primp"))

  ddg <- asa_test_import_from_path_or_skip("asa_backend.api.agent_api", python_path)
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

  .install_curl_cffi_fail_stub()
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
  asa_test_skip_if_no_selenium()

  python_path <- asa_test_skip_if_no_python(required_files = "asa_backend/api/agent_api.py")
  asa_test_skip_if_missing_python_modules(c("selenium", "bs4", "ddgs", "primp"))

  ddg <- asa_test_import_from_path_or_skip("asa_backend.api.agent_api", python_path)
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

  .install_curl_cffi_fail_stub()
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
