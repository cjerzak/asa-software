# test-webpage-reader.R
# Tests for the optional webpage reader tool (allow_read_webpages gate)

.extract_first_excerpt <- function(open_webpage_output) {
  if (!grepl("[1]", open_webpage_output, fixed = TRUE)) {
    return("")
  }
  excerpt <- sub("(?s).*\\[1\\]\\n", "", open_webpage_output, perl = TRUE)
  excerpt <- sub("(?s)\\n\\[2\\].*$", "", excerpt, perl = TRUE)
  excerpt
}

.with_restored_webpage_reader <- function(webpage_tool, expr) {
  cfg_prev <- webpage_tool$configure_webpage_reader()
  on.exit({
    try(webpage_tool$configure_webpage_reader(
      allow_read_webpages = cfg_prev$allow_read_webpages,
      relevance_mode = cfg_prev$relevance_mode,
      heuristic_profile = cfg_prev$heuristic_profile,
      embedding_provider = cfg_prev$embedding_provider,
      embedding_model = cfg_prev$embedding_model,
      prefilter_k = cfg_prev$prefilter_k,
      use_mmr = cfg_prev$use_mmr,
      mmr_lambda = cfg_prev$mmr_lambda,
      cache_enabled = cfg_prev$cache_enabled,
      blocked_cache_ttl_sec = cfg_prev$blocked_cache_ttl_sec,
      blocked_cache_max_entries = cfg_prev$blocked_cache_max_entries,
      blocked_probe_bytes = cfg_prev$blocked_probe_bytes,
      blocked_detect_on_200 = cfg_prev$blocked_detect_on_200,
      blocked_body_scan_bytes = cfg_prev$blocked_body_scan_bytes,
      pdf_enabled = cfg_prev$pdf_enabled,
      pdf_timeout = cfg_prev$pdf_timeout,
      pdf_max_bytes = cfg_prev$pdf_max_bytes,
      pdf_max_pages = cfg_prev$pdf_max_pages,
      pdf_max_text_chars = cfg_prev$pdf_max_text_chars
    ), silent = TRUE)
    try(webpage_tool$clear_webpage_reader_cache(), silent = TRUE)
  }, add = TRUE)
  eval.parent(substitute(expr))
}

test_that("OpenWebpage tool is gated by allow_read_webpages", {
  python_path <- asa_test_skip_if_no_python(required_files = "webpage_tool.py")
  asa_test_skip_if_missing_python_modules(
    c("curl_cffi", "bs4", "pydantic", "langchain_core")
  )

  webpage_tool <- asa_test_import_from_path_or_skip("webpage_tool", python_path)
  .with_restored_webpage_reader(webpage_tool, {
    tool <- webpage_tool$create_webpage_reader_tool()

    webpage_tool$configure_webpage_reader(
      allow_read_webpages = FALSE,
      relevance_mode = "lexical",
      cache_enabled = FALSE
    )
    out_disabled <- tool$`_run`(
      url = "https://connorjerzak.com/collaborators/",
      query = "Europe"
    )
    expect_match(out_disabled, "Webpage reading is disabled", fixed = TRUE)
    expect_match(out_disabled, "allow_read_webpages=FALSE", fixed = TRUE)

    webpage_tool$configure_webpage_reader(
      allow_read_webpages = TRUE,
      relevance_mode = "lexical",
      cache_enabled = FALSE
    )
    out_enabled <- tool$`_run`(
      url = "http://localhost/",
      query = "test"
    )
    expect_match(out_enabled, "Refusing to open URL", fixed = TRUE)
  })
})

test_that(".with_webpage_reader_config toggles Python allow_read_webpages", {
  python_path <- asa_test_skip_if_no_python(required_files = "webpage_tool.py")
  asa_test_skip_if_missing_python_modules(
    c("curl_cffi", "bs4", "pydantic", "langchain_core")
  )

  conda_env <- tryCatch(asa:::.get_default_conda_env(), error = function(e) NULL)
  if (is.null(conda_env) || !is.character(conda_env) || !nzchar(conda_env)) {
    skip("Default conda env not available")
  }

  webpage_tool <- asa_test_import_from_path_or_skip("webpage_tool", python_path)
  cfg_prev <- webpage_tool$configure_webpage_reader()

  inside <- tryCatch(
    asa:::.with_webpage_reader_config(
      allow_read_webpages = TRUE,
      relevance_mode = "lexical",
      max_chars = 12345,
      max_chunks = 2,
      chunk_chars = 2000,
      conda_env = conda_env,
      fn = function() {
        cfg <- webpage_tool$configure_webpage_reader()
        list(
          allow_read_webpages = cfg$allow_read_webpages,
          max_chars = cfg$max_chars,
          max_chunks = cfg$max_chunks,
          chunk_chars = cfg$chunk_chars
        )
      }
    ),
    error = function(e) {
      skip(paste0("Webpage reader config wrapper unavailable: ", conditionMessage(e)))
    }
  )

  expect_true(isTRUE(inside$allow_read_webpages))
  expect_equal(inside$max_chars, 12345)
  expect_equal(inside$max_chunks, 2)
  expect_equal(inside$chunk_chars, 2000)

  cfg_after <- webpage_tool$configure_webpage_reader()
  expect_equal(cfg_after$allow_read_webpages, cfg_prev$allow_read_webpages)
  expect_equal(cfg_after$max_chars, cfg_prev$max_chars)
  expect_equal(cfg_after$max_chunks, cfg_prev$max_chunks)
  expect_equal(cfg_after$chunk_chars, cfg_prev$chunk_chars)
})

test_that("OpenWebpage caches blocked responses (403/429/bot marker)", {
  python_path <- asa_test_skip_if_no_python(required_files = "webpage_tool.py")
  asa_test_skip_if_missing_python_modules(
    c("curl_cffi", "bs4", "pydantic", "langchain_core")
  )

  webpage_tool <- asa_test_import_from_path_or_skip("webpage_tool", python_path)
  .with_restored_webpage_reader(webpage_tool, {
    webpage_tool$configure_webpage_reader(
      allow_read_webpages = TRUE,
      relevance_mode = "lexical",
      cache_enabled = TRUE,
      blocked_cache_ttl_sec = 600L,
      blocked_cache_max_entries = 16L
    )
    tool <- webpage_tool$create_webpage_reader_tool()

    reticulate::py_run_string(paste0(
      "import webpage_tool\n",
      "_asa_old_fetch_html = webpage_tool._fetch_html\n",
      "_asa_fetch_calls = 0\n",
      "def _asa_fake_fetch(url, proxy=None, cfg=None):\n",
      "    global _asa_fetch_calls\n",
      "    _asa_fetch_calls += 1\n",
      "    return {\n",
      "        'ok': False,\n",
      "        'error': 'http_error',\n",
      "        'status_code': 403,\n",
      "        'content_type': 'text/html',\n",
      "        'final_url': url,\n",
      "        'is_blocked': True,\n",
      "        'blocked_reason': 'http_403_bot_marker',\n",
      "    }\n",
      "webpage_tool._fetch_html = _asa_fake_fetch\n"
    ))
    on.exit(
      try(reticulate::py_run_string(
        "import webpage_tool\nif '_asa_old_fetch_html' in globals(): webpage_tool._fetch_html = _asa_old_fetch_html"
      ), silent = TRUE),
      add = TRUE
    )

    out1 <- tool$`_run`(url = "https://example.com/blocked", query = "test")
    out2 <- tool$`_run`(url = "https://example.com/blocked", query = "test")

    expect_match(out1, "Blocked fetch detected.", fixed = TRUE)
    expect_match(out1, "Cache: MISS", fixed = TRUE)
    expect_match(out2, "Cache: HIT_BLOCKED", fixed = TRUE)
    expect_equal(as.integer(reticulate::py$`_asa_fetch_calls`), 1L)
  })
})

test_that("OpenWebpage treats HTTP 200 challenge interstitials as blocked and caches", {
  python_path <- asa_test_skip_if_no_python(required_files = "webpage_tool.py")
  asa_test_skip_if_missing_python_modules(
    c("curl_cffi", "bs4", "pydantic", "langchain_core")
  )

  webpage_tool <- asa_test_import_from_path_or_skip("webpage_tool", python_path)
  .with_restored_webpage_reader(webpage_tool, {
    webpage_tool$configure_webpage_reader(
      allow_read_webpages = TRUE,
      relevance_mode = "lexical",
      cache_enabled = TRUE,
      blocked_cache_ttl_sec = 600L,
      blocked_cache_max_entries = 16L,
      blocked_detect_on_200 = TRUE,
      blocked_body_scan_bytes = 8192L
    )
    tool <- webpage_tool$create_webpage_reader_tool()

    reticulate::py_run_string(paste0(
      "import webpage_tool\n",
      "_asa_old_requests_get = webpage_tool.requests.get\n",
      "_asa_fetch_calls = 0\n",
      "class _ASAResp:\n",
      "    def __init__(self, url):\n",
      "        self.status_code = 200\n",
      "        self.headers = {'Content-Type': 'text/html; charset=utf-8'}\n",
      "        self.url = url\n",
      "        self.encoding = 'utf-8'\n",
      "        self._body = (\n",
      "            '<html><head><title>One moment, please...</title></head>'\n",
      "            '<body>Please wait while your request is being verified.</body></html>'\n",
      "        ).encode('utf-8')\n",
      "    def iter_content(self, chunk_size=16384):\n",
      "        body = self._body\n",
      "        for i in range(0, len(body), chunk_size):\n",
      "            yield body[i:i+chunk_size]\n",
      "    def close(self):\n",
      "        return None\n",
      "def _asa_fake_requests_get(target_url, headers=None, timeout=None, proxies=None, stream=True, allow_redirects=True):\n",
      "    global _asa_fetch_calls\n",
      "    _asa_fetch_calls += 1\n",
      "    return _ASAResp(target_url)\n",
      "webpage_tool.requests.get = _asa_fake_requests_get\n"
    ))
    on.exit(
      try(reticulate::py_run_string(
        "import webpage_tool\nif '_asa_old_requests_get' in globals(): webpage_tool.requests.get = _asa_old_requests_get"
      ), silent = TRUE),
      add = TRUE
    )

    out1 <- tool$`_run`(url = "https://example.com/interstitial", query = "bio")
    out2 <- tool$`_run`(url = "https://example.com/interstitial", query = "bio")

    expect_match(out1, "Blocked fetch detected.", fixed = TRUE)
    expect_match(out1, "Reason: http_200_challenge_title", fixed = TRUE)
    expect_match(out2, "Cache: HIT_BLOCKED", fixed = TRUE)
    expect_equal(as.integer(reticulate::py$`_asa_fetch_calls`), 1L)
  })
})

test_that("200 blocked detection avoids single generic marker false positives", {
  python_path <- asa_test_skip_if_no_python(required_files = "webpage_tool.py")
  asa_test_skip_if_missing_python_modules(
    c("curl_cffi", "bs4", "pydantic", "langchain_core")
  )

  webpage_tool <- asa_test_import_from_path_or_skip("webpage_tool", python_path)
  .with_restored_webpage_reader(webpage_tool, {
    reticulate::py_run_string(paste0(
      "import webpage_tool\n",
      "_asa_detect = webpage_tool._detect_blocked_response(\n",
      "    200,\n",
      "    'This article studies blocked matrix methods in economics.',\n",
      "    title_text='Applied Methods'\n",
      ")\n"
    ))
    detect <- reticulate::py$`_asa_detect`
    expect_false(as.logical(detect[[1]]))
    expect_true(is.null(detect[[2]]))
  })
})

test_that("OpenWebpage does not blocked-cache generic HTTP errors", {
  python_path <- asa_test_skip_if_no_python(required_files = "webpage_tool.py")
  asa_test_skip_if_missing_python_modules(
    c("curl_cffi", "bs4", "pydantic", "langchain_core")
  )

  webpage_tool <- asa_test_import_from_path_or_skip("webpage_tool", python_path)
  .with_restored_webpage_reader(webpage_tool, {
    webpage_tool$configure_webpage_reader(
      allow_read_webpages = TRUE,
      relevance_mode = "lexical",
      cache_enabled = TRUE,
      blocked_cache_ttl_sec = 600L,
      blocked_cache_max_entries = 16L
    )
    tool <- webpage_tool$create_webpage_reader_tool()

    reticulate::py_run_string(paste0(
      "import webpage_tool\n",
      "_asa_old_fetch_html = webpage_tool._fetch_html\n",
      "_asa_fetch_calls = 0\n",
      "def _asa_fake_fetch(url, proxy=None, cfg=None):\n",
      "    global _asa_fetch_calls\n",
      "    _asa_fetch_calls += 1\n",
      "    return {\n",
      "        'ok': False,\n",
      "        'error': 'http_error',\n",
      "        'status_code': 500,\n",
      "        'content_type': 'text/html',\n",
      "        'final_url': url,\n",
      "        'is_blocked': False,\n",
      "    }\n",
      "webpage_tool._fetch_html = _asa_fake_fetch\n"
    ))
    on.exit(
      try(reticulate::py_run_string(
        "import webpage_tool\nif '_asa_old_fetch_html' in globals(): webpage_tool._fetch_html = _asa_old_fetch_html"
      ), silent = TRUE),
      add = TRUE
    )

    out1 <- tool$`_run`(url = "https://example.com/http500", query = "test")
    out2 <- tool$`_run`(url = "https://example.com/http500", query = "test")

    expect_match(out1, "Error: http_error", fixed = TRUE)
    expect_match(out2, "Error: http_error", fixed = TRUE)
    expect_false(grepl("HIT_BLOCKED", out2, fixed = TRUE))
    expect_equal(as.integer(reticulate::py$`_asa_fetch_calls`), 2L)
  })
})

test_that("PDF extraction helper enforces char budget via pdftotext output", {
  python_path <- asa_test_skip_if_no_python(required_files = "webpage_tool.py")
  asa_test_skip_if_missing_python_modules(
    c("curl_cffi", "bs4", "pydantic", "langchain_core")
  )

  webpage_tool <- asa_test_import_from_path_or_skip("webpage_tool", python_path)
  .with_restored_webpage_reader(webpage_tool, {
    reticulate::py_run_string(paste0(
      "import webpage_tool\n",
      "_asa_old_run = webpage_tool.subprocess.run\n",
      "_asa_old_which = webpage_tool.shutil.which\n",
      "_asa_pdf_cmd = None\n",
      "def _asa_fake_run(cmd, stdout=None, stderr=None, timeout=None, check=False):\n",
      "    global _asa_pdf_cmd\n",
      "    _asa_pdf_cmd = cmd\n",
      "    out_path = cmd[-1]\n",
      "    with open(out_path, 'w', encoding='utf-8') as f:\n",
      "        f.write('A' * 50000)\n",
      "    class _Proc:\n",
      "        returncode = 0\n",
      "        stderr = b''\n",
      "    return _Proc()\n",
      "webpage_tool.shutil.which = lambda name: '/usr/bin/pdftotext'\n",
      "webpage_tool.subprocess.run = _asa_fake_run\n",
      "_asa_cfg = webpage_tool.configure_webpage_reader(pdf_max_text_chars=1234, pdf_max_pages=2)\n",
      "_asa_pdf_result = webpage_tool._extract_pdf_text_from_bytes(b'not_a_real_pdf', cfg=_asa_cfg)\n"
    ))
    on.exit(
      try(reticulate::py_run_string(paste0(
        "import webpage_tool\n",
        "if '_asa_old_run' in globals(): webpage_tool.subprocess.run = _asa_old_run\n",
        "if '_asa_old_which' in globals(): webpage_tool.shutil.which = _asa_old_which\n"
      )), silent = TRUE),
      add = TRUE
    )

    expect_true(isTRUE(reticulate::py$`_asa_pdf_result`$ok))
    expect_equal(nchar(as.character(reticulate::py$`_asa_pdf_result`$text)), 1234L)
    cmd <- as.character(unlist(reticulate::py$`_asa_pdf_cmd`))
    expect_true(any(cmd == "-l"))
    expect_true(any(cmd == "2"))
  })
})

test_that("OpenWebpage can read collaborators page (live network)", {
  skip_on_cran()

  python_path <- asa_test_skip_if_no_python(required_files = "webpage_tool.py")
  asa_test_skip_if_missing_python_modules(
    c("curl_cffi", "bs4", "pydantic", "langchain_core")
  )

  webpage_tool <- asa_test_import_from_path_or_skip("webpage_tool", python_path)
  .with_restored_webpage_reader(webpage_tool, {
    webpage_tool$configure_webpage_reader(
      allow_read_webpages = TRUE,
      relevance_mode = "lexical",
      chunk_chars = 2000,
      max_chunks = 2,
      cache_enabled = FALSE
    )

    tool <- webpage_tool$create_webpage_reader_tool()
    out <- tool$`_run`(
      url = "https://connorjerzak.com/collaborators/",
      query = "Europe:"
    )

    excerpt1 <- .extract_first_excerpt(out)
    expect_true(nchar(excerpt1) > 0)

    pos_europe <- regexpr("Europe:", excerpt1, fixed = TRUE)
    expect_true(pos_europe[1] > -1)

    after <- substring(excerpt1, pos_europe[1] + attr(pos_europe, "match.length"))
    lines <- trimws(unlist(strsplit(after, "\n", fixed = TRUE)))
    lines <- lines[lines != ""]
    lines <- lines[!grepl("^[\\-–—]+$", lines)]

    expect_true(length(lines) >= 1)
    expect_true(grepl("Adel Daoud", lines[1], fixed = TRUE))
  })
})

test_that("Gemini reasons about fetched webpage content (live)", {
  asa_test_skip_api_tests()
  asa_test_require_gemini_key()

  agent <- asa::initialize_agent(
    backend = "gemini",
    model = "gemini-2.0-flash"
  )

  result <- asa::run_task(
    prompt = paste(
      "Open this webpage: https://connorjerzak.com/collaborators/",
      "and tell me the name of one collaborator listed under Europe."
    ),
    agent = agent,
    allow_read_webpages = TRUE
  )

  # If the LLM saw the webpage content, it can name a real collaborator
  expect_match(
    result$message,
    "Adel Daoud|Olga Gasparyan|Miguel Rueda",
    ignore.case = TRUE
  )
})
