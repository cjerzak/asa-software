# Ensure tests use the standard asa conda environment when available.
options(asa.default_conda_env = "asa_env")

if (requireNamespace("reticulate", quietly = TRUE)) {
  try(
    suppressWarnings(reticulate::use_condaenv("asa_env", required = FALSE)),
    silent = TRUE
  )
}

# ---------------------------------------------------------------------------
# Shared Python / prompt helpers (avoid duplication across test files)
# ---------------------------------------------------------------------------

asa_test_python_path <- function(required_files = character()) {
  candidates <- c(
    # Common dev/test entrypoints:
    file.path(getwd(), "inst", "python"),
    file.path(getwd(), "asa", "inst", "python"),
    file.path(getwd(), "..", "inst", "python"),
    file.path(getwd(), "..", "..", "inst", "python"),
    # Installed package path:
    system.file("python", package = "asa")
  )

  candidates <- unique(normalizePath(candidates, winslash = "/", mustWork = FALSE))
  candidates <- candidates[nzchar(candidates)]

  if (is.null(required_files)) {
    required_files <- character(0)
  }
  required_files <- as.character(required_files)
  required_files <- required_files[nzchar(required_files)]

  for (path in candidates) {
    if (!dir.exists(path)) next
    ok <- TRUE
    for (rf in required_files) {
      if (!file.exists(file.path(path, rf))) {
        ok <- FALSE
        break
      }
    }
    if (ok) return(path)
  }

  for (path in candidates) {
    if (dir.exists(path)) return(path)
  }

  ""
}

asa_test_skip_if_no_python <- function(required_files = character(),
                                      initialize = TRUE,
                                      conda_env = NULL) {
  if (!requireNamespace("testthat", quietly = TRUE)) {
    stop("testthat is required for asa_test_skip_if_no_python()", call. = FALSE)
  }
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    testthat::skip("reticulate not installed")
  }

  python_path <- asa_test_python_path(required_files = required_files)
  if (!nzchar(python_path) || !dir.exists(python_path)) {
    testthat::skip("Python modules not found")
  }

  if (is.null(conda_env)) {
    conda_env <- tryCatch(asa:::.get_default_conda_env(), error = function(e) NULL)
  }
  if (!is.null(conda_env) && is.character(conda_env) && nzchar(conda_env)) {
    try(reticulate::use_condaenv(conda_env, required = FALSE), silent = TRUE)
  }

  if (!reticulate::py_available(initialize = isTRUE(initialize))) {
    testthat::skip("Python not available")
  }

  python_path
}

asa_test_skip_if_missing_python_modules <- function(modules,
                                                   method = c("import", "py_module_available")) {
  if (!requireNamespace("testthat", quietly = TRUE)) {
    stop("testthat is required for asa_test_skip_if_missing_python_modules()", call. = FALSE)
  }
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    testthat::skip("reticulate not installed")
  }

  if (is.null(modules)) {
    modules <- character(0)
  }
  modules <- as.character(modules)
  modules <- modules[nzchar(modules)]
  if (length(modules) == 0) {
    return(invisible(TRUE))
  }

  method <- match.arg(method)

  for (module in modules) {
    ok <- if (method == "py_module_available") {
      isTRUE(reticulate::py_module_available(module))
    } else {
      tryCatch({
        reticulate::import(module, convert = FALSE)
        TRUE
      }, error = function(e) FALSE)
    }

    if (!ok) {
      testthat::skip(paste0("Python module not available: ", module))
    }
  }

  invisible(TRUE)
}

asa_test_import_from_path_or_skip <- function(module_name, python_path) {
  if (!requireNamespace("testthat", quietly = TRUE)) {
    stop("testthat is required for asa_test_import_from_path_or_skip()", call. = FALSE)
  }
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    testthat::skip("reticulate not installed")
  }

  tryCatch(
    reticulate::import_from_path(module_name, path = python_path),
    error = function(e) {
      testthat::skip(paste0("Failed to import ", module_name, ": ", conditionMessage(e)))
    }
  )
}

asa_test_import_module <- function(module_name,
                                   required_file = paste0(module_name, ".py"),
                                   required_modules = NULL,
                                   initialize = FALSE) {
  python_path <- asa_test_skip_if_no_python(
    required_files = required_file, initialize = initialize
  )
  if (!is.null(required_modules)) {
    asa_test_skip_if_missing_python_modules(required_modules)
  }
  reticulate::import_from_path(module_name, path = python_path)
}

# ---------------------------------------------------------------------------
# API key skip helpers (reduce 5-10 line boilerplate per test file)
# ---------------------------------------------------------------------------

asa_test_skip_api_tests <- function() {
  skip_on_cran()
  skip_if(
    tolower(Sys.getenv("ASA_CI_SKIP_API_TESTS")) %in% c("true", "1", "yes"),
    "ASA_CI_SKIP_API_TESTS is set"
  )
}

asa_test_require_gemini_key <- function() {
  api_key <- Sys.getenv("GOOGLE_API_KEY", unset = "")
  if (!nzchar(api_key)) {
    api_key <- Sys.getenv("GEMINI_API_KEY", unset = "")
  }
  skip_if_not(nzchar(api_key), "No Gemini API key available (GOOGLE_API_KEY or GEMINI_API_KEY)")
  api_key
}

asa_test_require_openai_or_groq_key <- function() {
  if (nzchar(Sys.getenv("OPENAI_API_KEY"))) {
    return(list(backend = "openai", model = "gpt-4.1-mini",
                key = Sys.getenv("OPENAI_API_KEY")))
  }
  if (nzchar(Sys.getenv("GROQ_API_KEY"))) {
    return(list(backend = "groq", model = "llama-3.3-70b-versatile",
                key = Sys.getenv("GROQ_API_KEY")))
  }
  skip("No API key available (OPENAI_API_KEY or GROQ_API_KEY)")
}

# ---------------------------------------------------------------------------
# Selenium skip helper
# ---------------------------------------------------------------------------

asa_test_skip_if_no_selenium <- function() {
  skip_on_cran()
  run_selenium <- tolower(Sys.getenv("ASA_RUN_SELENIUM_TESTS")) %in% c("true", "1", "yes")
  skip_if(!run_selenium, "Set ASA_RUN_SELENIUM_TESTS=true to run Selenium tier tests")
}

# ---------------------------------------------------------------------------
# Agent-init-or-skip helper
# ---------------------------------------------------------------------------

asa_test_initialize_agent_or_skip <- function(backend = "auto", model = "auto",
                                              verbose = FALSE, ...) {
  if (backend == "auto" && model == "auto") {
    info <- asa_test_require_openai_or_groq_key()
    backend <- info$backend
    model <- info$model
  }
  tryCatch(
    initialize_agent(backend = backend, model = model, verbose = verbose, ...),
    error = function(e) skip(paste("Could not initialize agent:", e$message))
  )
}

# ---------------------------------------------------------------------------
# Stub LLM factory (replaces 10+ inline py_run_string blocks)
# ---------------------------------------------------------------------------

asa_test_stub_llm <- function(mode = c("simple", "tool_call", "recording",
                                        "json_response"),
                               response_content = "done",
                               tool_name = "Search",
                               tool_args = list(query = "x"),
                               tool_call_id = "call_1",
                               json_content = '{"status":"partial","items":[{"name":"Ada"}]}',
                               var_name = "stub_llm") {
  mode <- match.arg(mode)

  code <- switch(mode,
    simple = paste0(
      "from langchain_core.messages import AIMessage\n\n",
      "class _StubLLM:\n",
      "    def bind_tools(self, tools):\n",
      "        return self\n",
      "    def invoke(self, messages):\n",
      "        return AIMessage(content=", deparse(response_content), ")\n\n",
      var_name, " = _StubLLM()\n"
    ),
    tool_call = paste0(
      "from langchain_core.messages import AIMessage\n\n",
      "class _StubToolCallLLM:\n",
      "    def __init__(self):\n",
      "        self.n = 0\n",
      "    def bind_tools(self, tools):\n",
      "        return self\n",
      "    def invoke(self, messages):\n",
      "        self.n += 1\n",
      "        if self.n == 1:\n",
      "            return AIMessage(\n",
      "                content='calling tool',\n",
      "                tool_calls=[{'name':", deparse(tool_name),
                   ",'args':", jsonlite::toJSON(tool_args, auto_unbox = TRUE),
                   ",'id':", deparse(tool_call_id), "}],\n",
      "            )\n",
      "        return AIMessage(content=", deparse(response_content), ")\n\n",
      var_name, " = _StubToolCallLLM()\n"
    ),
    recording = paste0(
      "from langchain_core.messages import AIMessage\n\n",
      "class _RecordingLLM:\n",
      "    def __init__(self):\n",
      "        self.n = 0\n",
      "        self.system_prompts = []\n",
      "        self.seen_messages = []\n",
      "    def bind_tools(self, tools):\n",
      "        return self\n",
      "    def invoke(self, messages):\n",
      "        self.n += 1\n",
      "        try:\n",
      "            self.system_prompts.append(getattr(messages[0], 'content', None))\n",
      "        except Exception:\n",
      "            self.system_prompts.append(None)\n",
      "        try:\n",
      "            snap = []\n",
      "            for m in messages:\n",
      "                snap.append({'type': type(m).__name__, 'content': getattr(m, 'content', None)})\n",
      "            self.seen_messages.append(snap)\n",
      "        except Exception:\n",
      "            self.seen_messages.append([])\n",
      "        return AIMessage(content=", deparse(response_content), ")\n\n",
      var_name, " = _RecordingLLM()\n"
    ),
    json_response = paste0(
      "class _StubResponse:\n",
      "    def __init__(self, content):\n",
      "        self.content = content\n",
      "        self.tool_calls = None\n\n",
      "class _StubJsonLLM:\n",
      "    def bind_tools(self, tools):\n",
      "        return self\n",
      "    def invoke(self, messages):\n",
      "        return _StubResponse(", deparse(json_content), ")\n\n",
      var_name, " = _StubJsonLLM()\n"
    )
  )

  reticulate::py_run_string(code)
  reticulate::py[[var_name]]
}

# ---------------------------------------------------------------------------
# Stub summarizer factory (for memory folding tests)
# ---------------------------------------------------------------------------

asa_test_stub_summarizer <- function(
    summary_json = '{"version":1,"facts":["FOLDED_SUMMARY"],"decisions":[],"open_questions":[],"sources":[],"warnings":[]}',
    var_name = "stub_summarizer") {
  code <- paste0(
    "class _StubResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n\n",
    "class _StubSummarizer:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "        self.last_prompt = None\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        self.last_prompt = messages[0].content if messages else None\n",
    "        return _StubResponse(", deparse(summary_json), ")\n\n",
    var_name, " = _StubSummarizer()\n"
  )
  reticulate::py_run_string(code)
  reticulate::py[[var_name]]
}

# ---------------------------------------------------------------------------
# Multi-response stub LLM factory (for tests needing cycling responses)
# ---------------------------------------------------------------------------

asa_test_stub_multi_response_llm <- function(
    responses,
    supports_bind_tools = TRUE,
    var_name = "stub_llm") {

  # responses is a list of R lists, each with:
  #   content (string) and optionally tool_calls (list of lists with name/args/id)
  # The stub cycles through them in order, repeating the last one if exhausted.

  response_defs <- vapply(seq_along(responses), function(i) {
    r <- responses[[i]]
    tc <- r$tool_calls
    if (!is.null(tc) && length(tc) > 0) {
      tc_str <- paste0("[", paste(vapply(tc, function(t) {
        sprintf("{'name':%s,'args':%s,'id':%s}",
                deparse(t$name),
                jsonlite::toJSON(t$args, auto_unbox = TRUE),
                deparse(t$id))
      }, character(1)), collapse = ","), "]")
      sprintf("            _ResponseObj(content=%s, tool_calls=%s)",
              deparse(r$content), tc_str)
    } else {
      sprintf("            _ResponseObj(content=%s, tool_calls=None)",
              deparse(r$content))
    }
  }, character(1))

  responses_block <- paste(response_defs, collapse = ",\n")

  bind_tools_line <- if (supports_bind_tools) {
    "    def bind_tools(self, tools):\n        return self\n"
  } else {
    ""
  }

  code <- paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _ResponseObj:\n",
    "    def __init__(self, content, tool_calls=None):\n",
    "        self.content = content\n",
    "        self.tool_calls = tool_calls\n",
    "        self.usage_metadata = {'total_tokens': 0}\n\n",
    "class _MultiResponseLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "        self._responses = [\n",
    responses_block, "\n",
    "        ]\n",
    bind_tools_line,
    "    def invoke(self, messages):\n",
    "        idx = min(self.n, len(self._responses) - 1)\n",
    "        self.n += 1\n",
    "        r = self._responses[idx]\n",
    "        if r.tool_calls:\n",
    "            return AIMessage(content=r.content, tool_calls=r.tool_calls)\n",
    "        return AIMessage(content=r.content)\n\n",
    var_name, " = _MultiResponseLLM()\n"
  )

  reticulate::py_run_string(code)
  reticulate::py[[var_name]]
}

# ---------------------------------------------------------------------------
# Test config factories (reduce SearchConfig boilerplate)
# ---------------------------------------------------------------------------

asa_test_fast_search_config <- function(ddg, ...) {
  defaults <- list(
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
  overrides <- list(...)
  args <- modifyList(defaults, overrides)
  do.call(ddg$SearchConfig, args)
}

# ---------------------------------------------------------------------------
# Mock data factories (reduce test object construction boilerplate)
# ---------------------------------------------------------------------------

asa_test_mock_enumerate_result <- function(
    data = data.frame(name = c("A", "B"), value = c(1, 2)),
    status = "complete",
    stop_reason = "target_reached",
    metrics = list(round_number = 3, queries_used = 15),
    provenance = NULL,
    plan = list(),
    query = "test query",
    ...) {
  asa_enumerate_result(
    data = data,
    status = status,
    stop_reason = stop_reason,
    metrics = metrics,
    provenance = provenance,
    plan = plan,
    query = query,
    ...
  )
}

asa_test_mock_audit_result <- function(
    data = data.frame(
      name = c("A", "B"),
      `_audit_flag` = c("ok", "warning"),
      check.names = FALSE
    ),
    audit_summary = "Test audit",
    issues = list(),
    recommendations = character(),
    completeness_score = 1.0,
    consistency_score = 1.0,
    backend_used = "claude_code",
    elapsed_time = 1.0,
    ...) {
  asa_audit_result(
    data = data,
    audit_summary = audit_summary,
    issues = issues,
    recommendations = recommendations,
    completeness_score = completeness_score,
    consistency_score = consistency_score,
    backend_used = backend_used,
    elapsed_time = elapsed_time,
    ...
  )
}

asa_test_mock_agent <- function(
    python_agent = NULL,
    backend = "openai",
    model = "gpt-4",
    config = list(use_memory_folding = TRUE),
    ...) {
  asa_agent(
    python_agent = python_agent,
    backend = backend,
    model = model,
    config = config,
    ...
  )
}

asa_test_recursion_limit_prompt <- function() {
  paste0(
    "You are doing a multi-step research task.\n",
    "1) You MUST call the Search tool first with the query: \"asa recursion limit test data\".\n",
    "   Do NOT output the final JSON until after you have tool output.\n",
    "2) After you get tool output, produce ONLY valid JSON with this exact schema:\n",
    "{\n",
    "  \"status\": \"complete\"|\"partial\",\n",
    "  \"items\": [\n",
    "    {\"name\": string, \"birth_year\": integer, \"field\": string, \"key_contribution\": string|null}\n",
    "  ],\n",
    "  \"missing\": [string],\n",
    "  \"notes\": string\n",
    "}\n",
    "Missing-data rules:\n",
    "- If you could not run Search or did not get tool output, set status=\"partial\".\n",
    "- Use null for unknown key_contribution.\n",
    "- List any unknown fields in missing.\n",
    "- Do NOT speculate.\n",
    "Known seed data (you may use this even without Search):\n",
    "- Ada Lovelace (birth_year=1815, field=\"mathematics\")\n",
    "- Alan Turing (birth_year=1912, field=\"computer science\")\n",
    "- Grace Hopper (birth_year=1906, field=\"computer science\")\n",
    "Your items MUST include these three people at minimum.\n"
  )
}
