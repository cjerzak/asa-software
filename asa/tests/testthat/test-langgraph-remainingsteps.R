# Tests for RemainingSteps-based recursion_limit handling in LangGraph graphs.

.get_python_path_langgraph <- function() {
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

.skip_if_no_python_langgraph <- function() {
  python_path <- .get_python_path_langgraph()
  if (!dir.exists(python_path)) {
    skip("Python modules not found")
  }
  # Prefer the package's default conda env when available so python modules
  # resolve consistently in CI/local dev.
  conda_env <- tryCatch(asa:::.get_default_conda_env(), error = function(e) NULL)
  if (!is.null(conda_env) && is.character(conda_env) && nzchar(conda_env)) {
    try(reticulate::use_condaenv(conda_env, required = FALSE), silent = TRUE)
  }

  if (!reticulate::py_available(initialize = TRUE)) {
    skip("Python not available")
  }
  python_path
}

.skip_if_missing_python_modules_langgraph <- function(modules) {
  if (is.null(modules) || length(modules) == 0) {
    return(invisible(TRUE))
  }

  for (module in modules) {
    ok <- tryCatch({
      reticulate::import(module, convert = FALSE)
      TRUE
    }, error = function(e) FALSE)

    if (!ok) {
      skip(paste0("Python module not available: ", module))
    }
  }

  invisible(TRUE)
}

test_that("research graph stops with stop_reason='recursion_limit' (RemainingSteps)", {
  python_path <- .skip_if_no_python_langgraph()
  .skip_if_missing_python_modules_langgraph(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ))

  research <- reticulate::import_from_path("research_graph", path = python_path)

  # Stub LLM for planner node: returns minimal valid JSON.
  reticulate::py_run_string(paste0(
    "class _StubResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "        self.usage_metadata = {'total_tokens': 0}\n",
    "\n",
    "class _StubLLM:\n",
    "    def invoke(self, messages):\n",
    "        return _StubResponse('{\"entity_type\":\"test\",\"wikidata_type\":null,\"search_queries\":[]}')\n",
    "\n",
    "stub_llm = _StubLLM()\n"
  ))

  cfg <- research$ResearchConfig(
    max_rounds = as.integer(999),
    budget_queries = as.integer(9999),
    budget_tokens = as.integer(999999),
    budget_time_sec = as.integer(999999),
    plateau_rounds = as.integer(0),
    novelty_min = as.numeric(0),
    use_wikidata = FALSE,
    use_web = FALSE,
    use_wikipedia = FALSE
  )

  graph <- research$create_research_graph(
    llm = reticulate::py$stub_llm,
    tools = list(),
    config = cfg,
    checkpointer = NULL,
    wikidata_tool = NULL
  )

  initial_state <- list(
    query = "test query",
    schema = list(name = "character"),
    config = list(use_web = FALSE, use_wikidata = FALSE, use_wikipedia = FALSE),
    plan = list(),
    entity_type = "",
    wikidata_type = NULL,
    results = list(),
    new_results = list(),
    seen_hashes = list(),
    novelty_history = list(),
    round_number = 0L,
    queries_used = 0L,
    tokens_used = 0L,
    start_time = 0,
    status = "planning",
    stop_reason = NULL,
    errors = list()
  )

  final_state <- NULL
  expect_silent({
    # With a very small recursion_limit, the graph should stop gracefully using
    # RemainingSteps (instead of raising GraphRecursionError).
    final_state <- graph$invoke(
      initial_state,
      config = list(recursion_limit = as.integer(3))
    )
  })

  expect_equal(final_state$status, "complete")
  expect_equal(final_state$stop_reason, "recursion_limit")
})

test_that("best-effort recursion_limit output populates required JSON fields (stubbed)", {
  python_path <- .skip_if_no_python_langgraph()
  .skip_if_missing_python_modules_langgraph(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ))

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  # Stub LLM: returns intentionally incomplete JSON so the recursion-limit
  # "best effort" path must populate schema-required keys.
  reticulate::py_run_string(paste0(
    "class _StubResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "        self.tool_calls = None\n",
    "\n",
    "class _StubLLM:\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        # Missing: missing, notes; and item is missing birth_year/field/key_contribution.\n",
    "        return _StubResponse('{\"status\":\"partial\",\"items\":[{\"name\":\"Ada Lovelace\"}]}')\n",
    "\n",
    "stub_llm = _StubLLM()\n"
  ))

  agent <- custom_ddg$create_standard_agent(
    model = reticulate::py$stub_llm,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE
  )

  expected_schema <- list(
    status = "complete|partial",
    items = list(list(
      name = "string",
      birth_year = "integer",
      field = "string",
      key_contribution = "string|null"
    )),
    missing = list("string"),
    notes = "string"
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return JSON only.")),
      expected_schema = expected_schema
    ),
    config = list(recursion_limit = as.integer(3))
  )

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_equal(final_state$expected_schema_source, "explicit")

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  parsed <- jsonlite::fromJSON(response_text)

  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))

  if (is.data.frame(parsed$items)) {
    expect_true(all(c("name", "birth_year", "field", "key_contribution") %in% names(parsed$items)))
  }

  # Observability: repair metadata recorded when keys were missing.
  expect_true(is.list(final_state$json_repair))
  expect_true(length(final_state$json_repair) >= 1)
})

test_that("JSON repair populates nested required keys (explicit schema)", {
  python_path <- .skip_if_no_python_langgraph()
  .skip_if_missing_python_modules_langgraph(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ))

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  reticulate::py_run_string(paste0(
    "class _StubResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "        self.tool_calls = None\n",
    "\n",
    "class _StubLLM:\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        # Missing: meta, and nested meta.counts.total; items[0].info.birth_year.\n",
    "        return _StubResponse('{\"status\":\"partial\",\"items\":[{\"name\":\"Ada\"}]}')\n",
    "\n",
    "stub_llm = _StubLLM()\n"
  ))

  agent <- custom_ddg$create_standard_agent(
    model = reticulate::py$stub_llm,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE
  )

  expected_schema <- list(
    status = "complete|partial",
    meta = list(
      source = "string",
      counts = list(total = "integer")
    ),
    items = list(list(
      name = "string",
      info = list(birth_year = "integer")
    ))
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return JSON only.")),
      expected_schema = expected_schema
    ),
    config = list(recursion_limit = as.integer(3))
  )

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  parsed <- jsonlite::fromJSON(response_text)

  expect_true(is.list(parsed))
  expect_true(all(c("status", "meta", "items") %in% names(parsed)))
  expect_true(is.list(parsed$meta))
  expect_true(all(c("source", "counts") %in% names(parsed$meta)))
  expect_true(all(c("total") %in% names(parsed$meta$counts)))

  # Items element can be parsed as data.frame by jsonlite.
  if (is.data.frame(parsed$items)) {
    expect_true("name" %in% names(parsed$items))
    expect_true(any(c("info", "info.birth_year") %in% names(parsed$items)))
    if ("info" %in% names(parsed$items)) {
      if (is.data.frame(parsed$items$info)) {
        expect_true("birth_year" %in% names(parsed$items$info))
      } else {
        expect_true(is.list(parsed$items$info[[1]]))
        expect_true(all("birth_year" %in% names(parsed$items$info[[1]])))
      }
    } else {
      expect_true("info.birth_year" %in% names(parsed$items))
    }
  } else if (is.list(parsed$items) && length(parsed$items) >= 1) {
    expect_true(all(c("name", "info") %in% names(parsed$items[[1]])))
    expect_true(all("birth_year" %in% names(parsed$items[[1]]$info)))
  }
})

test_that("repair_json_output_to_schema is idempotent", {
  python_path <- .skip_if_no_python_langgraph()
  .skip_if_missing_python_modules_langgraph(c("pydantic"))

  utils <- reticulate::import_from_path("state_utils", path = python_path)

  schema <- list(
    status = "complete|partial",
    items = list(list(
      name = "string",
      birth_year = "integer"
    )),
    notes = "string"
  )

  out1 <- utils$repair_json_output_to_schema(
    "{\"status\":\"partial\",\"items\":[{\"name\":\"Ada\"}]}",
    schema,
    fallback_on_failure = TRUE
  )
  out2 <- utils$repair_json_output_to_schema(out1, schema, fallback_on_failure = TRUE)
  expect_equal(out2, out1)
})

test_that("standard agent reaches recursion_limit and preserves JSON output (Gemini, best-effort)", {

  skip_on_cran()

  skip_if(
    tolower(Sys.getenv("ASA_CI_SKIP_API_TESTS")) %in% c("true", "1", "yes"),
    "ASA_CI_SKIP_API_TESTS is set"
  )

  api_key <- Sys.getenv("GOOGLE_API_KEY", unset = "")
  if (!nzchar(api_key)) {
    api_key <- Sys.getenv("GEMINI_API_KEY", unset = "")
  }
  skip_if_not(
    nzchar(api_key),
    "No Gemini API key available (GOOGLE_API_KEY or GEMINI_API_KEY)"
  )

  python_path <- .skip_if_no_python_langgraph()
  .skip_if_missing_python_modules_langgraph(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests",
    "langchain_google_genai"
  ))

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)
  chat_models <- reticulate::import("langchain_google_genai")

  gemini_model <- Sys.getenv("ASA_TEST_GEMINI_MODEL", unset = "")
  if (!nzchar(gemini_model)) {
    gemini_model <- Sys.getenv("ASA_GEMINI_MODEL", unset = "")
  }
  if (!nzchar(gemini_model)) {
    gemini_model <- asa:::ASA_DEFAULT_GEMINI_MODEL
  }

  llm <- chat_models$ChatGoogleGenerativeAI(
    model = gemini_model,
    temperature = 0,
    api_key = api_key
  )

  # Deterministic tool: we don't care about live web results, only that the
  # agent attempts tool usage before being cut off by recursion_limit.
  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "import json\n",
    "\n",
    "def _fake_search(query: str) -> str:\n",
    "    # Fixed payload so the agent has something it *could* use if the tool ran.\n",
    "    return json.dumps([\n",
    "        {\"name\": \"Ada Lovelace\", \"birth_year\": 1815, \"field\": \"mathematics\", \"key_contribution\": None},\n",
    "        {\"name\": \"Alan Turing\", \"birth_year\": 1912, \"field\": \"computer science\", \"key_contribution\": None},\n",
    "        {\"name\": \"Grace Hopper\", \"birth_year\": 1906, \"field\": \"computer science\", \"key_contribution\": None}\n",
    "    ])\n",
    "\n",
    "fake_search_tool = Tool(\n",
    "    name=\"Search\",\n",
    "    description=\"Deterministic Search tool for recursion-limit tests.\",\n",
    "    func=_fake_search,\n",
    ")\n"
  ))

  agent <- custom_ddg$create_standard_agent(
    model = llm,
    tools = list(reticulate::py$fake_search_tool),
    checkpointer = NULL,
    debug = FALSE
  )

  # A deliberately multi-part task that normally requires multiple tool calls.
  # We also require strict JSON output so the test can validate best-effort
  # formatting even when the step budget is exhausted.
  prompt <- paste0(
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

  final_state <- agent$invoke(
    list(messages = list(list(role = "user", content = prompt))),
    config = list(recursion_limit = as.integer(4))
  )

  expect_equal(final_state$stop_reason, "recursion_limit")

  # Extract response text using the same logic as run_task()/run_agent() so we
  # verify "best effort" formatting survives Gemini + recursion_limit.
  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  expect_true(nzchar(response_text))

  if (tolower(Sys.getenv("ASA_DEBUG_GEMINI_RECURSION_TEST")) %in% c("true", "1", "yes")) {
    message("\nGemini best-effort response (recursion_limit):\n", response_text)
  }

  parsed <- jsonlite::fromJSON(response_text)
  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))

  if (is.data.frame(parsed$items)) {
    expect_true(all(c("name", "birth_year", "field") %in% names(parsed$items)))
    expect_true(any(parsed$items$name %in% c("Ada Lovelace", "Alan Turing", "Grace Hopper")))
  }
})

test_that("run_task passes expected_schema into LangGraph state (explicit schema)", {
  python_path <- .skip_if_no_python_langgraph()
  .skip_if_missing_python_modules_langgraph(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ))

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  # Stub LLM: returns incomplete JSON; run_task(expected_schema=...) should
  # trigger best-effort repair even without prompt-based schema inference.
  reticulate::py_run_string(paste0(
    "class _StubResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "        self.tool_calls = None\n",
    "\n",
    "class _StubLLM:\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        return _StubResponse('{\"status\":\"partial\",\"items\":[{\"name\":\"Ada\"}]}')\n",
    "\n",
    "stub_llm = _StubLLM()\n"
  ))

  python_agent <- custom_ddg$create_standard_agent(
    model = reticulate::py$stub_llm,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE
  )

  agent <- asa::asa_agent(
    python_agent = python_agent,
    backend = "gemini",
    model = "stub",
    config = list(use_memory_folding = FALSE, backend = "gemini")
  )

  expected_schema <- list(
    status = "complete|partial",
    items = list(list(
      name = "string",
      birth_year = "integer"
    )),
    missing = list("string"),
    notes = "string"
  )

  result <- asa::run_task(
    prompt = "Return JSON only.",
    output_format = "raw",
    agent = agent,
    expected_schema = expected_schema,
    recursion_limit = 50L,
    verbose = FALSE
  )

  expect_true(is.list(result$raw_response))
  expect_equal(result$raw_response$expected_schema_source, "explicit")

  response_text <- result$message
  parsed <- jsonlite::fromJSON(response_text)
  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))

  if (is.data.frame(parsed$items)) {
    expect_true(all(c("name", "birth_year") %in% names(parsed$items)))
  }

  expect_true(is.list(result$raw_response$json_repair))
  expect_true(length(result$raw_response$json_repair) >= 1)
})
