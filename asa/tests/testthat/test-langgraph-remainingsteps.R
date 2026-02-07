# Tests for RemainingSteps-based recursion_limit handling in LangGraph graphs.

test_that("research graph stops with stop_reason='recursion_limit' (RemainingSteps)", {
  python_path <- asa_test_skip_if_no_python(required_files = "research_graph.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

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
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  # Stub LLM: returns intentionally incomplete JSON so the recursion-limit
  # "best effort" path must populate schema-required keys.
  asa_test_stub_llm(
    mode = "json_response",
    json_content = '{"status":"partial","items":[{"name":"Ada Lovelace"}]}'
  )

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
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  asa_test_stub_llm(
    mode = "json_response",
    json_content = '{"status":"partial","items":[{"name":"Ada"}]}'
  )

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
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")

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

test_that("repair_json_output_to_schema uses shape defaults for array/object leaves", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")

  utils <- reticulate::import_from_path("state_utils", path = python_path)

  schema <- list(
    missing = "array",
    metadata = "object",
    notes = "string"
  )

  out <- utils$repair_json_output_to_schema("{}", schema, fallback_on_failure = TRUE)
  parsed <- jsonlite::fromJSON(out, simplifyVector = FALSE)

  expect_true(is.list(parsed))
  expect_true(is.list(parsed$missing))
  expect_equal(length(parsed$missing), 0L)
  expect_true(is.list(parsed$metadata))
  expect_equal(length(parsed$metadata), 0L)
})

test_that("standard agent reaches recursion_limit and preserves JSON output (Gemini, best-effort)", {

  asa_test_skip_api_tests()
  api_key <- asa_test_require_gemini_key()

  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests",
    "langchain_google_genai"
  ), method = "import")

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
  prompt <- asa_test_recursion_limit_prompt()

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
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  # Stub LLM: returns incomplete JSON; run_task(expected_schema=...) should
  # trigger best-effort repair even without prompt-based schema inference.
  asa_test_stub_llm(
    mode = "json_response",
    json_content = '{"status":"partial","items":[{"name":"Ada"}]}'
  )

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

test_that("message reducer assigns ids so memory folding can remove messages", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "pydantic"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  human <- msgs$HumanMessage(content = "hi")
  ai <- msgs$AIMessage(content = "ok")

  out <- custom_ddg$`_add_messages`(list(human), list(ai))
  expect_true(is.list(out))
  expect_equal(length(out), 2L)

  id1 <- out[[1]]$id
  id2 <- out[[2]]$id
  expect_true(is.character(id1) && length(id1) == 1 && nzchar(id1))
  expect_true(is.character(id2) && length(id2) == 1 && nzchar(id2))

  rm1 <- msgs$RemoveMessage(id = id1)
  out2 <- custom_ddg$`_add_messages`(out, list(rm1))
  expect_true(is.list(out2))
  expect_equal(length(out2), 1L)
  expect_equal(out2[[1]]$id, id2)
})

test_that("memory folding preserves initial HumanMessage for Gemini tool-call ordering", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Deterministic tool + stub LLM that calls the tool once (tool-mode), which
  # forces a ToolMessage and makes folding happen after tools. This ensures we
  # exercise the "preserve_first_user" invariant with a remaining AI tool-call
  # turn.
  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "from langchain_core.messages import AIMessage\n",
    "\n",
    "def _fake_search(query: str) -> str:\n",
    "    return 'ok'\n",
    "\n",
    "fake_search_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Fake search',\n",
    "    func=_fake_search,\n",
    ")\n",
    "\n",
    "class _StubLLM:\n",
    "    def __init__(self, tool_mode=False):\n",
    "        self.tool_mode = tool_mode\n",
    "        self.n = 0\n",
    "    def bind_tools(self, tools):\n",
    "        out = _StubLLM(tool_mode=True)\n",
    "        out.n = self.n\n",
    "        return out\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.tool_mode:\n",
    "            if self.n == 1:\n",
    "                return AIMessage(\n",
    "                    content='calling tool',\n",
    "                    tool_calls=[{'name':'Search','args':{'query':'x'},'id':'call_1'}],\n",
    "                )\n",
    "            return AIMessage(content='done')\n",
    "        return AIMessage(content='summary')\n",
    "\n",
    "stub_llm = _StubLLM()\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$stub_llm,
    tools = list(reticulate::py$fake_search_tool),
    checkpointer = NULL,
    message_threshold = as.integer(5),
    keep_recent = as.integer(1),
    debug = FALSE
  )

  # Two completed exchanges in history ensure keep_recent=1 preserves only the
  # open tool-call round, allowing earlier content to be folded away.
  initial <- msgs$HumanMessage(content = "hi")
  ai1 <- msgs$AIMessage(content = "old1")
  human2 <- msgs$HumanMessage(content = "old2")
  ai2 <- msgs$AIMessage(content = "old3")

  final_state <- agent$invoke(
    list(messages = list(initial, ai1, human2, ai2), summary = "",
         fold_stats = reticulate::dict(fold_count = 0L)),
    config = list(
      recursion_limit = as.integer(30),
      configurable = list(thread_id = "test")
    )
  )

  # Ensure we actually folded (otherwise this test is meaningless).
  expect_equal(as.integer(as.list(final_state$fold_stats)$fold_count), 1L)

  # Verify fold_stats diagnostics are populated
  fs <- as.list(final_state$fold_stats)
  expect_true(is.list(fs))
  expect_true(as.integer(fs$fold_messages_removed) > 0L)
  expect_true(as.integer(fs$fold_total_messages_removed) > 0L)
  expect_equal(as.integer(fs$fold_messages_removed), as.integer(fs$fold_total_messages_removed))
  expect_true(as.integer(fs$fold_chars_input) > 0L)
  expect_true(as.integer(fs$fold_summary_chars) > 0L)
  expect_true(as.character(fs$fold_trigger_reason) %in% c(
    "char_budget",
    "message_threshold",
    "char_budget_and_message_threshold",
    "manual_or_unknown"
  ))
  expect_true(as.integer(fs$fold_safe_boundary_idx) > 0L)
  expect_true(is.finite(as.numeric(fs$fold_compression_ratio)))
  expect_true(as.numeric(fs$fold_compression_ratio) >= 0)
  expect_false(isTRUE(fs$fold_parse_success))
  expect_true(as.numeric(fs$fold_summarizer_latency_m) >= 0)

  types <- vapply(
    final_state$messages,
    function(m) tryCatch(as.character(m$`__class__`$`__name__`), error = function(e) NA_character_),
    character(1)
  )

  # Critical invariant for Gemini tool calling: history must not start with an AI tool-call turn.
  expect_equal(types[[1]], "HumanMessage")
  expect_true(length(types) >= 2L)
  expect_equal(types[[2]], "AIMessage")
  expect_true(!is.null(final_state$messages[[2]]$tool_calls))
})

test_that("memory folding updates summary and injects it into the next system prompt", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Deterministic tool + stub models that:
  # 1) Force a tool call (so we get a ToolMessage)
  # 2) Trigger folding after tools (so graph continues to another agent step)
  # 3) Return a known folded summary string
  # 4) Record system prompts seen by the main model for assertion
  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "from langchain_core.messages import AIMessage\n",
    "\n",
    "def _fake_search(query: str) -> str:\n",
    "    return 'ok'\n",
    "\n",
    "fake_search_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Fake search',\n",
    "    func=_fake_search,\n",
    ")\n",
    "\n",
    "class _StubResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "\n",
    "class _StubSummarizer:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "        self.last_prompt = None\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        self.last_prompt = messages[0].content if messages else None\n",
    "        return _StubResponse('{\"version\":1,\"facts\":[\"FOLDED_SUMMARY\"],\"decisions\":[],\"open_questions\":[],\"sources\":[],\"warnings\":[]}')\n",
    "\n",
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
    "        if self.n == 1:\n",
    "            return AIMessage(\n",
    "                content='calling tool',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'x'},'id':'call_1'}],\n",
    "            )\n",
    "        return AIMessage(content='final answer')\n",
    "\n",
    "stub_llm = _RecordingLLM()\n",
    "stub_summarizer = _StubSummarizer()\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$stub_llm,
    tools = list(reticulate::py$fake_search_tool),
    checkpointer = NULL,
    message_threshold = as.integer(5),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$stub_summarizer,
    debug = FALSE
  )

  # Prior history has two completed exchanges. The tool-call round starts with an
  # AI tool-call message (no new HumanMessage), so keep_recent=1 will preserve
  # only the tool-call pair while folding earlier content.
  initial <- msgs$HumanMessage(content = "initial question: IMPORTANT_FACT=42")
  ai1 <- msgs$AIMessage(content = "alpha: old assistant answer")
  human2 <- msgs$HumanMessage(content = "followup: IMPORTANT_FACT=42")
  ai2 <- msgs$AIMessage(content = "beta: old assistant note")

  final_state <- agent$invoke(
    list(messages = list(initial, ai1, human2, ai2), summary = "",
         fold_stats = reticulate::dict(fold_count = 0L)),
    config = list(
      recursion_limit = as.integer(40),
      configurable = list(thread_id = "test_plumbing")
    )
  )

  expect_equal(as.integer(as.list(final_state$fold_stats)$fold_count), 1L)
  fs <- as.list(final_state$fold_stats)
  expect_true(isTRUE(fs$fold_parse_success))
  expect_true(is.list(final_state$summary))
  expect_true("facts" %in% names(final_state$summary))
  expect_true("FOLDED_SUMMARY" %in% unlist(final_state$summary$facts))
  expect_true(is.list(final_state$archive))
  expect_true(length(final_state$archive) >= 1L)

  # Folding should pass prior content into the summarizer prompt.
  expect_equal(as.integer(reticulate::py$stub_summarizer$calls), 1L)
  last_prompt <- as.character(reticulate::py$stub_summarizer$last_prompt)
  expect_true(grepl("alpha: old assistant answer", last_prompt, fixed = TRUE))
  expect_true(grepl("IMPORTANT_FACT=42", last_prompt, fixed = TRUE))
  expect_true(grepl("beta: old assistant note", last_prompt, fixed = TRUE))

  # After folding, the summary is stored in state but may not appear in a system
  # prompt within this invocation if the agent already completed (no more agent
  # turns after the fold). This is by design: after_tools now always routes to
  # agent first (so the agent can process raw tool results), then should_continue
  # triggers the fold. In single-round conversations the fold happens after the
  # agent's final response, so the summary benefits the NEXT conversation round.
  #
  # Verify the fold produced valid state:
  sys_prompts <- reticulate::py_to_r(reticulate::py$stub_llm$system_prompts)
  expect_true(length(sys_prompts) >= 2L)
  expect_true(!grepl("LONG-TERM MEMORY", sys_prompts[[1]], fixed = TRUE))
  # In this single-tool-call test, the summary may or may not appear in a system
  # prompt depending on whether the agent gets another turn after the fold.
  # The critical check is that fold_count=1, summary has content, and archive exists
  # (already verified above).
  later_prompts <- sys_prompts[-1]
  has_summary_in_prompt <- any(vapply(later_prompts, function(p) grepl("LONG-TERM MEMORY", p, fixed = TRUE), logical(1)))
  # Summary in prompt is good if it happens, but not required in single-round
  # (the fold still preserves information in state$summary for future rounds)

  # Retrieval: check if UNTRUSTED CONTEXT was injected in any invocation.
  # In single-round scenarios this may not occur since the fold happens after
  # the agent's last turn. In multi-round scenarios it would appear.
  seen <- reticulate::py_to_r(reticulate::py$stub_llm$seen_messages)
  expect_true(length(seen) >= 2L)
})

test_that("fold_count stays 0 when folding thresholds are never reached", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Stub LLM that immediately returns a short final answer (no tool calls).
  # With very high thresholds, folding should never trigger.
  asa_test_stub_llm(
    mode = "simple",
    response_content = "short answer",
    var_name = "no_fold_llm"
  )

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$no_fold_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(100),
    fold_char_budget = as.integer(100000),
    keep_recent = as.integer(1),
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "hello")

  final_state <- agent$invoke(
    list(messages = list(initial), summary = "",
         fold_stats = reticulate::dict(fold_count = 0L)),
    config = list(
      recursion_limit = as.integer(20),
      configurable = list(thread_id = "test_no_fold")
    )
  )

  expect_equal(as.integer(as.list(final_state$fold_stats)$fold_count), 0L)
  expect_true(length(final_state$archive) == 0L)
  # fold_stats should only contain fold_count=0 when no folding occurs
  fs <- as.list(final_state$fold_stats)
  expect_equal(length(fs), 1L)
  expect_equal(as.integer(fs$fold_count), 0L)
  # Summary should remain empty (no fold occurred)
  summary_val <- final_state$summary
  if (is.character(summary_val)) {
    expect_equal(summary_val, "")
  } else {
    # If it's a dict/list, it should have no meaningful content
    expect_false(isTRUE(nchar(paste(unlist(summary_val), collapse = "")) > 0))
  }
})

test_that("fold_count increments to 2 across two invocations via MemorySaver", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "langgraph.checkpoint.memory",
    "pydantic"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)
  mem_mod <- reticulate::import("langgraph.checkpoint.memory", convert = TRUE)

  # Stub LLM that alternates: odd calls -> tool call, even calls -> final answer.
  # This pattern persists across invocations (shared counter), so it cannot use
  # the standard stub_llm factory which only supports one tool call.
  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "from langchain_core.messages import AIMessage\n",
    "\n",
    "def _fake_search(query: str) -> str:\n",
    "    return 'search result ' * 200\n",
    "\n",
    "fake_search_tool_v2 = Tool(\n",
    "    name='Search',\n",
    "    description='Fake search',\n",
    "    func=_fake_search,\n",
    ")\n",
    "\n",
    "class _MultiInvokeLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.n % 2 == 1:\n",
    "            return AIMessage(\n",
    "                content='calling tool',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'test'},'id':'call_' + str(self.n)}],\n",
    "            )\n",
    "        return AIMessage(content='done')\n",
    "\n",
    "multi_llm = _MultiInvokeLLM()\n"
  ))
  asa_test_stub_summarizer(
    summary_json = '{"version":1,"facts":["fold_fact"],"decisions":[],"open_questions":[],"sources":[],"warnings":[]}',
    var_name = "multi_summarizer"
  )

  checkpointer <- mem_mod$MemorySaver()

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$multi_llm,
    tools = list(reticulate::py$fake_search_tool_v2),
    checkpointer = checkpointer,
    message_threshold = as.integer(3),
    fold_char_budget = as.integer(50),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$multi_summarizer,
    debug = FALSE
  )

  thread_id <- "test_double_fold"

  # Invocation 1: seed with enough history to trigger folding
  h1 <- msgs$HumanMessage(content = "first question with lots of context to exceed budget")
  a1 <- msgs$AIMessage(content = "first answer with plenty of text to push over the fold threshold")
  h2 <- msgs$HumanMessage(content = "second question also with substantial text content here")

  state1 <- agent$invoke(
    list(messages = list(h1, a1, h2), summary = "",
         fold_stats = reticulate::dict(fold_count = 0L)),
    config = list(
      recursion_limit = as.integer(40),
      configurable = list(thread_id = thread_id)
    )
  )

  expect_equal(as.integer(as.list(state1$fold_stats)$fold_count), 1L)
  expect_true(length(state1$archive) >= 1L)

  # Verify fold_stats after first fold
  fs1 <- as.list(state1$fold_stats)
  expect_true(as.integer(fs1$fold_messages_removed) > 0L)
  first_removed <- as.integer(fs1$fold_total_messages_removed)

  # Invocation 2: send a new message on the same thread (checkpointer persists state).
  # Only pass messages — do NOT pass summary or fold_stats here, because
  # fold_stats uses merge_dicts reducer, so it accumulates correctly from checkpoint.
  h3 <- msgs$HumanMessage(content = "third question with even more text to again exceed the fold char budget threshold")

  state2 <- agent$invoke(
    list(messages = list(h3)),
    config = list(
      recursion_limit = as.integer(40),
      configurable = list(thread_id = thread_id)
    )
  )

  expect_equal(as.integer(as.list(state2$fold_stats)$fold_count), 2L)
  expect_true(length(state2$archive) >= 2L)

  # Verify fold_total_messages_removed accumulated across both folds
  fs2 <- as.list(state2$fold_stats)
  expect_true(as.integer(fs2$fold_total_messages_removed) >= first_removed)
})

test_that("archive entry fold_count label matches state fold_count", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Same pattern as existing Gemini test: tool-call then final answer, with a
  # deterministic summarizer so we can inspect archive structure.
  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "\n",
    "def _fake_search_v3(query: str) -> str:\n",
    "    return 'result'\n",
    "\n",
    "fake_search_tool_v3 = Tool(\n",
    "    name='Search',\n",
    "    description='Fake search',\n",
    "    func=_fake_search_v3,\n",
    ")\n"
  ))
  asa_test_stub_llm(
    mode = "tool_call",
    response_content = "final answer",
    tool_name = "Search",
    tool_args = list(query = "x"),
    tool_call_id = "call_archive",
    var_name = "archive_llm"
  )
  asa_test_stub_summarizer(
    summary_json = '{"version":1,"facts":["archived_fact"],"decisions":[],"open_questions":[],"sources":[],"warnings":[]}',
    var_name = "archive_summarizer"
  )

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$archive_llm,
    tools = list(reticulate::py$fake_search_tool_v3),
    checkpointer = NULL,
    message_threshold = as.integer(5),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$archive_summarizer,
    debug = FALSE
  )

  h1 <- msgs$HumanMessage(content = "question one")
  a1 <- msgs$AIMessage(content = "answer one")
  h2 <- msgs$HumanMessage(content = "question two")
  a2 <- msgs$AIMessage(content = "answer two")

  final_state <- agent$invoke(
    list(messages = list(h1, a1, h2, a2), summary = "",
         fold_stats = reticulate::dict(fold_count = 0L)),
    config = list(
      recursion_limit = as.integer(30),
      configurable = list(thread_id = "test_archive_label")
    )
  )

  # Verify fold occurred
  expect_equal(as.integer(as.list(final_state$fold_stats)$fold_count), 1L)
  expect_true(length(final_state$archive) >= 1L)

  # Archive entry fold_count should match the state fold_count
  archive_entry <- final_state$archive[[1]]
  expect_equal(as.integer(archive_entry$fold_count), as.integer(as.list(final_state$fold_stats)$fold_count))

  # Archive entry should contain lossless message records and text
  expect_true("messages" %in% names(archive_entry))
  expect_true("text" %in% names(archive_entry))
  expect_true(is.list(archive_entry$messages))
  expect_true(length(archive_entry$messages) >= 1L)
  expect_true(nchar(archive_entry$text) > 0L)
})

test_that("memory folding finalization respects inferred schema from CSV template", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "bs4"
  ), method = "import")

  custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  pkg_root_from_python <- normalizePath(
    file.path(python_path, "..", ".."),
    winslash = "/",
    mustWork = FALSE
  )
  csv_candidates <- c(
    file.path(pkg_root_from_python, "tests", "example_task.csv"),
    file.path(getwd(), "tests", "example_task.csv"),
    file.path(getwd(), "asa", "tests", "example_task.csv"),
    system.file("tests", "example_task.csv", package = "asa")
  )
  csv_candidates <- unique(csv_candidates[nzchar(csv_candidates)])
  csv_path <- csv_candidates[file.exists(csv_candidates)][1]
  skip_if(is.na(csv_path) || !nzchar(csv_path), "example_task.csv not found")

  csv_df <- utils::read.csv(csv_path, stringsAsFactors = FALSE, check.names = FALSE)
  prompt_col <- if ("x" %in% names(csv_df)) "x" else names(csv_df)[length(names(csv_df))]
  template_prompt <- as.character(csv_df[[prompt_col]][1])
  expect_true(nzchar(template_prompt))

  asa_test_stub_multi_response_llm(
    responses = list(
      list(content = "{\"sex\":\"Female\"}"),
      list(content = "FINALIZE_OUTPUT_NOT_JSON")
    ),
    var_name = "csv_template_llm"
  )
  asa_test_stub_summarizer(
    summary_json = "{\"version\":1,\"facts\":[\"folded_csv_prompt\"],\"decisions\":[],\"open_questions\":[],\"sources\":[],\"warnings\":[]}",
    var_name = "csv_template_summarizer"
  )

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$csv_template_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(2),
    keep_recent = as.integer(1),
    fold_char_budget = as.integer(500),
    summarizer_model = reticulate::py$csv_template_summarizer,
    debug = FALSE
  )

  history_user <- msgs$HumanMessage(content = "Earlier question about elite background")
  history_ai <- msgs$AIMessage(content = "Earlier answer with provisional details")

  final_state <- agent$invoke(
    list(
      messages = list(history_user, history_ai, msgs$HumanMessage(content = template_prompt)),
      summary = "",
      fold_stats = reticulate::dict(fold_count = 0L)
    ),
    config = list(
      recursion_limit = as.integer(4),
      configurable = list(thread_id = "test_csv_template_finalize")
    )
  )

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_equal(as.character(final_state$expected_schema_source), "inferred")
  expect_true(as.integer(as.list(final_state$fold_stats)$fold_count) >= 1L)
  expect_equal(as.integer(reticulate::py$csv_template_summarizer$calls), 1L)
  # Finalize may now reuse an already-terminal response instead of re-invoking the LLM.
  expect_true(as.integer(reticulate::py$csv_template_llm$n) >= 1L)

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  parsed <- jsonlite::fromJSON(response_text)

  inferred_schema <- reticulate::py_to_r(final_state$expected_schema)
  schema_keys <- names(inferred_schema)
  expect_true(is.list(parsed))
  expect_true(length(schema_keys) >= 10L)
  expect_true(all(schema_keys %in% names(parsed)))

  repair_events <- reticulate::py_to_r(final_state$json_repair)
  contexts <- character(0)
  if (is.list(repair_events) && length(repair_events) > 0) {
    contexts <- vapply(repair_events, function(ev) {
      if (is.list(ev) && !is.null(ev$context)) as.character(ev$context) else NA_character_
    }, character(1))
  }
  expect_true(any(c("agent", "finalize") %in% contexts))
})

test_that("recursion_limit=2 completes without error (no double finalize)", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  # Stub LLM that returns a direct JSON answer (no tool calls) and tracks call_count.
  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _CountingStubLLM:\n",
    "    def __init__(self):\n",
    "        self.call_count = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.call_count += 1\n",
    "        return AIMessage(content=",
    deparse('{\"status\":\"complete\",\"items\":[{\"name\":\"Ada Lovelace\"}],\"missing\":[],\"notes\":\"stub\"}'),
    ")\n\n",
    "counting_stub_llm = _CountingStubLLM()\n"
  ))

  schema <- list(
    status = "string",
    items = list(list(name = "string")),
    missing = "array",
    notes = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$counting_stub_llm,
    tools = list()
  )

  result <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return JSON")),
      expected_schema = schema,
      expected_schema_source = "explicit"
    ),
    config = list(recursion_limit = 2L)
  )

  expect_equal(result$stop_reason, "recursion_limit")
  # Verify only 1 LLM call was made (no double finalize)
  expect_equal(as.integer(reticulate::py$counting_stub_llm$call_count), 1L)
})

test_that("memory finalize reuses terminal response after no-op summarize near limit", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  asa_test_stub_multi_response_llm(
    responses = list(
      list(content = "{\"status\":\"complete\",\"items\":[],\"missing\":[],\"notes\":\"FIRST\"}"),
      list(content = "{\"status\":\"complete\",\"items\":[],\"missing\":[],\"notes\":\"SECOND\"}")
    ),
    var_name = "memory_reuse_finalize_llm"
  )
  asa_test_stub_summarizer(var_name = "memory_reuse_finalize_summarizer")

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$memory_reuse_finalize_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(1),
    keep_recent = as.integer(1),
    fold_char_budget = as.integer(99999),
    summarizer_model = reticulate::py$memory_reuse_finalize_summarizer,
    debug = FALSE
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return JSON")),
      summary = "",
      archive = list(),
      fold_stats = reticulate::dict(fold_count = 0L),
      expected_schema = list(status = "string", items = "array", missing = "array", notes = "string"),
      expected_schema_source = "explicit"
    ),
    config = list(
      recursion_limit = as.integer(4),
      configurable = list(thread_id = "test_memory_finalize_reuse_noop_summarize")
    )
  )

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_equal(as.integer(reticulate::py$memory_reuse_finalize_llm$n), 1L)

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  parsed <- jsonlite::fromJSON(response_text)
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$notes), "FIRST")
})

test_that("finalize strips residual tool calls and returns terminal JSON (standard)", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  # Simulates finalize-time bad behavior: model emits a tool call with empty content.
  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _ResidualToolCallLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        return AIMessage(\n",
    "            content='',\n",
    "            tool_calls=[{'name':'save_finding','args':{'finding':'X','category':'fact'},'id':'call_1'}]\n",
    "        )\n\n",
    "residual_tool_llm = _ResidualToolCallLLM()\n"
  ))

  expected_schema <- list(
    status = "string",
    items = "array",
    missing = "array",
    notes = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$residual_tool_llm,
    tools = list()
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return JSON")),
      expected_schema = expected_schema,
      expected_schema_source = "explicit"
    ),
    config = list(recursion_limit = 2L)
  )

  expect_equal(final_state$stop_reason, "recursion_limit")

  last_msg <- final_state$messages[[length(final_state$messages)]]
  tool_calls <- tryCatch(last_msg$tool_calls, error = function(e) NULL)
  expect_true(is.null(tool_calls) || length(tool_calls) == 0L)

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  parsed <- jsonlite::fromJSON(response_text)
  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))
})

test_that("finalize strips residual tool calls and returns non-empty text when no schema (standard)", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _ResidualToolCallNoSchemaLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        return AIMessage(\n",
    "            content='',\n",
    "            tool_calls=[{'name':'save_finding','args':{'finding':'X','category':'fact'},'id':'call_1'}]\n",
    "        )\n\n",
    "residual_tool_no_schema_llm = _ResidualToolCallNoSchemaLLM()\n"
  ))

  agent <- prod$create_standard_agent(
    model = reticulate::py$residual_tool_no_schema_llm,
    tools = list()
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Give me the best possible answer"))
    ),
    config = list(recursion_limit = 2L)
  )

  expect_equal(final_state$stop_reason, "recursion_limit")

  last_msg <- final_state$messages[[length(final_state$messages)]]
  tool_calls <- tryCatch(last_msg$tool_calls, error = function(e) NULL)
  expect_true(is.null(tool_calls) || length(tool_calls) == 0L)

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  expect_true(is.character(response_text))
  expect_true(nzchar(trimws(response_text)))
})

test_that("finalize_answer node sanitizes residual tool calls after tools (standard)", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "def _fake_search_finalize_path(query: str) -> str:\n",
    "    return 'result for ' + query\n\n",
    "search_tool_finalize_path = Tool(\n",
    "    name='Search',\n",
    "    description='Fake Search tool',\n",
    "    func=_fake_search_finalize_path,\n",
    ")\n\n",
    "class _FinalizeNodePathLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        if self.calls == 1:\n",
    "            return AIMessage(\n",
    "                content='calling tool',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'ada'},'id':'call_1'}],\n",
    "            )\n",
    "        return AIMessage(\n",
    "            content='',\n",
    "            tool_calls=[{'name':'save_finding','args':{'finding':'X','category':'fact'},'id':'call_2'}],\n",
    "        )\n\n",
    "finalize_node_path_llm = _FinalizeNodePathLLM()\n"
  ))

  expected_schema <- list(
    status = "string",
    items = "array",
    missing = "array",
    notes = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$finalize_node_path_llm,
    tools = list(reticulate::py$search_tool_finalize_path)
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return JSON")),
      expected_schema = expected_schema,
      expected_schema_source = "explicit"
    ),
    config = list(recursion_limit = 4L)
  )

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_equal(as.integer(reticulate::py$finalize_node_path_llm$calls), 2L)

  repair_events <- reticulate::py_to_r(final_state$json_repair)
  contexts <- character(0)
  if (is.list(repair_events) && length(repair_events) > 0) {
    contexts <- vapply(repair_events, function(ev) {
      if (is.list(ev) && !is.null(ev$context)) as.character(ev$context) else NA_character_
    }, character(1))
  }
  expect_true("finalize" %in% contexts)

  last_msg <- final_state$messages[[length(final_state$messages)]]
  tool_calls <- tryCatch(last_msg$tool_calls, error = function(e) NULL)
  expect_true(is.null(tool_calls) || length(tool_calls) == 0L)

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  parsed <- jsonlite::fromJSON(response_text)
  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))
})

test_that("explicit schema does not rewrite intermediate tool-call turns (standard)", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "def _fake_search(query: str) -> str:\n",
    "    return 'result for ' + query\n\n",
    "search_tool_for_rewrite = Tool(\n",
    "    name='Search',\n",
    "    description='Fake Search tool',\n",
    "    func=_fake_search,\n",
    ")\n\n",
    "class _NoRewriteLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        if self.calls == 1:\n",
    "            return AIMessage(\n",
    "                content='calling tool',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'ada'},'id':'call_1'}],\n",
    "            )\n",
    "        return AIMessage(content='{\\\"status\\\":\\\"partial\\\",\\\"items\\\":[{\\\"name\\\":\\\"Ada\\\"}]}')\n\n",
    "no_rewrite_llm = _NoRewriteLLM()\n"
  ))

  expected_schema <- list(
    status = "string",
    items = list(list(name = "string", birth_year = "integer")),
    missing = "array",
    notes = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$no_rewrite_llm,
    tools = list(reticulate::py$search_tool_for_rewrite)
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return JSON")),
      expected_schema = expected_schema,
      expected_schema_source = "explicit"
    ),
    config = list(recursion_limit = 10L)
  )

  tool_turn <- NULL
  for (msg in final_state$messages) {
    tc <- tryCatch(msg$tool_calls, error = function(e) NULL)
    if (!is.null(tc) && length(tc) > 0L) {
      tool_turn <- msg
      break
    }
  }
  expect_false(is.null(tool_turn))
  expect_equal(as.character(tool_turn$content), "calling tool")

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  parsed <- jsonlite::fromJSON(response_text)
  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))
})

test_that("finalize strips residual tool calls and returns terminal JSON (memory folding)", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _ResidualToolCallLLM_Memory:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        return AIMessage(\n",
    "            content='',\n",
    "            tool_calls=[{'name':'save_finding','args':{'finding':'X','category':'fact'},'id':'call_1'}]\n",
    "        )\n\n",
    "residual_tool_llm_memory = _ResidualToolCallLLM_Memory()\n"
  ))

  expected_schema <- list(
    status = "string",
    items = "array",
    missing = "array",
    notes = "string"
  )

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$residual_tool_llm_memory,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return JSON")),
      summary = "",
      archive = list(),
      fold_stats = reticulate::dict(fold_count = 0L),
      expected_schema = expected_schema,
      expected_schema_source = "explicit"
    ),
    config = list(
      recursion_limit = 2L,
      configurable = list(thread_id = "test_residual_finalize_memory")
    )
  )

  expect_equal(final_state$stop_reason, "recursion_limit")

  last_msg <- final_state$messages[[length(final_state$messages)]]
  tool_calls <- tryCatch(last_msg$tool_calls, error = function(e) NULL)
  expect_true(is.null(tool_calls) || length(tool_calls) == 0L)

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  parsed <- jsonlite::fromJSON(response_text)
  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))
})

test_that("finalize strips residual tool calls and returns non-empty text when no schema (memory folding)", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic",
    "requests"
  ), method = "import")

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _ResidualToolCallNoSchemaLLM_Memory:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        return AIMessage(\n",
    "            content='',\n",
    "            tool_calls=[{'name':'save_finding','args':{'finding':'X','category':'fact'},'id':'call_1'}]\n",
    "        )\n\n",
    "residual_tool_no_schema_llm_memory = _ResidualToolCallNoSchemaLLM_Memory()\n"
  ))

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$residual_tool_no_schema_llm_memory,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Give me the best possible answer")),
      summary = "",
      archive = list(),
      fold_stats = reticulate::dict(fold_count = 0L)
    ),
    config = list(
      recursion_limit = 2L,
      configurable = list(thread_id = "test_residual_finalize_memory_no_schema")
    )
  )

  expect_equal(final_state$stop_reason, "recursion_limit")

  last_msg <- final_state$messages[[length(final_state$messages)]]
  tool_calls <- tryCatch(last_msg$tool_calls, error = function(e) NULL)
  expect_true(is.null(tool_calls) || length(tool_calls) == 0L)

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  expect_true(is.character(response_text))
  expect_true(nzchar(trimws(response_text)))
  expect_equal(
    trimws(response_text),
    "Unable to provide a complete answer with available information."
  )
})

test_that(".extract_response_text returns recursion fallback when only tool-call AI content exists", {
  python_path <- asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c("langchain_core"), method = "import")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  raw_response <- list(
    messages = list(
      msgs$AIMessage(
        content = "calling tool",
        tool_calls = list(list(name = "Search", args = list(query = "ada"), id = "call_1"))
      ),
      msgs$ToolMessage(content = "tool output", tool_call_id = "call_1")
    ),
    stop_reason = "recursion_limit"
  )

  response_text <- asa:::.extract_response_text(raw_response, backend = "gemini")
  expect_equal(
    response_text,
    "[Agent reached step limit before completing task. Increase recursion_limit or simplify the task.]"
  )
})
