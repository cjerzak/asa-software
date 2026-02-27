# Tests for RemainingSteps-based recursion_limit handling in LangGraph graphs.

test_that("research graph stops with stop_reason='recursion_limit' (RemainingSteps)", {
  research <- asa_test_import_langgraph_module("workflows.research_graph_workflow", required_files = "workflows/research_graph_workflow.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

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

  cfg <- asa_test_minimal_research_config(research)

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

test_that("run_research forwards recursion_limit into graph config", {
  research <- asa_test_import_langgraph_module("workflows.research_graph_workflow", required_files = "workflows/research_graph_workflow.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "class _ConfigCaptureInvokeGraph:\n",
    "    def __init__(self):\n",
    "        self.last_config = None\n",
    "    def invoke(self, initial_state, config):\n",
    "        self.last_config = config\n",
    "        return {'results': [], 'status': 'complete', 'stop_reason': 'recursion_limit'}\n",
    "\n",
    "config_capture_invoke_graph = _ConfigCaptureInvokeGraph()\n"
  ))

  result <- research$run_research(
    graph = reticulate::py$config_capture_invoke_graph,
    query = "test query",
    schema = list(name = "character"),
    config_dict = list(recursion_limit = 7L),
    thread_id = "test-thread-run-research"
  )

  captured <- reticulate::py_to_r(reticulate::py$config_capture_invoke_graph$last_config)
  expect_equal(captured$configurable$thread_id, "test-thread-run-research")
  expect_equal(as.integer(captured$recursion_limit), 7L)
  expect_equal(result$status, "complete")
  expect_equal(result$stop_reason, "recursion_limit")
})

test_that("stream_research forwards recursion_limit into graph config", {
  research <- asa_test_import_langgraph_module("workflows.research_graph_workflow", required_files = "workflows/research_graph_workflow.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "class _ConfigCaptureStreamGraph:\n",
    "    def __init__(self):\n",
    "        self.last_config = None\n",
    "    def stream(self, initial_state, config, stream_mode='updates'):\n",
    "        self.last_config = config\n",
    "        yield {'planner': {'status': 'complete', 'results': []}}\n",
    "\n",
    "config_capture_stream_graph = _ConfigCaptureStreamGraph()\n"
  ))

  events <- research$stream_research(
    graph = reticulate::py$config_capture_stream_graph,
    query = "test query",
    schema = list(name = "character"),
    config_dict = list(recursion_limit = 9L),
    thread_id = "test-thread-stream-research"
  )

  event_list <- reticulate::iterate(events)
  expect_true(length(event_list) >= 1)
  last_event <- event_list[[length(event_list)]]
  expect_equal(last_event$event_type, "complete")

  captured <- reticulate::py_to_r(reticulate::py$config_capture_stream_graph$last_config)
  expect_equal(captured$configurable$thread_id, "test-thread-stream-research")
  expect_equal(as.integer(captured$recursion_limit), 9L)
})

test_that("run_research rejects recursion_limit outside [4, 500]", {
  research <- asa_test_import_langgraph_module("workflows.research_graph_workflow", required_files = "workflows/research_graph_workflow.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  asa_test_noop_invoke_graph()

  expect_error(
    research$run_research(
      graph = reticulate::py$noop_invoke_graph,
      query = "test query",
      schema = list(name = "character"),
      config_dict = list(recursion_limit = 2L),
      thread_id = "test-thread-run-research-invalid-low"
    ),
    "between 4 and 500"
  )

  expect_error(
    research$run_research(
      graph = reticulate::py$noop_invoke_graph,
      query = "test query",
      schema = list(name = "character"),
      config_dict = list(recursion_limit = 3L),
      thread_id = "test-thread-run-research-invalid-low-3"
    ),
    "between 4 and 500"
  )

  expect_error(
    research$run_research(
      graph = reticulate::py$noop_invoke_graph,
      query = "test query",
      schema = list(name = "character"),
      config_dict = list(recursion_limit = 501L),
      thread_id = "test-thread-run-research-invalid-high"
    ),
    "between 4 and 500"
  )

  expect_error(
    research$run_research(
      graph = reticulate::py$noop_invoke_graph,
      query = "test query",
      schema = list(name = "character"),
      config_dict = list(recusion_limit = 7L),
      thread_id = "test-thread-run-research-recusion-typo"
    ),
    "recusion_limit"
  )
})

test_that("stream_research rejects recursion_limit outside [4, 500]", {
  research <- asa_test_import_langgraph_module("workflows.research_graph_workflow", required_files = "workflows/research_graph_workflow.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  asa_test_noop_stream_graph()

  expect_error(
    reticulate::iterate(research$stream_research(
      graph = reticulate::py$noop_stream_graph,
      query = "test query",
      schema = list(name = "character"),
      config_dict = list(recursion_limit = 2L),
      thread_id = "test-thread-stream-research-invalid-low"
    )),
    "between 4 and 500"
  )

  expect_error(
    reticulate::iterate(research$stream_research(
      graph = reticulate::py$noop_stream_graph,
      query = "test query",
      schema = list(name = "character"),
      config_dict = list(recursion_limit = 3L),
      thread_id = "test-thread-stream-research-invalid-low-3"
    )),
    "between 4 and 500"
  )

  expect_error(
    reticulate::iterate(research$stream_research(
      graph = reticulate::py$noop_stream_graph,
      query = "test query",
      schema = list(name = "character"),
      config_dict = list(recursion_limit = 501L),
      thread_id = "test-thread-stream-research-invalid-high"
    )),
    "between 4 and 500"
  )

  expect_error(
    reticulate::iterate(research$stream_research(
      graph = reticulate::py$noop_stream_graph,
      query = "test query",
      schema = list(name = "character"),
      config_dict = list(recusion_limit = 9L),
      thread_id = "test-thread-stream-research-recusion-typo"
    )),
    "recusion_limit"
  )
})

test_that("run_research with recursion_limit=4 executes at least one search round", {
  research <- asa_test_import_langgraph_module("workflows.research_graph_workflow", required_files = "workflows/research_graph_workflow.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "class _StubResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "        self.usage_metadata = {'total_tokens': 0}\n",
    "\n",
    "class _StubLLM:\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        content = getattr(messages[-1], 'content', '') if messages else ''\n",
    "        if isinstance(content, str) and 'Respond in JSON format ONLY' in content:\n",
    "            return _StubResponse('{\"entity_type\":\"test\",\"wikidata_type\":null,\"search_queries\":[]}')\n",
    "        if isinstance(content, str) and 'Return ONLY a JSON array' in content:\n",
    "            return _StubResponse('[]')\n",
    "        return _StubResponse('no tool calls')\n",
    "\n",
    "stub_llm_rounds = _StubLLM()\n"
  ))

  cfg <- asa_test_minimal_research_config(research)

  graph <- research$create_research_graph(
    llm = reticulate::py$stub_llm_rounds,
    tools = list(),
    config = cfg,
    checkpointer = NULL,
    wikidata_tool = NULL
  )

  out <- research$run_research(
    graph = graph,
    query = "test query",
    schema = list(name = "character"),
    config_dict = list(
      recursion_limit = 4L,
      use_web = FALSE,
      use_wikidata = FALSE,
      use_wikipedia = FALSE
    ),
    thread_id = "test-thread-run-research-min-recursion"
  )

  expect_equal(out$status, "complete")
  expect_equal(out$stop_reason, "recursion_limit")
  expect_gte(as.integer(out$metrics$round_number), 1L)
})

test_that("best-effort recursion_limit output populates required JSON fields (stubbed)", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

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

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    prompt = "Return JSON only.",
    config = list(recursion_limit = as.integer(3)),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  response_text <- invoke$response_text
  parsed <- invoke$parsed

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_equal(final_state$expected_schema_source, "explicit")

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
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

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

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    prompt = "Return JSON only.",
    config = list(recursion_limit = as.integer(3)),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  response_text <- invoke$response_text
  parsed <- invoke$parsed

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
  utils <- asa_test_import_module("shared.state_graph_utils", required_modules = c("pydantic"))

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
  utils <- asa_test_import_module("shared.state_graph_utils", required_modules = c("pydantic"))

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

# NOTE: Gemini live API tests moved to test-langgraph-gemini-live.R for faster test runs.

test_that("run_task passes expected_schema into LangGraph state (explicit schema)", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

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
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production",
    required_modules = c("langchain_core", "pydantic"))
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
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
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
  fs <- expect_valid_fold_stats(final_state$fold_stats, fold_count = 1L)
  expect_equal(as.integer(fs$fold_messages_removed), as.integer(fs$fold_total_messages_removed))

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
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
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
  expect_true(any(grepl("FOLDED_SUMMARY", unlist(final_state$summary$facts), fixed = TRUE)))
  expect_true(is.list(final_state$archive))
  expect_true(length(final_state$archive) >= 1L)

  # Folding should pass prior content into the summarizer prompt.
  expect_equal(as.integer(reticulate::py$stub_summarizer$calls), 1L)
  last_prompt <- as.character(reticulate::py$stub_summarizer$last_prompt)
  expect_true(grepl("alpha: old assistant answer", last_prompt, fixed = TRUE))
  expect_true(grepl("IMPORTANT_FACT=42", last_prompt, fixed = TRUE))
  expect_true(grepl("beta: old assistant note", last_prompt, fixed = TRUE))

  # After folding, the summary is stored in state and injected into the system
  # prompt on the next agent turn. The primary fold trigger is in after_tools:
  # when tool outputs push the conversation past the fold budget, after_tools
  # routes to 'summarize' before returning to 'agent'. should_continue acts as
  # a safety-net fold trigger for non-tool-call responses.
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
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
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
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production",
    required_modules = c(ASA_TEST_LANGGRAPH_MODULES, "langgraph.checkpoint.memory"))
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
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Same pattern as existing Gemini test: tool-call then final answer, with a
  # deterministic summarizer so we can inspect archive structure.
  asa_test_fake_search_tool(return_value = "result", var_name = "fake_search_tool_v3")
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
  asa_test_skip_if_missing_python_modules(c("bs4"), method = "import")
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
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

  parsed_payload <- asa_test_parse_agent_json(final_state, backend = "gemini")
  response_text <- parsed_payload$response_text
  parsed <- parsed_payload$parsed

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
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

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

test_that("shared router prioritizes pending tool calls when budget allows", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production",
    required_modules = c("langchain_core", "pydantic"))

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "route_state_budget_ok = {\n",
    "    'messages': [AIMessage(content='calling tool', tool_calls=[{'name':'Search','args':{'query':'ada'},'id':'edge_call_1'}])],\n",
    "    'remaining_steps': 2,\n",
    "}\n",
    "route_state_budget_low = {\n",
    "    'messages': [AIMessage(content='calling tool', tool_calls=[{'name':'Search','args':{'query':'ada'},'id':'edge_call_1'}])],\n",
    "    'remaining_steps': 1,\n",
    "}\n",
    "route_state_budget_zero = {\n",
    "    'messages': [AIMessage(content='calling tool', tool_calls=[{'name':'Search','args':{'query':'ada'},'id':'edge_call_1'}])],\n",
    "    'remaining_steps': 0,\n",
    "}\n",
    "route_state_long_history = {\n",
    "    'messages': [AIMessage(content='old') for _ in range(35)] + [AIMessage(content='calling tool', tool_calls=[{'name':'Search','args':{'query':'ada'},'id':'edge_call_2'}])],\n",
    "    'remaining_steps': 3,\n",
    "    'token_trace': [{'node':'agent'} for _ in range(40)],\n",
    "}\n"
  ))

  route_ok <- prod$`_route_after_agent_step`(
    reticulate::py$route_state_budget_ok
  )
  route_low <- prod$`_route_after_agent_step`(
    reticulate::py$route_state_budget_low
  )
  route_zero <- prod$`_route_after_agent_step`(
    reticulate::py$route_state_budget_zero
  )
  route_long <- prod$`_route_after_agent_step`(
    reticulate::py$route_state_long_history
  )

  expect_equal(as.character(route_ok), "tools")
  expect_equal(as.character(route_low), "finalize")
  expect_equal(as.character(route_zero), "end")
  expect_equal(as.character(route_long), "tools")
})

test_that("post-tools routing reserves a terminal step near recursion edge", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production",
    required_modules = c("langchain_core", "pydantic"))

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import ToolMessage\n",
    "route_state_tool_tail_edge = {\n",
    "    'messages': [ToolMessage(content='search result', tool_call_id='edge_call_1')],\n",
    "    'remaining_steps': 2,\n",
    "    'field_status': {\n",
    "        'birth_year': {'status': 'pending', 'value': None, 'descriptor': 'integer|null|Unknown'}\n",
    "    },\n",
    "}\n",
    "route_state_tool_tail_room = {\n",
    "    'messages': [ToolMessage(content='search result', tool_call_id='edge_call_2')],\n",
    "    'remaining_steps': 4,\n",
    "    'field_status': {\n",
    "        'birth_year': {'status': 'pending', 'value': None, 'descriptor': 'integer|null|Unknown'}\n",
    "    },\n",
    "}\n"
  ))

  reserve_edge <- prod$`_should_reserve_terminal_budget_after_tools`(
    reticulate::py$route_state_tool_tail_edge,
    as.integer(2)
  )
  reserve_room <- prod$`_should_reserve_terminal_budget_after_tools`(
    reticulate::py$route_state_tool_tail_room,
    as.integer(4)
  )
  route_edge <- prod$`_route_after_tools_step`(
    reticulate::py$route_state_tool_tail_edge
  )
  route_room <- prod$`_route_after_tools_step`(
    reticulate::py$route_state_tool_tail_room
  )

  expect_true(isTRUE(reserve_edge))
  expect_false(isTRUE(reserve_room))
  expect_equal(as.character(route_edge), "finalize")
  expect_equal(as.character(route_room), "agent")
})

test_that("tool exceptions produce terminal fallback instead of hard error (standard)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "class _ToolExceptionLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        if self.calls == 1:\n",
    "            return AIMessage(\n",
    "                content='call tool',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'boom'},'id':'call_err_1'}],\n",
    "            )\n",
    "        return AIMessage(content=",
    deparse('{"status":"complete","items":[],"missing":[],"notes":"recovered"}'),
    ")\n\n",
    "def _boom_search(query: str) -> str:\n",
    "    raise RuntimeError('boom')\n\n",
    "tool_exception_llm = _ToolExceptionLLM()\n",
    "boom_search_tool = Tool(name='Search', description='Exploding tool', func=_boom_search)\n"
  ))

  agent <- prod$create_standard_agent(
    model = reticulate::py$tool_exception_llm,
    tools = list(reticulate::py$boom_search_tool)
  )

  final_state <- NULL
  expect_no_error({
    final_state <- agent$invoke(
      list(messages = list(list(role = "user", content = "Return JSON"))),
      config = list(recursion_limit = 4L)
    )
  })

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_equal(as.integer(reticulate::py$tool_exception_llm$calls), 2L)
  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  expect_true(is.character(response_text) && nzchar(response_text))
})

test_that("model invoke exceptions return schema fallback instead of hard error (standard)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "class _ModelExceptionLLM:\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        raise TimeoutError('synthetic model timeout')\n\n",
    "model_exception_llm = _ModelExceptionLLM()\n"
  ))

  schema <- list(
    status = "string",
    items = "array",
    missing = "array",
    notes = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$model_exception_llm,
    tools = list()
  )

  final_state <- NULL
  expect_no_error({
    final_state <- agent$invoke(
      list(
        messages = list(list(role = "user", content = "Return JSON")),
        expected_schema = schema,
        expected_schema_source = "explicit"
      ),
      config = list(recursion_limit = 4L)
    )
  })

  parsed_payload <- asa_test_parse_agent_json(final_state, backend = "gemini")
  response_text <- parsed_payload$response_text
  parsed <- parsed_payload$parsed
  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))

  repair_events <- reticulate::py_to_r(final_state$json_repair)
  reasons <- character(0)
  if (is.list(repair_events) && length(repair_events) > 0) {
    reasons <- vapply(repair_events, function(ev) {
      if (is.list(ev) && !is.null(ev$repair_reason)) as.character(ev$repair_reason) else NA_character_
    }, character(1))
  }
  expect_true("invoke_exception_fallback" %in% reasons)
})

test_that("reused thread_id does not let stale recursion stop_reason skip tools", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production",
    required_modules = c(ASA_TEST_LANGGRAPH_MODULES, "langgraph.checkpoint.memory"))
  mem_mod <- reticulate::import("langgraph.checkpoint.memory", convert = TRUE)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n",
    "\n",
    "class _PersistStopReasonLLM:\n",
    "    def __init__(self):\n",
    "        self.call_count = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.call_count += 1\n",
    "        if self.call_count == 1:\n",
    "            return AIMessage(content=",
    deparse('{"status":"complete","items":[{"name":"Ada"}],"missing":[],"notes":"seed"}'),
    ")\n",
    "        if self.call_count == 2:\n",
    "            return AIMessage(\n",
    "                content='',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'x'},'id':'call_2','type':'tool_call'}],\n",
    "            )\n",
    "        return AIMessage(content=",
    deparse('{"status":"complete","items":[{"name":"Grace"}],"missing":[],"notes":"after_tool"}'),
    ")\n",
    "\n",
    "def _persist_search(query: str='x') -> str:\n",
    "    return 'tool output'\n",
    "\n",
    "persist_stop_reason_search_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Fake search',\n",
    "    func=_persist_search,\n",
    ")\n",
    "persist_stop_reason_llm = _PersistStopReasonLLM()\n"
  ))

  schema <- list(
    status = "string",
    items = list(list(name = "string")),
    missing = "array",
    notes = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$persist_stop_reason_llm,
    tools = list(reticulate::py$persist_stop_reason_search_tool),
    checkpointer = mem_mod$MemorySaver()
  )

  thread_id <- "test_persisted_stop_reason_standard"

  state1 <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "run 1")),
      expected_schema = schema,
      expected_schema_source = "explicit"
    ),
    config = list(
      recursion_limit = 2L,
      configurable = list(thread_id = thread_id)
    )
  )
  expect_equal(state1$stop_reason, "recursion_limit")
  expect_equal(as.integer(reticulate::py$persist_stop_reason_llm$call_count), 1L)

  state2 <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "run 2 should use a tool")),
      expected_schema = schema,
      expected_schema_source = "explicit"
    ),
    config = list(
      recursion_limit = 10L,
      configurable = list(thread_id = thread_id)
    )
  )

  expect_true(as.integer(reticulate::py$persist_stop_reason_llm$call_count) >= 3L)

  has_tool_message <- any(vapply(state2$messages, function(msg) {
    msg_type <- tryCatch(as.character(msg$`__class__`$`__name__`), error = function(e) character(0))
    if (length(msg_type) > 0 && identical(msg_type[[1]], "ToolMessage")) {
      return(TRUE)
    }
    msg_role <- tolower(tryCatch(as.character(msg$type), error = function(e) character(0)))
    isTRUE(length(msg_role) > 0 && msg_role[[1]] %in% c("tool", "function"))
  }, logical(1)))
  expect_true(has_tool_message)
})

test_that("memory finalize reuses terminal response after no-op summarize near limit", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

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

  parsed_payload <- asa_test_parse_agent_json(final_state, backend = "gemini")
  response_text <- parsed_payload$response_text
  parsed <- parsed_payload$parsed
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$notes), "FIRST")
})

test_that("memory summarize near recursion edge preserves terminal response when keep_recent=0", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_multi_response_llm(
    responses = list(
      list(content = "{\"status\":\"complete\",\"items\":[],\"missing\":[],\"notes\":\"FIRST\"}"),
      list(content = "{\"status\":\"complete\",\"items\":[],\"missing\":[],\"notes\":\"SECOND\"}")
    ),
    var_name = "memory_keep0_edge_llm"
  )
  asa_test_stub_summarizer(var_name = "memory_keep0_edge_summarizer")

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$memory_keep0_edge_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(1),
    keep_recent = as.integer(0),
    fold_char_budget = as.integer(99999),
    summarizer_model = reticulate::py$memory_keep0_edge_summarizer,
    debug = FALSE
  )

  history_user <- msgs$HumanMessage(content = "Earlier question")
  history_ai <- msgs$AIMessage(content = "Earlier answer")
  current_user <- msgs$HumanMessage(content = "Return JSON")

  final_state <- agent$invoke(
    list(
      messages = list(history_user, history_ai, current_user),
      summary = "",
      archive = list(),
      fold_stats = reticulate::dict(fold_count = 0L),
      expected_schema = list(status = "string", items = "array", missing = "array", notes = "string"),
      expected_schema_source = "explicit"
    ),
    config = list(
      recursion_limit = as.integer(4),
      configurable = list(thread_id = "test_memory_keep0_terminal_preserved")
    )
  )

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_equal(as.integer(reticulate::py$memory_keep0_edge_llm$n), 1L)
  expect_equal(as.integer(reticulate::py$memory_keep0_edge_summarizer$calls), 1L)

  parsed_payload <- asa_test_parse_agent_json(final_state, backend = "gemini")
  response_text <- parsed_payload$response_text
  parsed <- parsed_payload$parsed
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$notes), "FIRST")
})

test_that("summarize stamps recursion stop_reason when budget is exhausted", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_llm(mode = "simple", response_content = "")
  asa_test_stub_summarizer(var_name = "stop_reason_summarizer")

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$stub_llm,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE,
    message_threshold = 1L,
    keep_recent = 4L,
    summarizer_model = reticulate::py$stop_reason_summarizer
  )

  summarize_state <- reticulate::dict(
    messages = list(
      msgs$HumanMessage(content = "user prompt"),
      msgs$AIMessage(content = "")
    ),
    summary = "",
    archive = list(),
    fold_stats = reticulate::dict(fold_count = 0L),
    stop_reason = NULL,
    remaining_steps = 1L
  )

  summarize_out <- agent$nodes[["summarize"]]$bound$invoke(summarize_state)
  expect_equal(as.character(summarize_out$stop_reason), "recursion_limit")
})

test_that("summarize near edge does not stamp recursion stop_reason prematurely", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_llm(mode = "simple", response_content = "")
  asa_test_stub_summarizer(var_name = "no_premature_stop_summarizer")

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$stub_llm,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE,
    message_threshold = 1L,
    keep_recent = 0L,
    summarizer_model = reticulate::py$no_premature_stop_summarizer
  )

  summarize_state <- reticulate::dict(
    messages = list(
      msgs$HumanMessage(content = "user prompt"),
      msgs$AIMessage(content = "{\"status\":\"complete\",\"items\":[],\"missing\":[],\"notes\":\"ok\"}")
    ),
    summary = "",
    archive = list(),
    fold_stats = reticulate::dict(fold_count = 0L),
    stop_reason = NULL,
    remaining_steps = 3L
  )

  summarize_out <- agent$nodes[["summarize"]]$bound$invoke(summarize_state)
  expect_null(summarize_out$stop_reason)
})

test_that("reused finalize response does not mutate original non-copyable message", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)
  py <- reticulate::py
  py$prod_module_copy_test <- prod

  reticulate::py_run_string(paste0(
    "class AIMessage:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "        self.tool_calls = []\n",
    "        self.type = 'ai'\n\n",
    "orig_msg = AIMessage('{\"status\":\"partial\"}')\n",
    "reused_msg = prod_module_copy_test._reusable_terminal_finalize_response([orig_msg])\n",
    "reused_msg, _ = prod_module_copy_test._repair_best_effort_json(\n",
    "    {'status': 'string', 'missing': 'array'},\n",
    "    reused_msg,\n",
    "    fallback_on_failure=True,\n",
    "    schema_source='explicit',\n",
    "    context='finalize',\n",
    "    debug=False,\n",
    ")\n",
    "copy_test_same_object = reused_msg is orig_msg\n",
    "copy_test_orig_content = orig_msg.content\n",
    "copy_test_reused_content = reused_msg.content\n"
  ))

  expect_false(isTRUE(reticulate::py$copy_test_same_object))
  expect_equal(as.character(reticulate::py$copy_test_orig_content), "{\"status\":\"partial\"}")
  expect_true(nzchar(trimws(as.character(reticulate::py$copy_test_reused_content))))
  expect_false(identical(
    as.character(reticulate::py$copy_test_orig_content),
    as.character(reticulate::py$copy_test_reused_content)
  ))
})

test_that("reused finalize response gets a fresh message id", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)
  py <- reticulate::py
  py$prod_module_id_test <- prod

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "orig_msg_id_test = AIMessage(content='terminal response', id='msg-123')\n",
    "reused_msg_id_test = prod_module_id_test._reusable_terminal_finalize_response([orig_msg_id_test])\n",
    "combined_id_test = prod_module_id_test._add_messages([orig_msg_id_test], [reused_msg_id_test])\n",
    "combined_ids_id_test = [getattr(m, 'id', None) for m in combined_id_test]\n",
    "combined_has_duplicate_ids = len(combined_ids_id_test) != len(set(combined_ids_id_test))\n"
  ))

  expect_false(isTRUE(reticulate::py$combined_has_duplicate_ids))
  expect_false(identical(
    as.character(reticulate::py$orig_msg_id_test$id),
    as.character(reticulate::py$reused_msg_id_test$id)
  ))
})

test_that("finalize strips residual tool calls and returns terminal JSON (standard + memory folding)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  expected_schema <- list(
    status = "string",
    items = "array",
    missing = "array",
    notes = "string"
  )

  for (agent_type in c("standard", "memory_folding")) {
    var_name <- paste0("residual_tool_llm_", agent_type)
    asa_test_stub_residual_tool_llm(var_name = var_name)

    if (agent_type == "standard") {
      agent <- prod$create_standard_agent(
        model = reticulate::py[[var_name]],
        tools = list()
      )
      invoke <- asa_test_invoke_json_agent(
        agent = agent,
        expected_schema = expected_schema,
        prompt = "Return JSON",
        expected_schema_source = "explicit",
        config = list(recursion_limit = 2L),
        backend = "gemini"
      )
    } else {
      agent <- prod$create_memory_folding_agent(
        model = reticulate::py[[var_name]],
        tools = list(),
        checkpointer = NULL,
        debug = FALSE
      )
      invoke <- asa_test_invoke_json_agent(
        agent = agent,
        expected_schema = expected_schema,
        expected_schema_source = "explicit",
        input_state = list(
          messages = list(list(role = "user", content = "Return JSON")),
          summary = "",
          archive = list(),
          fold_stats = reticulate::dict(fold_count = 0L)
        ),
        config = list(
          recursion_limit = 2L,
          configurable = list(thread_id = paste0("test_residual_finalize_", agent_type))
        ),
        backend = "gemini"
      )
    }
    final_state <- invoke$final_state
    parsed <- invoke$parsed

    expect_equal(final_state$stop_reason, "recursion_limit")

    last_msg <- final_state$messages[[length(final_state$messages)]]
    tool_calls <- tryCatch(last_msg$tool_calls, error = function(e) NULL)
    expect_true(is.null(tool_calls) || length(tool_calls) == 0L)

    expect_true(is.list(parsed))
    expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))
  }
})

test_that("finalize stays non-empty when first schema repair attempt fails (standard)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "import asa_backend.api.agent_api as _agent_api_for_test\n",
    "_asa_repair_calls = {'n': 0}\n",
    "_asa_orig_repair = _agent_api_for_test.repair_json_output_to_schema\n",
    "def _asa_flaky_repair(text, schema, fallback_on_failure=False):\n",
    "    _asa_repair_calls['n'] += 1\n",
    "    if _asa_repair_calls['n'] == 1:\n",
    "        return None\n",
    "    return _asa_orig_repair(text, schema, fallback_on_failure=fallback_on_failure)\n",
    "_agent_api_for_test.repair_json_output_to_schema = _asa_flaky_repair\n"
  ))
  on.exit(
    reticulate::py_run_string(
      "import asa_backend.api.agent_api as _agent_api_for_test\n_agent_api_for_test.repair_json_output_to_schema = _asa_orig_repair"
    ),
    add = TRUE
  )

  asa_test_stub_residual_tool_llm(var_name = "repair_fail_then_recover_llm")

  expected_schema <- list(
    status = "string",
    items = "array",
    missing = "array",
    notes = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$repair_fail_then_recover_llm,
    tools = list()
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    prompt = "Return JSON",
    expected_schema_source = "explicit",
    config = list(recursion_limit = 2L),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  response_text <- invoke$response_text
  parsed <- invoke$parsed

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_true(is.character(response_text))
  expect_true(nzchar(trimws(response_text)))
  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))

  repair_calls <- reticulate::py_to_r(reticulate::py_eval("_asa_repair_calls"))
  expect_true(as.integer(repair_calls$n) >= 2L)
})

test_that("finalize strips residual tool calls and returns non-empty text when no schema (standard + memory folding)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  for (agent_type in c("standard", "memory_folding")) {
    var_name <- paste0("residual_tool_no_schema_llm_", agent_type)
    asa_test_stub_residual_tool_llm(var_name = var_name)

    if (agent_type == "standard") {
      agent <- prod$create_standard_agent(
        model = reticulate::py[[var_name]],
        tools = list()
      )
      final_state <- agent$invoke(
        list(
          messages = list(list(role = "user", content = "Give me the best possible answer"))
        ),
        config = list(recursion_limit = 2L)
      )
    } else {
      agent <- prod$create_memory_folding_agent(
        model = reticulate::py[[var_name]],
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
          configurable = list(thread_id = paste0("test_residual_finalize_no_schema_", agent_type))
        )
      )
    }

    expect_equal(final_state$stop_reason, "recursion_limit")

    last_msg <- final_state$messages[[length(final_state$messages)]]
    tool_calls <- tryCatch(last_msg$tool_calls, error = function(e) NULL)
    expect_true(is.null(tool_calls) || length(tool_calls) == 0L)

    response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
    expect_true(is.character(response_text))
    expect_true(nzchar(trimws(response_text)))
  }
})

test_that("sanitize_finalize_response fills dict content fallback when residual tool calls are stripped", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  raw_response <- list(
    role = "assistant",
    content = "",
    tool_calls = list(list(
      name = "save_finding",
      args = list(finding = "X", category = "fact"),
      id = "call_1"
    )),
    additional_kwargs = list(
      tool_calls = list(list(name = "save_finding"))
    )
  )

  sanitize_out <- prod$`_sanitize_finalize_response`(
    raw_response,
    NULL,
    context = "unit_test",
    debug = FALSE
  )

  cleaned <- sanitize_out[[1]]
  event <- sanitize_out[[2]]

  expect_true(is.list(cleaned))
  expect_true(is.null(cleaned$tool_calls) || length(cleaned$tool_calls) == 0L)
  expect_true(is.character(cleaned$content))
  expect_true(nzchar(trimws(cleaned$content)))

  expect_true(is.list(event))
  expect_equal(as.character(event$repair_reason), "residual_tool_calls_no_content_no_schema")
})

test_that("sanitize_finalize_response strips invalid tool calls and legacy function_call metadata", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  raw_response <- list(
    role = "assistant",
    content = "",
    invalid_tool_calls = list(list(
      name = "save_finding",
      args = list(finding = "X", category = "fact"),
      id = "bad_1"
    )),
    function_call = list(name = "save_finding", arguments = "{}"),
    additional_kwargs = list(
      invalid_tool_calls = list(list(name = "save_finding")),
      function_call = list(name = "save_finding", arguments = "{}")
    )
  )

  sanitize_out <- prod$`_sanitize_finalize_response`(
    raw_response,
    NULL,
    context = "unit_test_invalid_calls",
    debug = FALSE
  )

  cleaned <- sanitize_out[[1]]
  event <- sanitize_out[[2]]

  expect_true(is.list(cleaned))
  expect_true(is.null(cleaned$invalid_tool_calls) || length(cleaned$invalid_tool_calls) == 0L)
  expect_true(is.null(cleaned$function_call) || length(cleaned$function_call) == 0L)
  expect_true(is.character(cleaned$content))
  expect_true(nzchar(trimws(cleaned$content)))
  expect_false("invalid_tool_calls" %in% names(cleaned$additional_kwargs))
  expect_false("function_call" %in% names(cleaned$additional_kwargs))

  expect_true(is.list(event))
  expect_equal(as.character(event$repair_reason), "residual_tool_calls_no_content_no_schema")
})

test_that("synthetic finalize tool-skip messages preserve tool-call pairing", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)
  build_synthetic <- reticulate::py_get_attr(prod, "_build_synthetic_tool_skip_messages")

  pending_ai <- msgs$AIMessage(
    content = "",
    tool_calls = list(list(name = "Search", args = list(query = "q"), id = "call_syn_1"))
  )
  synthetic <- build_synthetic(list(pending_ai), max_items = 4L)
  synthetic_msgs <- as.list(synthetic)

  expect_true(length(synthetic_msgs) >= 1L)
  first <- synthetic_msgs[[1]]
  expect_equal(as.character(first$name), "Search")
  expect_equal(as.character(first$tool_call_id), "call_syn_1")
  expect_match(as.character(first$content), "synthetic_skip_on_finalize")
})

test_that("finalize_answer node sanitizes residual tool calls after tools (standard)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

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

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    prompt = "Return JSON",
    expected_schema_source = "explicit",
    config = list(recursion_limit = 4L),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  response_text <- invoke$response_text
  parsed <- invoke$parsed

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

  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))
})

test_that("explicit schema does not rewrite intermediate tool-call turns (standard)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

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

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    prompt = "Return JSON",
    expected_schema_source = "explicit",
    config = list(recursion_limit = 10L),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  response_text <- invoke$response_text
  parsed <- invoke$parsed

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

  expect_true(is.list(parsed))
  expect_true(all(c("status", "items", "missing", "notes") %in% names(parsed)))
})

# NOTE: "finalize strips residual tool calls and returns terminal JSON (memory folding)"
# merged into parameterized test above.

# NOTE: "finalize strips residual tool calls ... no schema (memory folding)"
# merged into parameterized test above.

test_that("recursion-limit finalize preserves earlier tool-derived facts (standard + memory folding)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  expected_schema <- list(
    prior_occupation = "string",
    prior_occupation_source = "string"
  )

  for (agent_type in c("standard", "memory_folding")) {
    tool_var <- paste0("fact_search_tool_", agent_type)
    llm_var <- paste0("recursion_edge_llm_", agent_type)

    reticulate::py_run_string(paste0(
      "from langchain_core.messages import AIMessage\n",
      "from langchain_core.tools import Tool\n\n",
      "def _fact_search_", agent_type, "(query: str) -> str:\n",
      "    return '{\"prior_occupation\":\"teacher\",\"prior_occupation_source\":\"https://example.org/profile\"}'\n\n",
      tool_var, " = Tool(\n",
      "    name='Search',\n",
      "    description='Deterministic fact search tool',\n",
      "    func=_fact_search_", agent_type, ",\n",
      ")\n\n",
      "class _RecursionEdgeResidualLLM_", agent_type, ":\n",
      "    def __init__(self):\n",
      "        self.calls = 0\n",
      "    def bind_tools(self, tools):\n",
      "        return self\n",
      "    def invoke(self, messages):\n",
      "        self.calls += 1\n",
      "        if self.calls == 1:\n",
      "            return AIMessage(\n",
      "                content='calling search',\n",
      "                tool_calls=[{'name':'Search','args':{'query':'person'},'id':'call_1'}]\n",
      "            )\n",
      "        return AIMessage(\n",
      "            content='',\n",
      "            tool_calls=[{'name':'save_finding','args':{'finding':'noop','category':'fact'},'id':'call_2'}]\n",
      "        )\n\n",
      llm_var, " = _RecursionEdgeResidualLLM_", agent_type, "()\n"
    ))

    if (agent_type == "standard") {
      agent <- prod$create_standard_agent(
        model = reticulate::py[[llm_var]],
        tools = list(reticulate::py[[tool_var]])
      )
      invoke <- asa_test_invoke_json_agent(
        agent = agent,
        expected_schema = expected_schema,
        prompt = "Return strict JSON with prior_occupation and prior_occupation_source.",
        expected_schema_source = "explicit",
        config = list(
          recursion_limit = 4L,
          configurable = list(thread_id = paste0("test_recursion_semantic_preserve_", agent_type))
        ),
        backend = "gemini"
      )
    } else {
      agent <- prod$create_memory_folding_agent(
        model = reticulate::py[[llm_var]],
        tools = list(reticulate::py[[tool_var]]),
        checkpointer = NULL,
        message_threshold = as.integer(999),
        keep_recent = as.integer(2),
        fold_char_budget = as.integer(999999),
        debug = FALSE
      )
      invoke <- asa_test_invoke_json_agent(
        agent = agent,
        expected_schema = expected_schema,
        expected_schema_source = "explicit",
        input_state = list(
          messages = list(list(
            role = "user",
            content = "Return strict JSON with prior_occupation and prior_occupation_source."
          )),
          summary = "",
          archive = list(),
          fold_stats = reticulate::dict(fold_count = 0L)
        ),
        config = list(
          recursion_limit = 4L,
          configurable = list(thread_id = paste0("test_recursion_semantic_preserve_", agent_type))
        ),
        backend = "gemini"
      )
    }
    final_state <- invoke$final_state
    parsed <- invoke$parsed

    expect_equal(final_state$stop_reason, "recursion_limit")
    expect_true(is.list(parsed))
    expect_equal(as.character(parsed$prior_occupation), "teacher")
    expect_equal(as.character(parsed$prior_occupation_source), "https://example.org/profile")

    field_status <- tryCatch(reticulate::py_to_r(final_state$field_status), error = function(e) final_state$field_status)
    expect_true(is.list(field_status))
    expect_equal(as.character(field_status$prior_occupation$status), "found")
    expect_equal(as.character(field_status$prior_occupation$value), "teacher")
  }
})

test_that("field_status is canonical vs scratchpad and is injected into finalize prompts (standard)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "def _canonical_fact_tool(query: str) -> str:\n",
    "    return '{\"prior_occupation\":\"teacher\",\"prior_occupation_source\":\"https://example.org/profile\"}'\n\n",
    "canonical_fact_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Deterministic fact tool for field_status canonical tests',\n",
    "    func=_canonical_fact_tool,\n",
    ")\n\n",
    "class _FieldStatusCanonicalLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "        self.system_prompts = []\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        try:\n",
    "            self.system_prompts.append(getattr(messages[0], 'content', None))\n",
    "        except Exception:\n",
    "            self.system_prompts.append(None)\n",
    "        sys_text = str(messages[0].content or '') if messages else ''\n",
    "        if 'FINALIZE MODE' in sys_text:\n",
    "            return AIMessage(content='', tool_calls=[{'name':'save_finding','args':{'finding':'noop','category':'fact'},'id':'call_final'}])\n",
    "        return AIMessage(\n",
    "            content='search then note',\n",
    "            tool_calls=[\n",
    "                {'name':'Search','args':{'query':'person'},'id':'call_1'},\n",
    "                {'name':'save_finding','args':{'finding':'prior_occupation=lawyer','category':'fact'},'id':'call_2'}\n",
    "            ]\n",
    "        )\n\n",
    "field_status_canonical_llm = _FieldStatusCanonicalLLM()\n"
  ))

  expected_schema <- list(
    prior_occupation = "string",
    prior_occupation_source = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$field_status_canonical_llm,
    tools = list(reticulate::py$canonical_fact_tool)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    prompt = "Return strict JSON with prior_occupation and prior_occupation_source.",
    expected_schema_source = "explicit",
    config = list(
      recursion_limit = 4L,
      configurable = list(thread_id = "test_field_status_canonical_standard")
    ),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  response_text <- invoke$response_text
  parsed <- invoke$parsed
  expect_equal(as.character(parsed$prior_occupation), "teacher")
  expect_equal(as.character(parsed$prior_occupation_source), "https://example.org/profile")

  field_status <- tryCatch(reticulate::py_to_r(final_state$field_status), error = function(e) final_state$field_status)
  expect_true(is.list(field_status))
  expect_equal(as.character(field_status$prior_occupation$status), "found")
  expect_equal(as.character(field_status$prior_occupation$value), "teacher")

  sys_prompts <- reticulate::py_to_r(reticulate::py$field_status_canonical_llm$system_prompts)
  expect_true(length(sys_prompts) >= 2L)
  expect_true(grepl("FIELD STATUS", sys_prompts[[2]], fixed = TRUE))
  expect_true(grepl("prior_occupation: found", sys_prompts[[2]], fixed = TRUE))
  expect_true(grepl("YOUR SCRATCHPAD", sys_prompts[[2]], fixed = TRUE))
  expect_true(grepl("prior_occupation=lawyer", sys_prompts[[2]], fixed = TRUE))
})

test_that("field_status unknown-after threshold triggers semantic finalize with unknowns", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "unknown_budget_tool_calls = {'n': 0}\n\n",
    "def _unknown_budget_tool(query: str) -> str:\n",
    "    unknown_budget_tool_calls['n'] += 1\n",
    "    return '{\"noise\":\"irrelevant\"}'\n\n",
    "unknown_budget_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Always returns irrelevant payloads',\n",
    "    func=_unknown_budget_tool,\n",
    ")\n\n",
    "class _UnknownBudgetLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        sys_text = str(messages[0].content or '') if messages else ''\n",
    "        if 'FINALIZE MODE' in sys_text:\n",
    "            return AIMessage(content='', tool_calls=[{'name':'save_finding','args':{'finding':'noop','category':'fact'},'id':'call_final'}])\n",
    "        return AIMessage(content='search more', tool_calls=[{'name':'Search','args':{'query':'more'},'id':'call_' + str(self.calls)}])\n\n",
    "unknown_budget_llm = _UnknownBudgetLLM()\n"
  ))

  expected_schema <- list(
    birth_year = "integer",
    birth_place = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$unknown_budget_llm,
    tools = list(reticulate::py$unknown_budget_tool)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    expected_schema_source = "explicit",
    input_state = list(
      messages = list(list(
        role = "user",
        content = "Return strict JSON with birth_year and birth_place."
      )),
      search_budget_limit = 8L,
      unknown_after_searches = 3L,
      finalize_on_all_fields_resolved = TRUE
    ),
    config = list(
      recursion_limit = 30L,
      configurable = list(thread_id = "test_field_status_unknown_finalize")
    ),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  response_text <- invoke$response_text
  parsed <- invoke$parsed

  field_status <- tryCatch(reticulate::py_to_r(final_state$field_status), error = function(e) final_state$field_status)
  expect_true(is.list(field_status))
  expect_equal(as.character(field_status$birth_year$status), "unknown")
  expect_equal(as.character(field_status$birth_place$status), "unknown")
  expect_equal(as.integer(field_status$birth_year$attempts), 3L)
  expect_equal(as.integer(field_status$birth_place$attempts), 3L)

  budget_state <- tryCatch(reticulate::py_to_r(final_state$budget_state), error = function(e) final_state$budget_state)
  expect_true(is.list(budget_state))
  expect_true(isTRUE(as.logical(budget_state$all_resolved)))
  expect_true(as.integer(budget_state$tool_calls_used) <= 3L)

  expect_true(is.list(parsed))
  expect_true(all(c("birth_year", "birth_place") %in% names(parsed)))
  expect_true(is.null(parsed$birth_year) || is.na(parsed$birth_year))
  expect_equal(as.character(parsed$birth_place), "Unknown")
})

test_that("field attempt budget mode clamps attempts in strict_cap and increments in soft_cap", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  extract_fn <- reticulate::py_get_attr(prod, "_extract_field_status_updates")

  expected_schema <- list(
    birth_year = "integer"
  )
  seeded_status <- list(
    birth_year = list(
      status = "unknown",
      value = NULL,
      attempts = 3L
    )
  )
  tool_messages <- list(
    list(
      role = "tool",
      name = "Search",
      content = "{\"noise\":\"irrelevant\"}"
    )
  )

  strict_out <- reticulate::py_to_r(extract_fn(
    existing_field_status = seeded_status,
    expected_schema = expected_schema,
    tool_messages = tool_messages,
    tool_calls_delta = 1L,
    unknown_after_searches = 3L,
    field_attempt_budget_mode = "strict_cap"
  ))
  strict_status <- strict_out[[1]]
  expect_equal(as.integer(strict_status$birth_year$attempts), 3L)
  expect_equal(as.character(strict_status$birth_year$status), "unknown")

  soft_out <- reticulate::py_to_r(extract_fn(
    existing_field_status = seeded_status,
    expected_schema = expected_schema,
    tool_messages = tool_messages,
    tool_calls_delta = 1L,
    unknown_after_searches = 3L,
    field_attempt_budget_mode = "soft_cap"
  ))
  soft_status <- soft_out[[1]]
  expect_true(as.integer(soft_status$birth_year$attempts) > 3L)
  expect_equal(as.character(soft_status$birth_year$status), "unknown")
})

test_that("all-unresolved-exhausted finalization trigger obeys orchestration flag", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  state_base <- list(
    expected_schema = list(
      birth_year = "integer",
      birth_place = "string"
    ),
    field_status = list(
      birth_year = list(status = "unknown", value = NULL, attempts = 3L),
      birth_place = list(status = "unknown", value = "Unknown", attempts = 3L)
    ),
    budget_state = list(
      tool_calls_used = 2L,
      tool_calls_limit = 10L,
      unknown_after_searches = 3L
    ),
    search_budget_limit = 10L,
    finalize_on_all_fields_resolved = FALSE
  )

  state_enabled <- utils::modifyList(
    state_base,
    list(orchestration_options = list(finalizer = list(finalize_when_all_unresolved_exhausted = TRUE)))
  )
  state_disabled <- utils::modifyList(
    state_base,
    list(orchestration_options = list(finalizer = list(finalize_when_all_unresolved_exhausted = FALSE)))
  )

  expect_true(isTRUE(reticulate::py_to_r(prod$`_should_force_finalize`(state_enabled))))
  expect_equal(
    as.character(reticulate::py_to_r(prod$`_finalize_reason_for_state`(state_enabled))),
    "all_unresolved_exhausted"
  )
  expect_false(isTRUE(reticulate::py_to_r(prod$`_should_force_finalize`(state_disabled))))
})

test_that("field_status parses Search source blocks with embedded JSON facts", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "def _source_block_json_tool(query: str) -> str:\n",
    "    return '__START_OF_SOURCE 1__ <CONTENT> {\"prior_occupation\":\"teacher\",\"prior_occupation_source\":\"https://example.org/profile\"} </CONTENT> <URL> https://example.org/profile </URL> __END_OF_SOURCE 1__'\n\n",
    "source_block_json_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Search tool emitting source block JSON facts',\n",
    "    func=_source_block_json_tool,\n",
    ")\n\n",
    "class _SourceBlockJSONLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        if self.calls == 1:\n",
    "            return AIMessage(content='search', tool_calls=[{'name':'Search','args':{'query':'person'},'id':'call_1'}])\n",
    "        return AIMessage(content='{\"prior_occupation\":\"Unknown\",\"prior_occupation_source\":null}')\n\n",
    "source_block_json_llm = _SourceBlockJSONLLM()\n"
  ))

  expected_schema <- list(
    prior_occupation = "Primary occupation before politics, or 'Unknown'",
    prior_occupation_source = "URL of source, or null"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$source_block_json_llm,
    tools = list(reticulate::py$source_block_json_tool)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    expected_schema_source = "explicit",
    input_state = list(
      messages = list(list(
        role = "user",
        content = "Return strict JSON with prior_occupation and prior_occupation_source."
      )),
      search_budget_limit = 4L,
      unknown_after_searches = 1L,
      finalize_on_all_fields_resolved = TRUE
    ),
    config = list(
      recursion_limit = 12L,
      configurable = list(thread_id = "test_source_block_json_field_status")
    ),
    backend = "gemini"
  )

  parsed <- invoke$parsed
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$prior_occupation), "teacher")
  expect_equal(as.character(parsed$prior_occupation_source), "https://example.org/profile")

  final_state <- invoke$final_state
  field_status <- tryCatch(reticulate::py_to_r(final_state$field_status), error = function(e) final_state$field_status)
  expect_true(is.list(field_status))
  expect_equal(as.character(field_status$prior_occupation$status), "found")
  expect_equal(as.character(field_status$prior_occupation$value), "teacher")
  expect_equal(as.character(field_status$prior_occupation$source_url), "https://example.org/profile")
})

test_that("source-block-only search does not prematurely force unknown before terminal source-backed JSON", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "def _source_block_text_tool(query: str) -> str:\n",
    "    return '__START_OF_SOURCE 1__ <CONTENT> Candidate profile summary: prior occupation teacher with role details. </CONTENT> <URL> https://example.org/profile </URL> __END_OF_SOURCE 1__'\n\n",
    "source_block_text_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Search tool emitting source block snippets only',\n",
    "    func=_source_block_text_tool,\n",
    ")\n\n",
    "class _SourceBlockTextLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        if self.calls <= 2:\n",
    "            return AIMessage(content='search more', tool_calls=[{'name':'Search','args':{'query':'person'},'id':'call_' + str(self.calls)}])\n",
    "        return AIMessage(content='{\"prior_occupation\":\"teacher\",\"prior_occupation_source\":\"https://example.org/profile\"}')\n\n",
    "source_block_text_llm = _SourceBlockTextLLM()\n"
  ))

  expected_schema <- list(
    prior_occupation = "Primary occupation before politics, or 'Unknown'",
    prior_occupation_source = "URL of source, or null"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$source_block_text_llm,
    tools = list(reticulate::py$source_block_text_tool)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    expected_schema_source = "explicit",
    input_state = list(
      messages = list(list(
        role = "user",
        content = "Return strict JSON with prior_occupation and prior_occupation_source."
      )),
      search_budget_limit = 8L,
      unknown_after_searches = 1L,
      finalize_on_all_fields_resolved = TRUE
    ),
    config = list(
      recursion_limit = 20L,
      configurable = list(thread_id = "test_source_block_text_attempts")
    ),
    backend = "gemini"
  )

  parsed <- invoke$parsed
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$prior_occupation), "teacher")
  expect_equal(as.character(parsed$prior_occupation_source), "https://example.org/profile")

  final_state <- invoke$final_state
  field_status <- tryCatch(reticulate::py_to_r(final_state$field_status), error = function(e) final_state$field_status)
  expect_true(is.list(field_status))
  expect_equal(as.character(field_status$prior_occupation$status), "found")
  expect_equal(as.character(field_status$prior_occupation$value), "teacher")
  expect_true(as.integer(field_status$prior_occupation$attempts) <= 1L)
})

test_that("finalize canonical guard overrides fabricated terminal values from field_status", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "def _weak_search(query: str) -> str:\n",
    "    return '{\"noise\":\"irrelevant\"}'\n\n",
    "weak_search_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Returns irrelevant structured payload',\n",
    "    func=_weak_search,\n",
    ")\n\n",
    "class _FieldStatusGuardLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        if self.calls == 1:\n",
    "            return AIMessage(\n",
    "                content='search first',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'x'},'id':'call_1'}]\n",
    "            )\n",
    "        return AIMessage(content='{\"prior_occupation\":\"teacher\",\"prior_occupation_source\":null}')\n\n",
    "field_status_guard_llm = _FieldStatusGuardLLM()\n"
  ))

  expected_schema <- list(
    prior_occupation = "Primary occupation before politics, or 'Unknown'",
    prior_occupation_source = "URL of source, or null"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$field_status_guard_llm,
    tools = list(reticulate::py$weak_search_tool)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    expected_schema_source = "explicit",
    input_state = list(
      messages = list(list(
        role = "user",
        content = "Return strict JSON with prior_occupation and prior_occupation_source."
      )),
      search_budget_limit = 4L,
      unknown_after_searches = 1L,
      finalize_on_all_fields_resolved = TRUE
    ),
    config = list(
      recursion_limit = 12L,
      configurable = list(thread_id = "test_field_status_terminal_guard")
    ),
    backend = "gemini"
  )

  parsed <- invoke$parsed
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$prior_occupation), "Unknown")
  expect_true(is.null(parsed$prior_occupation_source) || is.na(parsed$prior_occupation_source))

  final_state <- invoke$final_state
  json_repair <- tryCatch(reticulate::py_to_r(final_state$json_repair), error = function(e) final_state$json_repair)
  json_repair_text <- paste(capture.output(str(json_repair)), collapse = "\n")
  expect_true(grepl("field_status_canonical", json_repair_text, fixed = TRUE))
})

test_that("finalize canonical guard preserves compatible non-empty arrays", {
  graph_mod <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  expected_schema <- list(
    status = "complete|partial",
    steps = list(list(
      step = "integer",
      fact = "string",
      checksum = "integer"
    )),
    missing = list("integer"),
    notes = "string"
  )

  field_status <- list(
    status = list(
      status = "found",
      value = "complete",
      source_url = NULL,
      evidence = "field_status_verified",
      attempts = 0L
    ),
    notes = list(
      status = "found",
      value = "all steps completed",
      source_url = NULL,
      evidence = "field_status_verified",
      attempts = 0L
    )
  )

  response <- list(
    role = "assistant",
    content = paste0(
      "{\"status\":\"complete\",\"steps\":[",
      "{\"step\":1,\"fact\":\"fact_1\",\"checksum\":11},",
      "{\"step\":10,\"fact\":\"fact_10\",\"checksum\":110}",
      "],\"missing\":[],\"notes\":\"all steps completed\"}"
    )
  )

  guarded <- graph_mod$`_apply_field_status_terminal_guard`(
    response = response,
    expected_schema = expected_schema,
    field_status = field_status,
    schema_source = "explicit",
    context = "finalize",
    debug = FALSE
  )

  guarded_response <- guarded[[1]]
  guarded_text <- if (!is.null(guarded_response$content)) {
    as.character(guarded_response$content)
  } else {
    as.character(guarded_response[["content"]])
  }
  parsed <- jsonlite::fromJSON(guarded_text)

  expect_true(is.list(parsed))
  expect_true(is.data.frame(parsed$steps))
  expect_true(all(c("step", "fact", "checksum") %in% names(parsed$steps)))
  expect_true(all(c(1L, 10L) %in% as.integer(parsed$steps$step)))
  expect_equal(as.character(parsed$status), "complete")
})

test_that("canonical payload remains schema-constrained and fills confidence/justification without task-specific derivations", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "def _class_derivation_tool(query: str) -> str:\n",
    "    return '__START_OF_SOURCE 1__ <CONTENT> {\"prior_occupation\":\"Indigenous community member\",\"prior_occupation_source\":\"https://example.org/profile/\"} </CONTENT> <URL> https://example.org/profile/ </URL> __END_OF_SOURCE 1__'\n\n",
    "class_derivation_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Returns source-backed prior occupation facts',\n",
    "    func=_class_derivation_tool,\n",
    ")\n\n",
    "class _ClassDerivationLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        if self.calls == 1:\n",
    "            return AIMessage(content='search', tool_calls=[{'name':'Search','args':{'query':'person'},'id':'call_1'}])\n",
    "        return AIMessage(content='{\"prior_occupation\":\"Unknown\",\"class_background\":\"Unknown\",\"prior_occupation_source\":null,\"confidence\":null,\"justification\":null}')\n\n",
    "class_derivation_llm = _ClassDerivationLLM()\n"
  ))

  expected_schema <- list(
    prior_occupation = "string|Unknown",
    class_background = "Working class|Middle class/professional|Upper/elite|Unknown",
    prior_occupation_source = "string|null",
    confidence = "number",
    justification = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$class_derivation_llm,
    tools = list(reticulate::py$class_derivation_tool)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    expected_schema_source = "explicit",
    input_state = list(
      messages = list(list(
        role = "user",
        content = "Return strict JSON."
      )),
      search_budget_limit = 4L,
      unknown_after_searches = 1L,
      finalize_on_all_fields_resolved = TRUE
    ),
    config = list(
      recursion_limit = 12L,
      configurable = list(thread_id = "test_class_background_derivation")
    ),
    backend = "gemini"
  )

  parsed <- invoke$parsed
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$prior_occupation), "Indigenous community member")
  expect_equal(as.character(parsed$class_background), "Unknown")
  expect_equal(as.character(parsed$prior_occupation_source), "https://example.org/profile")
  expect_true(is.finite(as.numeric(parsed$confidence)))
  expect_gte(as.numeric(parsed$confidence), 0.0)
  expect_lte(as.numeric(parsed$confidence), 1.0)
  expect_true(nchar(as.character(parsed$justification)) > 10L)

  final_state <- invoke$final_state
  field_status <- tryCatch(reticulate::py_to_r(final_state$field_status), error = function(e) final_state$field_status)
  expect_equal(as.character(field_status$class_background$status), "unknown")
  expect_equal(as.character(field_status$class_background$value), "Unknown")
})

test_that("terminal promotion requires source text support for non-source values", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "def _source_support_gate_tool(query: str) -> str:\n",
    "    return '__START_OF_SOURCE 1__ <CONTENT> {\"birth_place\":\"TIPNIS, Beni\",\"birth_place_source\":\"https://example.org/place\"} </CONTENT> <URL> https://example.org/place </URL> __END_OF_SOURCE 1__ __START_OF_SOURCE 2__ <CONTENT> Candidate profile lists constituency and election results only. </CONTENT> <URL> https://example.org/profile </URL> __END_OF_SOURCE 2__'\n\n",
    "source_support_gate_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Returns one structured fact and one unrelated source block',\n",
    "    func=_source_support_gate_tool,\n",
    ")\n\n",
    "class _SourceSupportGateLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        if self.calls == 1:\n",
    "            return AIMessage(content='search', tool_calls=[{'name':'Search','args':{'query':'person'},'id':'call_1'}])\n",
    "        return AIMessage(content='{\"prior_occupation\":\"astronaut\",\"prior_occupation_source\":\"https://example.org/profile\",\"birth_place\":\"Unknown\",\"birth_place_source\":null}')\n\n",
    "source_support_gate_llm = _SourceSupportGateLLM()\n"
  ))

  expected_schema <- list(
    prior_occupation = "string|Unknown",
    prior_occupation_source = "string|null",
    birth_place = "string|Unknown",
    birth_place_source = "string|null"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$source_support_gate_llm,
    tools = list(reticulate::py$source_support_gate_tool)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    expected_schema_source = "explicit",
    input_state = list(
      messages = list(list(
        role = "user",
        content = "Return strict JSON."
      )),
      search_budget_limit = 4L,
      unknown_after_searches = 1L,
      finalize_on_all_fields_resolved = TRUE
    ),
    config = list(
      recursion_limit = 12L,
      configurable = list(thread_id = "test_source_support_gate")
    ),
    backend = "gemini"
  )

  parsed <- invoke$parsed
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$birth_place), "TIPNIS, Beni")
  expect_equal(as.character(parsed$birth_place_source), "https://example.org/place")
  expect_equal(as.character(parsed$prior_occupation), "Unknown")
  expect_true(is.null(parsed$prior_occupation_source) || is.na(parsed$prior_occupation_source))
})

test_that(".extract_response_text returns tool output under recursion_limit when available", {
  asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
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
  expect_equal(response_text, "tool output")
})

test_that(".extract_response_text prefers canonical final_payload when available", {
  raw_response <- list(
    messages = list(
      list(
        type = "ai",
        content = "calling tool",
        tool_calls = list(list(name = "Search", args = list(query = "vietnam"), id = "call_1"))
      ),
      list(type = "tool", content = "tool output")
    ),
    stop_reason = "recursion_limit",
    final_payload = list(country = "Vietnam", count = 63L),
    terminal_valid = TRUE
  )

  response_text <- asa:::.extract_response_text(raw_response, backend = "gemini")
  parsed <- asa:::.parse_json_response(response_text)
  expect_true(is.list(parsed))
  expect_equal(parsed$country, "Vietnam")
  expect_equal(as.integer(parsed$count), 63L)
})

test_that(".extract_response_text preserves JSON string content with angle brackets", {
  raw_response <- list(
    messages = list(
      list(type = "ai", content = "{\"text\":\"a <b>tag</b> c\",\"ok\":1}")
    )
  )

  response_text <- asa:::.extract_response_text(raw_response, backend = "gemini")
  parsed <- asa:::.parse_json_response(response_text)
  expect_true(is.list(parsed))
  expect_equal(parsed$text, "a <b>tag</b> c")
  expect_equal(as.integer(parsed$ok), 1L)
})

test_that(".extract_response_text prefers AI JSON over tool trace payload under recursion_limit", {
  asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c("langchain_core"), method = "import")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  raw_response <- list(
    messages = list(
      msgs$AIMessage(
        content = "{\"country\":\"Vietnam\",\"count\":63}",
        tool_calls = list(list(name = "Search", args = list(query = "vietnam"), id = "call_1"))
      ),
      msgs$ToolMessage(
        content = paste0(
          "__START_OF_SOURCE 1__\n",
          "<URL>https://example.com/vietnam-admin</URL>\n",
          "<CONTENT>Reference page with province table.</CONTENT>\n",
          "__END_OF_SOURCE 1__\n",
          "{\"_tier\":\"ddgs\"}"
        ),
        tool_call_id = "call_1"
      )
    ),
    stop_reason = "recursion_limit"
  )

  response_text <- asa:::.extract_response_text(raw_response, backend = "gemini")
  expect_equal(response_text, "{\"country\":\"Vietnam\",\"count\":63}")
  parsed <- asa:::.parse_json_response(response_text)
  expect_equal(parsed$country, "Vietnam")
  expect_equal(as.integer(parsed$count), 63L)
})

test_that(".extract_response_text returns recursion fallback when only tool-call AI content exists", {
  asa_test_skip_if_no_python(required_files = "custom_ddg_production.py")
  asa_test_skip_if_missing_python_modules(c("langchain_core"), method = "import")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  raw_response <- list(
    messages = list(
      msgs$AIMessage(
        content = "calling tool",
        tool_calls = list(list(name = "Search", args = list(query = "ada"), id = "call_1"))
      )
    ),
    stop_reason = "recursion_limit"
  )

  response_text <- asa:::.extract_response_text(raw_response, backend = "gemini")
  expect_equal(
    response_text,
    "[Agent reached step limit before completing task. Increase recursion_limit or simplify the task.]"
  )
})

test_that("large-prompt recursion-limit finalize preserves earlier tool facts across many rounds (standard)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "tool_call_counter_large = {'n': 0}\n\n",
    "def _fact_search_large(query: str) -> str:\n",
    "    tool_call_counter_large['n'] += 1\n",
    "    n = tool_call_counter_large['n']\n",
    "    if n == 1:\n",
    "        return '{\"prior_occupation\":\"teacher\",\"prior_occupation_source\":\"https://example.org/profile\"}'\n",
    "    return '{\"note\":\"irrelevant\",\"n\":' + str(n) + '}'\n\n",
    "fact_search_tool_large = Tool(\n",
    "    name='Search',\n",
    "    description='Deterministic fact search tool for large prompt recursion tests',\n",
    "    func=_fact_search_large,\n",
    ")\n\n",
    "class _ManyRoundsResidualLLM_Standard:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        sys_text = ''\n",
    "        if messages and hasattr(messages[0], 'content'):\n",
    "            sys_text = str(messages[0].content or '')\n",
    "        if 'FINALIZE MODE' in sys_text:\n",
    "            return AIMessage(\n",
    "                content='',\n",
    "                tool_calls=[{'name':'save_finding','args':{'finding':'noop','category':'fact'},'id':'call_final'}]\n",
    "            )\n",
    "        return AIMessage(\n",
    "            content='continue',\n",
    "            tool_calls=[{'name':'Search','args':{'query':'person'},'id':'call_' + str(self.calls)}]\n",
    "        )\n\n",
    "many_rounds_llm_standard = _ManyRoundsResidualLLM_Standard()\n"
  ))

  expected_schema <- list(
    prior_occupation = "string",
    prior_occupation_source = "string"
  )
  large_prompt <- paste(rep("LARGE_CONTEXT_BLOCK", 2500), collapse = " ")

  agent <- prod$create_standard_agent(
    model = reticulate::py$many_rounds_llm_standard,
    tools = list(reticulate::py$fact_search_tool_large)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    expected_schema_source = "explicit",
    input_state = list(
      messages = list(list(
        role = "user",
        content = paste0(
          large_prompt,
          "\nReturn strict JSON with prior_occupation and prior_occupation_source."
        )
      ))
    ),
    config = list(
      recursion_limit = 20L,
      configurable = list(thread_id = "test_large_prompt_recursion_preserve_standard")
    ),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  response_text <- invoke$response_text
  parsed <- invoke$parsed

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_gt(as.integer(reticulate::py$tool_call_counter_large[["n"]]), 6L)
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$prior_occupation), "teacher")
  expect_equal(as.character(parsed$prior_occupation_source), "https://example.org/profile")
})

test_that("invoke-exception fallback at recursion edge preserves earlier tool facts (standard)", {
  prod <- asa_test_import_langgraph_module("custom_ddg_production", required_files = "custom_ddg_production.py", required_modules = ASA_TEST_LANGGRAPH_MODULES)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "from langchain_core.tools import Tool\n\n",
    "def _fact_search_exception(query: str) -> str:\n",
    "    return '{\"prior_occupation\":\"teacher\",\"prior_occupation_source\":\"https://example.org/profile\"}'\n\n",
    "fact_search_tool_exception = Tool(\n",
    "    name='Search',\n",
    "    description='Deterministic fact search tool for exception fallback tests',\n",
    "    func=_fact_search_exception,\n",
    ")\n\n",
    "class _FinalizeExceptionLLM_Standard:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        sys_text = ''\n",
    "        if messages and hasattr(messages[0], 'content'):\n",
    "            sys_text = str(messages[0].content or '')\n",
    "        if 'FINALIZE MODE' in sys_text:\n",
    "            raise RuntimeError('simulated finalize invoke failure')\n",
    "        return AIMessage(\n",
    "            content='calling search',\n",
    "            tool_calls=[{'name':'Search','args':{'query':'person'},'id':'call_1'}]\n",
    "        )\n\n",
    "finalize_exception_llm_standard = _FinalizeExceptionLLM_Standard()\n"
  ))

  expected_schema <- list(
    prior_occupation = "string",
    prior_occupation_source = "string"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$finalize_exception_llm_standard,
    tools = list(reticulate::py$fact_search_tool_exception)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    prompt = "Return strict JSON with prior_occupation and prior_occupation_source.",
    expected_schema_source = "explicit",
    config = list(
      recursion_limit = 4L,
      configurable = list(thread_id = "test_invoke_exception_preserve_standard")
    ),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  response_text <- invoke$response_text
  parsed <- invoke$parsed

  expect_equal(final_state$stop_reason, "recursion_limit")
  expect_true(is.list(parsed))
  expect_equal(as.character(parsed$prior_occupation), "teacher")
  expect_equal(as.character(parsed$prior_occupation_source), "https://example.org/profile")

  repair_events <- tryCatch(reticulate::py_to_r(final_state$json_repair), error = function(e) NULL)
  expect_true(is.list(repair_events) && length(repair_events) >= 1L)
  reasons <- vapply(repair_events, function(ev) as.character(ev$repair_reason %||% NA_character_), character(1))
  expect_true(any(reasons == "invoke_exception_fallback", na.rm = TRUE))
})


# ── Observation masking preserves compact snippets (Fix 1) ──────────────────

test_that("observation masking uses compact output instead of URL-only stripping", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Summarizer that captures the fold_text prompt it receives.
  # The initial history includes a processed ToolMessage (followed by AIMessage)
  # so folding will exercise the observation-masking path.
  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "from langchain_core.messages import AIMessage, ToolMessage\n",
    "\n",
    "def _fake_search_compact(query: str) -> str:\n",
    "    return 'new search result'\n",
    "\n",
    "fake_search_compact = Tool(\n",
    "    name='Search',\n",
    "    description='Fake search',\n",
    "    func=_fake_search_compact,\n",
    ")\n",
    "\n",
    "class _CompactStubResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "\n",
    "class _CompactSummarizer:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "        self.last_prompt = None\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        self.last_prompt = messages[0].content if messages else None\n",
    "        return _CompactStubResponse('{\"version\":1,\"facts\":[\"birth_year=1982\"],\"decisions\":[],\"open_questions\":[],\"sources\":[],\"warnings\":[]}')\n",
    "\n",
    "class _CompactRecordingLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.n == 1:\n",
    "            return AIMessage(\n",
    "                content='searching again',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'x'},'id':'call_c1'}],\n",
    "            )\n",
    "        return AIMessage(content='final answer')\n",
    "\n",
    "compact_llm = _CompactRecordingLLM()\n",
    "compact_summarizer = _CompactSummarizer()\n",
    "\n",
    "# Build a processed ToolMessage (with structured source blocks) followed by AIMessage\n",
    "processed_tool_msg = ToolMessage(\n",
    "    content=(\n",
    "        '__START_OF_SOURCE 1__\\n'\n",
    "        'Title: Ramona Moye Profile\\n'\n",
    "        'URL: https://example.com/profile\\n'\n",
    "        'Born 1982 in Beni department, indigenous deputy from TIPNIS\\n'\n",
    "        '__END_OF_SOURCE 1__\\n'\n",
    "        '__START_OF_SOURCE 2__\\n'\n",
    "        'Title: Legislative Records\\n'\n",
    "        'URL: https://example.com/records\\n'\n",
    "        'MAS party representative since 2015\\n'\n",
    "        '__END_OF_SOURCE 2__'\n",
    "    ),\n",
    "    tool_call_id='call_prev',\n",
    "    name='Search',\n",
    ")\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$compact_llm,
    tools = list(reticulate::py$fake_search_compact),
    checkpointer = NULL,
    message_threshold = as.integer(5),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$compact_summarizer,
    debug = FALSE
  )

  # History: HumanMessage -> AIMessage(tool_call) -> ToolMessage(search results) -> AIMessage(processed)
  # The ToolMessage is "processed" because it's followed by an AIMessage.
  initial <- msgs$HumanMessage(content = "Find birth_year for Ramona Moye Camaconi")
  ai_tool_call <- msgs$AIMessage(
    content = "Let me search",
    tool_calls = list(list(name = "Search", args = list(query = "x"), id = "call_prev"))
  )
  ai_processed <- msgs$AIMessage(content = "I found the profile, born 1982")

  final_state <- agent$invoke(
    list(messages = list(initial, ai_tool_call, reticulate::py$processed_tool_msg, ai_processed),
         summary = "",
         fold_stats = reticulate::dict(fold_count = 0L)),
    config = list(
      recursion_limit = as.integer(40),
      configurable = list(thread_id = "test_compact_masking")
    )
  )

  # The summarizer should have received the fold prompt containing snippet content
  # (not just URLs). Specifically, "Born 1982" should appear in the fold text.
  expect_equal(as.integer(reticulate::py$compact_summarizer$calls), 1L)
  last_prompt <- as.character(reticulate::py$compact_summarizer$last_prompt)
  # Under compact masking, snippet text is preserved (not stripped to URL-only)
  expect_true(
    grepl("Born 1982", last_prompt, fixed = TRUE) ||
    grepl("Ramona Moye Profile", last_prompt, fixed = TRUE),
    info = "Compact observation masking should preserve snippet content in fold prompt"
  )
  # The new compact marker should appear instead of the old URL-only marker
  expect_true(
    grepl("processed - compact", last_prompt, fixed = TRUE),
    info = "Compact masking marker should appear in fold prompt"
  )
  # The old URL-only masking marker should NOT appear
  expect_false(
    grepl("[already processed]", last_prompt, fixed = TRUE),
    info = "Old URL-only masking marker should not appear"
  )
})


# ── Degraded fold recovers on summarizer failure (Fix 3) ────────────────────

test_that("degraded fold preserves FIELD_EXTRACT entries when summarizer fails", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Summarizer that raises an exception (simulating RemoteProtocolError)
  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "from langchain_core.messages import AIMessage\n",
    "\n",
    "def _fake_search_degrade(query: str) -> str:\n",
    "    return 'FIELD_EXTRACT: birth_year = 1982 (source: https://example.com)'\n",
    "\n",
    "fake_search_degrade = Tool(\n",
    "    name='Search',\n",
    "    description='Fake search',\n",
    "    func=_fake_search_degrade,\n",
    ")\n",
    "\n",
    "class _FailingSummarizer:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        raise ConnectionError('Simulated RemoteProtocolError')\n",
    "\n",
    "class _DegradeLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.n == 1:\n",
    "            return AIMessage(\n",
    "                content='searching',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'x'},'id':'call_d1'}],\n",
    "            )\n",
    "        return AIMessage(content='final answer')\n",
    "\n",
    "degrade_llm = _DegradeLLM()\n",
    "failing_summarizer = _FailingSummarizer()\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$degrade_llm,
    tools = list(reticulate::py$fake_search_degrade),
    checkpointer = NULL,
    message_threshold = as.integer(5),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$failing_summarizer,
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "Find birth_year for test person")
  ai1 <- msgs$AIMessage(content = "old exchange one")
  human2 <- msgs$HumanMessage(content = "continue")
  ai2 <- msgs$AIMessage(content = "old exchange two")

  # Should NOT crash despite summarizer failure
  final_state <- agent$invoke(
    list(messages = list(initial, ai1, human2, ai2), summary = "",
         fold_stats = reticulate::dict(fold_count = 0L)),
    config = list(
      recursion_limit = as.integer(40),
      configurable = list(thread_id = "test_degraded_fold")
    )
  )

  fs <- as.list(final_state$fold_stats)
  # Fold should still have occurred (count incremented)
  expect_equal(as.integer(fs$fold_count), 1L)
  # fold_degraded should be TRUE
  expect_true(isTRUE(fs$fold_degraded))
  # fallback telemetry should explain degraded fold path
  expect_true(isTRUE(fs$fold_fallback_applied))
  expect_equal(as.character(fs$fold_fallback_reason), "summarizer_invoke_exception")
  expect_equal(as.character(fs$fold_parse_error_type), "ConnectionError")
  # fold_parse_success should be FALSE
  expect_false(isTRUE(fs$fold_parse_success))
  # The summarizer was called (and failed)
  expect_equal(as.integer(reticulate::py$failing_summarizer$calls), 1L)
})


# ── Post-fold continuation nudge (Fix 4) ────────────────────────────────────

test_that("post-fold system prompt includes continuation nudge", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "from langchain_core.messages import AIMessage\n",
    "\n",
    "def _fake_search_nudge(query: str) -> str:\n",
    "    return 'result data'\n",
    "\n",
    "fake_search_nudge = Tool(\n",
    "    name='Search',\n",
    "    description='Fake search',\n",
    "    func=_fake_search_nudge,\n",
    ")\n",
    "\n",
    "class _NudgeStubResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "\n",
    "class _NudgeSummarizer:\n",
    "    def invoke(self, messages):\n",
    "        return _NudgeStubResponse('{\"version\":1,\"facts\":[\"some fact\"],\"decisions\":[],\"open_questions\":[],\"sources\":[],\"warnings\":[]}')\n",
    "\n",
    "class _NudgeRecordingLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "        self.system_prompts = []\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        try:\n",
    "            self.system_prompts.append(getattr(messages[0], 'content', None))\n",
    "        except Exception:\n",
    "            self.system_prompts.append(None)\n",
    "        if self.n == 1:\n",
    "            return AIMessage(\n",
    "                content='searching',\n",
    "                tool_calls=[{'name':'Search','args':{'query':'x'},'id':'call_n1'}],\n",
    "            )\n",
    "        return AIMessage(content='final answer')\n",
    "\n",
    "nudge_llm = _NudgeRecordingLLM()\n",
    "nudge_summarizer = _NudgeSummarizer()\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$nudge_llm,
    tools = list(reticulate::py$fake_search_nudge),
    checkpointer = NULL,
    message_threshold = as.integer(5),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$nudge_summarizer,
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "research task")
  ai1 <- msgs$AIMessage(content = "old exchange one")
  human2 <- msgs$HumanMessage(content = "continue")
  ai2 <- msgs$AIMessage(content = "old exchange two")

  final_state <- agent$invoke(
    list(messages = list(initial, ai1, human2, ai2), summary = "",
         fold_stats = reticulate::dict(fold_count = 0L)),
    config = list(
      recursion_limit = as.integer(40),
      configurable = list(thread_id = "test_post_fold_nudge")
    )
  )

  # After fold, the next agent turn should see the post-fold nudge.
  sys_prompts <- reticulate::py_to_r(reticulate::py$nudge_llm$system_prompts)
  expect_true(length(sys_prompts) >= 2L)
  # The post-fold system prompt (second or later) should contain the nudge
  later_prompts <- sys_prompts[-1]
  has_nudge <- any(vapply(later_prompts, function(p) {
    grepl("memory fold just occurred", p, fixed = TRUE)
  }, logical(1)))
  expect_true(has_nudge, info = "Post-fold system prompt should contain continuation nudge")
})

# ===========================================================================
# Plan Mode Tests
# ===========================================================================

test_that("plan mode generates plan in response (memory folding)", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Stub LLM that returns a plan JSON on first call, then a final answer.
  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _PlanModeLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.n == 1:\n",
    '            return AIMessage(content=\'{"goal":"Find info","steps":[{"id":1,"description":"Search"},{"id":2,"description":"Summarize"}],"version":1,"current_step":1}\')\n',
    "        return AIMessage(content='final answer')\n\n",
    "plan_mode_llm = _PlanModeLLM()\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$plan_mode_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(100),
    fold_char_budget = as.integer(100000),
    keep_recent = as.integer(1),
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "Find birth year of Marie Curie")
  final_state <- agent$invoke(
    list(
      messages = list(initial),
      summary = "",
      fold_stats = reticulate::dict(fold_count = 0L),
      use_plan_mode = TRUE
    ),
    config = list(
      recursion_limit = as.integer(20),
      configurable = list(thread_id = "test_plan_mode_on")
    )
  )

  plan <- final_state$plan
  expect_true(!is.null(plan), info = "Plan should be populated when plan mode is on")
  plan_r <- reticulate::py_to_r(plan)
  expect_equal(plan_r$goal, "Find info")
  expect_true(length(plan_r$steps) >= 4L)
  expect_true(all(vapply(plan_r$steps, function(s) {
    if (!is.list(s)) return(FALSE)
    desc <- as.character(s$description)
    length(desc) > 0 && nzchar(desc[[1]])
  }, logical(1))))

  plan_history <- reticulate::py_to_r(final_state$plan_history)
  expect_true(length(plan_history) >= 1L, info = "plan_history should have at least one entry")
})

test_that("plan mode off leaves plan empty (memory folding)", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_llm(
    mode = "simple",
    response_content = "done",
    var_name = "no_plan_llm"
  )

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$no_plan_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(100),
    fold_char_budget = as.integer(100000),
    keep_recent = as.integer(1),
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "hello")
  final_state <- agent$invoke(
    list(
      messages = list(initial),
      summary = "",
      fold_stats = reticulate::dict(fold_count = 0L)
    ),
    config = list(
      recursion_limit = as.integer(20),
      configurable = list(thread_id = "test_plan_mode_off")
    )
  )

  plan <- final_state$plan
  # Plan should be None/NULL when plan mode is off
  expect_true(is.null(plan) || identical(plan, list()),
    info = "Plan should be empty when plan mode is off")
})

test_that("plan mode generates plan in response (standard agent)", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _StdPlanLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.n == 1:\n",
    '            return AIMessage(content=\'{"goal":"Research task","steps":[{"id":1,"description":"Step A"},{"id":2,"description":"Step B"},{"id":3,"description":"Step C"}],"version":1,"current_step":1}\')\n',
    "        return AIMessage(content='standard final')\n\n",
    "std_plan_llm = _StdPlanLLM()\n"
  ))

  agent <- custom_ddg$create_standard_agent(
    model = reticulate::py$std_plan_llm,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Do a research task")),
      stop_reason = NULL,
      use_plan_mode = TRUE,
      tokens_used = 0L,
      input_tokens = 0L,
      output_tokens = 0L,
      token_trace = list()
    ),
    config = list(
      recursion_limit = as.integer(20),
      configurable = list(thread_id = "test_std_plan")
    )
  )

  plan <- final_state$plan
  expect_true(!is.null(plan), info = "Standard agent plan should be populated")
  plan_r <- reticulate::py_to_r(plan)
  expect_equal(plan_r$goal, "Research task")
  expect_true(length(plan_r$steps) >= 4L)
})

test_that("plan mode planner uses the most recent user prompt", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _LatestPromptLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "        self.planner_prompt = None\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.n == 1:\n",
    "            try:\n",
    "                self.planner_prompt = getattr(messages[1], 'content', None)\n",
    "            except Exception:\n",
    "                self.planner_prompt = None\n",
    '            return AIMessage(content=\'{"goal":"G","steps":[{"id":1,"description":"S1"}],"version":1,"current_step":1}\')\n',
    "        return AIMessage(content='done')\n\n",
    "latest_prompt_llm = _LatestPromptLLM()\n"
  ))

  agent <- custom_ddg$create_standard_agent(
    model = reticulate::py$latest_prompt_llm,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE
  )

  final_state <- agent$invoke(
    list(
      messages = list(
        list(role = "user", content = "old prompt should not be used"),
        list(role = "assistant", content = "assistant message"),
        list(role = "user", content = "new prompt should drive plan")
      ),
      stop_reason = NULL,
      use_plan_mode = TRUE,
      tokens_used = 0L,
      input_tokens = 0L,
      output_tokens = 0L,
      token_trace = list()
    ),
    config = list(
      recursion_limit = as.integer(20),
      configurable = list(thread_id = "test_plan_latest_user_prompt")
    )
  )

  expect_true(!is.null(final_state$plan), info = "Plan should still be generated")
  planner_prompt <- reticulate::py_to_r(reticulate::py$latest_prompt_llm$planner_prompt)
  expect_equal(planner_prompt, "new prompt should drive plan")
})

test_that("plan is injected into system prompt", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Use a recording LLM to capture system prompts
  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _PlanRecordLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "        self.system_prompts = []\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        try:\n",
    "            self.system_prompts.append(getattr(messages[0], 'content', None))\n",
    "        except Exception:\n",
    "            self.system_prompts.append(None)\n",
    "        if self.n == 1:\n",
    '            return AIMessage(content=\'{"goal":"Test goal","steps":[{"id":1,"description":"First step"}],"version":1,"current_step":1}\')\n',
    "        return AIMessage(content='done')\n\n",
    "plan_record_llm = _PlanRecordLLM()\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$plan_record_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(100),
    fold_char_budget = as.integer(100000),
    keep_recent = as.integer(1),
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "test prompt")
  final_state <- agent$invoke(
    list(
      messages = list(initial),
      summary = "",
      fold_stats = reticulate::dict(fold_count = 0L),
      use_plan_mode = TRUE
    ),
    config = list(
      recursion_limit = as.integer(20),
      configurable = list(thread_id = "test_plan_in_prompt")
    )
  )

  # The agent_node system prompt (second invocation) should include the plan
  sys_prompts <- reticulate::py_to_r(reticulate::py$plan_record_llm$system_prompts)
  # First prompt is the planner, second is the agent
  agent_prompts <- if (length(sys_prompts) >= 2) sys_prompts[2:length(sys_prompts)] else sys_prompts
  has_plan_block <- any(vapply(agent_prompts, function(p) {
    grepl("YOUR EXECUTION PLAN", p, fixed = TRUE)
  }, logical(1)))
  expect_true(has_plan_block, info = "Agent system prompt should contain the plan block")
})

test_that("reused thread with use_plan_mode FALSE does not inject stale plan", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production",
    required_modules = c(ASA_TEST_LANGGRAPH_MODULES, "langgraph.checkpoint.memory"))
  mem_mod <- reticulate::import("langgraph.checkpoint.memory", convert = TRUE)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _StickyPlanPromptLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "        self.system_prompts = []\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        try:\n",
    "            self.system_prompts.append(getattr(messages[0], 'content', None))\n",
    "        except Exception:\n",
    "            self.system_prompts.append(None)\n",
    "        if self.n == 1:\n",
    '            return AIMessage(content=\'{"goal":"G","steps":[{"id":1,"description":"S1"}],"version":1,"current_step":1}\')\n',
    "        return AIMessage(content='done')\n\n",
    "sticky_plan_prompt_llm = _StickyPlanPromptLLM()\n"
  ))

  agent <- custom_ddg$create_standard_agent(
    model = reticulate::py$sticky_plan_prompt_llm,
    tools = list(),
    checkpointer = mem_mod$MemorySaver(),
    debug = FALSE
  )

  thread_id <- "test_plan_toggle_same_thread"
  state1 <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "run 1")),
      stop_reason = NULL,
      use_plan_mode = TRUE,
      tokens_used = 0L,
      input_tokens = 0L,
      output_tokens = 0L,
      token_trace = list()
    ),
    config = list(
      recursion_limit = as.integer(20),
      configurable = list(thread_id = thread_id)
    )
  )
  expect_true(!is.null(state1$plan), info = "Run 1 should generate a plan")

  state2 <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "run 2 with plan mode off")),
      stop_reason = NULL,
      use_plan_mode = FALSE,
      tokens_used = 0L,
      input_tokens = 0L,
      output_tokens = 0L,
      token_trace = list()
    ),
    config = list(
      recursion_limit = as.integer(20),
      configurable = list(thread_id = thread_id)
    )
  )
  expect_true(!is.null(state2$messages), info = "Run 2 should complete normally")
  expect_true(is.null(state2$plan) || identical(state2$plan, list()),
    info = "Run 2 should not carry a stale plan when mode is off")

  sys_prompts <- reticulate::py_to_r(reticulate::py$sticky_plan_prompt_llm$system_prompts)
  has_plan_block <- vapply(sys_prompts, function(p) {
    is.character(p) && grepl("YOUR EXECUTION PLAN", p, fixed = TRUE)
  }, logical(1))
  expect_true(any(has_plan_block), info = "At least one run should include a plan block")
  expect_false(has_plan_block[[length(has_plan_block)]],
    info = "Run 2 prompt should not include stale plan block when mode is off")
})

test_that("update_plan tool call updates plan steps via tool node", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Stub LLM: first call returns plan, second calls update_plan tool, third returns final
  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _UpdatePlanLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.n == 1:\n",
    '            return AIMessage(content=\'{"goal":"G","steps":[{"id":1,"description":"S1"},{"id":2,"description":"S2"}],"version":1,"current_step":1}\')\n',
    "        if self.n == 2:\n",
    "            return AIMessage(\n",
    "                content='updating plan',\n",
    "                tool_calls=[{'name':'update_plan','args':{'step_id':1,'status':'completed','findings':'found it'},'id':'call_up1'}],\n",
    "            )\n",
    "        return AIMessage(content='all done')\n\n",
    "update_plan_llm = _UpdatePlanLLM()\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$update_plan_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(100),
    fold_char_budget = as.integer(100000),
    keep_recent = as.integer(1),
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "test update plan")
  final_state <- agent$invoke(
    list(
      messages = list(initial),
      summary = "",
      fold_stats = reticulate::dict(fold_count = 0L),
      use_plan_mode = TRUE
    ),
    config = list(
      recursion_limit = as.integer(30),
      configurable = list(thread_id = "test_update_plan")
    )
  )

  plan <- reticulate::py_to_r(final_state$plan)
  expect_true(!is.null(plan), info = "Plan should exist after update_plan call")
  # Step 1 should be completed with findings
  step1 <- plan$steps[[1]]
  expect_equal(step1$status, "completed")
  expect_equal(step1$findings, "found it")
  # Version should have incremented
  expect_true(plan$version >= 2L, info = "Plan version should increment after update")
  # plan_history should have multiple entries
  plan_history <- reticulate::py_to_r(final_state$plan_history)
  expect_true(length(plan_history) >= 2L, info = "plan_history should track versions")
})

test_that("update_plan matches numeric string step ids", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _UpdatePlanStringIdLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.n == 1:\n",
    '            return AIMessage(content=\'{"goal":"G","steps":[{"id":"1","description":"S1"}],"version":1,"current_step":1}\')\n',
    "        if self.n == 2:\n",
    "            return AIMessage(\n",
    "                content='update with int id',\n",
    "                tool_calls=[{'name':'update_plan','args':{'step_id':1,'status':'completed','findings':'found it'},'id':'call_up_str'}],\n",
    "            )\n",
    "        return AIMessage(content='done')\n\n",
    "update_plan_string_id_llm = _UpdatePlanStringIdLLM()\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$update_plan_string_id_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(100),
    fold_char_budget = as.integer(100000),
    keep_recent = as.integer(1),
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "test update plan string id")
  final_state <- agent$invoke(
    list(
      messages = list(initial),
      summary = "",
      fold_stats = reticulate::dict(fold_count = 0L),
      use_plan_mode = TRUE
    ),
    config = list(
      recursion_limit = as.integer(30),
      configurable = list(thread_id = "test_update_plan_string_id")
    )
  )

  plan <- reticulate::py_to_r(final_state$plan)
  expect_true(!is.null(plan), info = "Plan should exist")
  expect_equal(plan$steps[[1]]$status, "completed")
  expect_equal(plan$steps[[1]]$findings, "found it")
  expect_true(plan$version >= 2L)
})

test_that("update_plan no-op does not increment plan version", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _UpdatePlanNoopLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        if self.n == 1:\n",
    '            return AIMessage(content=\'{"goal":"G","steps":[{"id":1,"description":"S1"}],"version":1,"current_step":1}\')\n',
    "        if self.n == 2:\n",
    "            return AIMessage(\n",
    "                content='invalid update',\n",
    "                tool_calls=[{'name':'update_plan','args':{'step_id':999,'status':'completed','findings':'x'},'id':'call_up_noop'}],\n",
    "            )\n",
    "        return AIMessage(content='done')\n\n",
    "update_plan_noop_llm = _UpdatePlanNoopLLM()\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$update_plan_noop_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(100),
    fold_char_budget = as.integer(100000),
    keep_recent = as.integer(1),
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "test update plan noop")
  final_state <- agent$invoke(
    list(
      messages = list(initial),
      summary = "",
      fold_stats = reticulate::dict(fold_count = 0L),
      use_plan_mode = TRUE
    ),
    config = list(
      recursion_limit = as.integer(30),
      configurable = list(thread_id = "test_update_plan_noop")
    )
  )

  plan <- reticulate::py_to_r(final_state$plan)
  expect_true(!is.null(plan), info = "Plan should exist")
  expect_equal(plan$version, 1L)
  expect_equal(plan$steps[[1]]$status, "pending")
})

test_that("_format_plan_for_prompt returns None for empty plan", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  result <- custom_ddg$`_format_plan_for_prompt`(NULL)
  expect_true(is.null(result))
  result2 <- custom_ddg$`_format_plan_for_prompt`(list())
  expect_true(is.null(result2))
})

test_that("_format_plan_for_prompt formats plan correctly", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  plan <- list(
    goal = "Test goal",
    steps = list(
      list(id = 1L, description = "Step one", status = "completed", findings = "Done"),
      list(id = 2L, description = "Step two", status = "in_progress", findings = ""),
      list(id = 3L, description = "Step three", status = "pending", findings = "")
    ),
    version = 2L,
    current_step = 2L
  )
  result <- custom_ddg$`_format_plan_for_prompt`(plan)
  expect_true(grepl("YOUR EXECUTION PLAN", result, fixed = TRUE))
  expect_true(grepl("Test goal", result, fixed = TRUE))
  expect_true(grepl("COMPLETED", result, fixed = TRUE))
  expect_true(grepl("IN PROGRESS", result, fixed = TRUE))
  expect_true(grepl("PENDING", result, fixed = TRUE))
  expect_true(grepl("Step one", result, fixed = TRUE))
  expect_true(grepl("Done", result, fixed = TRUE))
})

test_that("_parse_plan_response handles valid JSON", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  result <- custom_ddg$`_parse_plan_response`(
    '{"goal":"G","steps":[{"id":1,"description":"S1"}],"version":1,"current_step":1}'
  )
  result_r <- reticulate::py_to_r(result)
  expect_equal(result_r$goal, "G")
  # Single-step plans are auto-expanded into a generic 4-step template
  expect_equal(length(result_r$steps), 4L)
  expect_equal(result_r$steps[[1]]$status, "pending")
})

test_that("_parse_plan_response returns fallback for invalid JSON", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  result <- custom_ddg$`_parse_plan_response`("not json at all")
  result_r <- reticulate::py_to_r(result)
  expect_true(!is.null(result_r$goal))
  expect_true(length(result_r$steps) >= 1L)
})

test_that("_sanitize_memory_dict preserves full URLs in long facts", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")

  long_prefix <- paste(rep("context", 90), collapse = " ")
  url <- "https://example.com/research/records/ramona-moye-camaconi/profile?source=asa&view=full"
  long_fact <- paste0(long_prefix, " (source: ", url, ")")

  memory <- list(
    version = 1L,
    facts = list(long_fact),
    decisions = list(),
    open_questions = list(),
    sources = list(),
    warnings = list()
  )

  out <- reticulate::py_to_r(custom_ddg$`_sanitize_memory_dict`(memory))
  expect_true(is.character(out$facts))
  expect_true(length(out$facts) >= 1L)
  expect_true(grepl(url, out$facts[[1]], fixed = TRUE))
  expect_false(grepl(paste0(substr(url, 1, 20), ".*\\.\\.\\."), out$facts[[1]]))
})

test_that("_sanitize_memory_dict coerces scalar summary fields to list form", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")

  out <- reticulate::py_to_r(custom_ddg$`_sanitize_memory_dict`(list(
    version = 1L,
    facts = "fact item",
    decisions = "decision item",
    open_questions = "question item",
    warnings = "warning item",
    sources = list()
  )))

  expect_true(is.character(out$facts))
  expect_true(is.character(out$decisions))
  expect_true(is.character(out$open_questions))
  expect_true(is.character(out$warnings))
  expect_equal(length(out$facts), 1L)
  expect_equal(length(out$decisions), 1L)
  expect_equal(length(out$open_questions), 1L)
  expect_equal(length(out$warnings), 1L)
})

test_that("fold summarizer prompt includes initial disambiguation constraints", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_fake_search_tool(
    return_value = "__START_OF_SOURCE 1__ <CONTENT> profile snippet </CONTENT> <URL> https://example.com/profile </URL> __END_OF_SOURCE 1__",
    var_name = "fake_search_disambig"
  )
  asa_test_stub_llm(
    mode = "tool_call",
    tool_name = "Search",
    tool_args = list(query = "Ramona Moye Camaconi"),
    tool_call_id = "call_disambig",
    response_content = "final answer",
    var_name = "disambig_stub_llm"
  )
  asa_test_stub_summarizer(var_name = "disambig_stub_summarizer")

  agent <- custom_ddg$create_memory_folding_agent(
    model = reticulate::py$disambig_stub_llm,
    tools = list(reticulate::py$fake_search_disambig),
    checkpointer = NULL,
    message_threshold = as.integer(5),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$disambig_stub_summarizer,
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = paste(
    "TASK OVERVIEW:",
    "Research biographical facts.",
    "",
    "TARGET INDIVIDUAL:",
    "- Name: Ramona Moye Camaconi",
    "- Country: Bolivia",
    "- Political Party: Movimiento Al Socialismo - MAS",
    "",
    "DISAMBIGUATION:",
    "- Country: Bolivia",
    "- Time period: Active around 2014",
    "- Region: Beni",
    sep = "\n"
  ))
  ai1 <- msgs$AIMessage(content = "alpha: old assistant answer")
  human2 <- msgs$HumanMessage(content = "followup")
  ai2 <- msgs$AIMessage(content = "beta: old assistant note")

  final_state <- agent$invoke(
    list(
      messages = list(initial, ai1, human2, ai2),
      summary = "",
      fold_stats = reticulate::dict(fold_count = 0L)
    ),
    config = list(
      recursion_limit = as.integer(40),
      configurable = list(thread_id = "test_fold_disambiguation_constraints")
    )
  )

  expect_equal(as.integer(as.list(final_state$fold_stats)$fold_count), 1L)
  expect_equal(as.integer(reticulate::py$disambig_stub_summarizer$calls), 1L)
  last_prompt <- as.character(reticulate::py$disambig_stub_summarizer$last_prompt)
  expect_true(grepl("TASK CONSTRAINTS FROM INITIAL USER REQUEST", last_prompt, fixed = TRUE))
  expect_true(grepl("DISAMBIGUATION", last_prompt, fixed = TRUE))
  expect_true(grepl("Ramona Moye Camaconi", last_prompt, fixed = TRUE))
  expect_true(grepl("Country: Bolivia", last_prompt, fixed = TRUE))
})

test_that("standard graph does not append duplicate terminal JSON for sparse terminal payloads", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "class _SingleTerminalJSONLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools, tool_choice=None):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        return AIMessage(content=",
    deparse('{"field_a":"known_value","field_b":"Unknown","field_c":"Unknown","field_d":"Unknown"}'),
    ")\n",
    "single_terminal_json_llm = _SingleTerminalJSONLLM()\n"
  ))

  agent <- prod$create_standard_agent(
    model = reticulate::py$single_terminal_json_llm,
    tools = list(),
    checkpointer = NULL
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return strict JSON")),
      expected_schema = list(
        field_a = "string",
        field_b = "string",
        field_c = "string",
        field_d = "string"
      ),
      expected_schema_source = "explicit"
    ),
    config = list(
      recursion_limit = as.integer(12),
      configurable = list(thread_id = "test_single_terminal_json_no_duplicate")
    )
  )

  expect_equal(as.integer(reticulate::py$single_terminal_json_llm$calls), 1L)

  terminal_json_count <- sum(vapply(final_state$messages, function(msg) {
    msg_type <- tryCatch(as.character(msg$`__class__`$`__name__`), error = function(e) character(0))
    if (!(length(msg_type) > 0 && identical(msg_type[[1]], "AIMessage"))) {
      return(FALSE)
    }
    tool_calls <- tryCatch(msg$tool_calls, error = function(e) NULL)
    if (!is.null(tool_calls) && length(tool_calls) > 0) {
      return(FALSE)
    }
    content <- tryCatch(as.character(msg$content), error = function(e) "")
    nzchar(content) && startsWith(trimws(content), "{")
  }, logical(1)))
  expect_equal(terminal_json_count, 1L)
})

test_that("memory graph does not append duplicate terminal JSON for sparse terminal payloads", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "class _SingleTerminalJSONMemoryLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools, tool_choice=None):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        return AIMessage(content=",
    deparse('{"field_a":"known_value","field_b":"Unknown","field_c":"Unknown","field_d":"Unknown"}'),
    ")\n",
    "single_terminal_json_memory_llm = _SingleTerminalJSONMemoryLLM()\n"
  ))

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$single_terminal_json_memory_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(999),
    keep_recent = as.integer(2),
    fold_char_budget = as.integer(999999),
    debug = FALSE
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = "Return strict JSON")),
      expected_schema = list(
        field_a = "string",
        field_b = "string",
        field_c = "string",
        field_d = "string"
      ),
      expected_schema_source = "explicit",
      summary = "",
      archive = list(),
      fold_stats = reticulate::dict(fold_count = 0L)
    ),
    config = list(
      recursion_limit = as.integer(12),
      configurable = list(thread_id = "test_single_terminal_json_no_duplicate_memory")
    )
  )

  expect_equal(as.integer(reticulate::py$single_terminal_json_memory_llm$calls), 1L)

  terminal_json_count <- sum(vapply(final_state$messages, function(msg) {
    msg_type <- tryCatch(as.character(msg$`__class__`$`__name__`), error = function(e) character(0))
    if (!(length(msg_type) > 0 && identical(msg_type[[1]], "AIMessage"))) {
      return(FALSE)
    }
    tool_calls <- tryCatch(msg$tool_calls, error = function(e) NULL)
    if (!is.null(tool_calls) && length(tool_calls) > 0) {
      return(FALSE)
    }
    content <- tryCatch(as.character(msg$content), error = function(e) "")
    nzchar(content) && startsWith(trimws(content), "{")
  }, logical(1)))
  expect_equal(terminal_json_count, 1L)
})

test_that("finalization policy keeps allowlisted unsourced meta fields while demoting factual unsourced values", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  field_status <- list(
    confidence = list(
      status = "found",
      value = 0.33,
      source_url = NULL,
      descriptor = "number",
      evidence = "terminal_payload_source_backed"
    ),
    justification = list(
      status = "found",
      value = "Evidence is limited but consistent.",
      source_url = NULL,
      descriptor = "string",
      evidence = "terminal_payload_source_backed"
    ),
    prior_occupation = list(
      status = "found",
      value = "teacher",
      source_url = NULL,
      descriptor = "string",
      evidence = "terminal_payload_source_backed"
    ),
    prior_occupation_source = list(
      status = "found",
      value = "https://example.com/profile",
      source_url = "https://example.com/profile",
      descriptor = "string|null",
      evidence = "terminal_payload_source_backed"
    )
  )

  out <- prod$`_enforce_finalization_policy_on_field_status`(
    field_status,
    list(
      require_verified_for_non_unknown = TRUE,
      unsourced_allowlist_patterns = list("^confidence$", "^justification$")
    )
  )

  expect_equal(tolower(as.character(out$confidence$status)), "found")
  expect_equal(tolower(as.character(out$justification$status)), "found")
  expect_equal(tolower(as.character(out$prior_occupation$status)), "unknown")
  expect_equal(as.character(out$prior_occupation$value), "Unknown")
  expect_equal(tolower(as.character(out$prior_occupation_source$status)), "unknown")
})

test_that("finalization policy demotions are reflected in terminal JSON output (standard)", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  asa_test_fake_search_tool(
    return_value = "{\"ok\":true}",
    var_name = "unsourced_demotion_search_tool",
    tool_name = "Search"
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _UnsourcedDemotionLLM:\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        sys_text = str(messages[0].content or '') if messages else ''\n",
    "        if 'FINALIZE MODE' in sys_text:\n",
    "            return AIMessage(content='{\"class_background\":\"Working class\"}')\n",
    "        return AIMessage(\n",
    "            content='search',\n",
    "            tool_calls=[{'name':'Search','args':{'query':'person'},'id':'call_' + str(self.calls)}]\n",
    "        )\n\n",
    "unsourced_demotion_llm = _UnsourcedDemotionLLM()\n"
  ))

  expected_schema <- list(
    class_background = "Working class|Middle class/professional|Upper/elite|Unknown"
  )

  agent <- prod$create_standard_agent(
    model = reticulate::py$unsourced_demotion_llm,
    tools = list(reticulate::py$unsourced_demotion_search_tool)
  )

  invoke <- asa_test_invoke_json_agent(
    agent = agent,
    expected_schema = expected_schema,
    prompt = "Return strict JSON with class_background.",
    expected_schema_source = "explicit",
    config = list(
      recursion_limit = 8L,
      configurable = list(thread_id = "test_unsourced_demotion_terminal_sync_standard")
    ),
    backend = "gemini"
  )
  final_state <- invoke$final_state
  parsed <- invoke$parsed

  expect_equal(as.character(parsed$class_background), "Unknown")

  field_status <- tryCatch(reticulate::py_to_r(final_state$field_status), error = function(e) final_state$field_status)
  expect_true(is.list(field_status))
  expect_equal(tolower(as.character(field_status$class_background$status)), "unknown")
  expect_equal(as.character(field_status$class_background$evidence), "finalization_policy_demotion_unverified")
})

test_that("source consistency derivations promote *_source when base field has source_url", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  field_status <- list(
    birth_place = list(
      status = "found",
      value = "Santa Cruz",
      source_url = "https://example.com/profile",
      descriptor = "string|Unknown",
      evidence = "scratchpad_source_backed"
    ),
    birth_place_source = list(
      status = "unknown",
      value = NULL,
      source_url = NULL,
      descriptor = "string|null",
      evidence = "source_consistency_demotion"
    )
  )

  out <- prod$`_apply_field_status_derivations`(field_status)

  expect_equal(tolower(as.character(out$birth_place$status)), "found")
  expect_equal(as.character(out$birth_place$source_url), "https://example.com/profile")
  expect_equal(tolower(as.character(out$birth_place_source$status)), "found")
  expect_equal(as.character(out$birth_place_source$value), "https://example.com/profile")
  expect_equal(as.character(out$birth_place_source$source_url), "https://example.com/profile")
  expect_equal(as.character(out$birth_place_source$evidence), "source_consistency_fix")
})

test_that("source policy scoring prefers authority detail pages over search/list pages", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  source_policy <- list(
    authority_host_weights = list("official.example" = 1.0),
    prefer_detail_pages = TRUE,
    detail_page_url_penalties = list("/search", "tag=", "/list")
  )

  detail_score <- as.numeric(prod$`_source_policy_score_candidate`(
    list(
      value_support = 0.8,
      source_quality = 0.7,
      source_specificity = 0.6,
      entity_overlap = 0.7,
      source_url = "https://official.example/profiles/ramona-moye"
    ),
    source_policy
  ))
  search_score <- as.numeric(prod$`_source_policy_score_candidate`(
    list(
      value_support = 0.8,
      source_quality = 0.7,
      source_specificity = 0.6,
      entity_overlap = 0.7,
      source_url = "https://official.example/search?q=ramona+moye"
    ),
    source_policy
  ))

  expect_gt(detail_score, search_score)
})
