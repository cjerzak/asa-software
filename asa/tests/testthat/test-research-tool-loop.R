# Tests for LangGraph ToolNode/tools_condition tool loop in research_graph.py

test_that("research searcher tool loop respects max_tool_calls_per_round", {
  research <- asa_test_import_langgraph_module(
    "research_graph",
    required_files = "research_graph.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  # Define a deterministic Search tool that records its own call count.
  reticulate::py_run_string(paste0(
    "from langchain_core.tools import tool\n",
    "\n",
    "search_calls = 0\n",
    "\n",
    "@tool('Search')\n",
    "def search(query: str) -> str:\n",
    "    \"\"\"Test search tool.\"\"\"\n",
    "    global search_calls\n",
    "    search_calls += 1\n",
    "    return f'result:{search_calls}:{query}'\n",
    "\n",
    "search_tool = search\n"
  ))

  # Two tool-call agent steps, then one extraction step that returns JSON.
  asa_test_stub_multi_response_llm(
    responses = list(
      list(
        content = "call tool 1",
        tool_calls = list(list(name = "Search", args = list(query = "x"), id = "call_1"))
      ),
      list(
        content = "call tool 2",
        tool_calls = list(list(name = "Search", args = list(query = "y"), id = "call_2"))
      ),
      list(content = "[]")
    ),
    var_name = "stub_llm_tool_loop"
  )

  cfg <- research$ResearchConfig(max_tool_calls_per_round = as.integer(2))

  searcher <- research$create_searcher_node(
    llm = reticulate::py$stub_llm_tool_loop,
    tools = list(reticulate::py$search_tool),
    wikidata_tool = NULL,
    research_config = cfg
  )

  state <- list(
    wikidata_type = NULL,
    query = "test query",
    schema = list(name = "character"),
    config = list(
      use_web = TRUE,
      use_wikidata = FALSE,
      use_wikipedia = FALSE
    ),
    seen_hashes = list(),
    plan = list(),
    results = list(),
    novelty_history = list(),
    round_number = 0L,
    queries_used = 0L,
    tokens_used = 0L,
    input_tokens = 0L,
    output_tokens = 0L,
    start_time = 0
  )

  out <- searcher(state)

  # Tool should be executed twice (two ToolNode runs).
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$search_calls)), 2L)

  # LLM should be invoked exactly 3 times:
  #  - 2 tool-loop agent steps (capped by max_tool_calls_per_round)
  #  - 1 extraction invocation
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$stub_llm_tool_loop$n)), 3L)

  # Sanity: searcher returns expected keys.
  expect_true(is.list(out))
  expect_true(all(c("queries_used", "tokens_used", "round_number") %in% names(out)))
})

