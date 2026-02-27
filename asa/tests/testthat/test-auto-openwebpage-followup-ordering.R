test_that("auto OpenWebpage follow-up preserves tool-call pairing", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(c(ASA_TEST_LANGGRAPH_MODULES, "langgraph.runtime"))

  graph <- reticulate::import_from_path("asa_backend.graph.agent_graph_core", path = python_path)

  # Monkeypatch the follow-up decision so we deterministically trigger it.
  original_should <- reticulate::py_get_attr(graph, "_should_auto_openwebpage_followup")
  original_select <- reticulate::py_get_attr(graph, "_select_round_openwebpage_candidate")
  original_critical <- reticulate::py_get_attr(graph, "_is_critical_recursion_step")
  on.exit({
    reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup", original_should)
    reticulate::py_set_attr(graph, "_select_round_openwebpage_candidate", original_select)
    reticulate::py_set_attr(graph, "_is_critical_recursion_step", original_critical)
  }, add = TRUE)

  reticulate::py_run_string(paste0(
    "def __asa_test_true(*args, **kwargs):\n",
    "    return True\n",
    "\n",
    "def __asa_test_false(*args, **kwargs):\n",
    "    return False\n",
    "\n",
    "def __asa_test_pick_url(*args, **kwargs):\n",
    "    return 'https://example.com'\n"
  ))

  reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup",
    reticulate::py_eval("__asa_test_true", convert = FALSE))
  reticulate::py_set_attr(graph, "_is_critical_recursion_step",
    reticulate::py_eval("__asa_test_false", convert = FALSE))
  reticulate::py_set_attr(graph, "_select_round_openwebpage_candidate",
    reticulate::py_eval("__asa_test_pick_url", convert = FALSE))

  # Deterministic tools + ToolNode.
  reticulate::py_run_string(paste0(
    "from langchain_core.tools import tool\n",
    "from langchain_core.messages import AIMessage\n",
    "from langgraph.prebuilt import ToolNode\n",
    "from langgraph.runtime import Runtime\n",
    "\n",
    "search_calls = 0\n",
    "open_calls = 0\n",
    "\n",
    "@tool('Search')\n",
    "def Search(query: str) -> str:\n",
    "    \"\"\"Test Search tool.\"\"\"\n",
    "    global search_calls\n",
    "    search_calls += 1\n",
    "    return f'search:{search_calls}:{query}'\n",
    "\n",
    "@tool('OpenWebpage')\n",
    "def OpenWebpage(url: str) -> str:\n",
    "    \"\"\"Test OpenWebpage tool.\"\"\"\n",
    "    global open_calls\n",
    "    open_calls += 1\n",
    "    return f'open:{open_calls}:{url}'\n",
    "\n",
    "search_tool = Search\n",
    "open_tool = OpenWebpage\n",
    "tool_node = ToolNode([search_tool, open_tool])\n",
    "runtime_config = {'configurable': {'__pregel_runtime': Runtime()}}\n",
    "\n",
    "class _ToolNodeShim:\n",
    "    def __init__(self, node, config):\n",
    "        self._node = node\n",
    "        self._config = config\n",
    "    def invoke(self, state):\n",
    "        return self._node.invoke(state, config=self._config)\n",
    "\n",
    "base_tool_node = _ToolNodeShim(tool_node, runtime_config)\n"
  ))

  wrapped_tool_node <- graph$`_create_tool_node_with_scratchpad`(
    reticulate::py$base_tool_node,
    debug = FALSE,
    selector_model = NULL
  )

  # Invoke with a Search tool call; wrapper should auto-follow with OpenWebpage.
  ai_search <- reticulate::py_eval(
    "AIMessage(content='', tool_calls=[{'name':'Search','args':{'query':'q'},'id':'call_1'}])",
    convert = FALSE
  )
  out <- wrapped_tool_node(list(messages = list(ai_search)))

  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$search_calls)), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$open_calls)), 1L)

  types <- vapply(
    out$messages,
    function(m) tryCatch(as.character(m$`__class__`$`__name__`), error = function(e) NA_character_),
    character(1)
  )

  # The follow-up must include an AIMessage with tool_calls immediately before the ToolMessage.
  expect_equal(types, c("ToolMessage", "AIMessage", "ToolMessage"))
  expect_true(!is.null(out$messages[[2]]$tool_calls))
  expect_equal(as.character(out$messages[[2]]$tool_calls[[1]]$name), "OpenWebpage")
  expect_equal(as.character(out$messages[[3]]$name), "OpenWebpage")
  expect_equal(
    as.character(out$messages[[3]]$tool_call_id),
    as.character(out$messages[[2]]$tool_calls[[1]]$id)
  )
})

test_that("auto OpenWebpage follow-up retries alternate candidate after hard failure", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(c(ASA_TEST_LANGGRAPH_MODULES, "langgraph.runtime"))

  graph <- reticulate::import_from_path("asa_backend.graph.agent_graph_core", path = python_path)

  original_should <- reticulate::py_get_attr(graph, "_should_auto_openwebpage_followup")
  original_select <- reticulate::py_get_attr(graph, "_select_round_openwebpage_candidate")
  original_critical <- reticulate::py_get_attr(graph, "_is_critical_recursion_step")
  on.exit({
    reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup", original_should)
    reticulate::py_set_attr(graph, "_select_round_openwebpage_candidate", original_select)
    reticulate::py_set_attr(graph, "_is_critical_recursion_step", original_critical)
  }, add = TRUE)

  reticulate::py_run_string(paste0(
    "def __asa_test_true_retry(*args, **kwargs):\n",
    "    return True\n",
    "\n",
    "def __asa_test_false_retry(*args, **kwargs):\n",
    "    return False\n",
    "\n",
    "_retry_urls = ['https://blocked.example.com/profile', 'https://mirror.example.net/profile']\n",
    "_retry_idx = {'i': 0}\n",
    "def __asa_test_pick_url_retry(*args, **kwargs):\n",
    "    i = _retry_idx['i']\n",
    "    if i >= len(_retry_urls):\n",
    "        return None\n",
    "    _retry_idx['i'] = i + 1\n",
    "    return _retry_urls[i]\n"
  ))

  reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup",
    reticulate::py_eval("__asa_test_true_retry", convert = FALSE))
  reticulate::py_set_attr(graph, "_is_critical_recursion_step",
    reticulate::py_eval("__asa_test_false_retry", convert = FALSE))
  reticulate::py_set_attr(graph, "_select_round_openwebpage_candidate",
    reticulate::py_eval("__asa_test_pick_url_retry", convert = FALSE))

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import tool\n",
    "from langchain_core.messages import AIMessage\n",
    "from langgraph.prebuilt import ToolNode\n",
    "from langgraph.runtime import Runtime\n",
    "\n",
    "search_calls_retry = 0\n",
    "open_calls_retry = 0\n",
    "\n",
    "@tool('Search')\n",
    "def Search(query: str) -> str:\n",
    "    \"\"\"Test Search tool for retry ordering.\"\"\"\n",
    "    global search_calls_retry\n",
    "    search_calls_retry += 1\n",
    "    return f'search:{search_calls_retry}:{query}'\n",
    "\n",
    "@tool('OpenWebpage')\n",
    "def OpenWebpage(url: str) -> str:\n",
    "    \"\"\"Test OpenWebpage tool for retry ordering.\"\"\"\n",
    "    global open_calls_retry\n",
    "    open_calls_retry += 1\n",
    "    if 'blocked' in url:\n",
    "        return 'URL: ' + url + '\\nFinal URL: ' + url + '\\nBlocked fetch detected.\\nReason: http_403_bot_marker (status=403)'\n",
    "    return 'URL: ' + url + '\\nFinal URL: ' + url + '\\nRelevant excerpts:\\n[1]\\nProfile evidence line'\n",
    "\n",
    "search_tool_retry = Search\n",
    "open_tool_retry = OpenWebpage\n",
    "tool_node_retry = ToolNode([search_tool_retry, open_tool_retry])\n",
    "runtime_config_retry = {'configurable': {'__pregel_runtime': Runtime()}}\n",
    "\n",
    "class _ToolNodeShimRetry:\n",
    "    def __init__(self, node, config):\n",
    "        self._node = node\n",
    "        self._config = config\n",
    "    def invoke(self, state):\n",
    "        return self._node.invoke(state, config=self._config)\n",
    "\n",
    "base_tool_node_retry = _ToolNodeShimRetry(tool_node_retry, runtime_config_retry)\n"
  ))

  wrapped_tool_node <- graph$`_create_tool_node_with_scratchpad`(
    reticulate::py$base_tool_node_retry,
    debug = FALSE,
    selector_model = NULL
  )

  ai_search <- reticulate::py_eval(
    "AIMessage(content='', tool_calls=[{'name':'Search','args':{'query':'q'},'id':'call_retry_1'}])",
    convert = FALSE
  )
  out <- wrapped_tool_node(list(
    messages = list(ai_search),
    webpage_policy = list(
      enabled = TRUE,
      parallel_open_limit = 2L,
      max_open_calls = 3L,
      host_cooldown_seconds = 0L,
      blocked_host_ttl_seconds = 900L,
      open_only_if_score_ge = 0
    )
  ))

  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$search_calls_retry)), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$open_calls_retry)), 2L)

  types <- vapply(
    out$messages,
    function(m) tryCatch(as.character(m$`__class__`$`__name__`), error = function(e) NA_character_),
    character(1)
  )
  expect_equal(sum(types == "AIMessage"), 2L)
  expect_equal(sum(types == "ToolMessage"), 3L)

  tool_msgs <- out$messages[types == "ToolMessage"]
  open_tool_msgs <- Filter(function(m) {
    identical(as.character(m$name), "OpenWebpage")
  }, tool_msgs)
  tool_ids <- vapply(open_tool_msgs, function(m) as.character(m$tool_call_id), character(1))
  expect_equal(length(unique(tool_ids[tool_ids != ""])), 2L)
})
