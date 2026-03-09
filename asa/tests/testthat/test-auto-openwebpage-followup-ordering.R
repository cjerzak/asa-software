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
  original_escalation_gate <- reticulate::py_get_attr(graph, "_search_snippet_candidate_supports_openwebpage_escalation")
  on.exit({
    reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup", original_should)
    reticulate::py_set_attr(graph, "_select_round_openwebpage_candidate", original_select)
    reticulate::py_set_attr(graph, "_is_critical_recursion_step", original_critical)
    reticulate::py_set_attr(graph, "_search_snippet_candidate_supports_openwebpage_escalation", original_escalation_gate)
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

test_that("auto OpenWebpage follow-up prefers carried snippet escalation URL", {
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
    "select_pref_calls = 0\n",
    "def __asa_test_true_pref(*args, **kwargs):\n",
    "    return True\n",
    "\n",
    "def __asa_test_false_pref(*args, **kwargs):\n",
    "    return False\n",
    "\n",
    "def __asa_test_pick_url_pref(*args, **kwargs):\n",
    "    global select_pref_calls\n",
    "    select_pref_calls += 1\n",
    "    return 'https://fallback.example.net/profile'\n"
  ))

  reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup",
    reticulate::py_eval("__asa_test_true_pref", convert = FALSE))
  reticulate::py_set_attr(graph, "_is_critical_recursion_step",
    reticulate::py_eval("__asa_test_false_pref", convert = FALSE))
  reticulate::py_set_attr(graph, "_select_round_openwebpage_candidate",
    reticulate::py_eval("__asa_test_pick_url_pref", convert = FALSE))

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import tool\n",
    "from langchain_core.messages import AIMessage\n",
    "from langgraph.prebuilt import ToolNode\n",
    "from langgraph.runtime import Runtime\n",
    "\n",
    "search_calls_pref = 0\n",
    "open_calls_pref = 0\n",
    "\n",
    "@tool('Search')\n",
    "def Search(query: str) -> str:\n",
    "    \"\"\"Test Search tool for preferred follow-up ordering.\"\"\"\n",
    "    global search_calls_pref\n",
    "    search_calls_pref += 1\n",
    "    return f'search:{search_calls_pref}:{query}'\n",
    "\n",
    "@tool('OpenWebpage')\n",
    "def OpenWebpage(url: str) -> str:\n",
    "    \"\"\"Test OpenWebpage tool for preferred follow-up ordering.\"\"\"\n",
    "    global open_calls_pref\n",
    "    open_calls_pref += 1\n",
    "    return 'URL: ' + url + '\\nFinal URL: ' + url + '\\nRelevant excerpts:\\n[1]\\nProfile evidence line'\n",
    "\n",
    "search_tool_pref = Search\n",
    "open_tool_pref = OpenWebpage\n",
    "tool_node_pref = ToolNode([search_tool_pref, open_tool_pref])\n",
    "runtime_config_pref = {'configurable': {'__pregel_runtime': Runtime()}}\n",
    "\n",
    "class _ToolNodeShimPref:\n",
    "    def __init__(self, node, config):\n",
    "        self._node = node\n",
    "        self._config = config\n",
    "    def invoke(self, state):\n",
    "        return self._node.invoke(state, config=self._config)\n",
    "\n",
    "base_tool_node_pref = _ToolNodeShimPref(tool_node_pref, runtime_config_pref)\n"
  ))

  wrapped_tool_node <- graph$`_create_tool_node_with_scratchpad`(
    reticulate::py$base_tool_node_pref,
    debug = FALSE,
    selector_model = NULL
  )

  ai_search <- reticulate::py_eval(
    "AIMessage(content='', tool_calls=[{'name':'Search','args':{'query':'q'},'id':'call_pref_1'}])",
    convert = FALSE
  )
  out <- wrapped_tool_node(list(
    messages = list(ai_search),
    budget_state = list(
      preferred_openwebpage_url = "https://preferred.example.gov/profile"
    ),
    webpage_policy = list(
      enabled = TRUE,
      parallel_open_limit = 1L,
      max_open_calls = 3L,
      host_cooldown_seconds = 0L,
      blocked_host_ttl_seconds = 900L,
      open_only_if_score_ge = 0
    )
  ))

  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$search_calls_pref)), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$open_calls_pref)), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$select_pref_calls)), 0L)

  types <- vapply(
    out$messages,
    function(m) tryCatch(as.character(m$`__class__`$`__name__`), error = function(e) NA_character_),
    character(1)
  )
  expect_equal(types, c("ToolMessage", "AIMessage", "ToolMessage"))
  expect_equal(
    as.character(out$messages[[2]]$tool_calls[[1]]$args$url),
    "https://preferred.example.gov/profile"
  )
  expect_match(
    as.character(out$messages[[3]]$content),
    "preferred\\.example\\.gov/profile",
    perl = TRUE
  )
  expect_true(is.null(out$budget_state$preferred_openwebpage_url))
  expect_length(out$budget_state$preferred_openwebpage_urls, 0L)
})

test_that("snippet escalation can trigger same-round OpenWebpage follow-up", {
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
    "select_same_round_calls = 0\n",
    "def __asa_test_true_same_round(*args, **kwargs):\n",
    "    return True\n",
    "\n",
    "def __asa_test_false_same_round(*args, **kwargs):\n",
    "    return False\n",
    "\n",
    "def __asa_test_true_same_round_gate(*args, **kwargs):\n",
    "    return True\n",
    "\n",
    "def __asa_test_pick_url_same_round(*args, **kwargs):\n",
    "    global select_same_round_calls\n",
    "    select_same_round_calls += 1\n",
    "    return 'https://fallback.example.net/profile'\n"
  ))

  reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup",
    reticulate::py_eval("__asa_test_true_same_round", convert = FALSE))
  reticulate::py_set_attr(graph, "_is_critical_recursion_step",
    reticulate::py_eval("__asa_test_false_same_round", convert = FALSE))
  reticulate::py_set_attr(graph, "_search_snippet_candidate_supports_openwebpage_escalation",
    reticulate::py_eval("__asa_test_true_same_round_gate", convert = FALSE))
  reticulate::py_set_attr(graph, "_select_round_openwebpage_candidate",
    reticulate::py_eval("__asa_test_pick_url_same_round", convert = FALSE))

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import tool\n",
    "from langchain_core.messages import AIMessage\n",
    "from langgraph.prebuilt import ToolNode\n",
    "from langgraph.runtime import Runtime\n",
    "import json\n",
    "\n",
    "search_calls_same_round = 0\n",
    "open_calls_same_round = 0\n",
    "\n",
    "@tool('Search')\n",
    "def Search(query: str) -> str:\n",
    "    \"\"\"Test Search tool for same-round snippet escalation.\"\"\"\n",
    "    global search_calls_same_round\n",
    "    search_calls_same_round += 1\n",
    "    return ('__START_OF_SOURCE 1__ <CONTENT> Ada Lovelace official profile overview with prior occupation section. </CONTENT> ' \\\n",
    "            '<URL> https://preferred.example.gov/profile </URL> __END_OF_SOURCE 1__')\n",
    "\n",
    "@tool('OpenWebpage')\n",
    "def OpenWebpage(url: str) -> str:\n",
    "    \"\"\"Test OpenWebpage tool for same-round snippet escalation.\"\"\"\n",
    "    global open_calls_same_round\n",
    "    open_calls_same_round += 1\n",
    "    return 'URL: ' + url + '\\nFinal URL: ' + url + '\\nRelevant excerpts:\\n[1]\\nProfile evidence line'\n",
    "\n",
    "class _ASASameRoundSnippetModel:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        return AIMessage(content='{}')\n",
    "\n",
    "search_tool_same_round = Search\n",
    "open_tool_same_round = OpenWebpage\n",
    "tool_node_same_round = ToolNode([search_tool_same_round, open_tool_same_round])\n",
    "runtime_config_same_round = {'configurable': {'__pregel_runtime': Runtime()}}\n",
    "\n",
    "class _ToolNodeShimSameRound:\n",
    "    def __init__(self, node, config):\n",
    "        self._node = node\n",
    "        self._config = config\n",
    "    def invoke(self, state):\n",
    "        return self._node.invoke(state, config=self._config)\n",
    "\n",
    "base_tool_node_same_round = _ToolNodeShimSameRound(tool_node_same_round, runtime_config_same_round)\n",
    "same_round_selector_model = _ASASameRoundSnippetModel()\n"
  ))

  wrapped_tool_node <- graph$`_create_tool_node_with_scratchpad`(
    reticulate::py$base_tool_node_same_round,
    debug = FALSE,
    selector_model = reticulate::py$same_round_selector_model
  )

  ai_search <- reticulate::py_eval(
    "AIMessage(content='', tool_calls=[{'name':'Search','args':{'query':'\"Ada Lovelace\" prior occupation'},'id':'call_same_round_1'}])",
    convert = FALSE
  )
  out <- wrapped_tool_node(list(
    messages = list(ai_search),
    expected_schema = list(
      prior_occupation = "string|Unknown",
      prior_occupation_source = "string|null"
    ),
    field_status = list(
      prior_occupation = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
      prior_occupation_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
    ),
    source_policy = list(
      preferred_domains = list("preferred.example.gov"),
      authority_host_weights = list("preferred.example.gov" = 1.0)
    ),
    orchestration_options = list(
      field_resolver = list(
        webpage_extraction_enabled = FALSE,
        search_snippet_extraction_engine = "deterministic_then_legacy"
      )
    ),
    webpage_policy = list(
      enabled = TRUE,
      parallel_open_limit = 1L,
      max_open_calls = 3L,
      host_cooldown_seconds = 0L,
      blocked_host_ttl_seconds = 900L,
      open_only_if_score_ge = 0
    )
  ))

  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$search_calls_same_round)), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$open_calls_same_round)), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$select_same_round_calls)), 0L)

  types <- vapply(
    out$messages,
    function(m) tryCatch(as.character(m$`__class__`$`__name__`), error = function(e) NA_character_),
    character(1)
  )
  expect_equal(types, c("ToolMessage", "AIMessage", "ToolMessage"))
  expect_equal(
    as.character(out$messages[[2]]$tool_calls[[1]]$args$url),
    "https://preferred.example.gov/profile"
  )
  expect_match(
    as.character(out$messages[[3]]$content),
    "preferred\\.example\\.gov/profile",
    perl = TRUE
  )
  expect_true(is.null(out$budget_state$preferred_openwebpage_url))
  expect_length(out$budget_state$preferred_openwebpage_urls, 0L)
  expect_equal(as.integer(out$diagnostics$search_snippet_openwebpage_escalation_events), 1L)
  expect_equal(as.integer(out$diagnostics$search_snippet_openwebpage_escalation_successes), 1L)
  expect_equal(as.integer(out$diagnostics$search_snippet_llm_extraction_calls), 1L)
  expect_equal(as.integer(out$diagnostics$search_snippet_langextract_calls), 0L)
})

test_that("snippet timeout can force same-round OpenWebpage follow-up even when generic gate is off", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(c(ASA_TEST_LANGGRAPH_MODULES, "langgraph.runtime"))

  graph <- reticulate::import_from_path("asa_backend.graph.agent_graph_core", path = python_path)

  original_should <- reticulate::py_get_attr(graph, "_should_auto_openwebpage_followup")
  original_snippet_extract <- reticulate::py_get_attr(graph, "_llm_extract_schema_payloads_from_search_snippets")
  original_critical <- reticulate::py_get_attr(graph, "_is_critical_recursion_step")
  on.exit({
    reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup", original_should)
    reticulate::py_set_attr(graph, "_llm_extract_schema_payloads_from_search_snippets", original_snippet_extract)
    reticulate::py_set_attr(graph, "_is_critical_recursion_step", original_critical)
  }, add = TRUE)

  reticulate::py_run_string(paste0(
    "def __asa_test_false_forced(*args, **kwargs):\n",
    "    return False\n",
    "\n",
    "def __asa_test_snippet_timeout_followup(**kwargs):\n",
    "    return ([], [], {\n",
    "        'provider_timeout': 1,\n",
    "        'snippet_openwebpage_escalation_events': 1,\n",
    "        'preferred_openwebpage_urls': ['https://preferred.example.gov/profile'],\n",
    "        'preferred_openwebpage_url': 'https://preferred.example.gov/profile'\n",
    "    })\n"
  ))

  reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup",
    reticulate::py_eval("__asa_test_false_forced", convert = FALSE))
  reticulate::py_set_attr(graph, "_is_critical_recursion_step",
    reticulate::py_eval("__asa_test_false_forced", convert = FALSE))
  reticulate::py_set_attr(graph, "_llm_extract_schema_payloads_from_search_snippets",
    reticulate::py_eval("__asa_test_snippet_timeout_followup", convert = FALSE))

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import tool\n",
    "from langchain_core.messages import AIMessage\n",
    "from langgraph.prebuilt import ToolNode\n",
    "from langgraph.runtime import Runtime\n",
    "\n",
    "search_calls_forced = 0\n",
    "open_calls_forced = 0\n",
    "\n",
    "@tool('Search')\n",
    "def Search(query: str) -> str:\n",
    "    \"\"\"Test Search tool for forced snippet follow-up.\"\"\"\n",
    "    global search_calls_forced\n",
    "    search_calls_forced += 1\n",
    "    return '__START_OF_SOURCE 1__ <CONTENT> overview </CONTENT> <URL> https://fallback.example.net/profile </URL> __END_OF_SOURCE 1__'\n",
    "\n",
    "@tool('OpenWebpage')\n",
    "def OpenWebpage(url: str) -> str:\n",
    "    \"\"\"Test OpenWebpage tool for forced snippet follow-up.\"\"\"\n",
    "    global open_calls_forced\n",
    "    open_calls_forced += 1\n",
    "    return 'URL: ' + url + '\\nFinal URL: ' + url + '\\nRelevant excerpts:\\n[1]\\nProfile evidence line'\n",
    "\n",
    "tool_node_forced = ToolNode([Search, OpenWebpage])\n",
    "runtime_config_forced = {'configurable': {'__pregel_runtime': Runtime()}}\n",
    "\n",
    "class _ToolNodeShimForced:\n",
    "    def __init__(self, node, config):\n",
    "        self._node = node\n",
    "        self._config = config\n",
    "    def invoke(self, state):\n",
    "        return self._node.invoke(state, config=self._config)\n",
    "\n",
    "base_tool_node_forced = _ToolNodeShimForced(tool_node_forced, runtime_config_forced)\n"
  ))

  wrapped_tool_node <- graph$`_create_tool_node_with_scratchpad`(
    reticulate::py$base_tool_node_forced,
    debug = FALSE,
    selector_model = NULL
  )

  ai_search <- reticulate::py_eval(
    "AIMessage(content='', tool_calls=[{'name':'Search','args':{'query':'q'},'id':'call_forced_1'}])",
    convert = FALSE
  )
  out <- wrapped_tool_node(list(
    messages = list(ai_search),
    expected_schema = list(
      prior_occupation = "string|Unknown",
      prior_occupation_source = "string|null"
    ),
    field_status = list(
      prior_occupation = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
      prior_occupation_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
    ),
    orchestration_options = list(
      field_resolver = list(
        webpage_extraction_enabled = FALSE,
        search_snippet_extraction_engine = "triage_then_structured"
      )
    ),
    webpage_policy = list(
      enabled = TRUE,
      parallel_open_limit = 1L,
      max_open_calls = 3L,
      host_cooldown_seconds = 0L,
      blocked_host_ttl_seconds = 900L,
      open_only_if_score_ge = 0
    )
  ))

  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$search_calls_forced)), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$open_calls_forced)), 1L)
  expect_equal(
    as.character(out$messages[[2]]$tool_calls[[1]]$args$url),
    "https://preferred.example.gov/profile"
  )
  expect_true(is.null(out$budget_state$preferred_openwebpage_url))
  expect_length(out$budget_state$preferred_openwebpage_urls, 0L)
  expect_equal(as.integer(out$budget_state$search_snippet_timeout_count), 1L)
  expect_false(isTRUE(out$budget_state$search_snippet_timeout_circuit_open))
  expect_equal(as.integer(out$diagnostics$search_snippet_forced_openwebpage_followup_rounds), 1L)
})

test_that("quality recovery consumes two preferred OpenWebpage URLs from queue", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(c(ASA_TEST_LANGGRAPH_MODULES, "langgraph.runtime"))

  graph <- reticulate::import_from_path("asa_backend.graph.agent_graph_core", path = python_path)

  original_should <- reticulate::py_get_attr(graph, "_should_auto_openwebpage_followup")
  original_critical <- reticulate::py_get_attr(graph, "_is_critical_recursion_step")
  on.exit({
    reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup", original_should)
    reticulate::py_set_attr(graph, "_is_critical_recursion_step", original_critical)
  }, add = TRUE)

  reticulate::py_run_string(paste0(
    "def __asa_test_true_queue(*args, **kwargs):\n",
    "    return True\n",
    "\n",
    "def __asa_test_false_queue(*args, **kwargs):\n",
    "    return False\n"
  ))
  reticulate::py_set_attr(graph, "_should_auto_openwebpage_followup",
    reticulate::py_eval("__asa_test_true_queue", convert = FALSE))
  reticulate::py_set_attr(graph, "_is_critical_recursion_step",
    reticulate::py_eval("__asa_test_false_queue", convert = FALSE))

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import tool\n",
    "from langchain_core.messages import AIMessage\n",
    "from langgraph.prebuilt import ToolNode\n",
    "from langgraph.runtime import Runtime\n",
    "\n",
    "open_calls_queue = []\n",
    "\n",
    "@tool('Search')\n",
    "def Search(query: str) -> str:\n",
    "    \"\"\"Test Search tool for preferred queue follow-up.\"\"\"\n",
    "    return ('__START_OF_SOURCE 1__ <CONTENT> queue test result </CONTENT> ' \\\n",
    "            '<URL> https://fallback.example.net/profile </URL> __END_OF_SOURCE 1__')\n",
    "\n",
    "@tool('OpenWebpage')\n",
    "def OpenWebpage(url: str) -> str:\n",
    "    \"\"\"Test OpenWebpage tool for preferred queue follow-up.\"\"\"\n",
    "    open_calls_queue.append(url)\n",
    "    return 'URL: ' + url + '\\nFinal URL: ' + url + '\\nRelevant excerpts:\\n[1]\\nProfile evidence line'\n",
    "\n",
    "tool_node_queue = ToolNode([Search, OpenWebpage])\n",
    "runtime_config_queue = {'configurable': {'__pregel_runtime': Runtime()}}\n",
    "\n",
    "class _ToolNodeShimQueue:\n",
    "    def __init__(self, node, config):\n",
    "        self._node = node\n",
    "        self._config = config\n",
    "    def invoke(self, state):\n",
    "        return self._node.invoke(state, config=self._config)\n",
    "\n",
    "base_tool_node_queue = _ToolNodeShimQueue(tool_node_queue, runtime_config_queue)\n"
  ))

  wrapped_tool_node <- graph$`_create_tool_node_with_scratchpad`(
    reticulate::py$base_tool_node_queue,
    debug = FALSE,
    selector_model = NULL
  )

  ai_search <- reticulate::py_eval(
    "AIMessage(content='', tool_calls=[{'name':'Search','args':{'query':'q'},'id':'call_queue_1'}])",
    convert = FALSE
  )
  out <- wrapped_tool_node(list(
    messages = list(ai_search),
    budget_state = list(
      preferred_openwebpage_urls = list(
        "https://preferred-a.example.gov/profile",
        "https://preferred-b.example.gov/profile"
      ),
      preferred_openwebpage_url = "https://preferred-a.example.gov/profile",
      quality_gate_recovery_active = TRUE
    ),
    source_policy = list(
      preferred_domains = list("preferred-a.example.gov", "preferred-b.example.gov"),
      authority_host_weights = list(
        "preferred-a.example.gov" = 1.0,
        "preferred-b.example.gov" = 1.0
      )
    ),
    webpage_policy = list(
      enabled = TRUE,
      parallel_open_limit = 1L,
      max_open_calls = 3L,
      host_cooldown_seconds = 0L,
      blocked_host_ttl_seconds = 900L,
      open_only_if_score_ge = 0
    )
  ))

  opened_urls <- reticulate::py_to_r(reticulate::py$open_calls_queue)
  expect_equal(length(opened_urls), 2L)
  expect_equal(
    opened_urls,
    c("https://preferred-a.example.gov/profile", "https://preferred-b.example.gov/profile")
  )
  expect_true(is.null(out$budget_state$preferred_openwebpage_url))
  expect_length(out$budget_state$preferred_openwebpage_urls, 0L)
  expect_equal(as.integer(out$diagnostics$search_snippet_preferred_queue_size_max), 2L)
})

test_that("quality-gate recovery widens webpage extraction page budget", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(c(ASA_TEST_LANGGRAPH_MODULES, "langgraph.runtime"))

  graph <- reticulate::import_from_path("asa_backend.graph.agent_graph_core", path = python_path)

  original_webpage_extract <- reticulate::py_get_attr(graph, "_llm_extract_schema_payloads_from_openwebpages")
  original_critical <- reticulate::py_get_attr(graph, "_is_critical_recursion_step")
  on.exit({
    reticulate::py_set_attr(graph, "_llm_extract_schema_payloads_from_openwebpages", original_webpage_extract)
    reticulate::py_set_attr(graph, "_is_critical_recursion_step", original_critical)
  }, add = TRUE)

  reticulate::py_run_string(paste0(
    "captured_recovery_max_pages = None\n",
    "\n",
    "def __asa_capture_openwebpage_extract(**kwargs):\n",
    "    global captured_recovery_max_pages\n",
    "    captured_recovery_max_pages = kwargs.get('max_pages')\n",
    "    return ([], [], {'structured_output_calls': 0})\n",
    "\n",
    "def __asa_test_false_recovery(*args, **kwargs):\n",
    "    return False\n"
  ))
  reticulate::py_set_attr(
    graph,
    "_llm_extract_schema_payloads_from_openwebpages",
    reticulate::py_eval("__asa_capture_openwebpage_extract", convert = FALSE)
  )
  reticulate::py_set_attr(
    graph,
    "_is_critical_recursion_step",
    reticulate::py_eval("__asa_test_false_recovery", convert = FALSE)
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import tool\n",
    "from langchain_core.messages import AIMessage\n",
    "from langgraph.prebuilt import ToolNode\n",
    "from langgraph.runtime import Runtime\n",
    "\n",
    "@tool('OpenWebpage')\n",
    "def OpenWebpage(url: str) -> str:\n",
    "    \"\"\"Test OpenWebpage tool for recovery-budget widening.\"\"\"\n",
    "    return 'URL: ' + url + '\\nFinal URL: ' + url + '\\nRelevant excerpts:\\n[1]\\nEvidence line'\n",
    "\n",
    "tool_node_recovery = ToolNode([OpenWebpage])\n",
    "runtime_config_recovery = {'configurable': {'__pregel_runtime': Runtime()}}\n",
    "\n",
    "class _ToolNodeShimRecovery:\n",
    "    def __init__(self, node, config):\n",
    "        self._node = node\n",
    "        self._config = config\n",
    "    def invoke(self, state):\n",
    "        return self._node.invoke(state, config=self._config)\n",
    "\n",
    "class _ASARecoverySelectorModel:\n",
    "    pass\n",
    "\n",
    "base_tool_node_recovery = _ToolNodeShimRecovery(tool_node_recovery, runtime_config_recovery)\n",
    "recovery_selector_model = _ASARecoverySelectorModel()\n"
  ))

  wrapped_tool_node <- graph$`_create_tool_node_with_scratchpad`(
    reticulate::py$base_tool_node_recovery,
    debug = FALSE,
    selector_model = reticulate::py$recovery_selector_model
  )

  ai_open <- reticulate::py_eval(
    "AIMessage(content='', tool_calls=[{'name':'OpenWebpage','args':{'url':'https://example.com/profile'},'id':'call_recovery_1'}])",
    convert = FALSE
  )
  wrapped_tool_node(list(
    messages = list(ai_open),
    expected_schema = list(
      birth_place = "string|Unknown",
      birth_place_source = "string|null"
    ),
    field_status = list(
      birth_place = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
      birth_place_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
    ),
    orchestration_options = list(
      field_resolver = list(
        webpage_extraction_enabled = TRUE,
        llm_webpage_extraction = TRUE,
        llm_webpage_extraction_max_pages_per_round = 1L,
        llm_webpage_extraction_max_total_pages = 1L
      )
    ),
    budget_state = list(
      quality_gate_recovery_active = TRUE
    )
  ))

  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$captured_recovery_max_pages)), 2L)
})

test_that("generic follow-up selection mildly prefers authoritative detail pages", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )

  graph <- reticulate::import_from_path("asa_backend.graph.agent_graph_core", path = python_path)
  original_score <- reticulate::py_get_attr(graph, "_score_primary_source_url")
  on.exit({
    reticulate::py_set_attr(graph, "_score_primary_source_url", original_score)
  }, add = TRUE)

  reticulate::py_run_string(paste0(
    "def __asa_test_followup_url_score(url):\n",
    "    text = str(url or '')\n",
    "    if 'neutral.example.net' in text:\n",
    "        return 1.20\n",
    "    if 'official.example' in text:\n",
    "        return 1.00\n",
    "    return 0.0\n"
  ))
  reticulate::py_set_attr(graph, "_score_primary_source_url",
    reticulate::py_eval("__asa_test_followup_url_score", convert = FALSE))

  search_msg <- list(
    role = "tool",
    name = "Search",
    content = paste(
      "__START_OF_SOURCE 1__ <CONTENT>Ada Lovelace prior occupation overview search page.</CONTENT>",
      "<URL>https://neutral.example.net/search/ada-lovelace</URL> __END_OF_SOURCE 1__",
      "__START_OF_SOURCE 2__ <CONTENT>Ada Lovelace prior occupation official legislator profile.</CONTENT>",
      "<URL>https://official.example/people/profile/123</URL> __END_OF_SOURCE 2__"
    )
  )

  selected <- graph$`_select_round_openwebpage_candidate`(
    state_messages = list(),
    round_tool_messages = list(search_msg),
    search_queries = list("Ada Lovelace official biography"),
    selector_model = NULL,
    source_policy = list(
      preferred_domains = list("official.example"),
      authority_host_weights = list("official.example" = 1.0),
      openwebpage_authority_bonus_weight = 0.20,
      openwebpage_detail_bonus_weight = 0.15,
      openwebpage_preferred_domain_bonus_weight = 0.10
    ),
    webpage_policy = list(
      enabled = TRUE,
      open_only_if_score_ge = 0
    )
  )

  expect_equal(
    as.character(selected),
    "https://official.example/people/profile/123"
  )
})

test_that("snippet escalation preferred URL uses the same generic authority/detail rerank", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )

  graph <- reticulate::import_from_path("asa_backend.graph.agent_graph_core", path = python_path)
  original_score <- reticulate::py_get_attr(graph, "_score_primary_source_url")
  original_escalation_gate <- reticulate::py_get_attr(graph, "_search_snippet_candidate_supports_openwebpage_escalation")
  on.exit({
    reticulate::py_set_attr(graph, "_score_primary_source_url", original_score)
    reticulate::py_set_attr(graph, "_search_snippet_candidate_supports_openwebpage_escalation", original_escalation_gate)
  }, add = TRUE)

  reticulate::py_run_string(paste0(
    "def __asa_test_snippet_url_score(url):\n",
    "    text = str(url or '')\n",
    "    if 'neutral.example.net' in text:\n",
    "        return 1.20\n",
    "    if 'official.example' in text:\n",
    "        return 1.00\n",
    "    return 0.0\n",
    "\n",
    "def __asa_test_true_snippet_gate(*args, **kwargs):\n",
    "    return True\n"
  ))
  reticulate::py_set_attr(graph, "_score_primary_source_url",
    reticulate::py_eval("__asa_test_snippet_url_score", convert = FALSE))
  reticulate::py_set_attr(graph, "_search_snippet_candidate_supports_openwebpage_escalation",
    reticulate::py_eval("__asa_test_true_snippet_gate", convert = FALSE))

  search_msg <- list(
    role = "tool",
    name = "Search",
    content = paste(
      "__START_OF_SOURCE 1__ <CONTENT>Ada Lovelace overview search page.</CONTENT>",
      "<URL>https://neutral.example.net/search/ada-lovelace</URL> __END_OF_SOURCE 1__",
      "__START_OF_SOURCE 2__ <CONTENT>Ada Lovelace official legislator profile.</CONTENT>",
      "<URL>https://official.example/people/profile/123</URL> __END_OF_SOURCE 2__"
    )
  )

  out <- graph$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = NULL,
    expected_schema = list(
      prior_occupation = "string|Unknown",
      prior_occupation_source = "string|null"
    ),
    field_status = list(
      prior_occupation = list(status = "unknown", value = "Unknown", attempts = 0L),
      prior_occupation_source = list(status = "pending", value = NULL, attempts = 0L)
    ),
    tool_messages = list(search_msg),
    source_policy = list(
      preferred_domains = list("official.example"),
      authority_host_weights = list("official.example" = 1.0),
      openwebpage_authority_bonus_weight = 0.20,
      openwebpage_detail_bonus_weight = 0.15,
      openwebpage_preferred_domain_bonus_weight = 0.10
    ),
    max_sources = 2L,
    extraction_engine = "legacy",
    debug = FALSE
  )

  meta <- out[[3]]
  expect_equal(as.integer(meta$snippet_openwebpage_escalation_events), 1L)
  expect_equal(
    as.character(meta$preferred_openwebpage_url),
    "https://official.example/people/profile/123"
  )
})

test_that("round OpenWebpage selection admits sparse official profiles via authority lane", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )

  graph <- reticulate::import_from_path("asa_backend.graph.agent_graph_core", path = python_path)
  original_score <- reticulate::py_get_attr(graph, "_score_primary_source_url")
  on.exit({
    reticulate::py_set_attr(graph, "_score_primary_source_url", original_score)
  }, add = TRUE)

  reticulate::py_run_string(paste0(
    "def __asa_test_sparse_followup_score(url):\n",
    "    text = str(url or '')\n",
    "    if 'news.example.net' in text:\n",
    "        return 1.20\n",
    "    if 'official.example' in text:\n",
    "        return 0.95\n",
    "    return 0.0\n"
  ))
  reticulate::py_set_attr(graph, "_score_primary_source_url",
    reticulate::py_eval("__asa_test_sparse_followup_score", convert = FALSE))

  search_msg <- list(
    role = "tool",
    name = "Search",
    content = paste(
      "__START_OF_SOURCE 1__ <CONTENT>Ada Lovelace prior occupation and biography article.</CONTENT>",
      "<URL>https://news.example.net/article/ada-lovelace</URL> __END_OF_SOURCE 1__",
      "__START_OF_SOURCE 2__ <CONTENT>Official parliamentary profile page.</CONTENT>",
      "<URL>https://official.example/parliamentarian?id=428</URL> __END_OF_SOURCE 2__"
    )
  )

  selected <- graph$`_select_round_openwebpage_candidate`(
    state_messages = list(),
    round_tool_messages = list(search_msg),
    search_queries = list("Ada Lovelace prior occupation"),
    selector_model = NULL,
    source_policy = list(
      preferred_domains = list("official.example"),
      authority_host_weights = list("official.example" = 1.0),
      openwebpage_authority_bonus_weight = 0.20,
      openwebpage_detail_bonus_weight = 0.15,
      openwebpage_preferred_domain_bonus_weight = 0.10
    ),
    webpage_policy = list(
      enabled = TRUE,
      open_only_if_score_ge = 0
    )
  )

  expect_equal(
    as.character(selected),
    "https://official.example/parliamentarian?id=428"
  )
})

test_that("OpenWebpage URL accounting counts only canonical opened targets", {
  graph <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )
  builtins <- reticulate::import_builtins()

  polluted_messages <- list(
    list(
      role = "tool",
      name = "OpenWebpage",
      content = paste(
        "URL: https://www.legislativo.gob.bo/asambleista/ramona",
        "Final URL: https://www.legislativo.gob.bo/asambleista/ramona",
        "Relevant excerpts:",
        "[1]",
        "[link: https://www.legislativo.gob.bo/legislatura]",
        "[link: https://www.facebook.com/public/Ramona-Moye-Camaconi]",
        sep = "\n"
      )
    ),
    list(
      role = "tool",
      name = "OpenWebpage",
      content = paste(
        "URL: https://www.legislativo.gob.bo/asambleistas?utf8=%E2%9C%93&page=2",
        "Final URL: https://www.legislativo.gob.bo/asambleistas?utf8=%E2%9C%93&page=2",
        "Relevant excerpts:",
        "[1]",
        "[link: https://www.legislativo.gob.bo/asambleista/ramona]",
        "[link: https://prezi.com/p/84pdg0dsextr/una-biografia]",
        sep = "\n"
      )
    )
  )

  opened_urls <- sort(reticulate::py_to_r(
    builtins$list(graph$`_collect_openwebpage_urls_from_messages`(polluted_messages, max_urls = 16L))
  ))
  successful_urls <- sort(reticulate::py_to_r(
    builtins$list(graph$`_collect_successful_openwebpage_urls_from_messages`(polluted_messages, max_urls = 16L))
  ))

  expect_equal(
    opened_urls,
    c(
      "https://www.legislativo.gob.bo/asambleista/ramona",
      "https://www.legislativo.gob.bo/asambleistas?utf8=%E2%9C%93&page=2"
    )
  )
  expect_equal(successful_urls, opened_urls)
})

test_that("OpenWebpage attempt accounting includes hard-fail tool calls", {
  graph <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )
  builtins <- reticulate::import_builtins()

  attempted_messages <- list(
    list(
      role = "assistant",
      tool_calls = list(list(
        name = "OpenWebpage",
        args = list(url = "https://official.example/parliamentarian?id=428")
      ))
    ),
    list(
      role = "tool",
      name = "OpenWebpage",
      content = "{\"ok\": false, \"error_type\": \"request_exception\"}"
    ),
    list(
      role = "assistant",
      tool_calls = list(list(
        name = "OpenWebpage",
        args = list(url = "https://www.legislativo.gob.bo/asambleistas?page=3")
      ))
    ),
    list(
      role = "tool",
      name = "OpenWebpage",
      content = paste(
        "URL: https://www.legislativo.gob.bo/asambleistas?page=3",
        "Final URL: https://www.legislativo.gob.bo/asambleistas?page=3",
        "Relevant excerpts:",
        "[1]",
        "Profile evidence line",
        sep = "\n"
      )
    )
  )

  attempted_urls <- sort(reticulate::py_to_r(
    builtins$list(graph$`_collect_attempted_openwebpage_urls_from_messages`(attempted_messages, max_urls = 16L))
  ))

  expect_equal(
    attempted_urls,
    c(
      "https://official.example/parliamentarian?id=428",
      "https://www.legislativo.gob.bo/asambleistas?page=3"
    )
  )
})

test_that("forced snippet follow-up is not blocked by embedded links in prior OpenWebpage output", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(c(ASA_TEST_LANGGRAPH_MODULES, "langgraph.runtime"))

  graph <- reticulate::import_from_path("asa_backend.graph.agent_graph_core", path = python_path)

  original_critical <- reticulate::py_get_attr(graph, "_is_critical_recursion_step")
  on.exit({
    reticulate::py_set_attr(graph, "_is_critical_recursion_step", original_critical)
  }, add = TRUE)

  reticulate::py_run_string(paste0(
    "def __asa_test_false_polluted(*args, **kwargs):\n",
    "    return False\n"
  ))
  reticulate::py_set_attr(
    graph,
    "_is_critical_recursion_step",
    reticulate::py_eval("__asa_test_false_polluted", convert = FALSE)
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import tool\n",
    "from langgraph.prebuilt import ToolNode\n",
    "from langgraph.runtime import Runtime\n",
    "\n",
    "open_calls_polluted = []\n",
    "\n",
    "@tool('OpenWebpage')\n",
    "def OpenWebpage(url: str) -> str:\n",
    "    \"\"\"Test OpenWebpage tool for polluted quota accounting.\"\"\"\n",
    "    open_calls_polluted.append(url)\n",
    "    return 'URL: ' + url + '\\nFinal URL: ' + url + '\\nRelevant excerpts:\\n[1]\\nProfile evidence line'\n",
    "\n",
    "tool_node_polluted = ToolNode([OpenWebpage])\n",
    "runtime_config_polluted = {'configurable': {'__pregel_runtime': Runtime()}}\n",
    "\n",
    "class _ToolNodeShimPolluted:\n",
    "    def __init__(self, node, config):\n",
    "        self._node = node\n",
    "        self._config = config\n",
    "    def invoke(self, state):\n",
    "        return self._node.invoke(state, config=self._config)\n",
    "\n",
    "base_tool_node_polluted = _ToolNodeShimPolluted(tool_node_polluted, runtime_config_polluted)\n"
  ))

  polluted_prior_messages <- list(
    list(
      role = "tool",
      name = "OpenWebpage",
      content = paste(
        "URL: https://www.legislativo.gob.bo/asambleista/ramona",
        "Final URL: https://www.legislativo.gob.bo/asambleista/ramona",
        "Relevant excerpts:",
        "[1]",
        "[link: https://www.legislativo.gob.bo/legislatura]",
        "[link: https://www.facebook.com/public/Ramona-Moye-Camaconi]",
        sep = "\n"
      )
    ),
    list(
      role = "tool",
      name = "OpenWebpage",
      content = paste(
        "URL: https://www.legislativo.gob.bo/asambleistas?utf8=%E2%9C%93&page=2",
        "Final URL: https://www.legislativo.gob.bo/asambleistas?utf8=%E2%9C%93&page=2",
        "Relevant excerpts:",
        "[1]",
        "[link: https://www.legislativo.gob.bo/asambleista/ramona]",
        "[link: https://prezi.com/p/84pdg0dsextr/una-biografia]",
        sep = "\n"
      )
    )
  )
  current_round_messages <- list(list(
    role = "tool",
    name = "Search",
    content = paste0(
      "__START_OF_SOURCE 1__ <CONTENT> Official parliamentary listing. </CONTENT> ",
      "<URL> https://www.legislativo.gob.bo/asambleistas?page=3 </URL> __END_OF_SOURCE 1__"
    )
  ))

  result <- graph$`_apply_auto_openwebpage_followup`(
    state = list(
      messages = polluted_prior_messages,
      budget_state = list(
        tool_calls_remaining = 5L,
        preferred_openwebpage_urls = list("https://www.legislativo.gob.bo/asambleistas?page=3"),
        preferred_openwebpage_url = "https://www.legislativo.gob.bo/asambleistas?page=3"
      ),
      webpage_policy = list(
        enabled = TRUE,
        parallel_open_limit = 1L,
        max_open_calls = 3L,
        host_cooldown_seconds = 0L,
        blocked_host_ttl_seconds = 900L,
        open_only_if_score_ge = 0
      )
    ),
    base_tool_node = reticulate::py$base_tool_node_polluted,
    tool_messages = current_round_messages,
    search_queries = list("Ramona Moye Camaconi"),
    had_explicit_openwebpage_call = FALSE,
    selector_model = NULL,
    force_due_to_snippet_recovery = TRUE,
    return_metadata = TRUE,
    debug = FALSE
  )

  result_r <- reticulate::py_to_r(result)
  out_messages <- result_r[[1]]
  meta <- result_r[[2]]

  expect_equal(length(reticulate::py_to_r(reticulate::py$open_calls_polluted)), 1L)
  expect_equal(
    reticulate::py_to_r(reticulate::py$open_calls_polluted)[[1]],
    "https://www.legislativo.gob.bo/asambleistas?page=3"
  )
  expect_equal(length(out_messages), length(current_round_messages) + 2L)
  expect_equal(as.character(meta$attempted_urls[[1]]), "https://www.legislativo.gob.bo/asambleistas?page=3")
  expect_true(is.null(meta$noop_reason) || identical(meta$noop_reason, ""))
})
