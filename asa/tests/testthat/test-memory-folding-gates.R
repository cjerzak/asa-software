# Tests for memory folding gates that prevent tiny/low-value folds.

test_that("message-count folding requires minimum transcript size", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  asa_test_stub_llm(
    mode = "tool_call",
    response_content = "done",
    tool_name = "Search",
    tool_args = list(query = "x"),
    var_name = "fold_gate_llm"
  )
  asa_test_stub_summarizer(var_name = "fold_gate_summarizer")
  tool <- asa_test_fake_search_tool(
    return_value = "ok",
    var_name = "fold_gate_tool"
  )

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$fold_gate_llm,
    tools = list(tool),
    checkpointer = NULL,
    message_threshold = as.integer(2),
    keep_recent = as.integer(1),
    fold_char_budget = as.integer(999999),
    min_fold_batch = as.integer(1),
    min_fold_chars = as.integer(10000),
    min_fold_tokens_est = as.integer(10000),
    summarizer_model = reticulate::py$fold_gate_summarizer,
    debug = FALSE,
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE)
  )

  state <- reticulate::dict(
    messages = list(reticulate::dict(role = "user", content = "hi")),
    summary = "",
    archive = list(),
    fold_stats = reticulate::dict(fold_count = 0L),
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE),
    stop_reason = NULL
  )

  out <- agent$invoke(state, config = list(recursion_limit = as.integer(6)))
  fs <- as.list(out$fold_stats)

  expect_equal(as.integer(fs$fold_count), 0L)
  expect_equal(as.integer(reticulate::py$fold_gate_summarizer$calls), 0L)
})

test_that("summarizer output cap is passed via model.bind", {
  old_env <- Sys.getenv("ASA_MEMORY_SUMMARIZER_MAX_OUTPUT_TOKENS", unset = NA_character_)
  on.exit({
    if (is.na(old_env)) {
      Sys.unsetenv("ASA_MEMORY_SUMMARIZER_MAX_OUTPUT_TOKENS")
    } else {
      Sys.setenv(ASA_MEMORY_SUMMARIZER_MAX_OUTPUT_TOKENS = old_env)
    }
  }, add = TRUE)
  Sys.unsetenv("ASA_MEMORY_SUMMARIZER_MAX_OUTPUT_TOKENS")

  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  asa_test_stub_llm(mode = "simple", response_content = "done", var_name = "cap_llm")

  reticulate::py_run_string(paste(
    "class _StubResponse:",
    "    def __init__(self, content):",
    "        self.content = content",
    "",
    "class _CapSummarizer:",
    "    def __init__(self):",
    "        self.calls = 0",
    "        self.last_bind_kwargs = None",
    "    def bind(self, **kwargs):",
    "        self.last_bind_kwargs = kwargs",
    "        return self",
    "    def invoke(self, messages):",
    "        self.calls += 1",
    "        return _StubResponse('{\"version\":1,\"facts\":[],\"decisions\":[],\"open_questions\":[],\"sources\":[],\"warnings\":[]}')",
    "",
    "cap_summarizer = _CapSummarizer()",
    sep = "\n"
  ))

  agent <- prod$create_memory_folding_agent(
    model = reticulate::py$cap_llm,
    tools = list(),
    checkpointer = NULL,
    message_threshold = as.integer(2),
    keep_recent = as.integer(1),
    summarizer_model = reticulate::py$cap_summarizer,
    summarizer_max_output_tokens = as.integer(400),
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE),
    debug = FALSE
  )

  summarize_state <- reticulate::dict(
    messages = list(
      msgs$HumanMessage(content = "user 1", id = "h1"),
      msgs$AIMessage(content = "assistant 1", id = "a1"),
      msgs$HumanMessage(content = "user 2", id = "h2"),
      msgs$AIMessage(content = "assistant 2", id = "a2")
    ),
    summary = "",
    archive = list(),
    fold_stats = reticulate::dict(fold_count = 0L),
    om_config = list(enabled = FALSE, cross_thread_memory = FALSE),
    stop_reason = NULL
  )

  agent$nodes[["summarize"]]$bound$invoke(summarize_state)

  kwargs <- reticulate::py_to_r(reticulate::py$cap_summarizer$last_bind_kwargs)
  expect_equal(as.integer(kwargs$max_output_tokens), 400L)
  expect_equal(as.integer(reticulate::py$cap_summarizer$calls), 1L)
})

