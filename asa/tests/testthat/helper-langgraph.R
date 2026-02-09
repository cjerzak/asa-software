asa_test_import_langgraph_module <- function(module_name,
                                             required_files = paste0(module_name, ".py"),
                                             required_modules = ASA_TEST_LANGGRAPH_MODULES,
                                             initialize = TRUE) {
  python_path <- asa_test_skip_if_no_python(
    required_files = required_files,
    initialize = initialize
  )
  if (!is.null(required_modules)) {
    asa_test_require_langgraph_stack(required_modules)
  }
  reticulate::import_from_path(module_name, path = python_path)
}


asa_test_parse_agent_json <- function(state_or_response, backend = "gemini") {
  response_text <- if (is.character(state_or_response) && length(state_or_response) > 0) {
    as.character(state_or_response[[1]])
  } else {
    asa:::.extract_response_text(state_or_response, backend = backend)
  }

  list(
    response_text = response_text,
    parsed = jsonlite::fromJSON(response_text)
  )
}


# ---------------------------------------------------------------------------
# Stub LLM: always returns empty content + residual tool call (save_finding)
# Used in finalize-sanitize tests (standard & memory folding)
# ---------------------------------------------------------------------------

asa_test_stub_residual_tool_llm <- function(var_name = "residual_tool_llm",
                                            tool_name = "save_finding",
                                            tool_args = "{'finding':'X','category':'fact'}",
                                            tool_call_id = "call_1") {
  code <- paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "class _ResidualToolCallLLM_", var_name, ":\n",
    "    def __init__(self):\n",
    "        self.calls = 0\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.calls += 1\n",
    "        return AIMessage(\n",
    "            content='',\n",
    "            tool_calls=[{'name':'", tool_name, "','args':", tool_args,
    ",'id':'", tool_call_id, "'}]\n",
    "        )\n\n",
    var_name, " = _ResidualToolCallLLM_", var_name, "()\n"
  )
  reticulate::py_run_string(code)
  reticulate::py[[var_name]]
}

# ---------------------------------------------------------------------------
# Minimal ResearchConfig with max/disabled defaults for recursion tests
# ---------------------------------------------------------------------------

asa_test_minimal_research_config <- function(research, ...) {
  defaults <- list(
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
  overrides <- list(...)
  args <- modifyList(defaults, overrides)
  do.call(research$ResearchConfig, args)
}

# ---------------------------------------------------------------------------
# Validate fold_stats diagnostics after a fold
# ---------------------------------------------------------------------------

expect_valid_fold_stats <- function(fold_stats, fold_count = 1L,
                                    expect_parse_success = NULL) {
  fs <- as.list(fold_stats)
  expect_true(is.list(fs))
  expect_equal(as.integer(fs$fold_count), as.integer(fold_count))
  if (as.integer(fold_count) >= 1L) {
    expect_true(as.integer(fs$fold_messages_removed) > 0L)
    expect_true(as.integer(fs$fold_total_messages_removed) > 0L)
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
    expect_true(as.numeric(fs$fold_summarizer_latency_m) >= 0)
  }
  if (!is.null(expect_parse_success)) {
    if (isTRUE(expect_parse_success)) {
      expect_true(isTRUE(fs$fold_parse_success))
    } else {
      expect_false(isTRUE(fs$fold_parse_success))
    }
  }
  invisible(fs)
}

asa_test_invoke_json_agent <- function(agent,
                                       expected_schema = NULL,
                                       prompt = "Return JSON only.",
                                       input_state = NULL,
                                       config = list(recursion_limit = as.integer(3)),
                                       expected_schema_source = NULL,
                                       backend = "gemini") {
  state <- if (is.null(input_state)) {
    list(messages = list(list(role = "user", content = prompt)))
  } else {
    input_state
  }

  if (!is.null(expected_schema)) {
    state$expected_schema <- expected_schema
  }
  if (!is.null(expected_schema_source) && is.null(state$expected_schema_source)) {
    state$expected_schema_source <- expected_schema_source
  }

  final_state <- agent$invoke(state, config = config)
  parsed_payload <- asa_test_parse_agent_json(final_state, backend = backend)

  list(
    final_state = final_state,
    response_text = parsed_payload$response_text,
    parsed = parsed_payload$parsed
  )
}
