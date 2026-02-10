# Live Gemini API tests for LangGraph agent integration.
# Extracted from test-langgraph-remainingsteps.R for faster unit test runs.
# These tests require a valid GOOGLE_API_KEY / GEMINI_API_KEY and network access.

test_that("standard agent reaches recursion_limit and preserves JSON output (Gemini, best-effort)", {

  asa_test_skip_api_tests()
  api_key <- asa_test_require_gemini_key()

  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production",
    required_modules = c(ASA_TEST_LANGGRAPH_MODULES, "langchain_google_genai"))
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

test_that("Gemini 3 Flash multi-step folding preserves semantic correctness across 10 tool steps", {

  asa_test_skip_api_tests()
  api_key <- asa_test_require_gemini_key()

  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production",
    required_modules = c(ASA_TEST_LANGGRAPH_MODULES, "langchain_google_genai"))
  chat_models <- reticulate::import("langchain_google_genai")

  gemini_model <- Sys.getenv("ASA_TEST_GEMINI_MODEL", unset = "")
  if (!nzchar(gemini_model)) {
    gemini_model <- "gemini-3-flash-preview"
  }

  llm <- chat_models$ChatGoogleGenerativeAI(
    model = gemini_model,
    temperature = 0,
    api_key = api_key
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "import json\n",
    "import re\n\n",
    "multi_step_counter = {'n': 0}\n\n",
    "def _multi_step_search(query: str) -> str:\n",
    "    multi_step_counter['n'] += 1\n",
    "    m = re.search(r'step_(\\\\d+)', str(query))\n",
    "    step = int(m.group(1)) if m else int(multi_step_counter['n'])\n",
    "    payload = {\n",
    "        'step': step,\n",
    "        'fact': f'fact_{step}',\n",
    "        'checksum': step * 11,\n",
    "        'context_blob': ('STEP_' + str(step) + '_DETAIL_') * 80\n",
    "    }\n",
    "    return json.dumps(payload)\n\n",
    "multi_step_search_tool = Tool(\n",
    "    name='Search',\n",
    "    description='Deterministic Search tool for 10-step memory-folding semantic tests.',\n",
    "    func=_multi_step_search,\n",
    ")\n"
  ))

  agent <- custom_ddg$create_memory_folding_agent(
    model = llm,
    tools = list(reticulate::py$multi_step_search_tool),
    checkpointer = NULL,
    message_threshold = as.integer(4),
    keep_recent = as.integer(1),
    fold_char_budget = as.integer(2000),
    debug = FALSE
  )

  prompt <- paste0(
    "You MUST execute a 10-step process and use the Search tool at each step.\n",
    "Step plan:\n",
    "1) Call Search with query 'step_1'.\n",
    "2) Call Search with query 'step_2'.\n",
    "3) Call Search with query 'step_3'.\n",
    "4) Call Search with query 'step_4'.\n",
    "5) Call Search with query 'step_5'.\n",
    "6) Call Search with query 'step_6'.\n",
    "7) Call Search with query 'step_7'.\n",
    "8) Call Search with query 'step_8'.\n",
    "9) Call Search with query 'step_9'.\n",
    "10) Call Search with query 'step_10'.\n",
    "After completing all 10 searches, output STRICT JSON only with this schema:\n",
    "{\n",
    "  \"status\": \"complete\"|\"partial\",\n",
    "  \"steps\": [{\"step\": integer, \"fact\": string, \"checksum\": integer}],\n",
    "  \"missing\": [integer],\n",
    "  \"notes\": string\n",
    "}\n",
    "Rules:\n",
    "- Include one entry for each completed step.\n",
    "- For each step k, fact must be \"fact_k\" and checksum must be k*11.\n",
    "- If any step is missing, set status=\"partial\" and list missing step numbers.\n",
    "- Do not include markdown."
  )

  final_state <- agent$invoke(
    list(
      messages = list(list(role = "user", content = prompt)),
      summary = "",
      archive = list(),
      fold_stats = reticulate::dict(fold_count = 0L)
    ),
    config = list(
      recursion_limit = as.integer(80),
      configurable = list(thread_id = "test_gemini_multistep_memory_folding_semantics")
    )
  )

  expect_true(as.integer(reticulate::py$multi_step_counter[["n"]]) >= 10L)
  expect_true(is.list(final_state$fold_stats))
  expect_true(as.integer(as.list(final_state$fold_stats)$fold_count) >= 1L)
  expect_true(is.list(final_state$archive))
  expect_true(length(final_state$archive) >= 1L)

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  expect_true(is.character(response_text) && nzchar(response_text))

  parsed <- jsonlite::fromJSON(response_text)
  expect_true(is.list(parsed))
  expect_true(all(c("status", "steps", "missing", "notes") %in% names(parsed)))

  steps_df <- parsed$steps
  expect_true(is.data.frame(steps_df))
  expect_true(all(c("step", "fact", "checksum") %in% names(steps_df)))
  expect_true(all(1:10 %in% as.integer(steps_df$step)))

  step_1 <- steps_df[steps_df$step == 1, , drop = FALSE]
  step_10 <- steps_df[steps_df$step == 10, , drop = FALSE]
  expect_true(nrow(step_1) >= 1L)
  expect_true(nrow(step_10) >= 1L)
  expect_equal(as.character(step_1$fact[[1]]), "fact_1")
  expect_equal(as.integer(step_1$checksum[[1]]), 11L)
  expect_equal(as.character(step_10$fact[[1]]), "fact_10")
  expect_equal(as.integer(step_10$checksum[[1]]), 110L)
})
