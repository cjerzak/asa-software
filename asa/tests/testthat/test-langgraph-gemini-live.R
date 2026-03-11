# Live Gemini API tests for LangGraph agent integration.
# Extracted from test-langgraph-remainingsteps.R for faster unit test runs.
# These tests require a valid GOOGLE_API_KEY / GEMINI_API_KEY and network access.

.asa_test_coerce_records_df <- function(x, required_names) {
  if (is.data.frame(x)) {
    return(x)
  }
  if (is.list(x) && length(x) > 0L) {
    return(tryCatch({
      if (!is.null(names(x)) && all(required_names %in% names(x))) {
        as.data.frame(x, stringsAsFactors = FALSE)
      } else if (all(vapply(x, is.list, logical(1)))) {
        do.call(
          rbind,
          lapply(x, function(entry) as.data.frame(entry, stringsAsFactors = FALSE))
        )
      } else {
        NULL
      }
    }, error = function(e) NULL))
  }
  NULL
}

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
  expect_true(isTRUE(asa_test_has_search_tool_activity(final_state$messages)))

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

  fixture_items <- asa_test_recursion_limit_fixture_items()
  items_df <- .asa_test_coerce_records_df(parsed$items, c("name", "birth_year", "field"))
  if (!is.null(items_df) && nrow(items_df) >= 1L) {
    expect_true(all(c("name", "birth_year", "field") %in% names(items_df)))
    matched_idx <- match(as.character(items_df$name), fixture_items$name)
    expect_true(all(!is.na(matched_idx)))
    expect_equal(as.integer(items_df$birth_year), fixture_items$birth_year[matched_idx])
    expect_equal(as.character(items_df$field), fixture_items$field[matched_idx])
  }
})

test_that("Gemini 3 Flash multi-step folding triggers folding and returns schema-valid output", {

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

  expected_step_records <- data.frame(
    step = 1:10,
    marker = c(
      "RUNE_ATLAS", "RUNE_BIRCH", "RUNE_CINDER", "RUNE_DRIFT", "RUNE_ECHO",
      "RUNE_FABLE", "RUNE_GLADE", "RUNE_HARBOR", "RUNE_IVORY", "RUNE_JUNIPER"
    ),
    checksum = c(103L, 211L, 307L, 401L, 509L, 613L, 709L, 811L, 907L, 1009L),
    stringsAsFactors = FALSE
  )
  expected_step_json <- jsonlite::toJSON(
    lapply(seq_len(nrow(expected_step_records)), function(i) {
      list(
        step = unname(expected_step_records$step[[i]]),
        marker = unname(expected_step_records$marker[[i]]),
        checksum = unname(expected_step_records$checksum[[i]])
      )
    }),
    auto_unbox = TRUE
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.tools import Tool\n",
    "import json\n",
    "import re\n\n",
    "expected_step_records = ", expected_step_json, "\n",
    "multi_step_counter = {'n': 0}\n\n",
    "def _multi_step_search(query: str) -> str:\n",
    "    multi_step_counter['n'] += 1\n",
    "    m = re.search(r'step_(\\\\d+)', str(query))\n",
    "    step = int(m.group(1)) if m else int(multi_step_counter['n'])\n",
    "    idx = max(0, min(step - 1, len(expected_step_records) - 1))\n",
    "    payload = dict(expected_step_records[idx])\n",
    "    payload['summary_tag'] = f\"STEP_REC::{payload['step']}::{payload['marker']}::{payload['checksum']}\"\n",
    "    payload['context_blob'] = ('STEP_' + payload['marker'] + '_DETAIL_') * 80\n",
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
    "After each Search call, keep the exact step, marker, and checksum returned by the tool.\n",
    "After completing all 10 searches, output STRICT JSON only with this schema:\n",
    "{\n",
    "  \"status\": \"complete\"|\"partial\",\n",
    "  \"steps\": [{\"step\": integer, \"marker\": string, \"checksum\": integer}],\n",
    "  \"missing\": [integer],\n",
    "  \"notes\": string\n",
    "}\n",
    "Rules:\n",
    "- Include one entry for each completed step.\n",
    "- Copy marker and checksum exactly from Search results.\n",
    "- If any step is missing, set status=\"partial\" and list missing step numbers.\n",
    "- Do not infer or synthesize values that were not returned by Search.\n",
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

  # Live models are non-deterministic; assert useful activity happened.
  expect_true(as.integer(reticulate::py$multi_step_counter[["n"]]) >= 1L)
  expect_true(is.list(final_state$fold_stats))
  expect_true(as.integer(as.list(final_state$fold_stats)$fold_count) >= 1L)
  expect_true(is.list(final_state$archive))
  expect_true(length(final_state$archive) >= 1L)

  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  expect_true(is.character(response_text) && nzchar(response_text))

  parsed <- jsonlite::fromJSON(response_text)
  expect_true(is.list(parsed))
  expect_true(all(c("status", "steps", "missing", "notes") %in% names(parsed)))
  parsed_status <- tolower(as.character(parsed$status %||% ""))
  parsed_status <- parsed_status[!is.na(parsed_status) & nzchar(parsed_status)]
  parsed_status <- if (length(parsed_status) > 0L) parsed_status[[1]] else ""
  expect_true(parsed_status %in% c("complete", "partial"))

  steps_df <- .asa_test_coerce_records_df(parsed$steps, c("step", "marker", "checksum"))

  # Live provider behavior varies; if no usable rows are returned, require
  # only schema-level validity.
  if (is.null(steps_df) || !all(c("step", "marker", "checksum") %in% names(steps_df)) || nrow(steps_df) < 1L) {
    return(invisible(NULL))
  }

  step_int <- suppressWarnings(as.integer(steps_df$step))
  checksum_int <- suppressWarnings(as.integer(steps_df$checksum))
  marker_chr <- as.character(steps_df$marker)

  valid <- which(!is.na(step_int) & !is.na(checksum_int) & !is.na(marker_chr))
  if (length(valid) == 0L) {
    return(invisible(NULL))
  }

  expect_true(any(step_int[valid] >= 1L & step_int[valid] <= 10L))
  matched_idx <- match(step_int[valid], expected_step_records$step)
  expect_true(all(!is.na(matched_idx)))
  expect_equal(marker_chr[valid], expected_step_records$marker[matched_idx])
  expect_equal(checksum_int[valid], expected_step_records$checksum[matched_idx])
})
