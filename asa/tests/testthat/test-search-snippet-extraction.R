# test-search-snippet-extraction.R

test_that("search snippet extraction produces structured payloads promotable into field_status", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  reticulate::py_run_string(paste0(
    "import json\n",
    "class _ASASnippetStubModel:\n",
    "    def __init__(self, payload):\n",
    "        self._payload = payload\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        class _Resp:\n",
    "            def __init__(self, content):\n",
    "                self.content = content\n",
    "        return _Resp(json.dumps(self._payload))\n",
    "_asa_stub_model = _ASASnippetStubModel({'birth_place': 'London'})\n"
  ))

  schema <- list(
    birth_place = "string|Unknown",
    birth_place_source = "string|null"
  )

  field_status <- list(
    birth_place = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    birth_place_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )

  tool_text <- paste0(
    "__START_OF_SOURCE 1__ <CONTENT> Ada Lovelace was born in London. </CONTENT> ",
    "<URL> https://www.example.gov/profile </URL> __END_OF_SOURCE 1__"
  )
  tool_messages <- list(list(role = "tool", name = "Search", content = tool_text))

  snippet_result <- core$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = reticulate::py$`_asa_stub_model`,
    expected_schema = schema,
    field_status = field_status,
    tool_messages = tool_messages,
    entity_tokens = list("Ada", "Lovelace"),
    extracted_urls = list(),
    max_sources = 1L,
    max_chars = 800L,
    extraction_engine = "legacy",
    debug = FALSE
  )

  snippet_r <- reticulate::py_to_r(snippet_result)
  payloads <- snippet_r[[1]]
  urls <- snippet_r[[2]]
  meta <- snippet_r[[3]]

  expect_true(is.list(payloads))
  expect_true(length(payloads) >= 1L)
  expect_true(is.character(urls))
  expect_true(length(urls) >= 1L)
  expect_true(is.list(meta))

  updates <- core$`_extract_field_status_updates`(
    existing_field_status = field_status,
    expected_schema = schema,
    tool_messages = tool_messages,
    extra_payloads = payloads,
    tool_calls_delta = 1L,
    unknown_after_searches = 3L,
    entity_name_tokens = list("Ada", "Lovelace"),
    evidence_enabled = TRUE
  )
  updates_r <- reticulate::py_to_r(updates)
  updated_fs <- updates_r[[1]]

  expect_equal(as.character(updated_fs$birth_place$status), "found")
  expect_equal(as.character(updated_fs$birth_place$value), "London")
  expect_match(as.character(updated_fs$birth_place$source_url), "example\\.gov", perl = TRUE)
  expect_equal(as.character(updated_fs$birth_place_source$status), "found")
})

test_that("default snippet engine resolves deterministic facts without selector fallback", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  reticulate::py_run_string(paste0(
    "import json\n",
    "_asa_default_snippet_selector_calls = 0\n",
    "class _ASADefaultSnippetModel:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        global _asa_default_snippet_selector_calls\n",
    "        _asa_default_snippet_selector_calls += 1\n",
    "        class _Resp:\n",
    "            def __init__(self, content):\n",
    "                self.content = content\n",
    "        return _Resp(json.dumps({'birth_year': 1970}))\n",
    "_asa_default_snippet_model = _ASADefaultSnippetModel()\n"
  ))

  schema <- list(
    birth_year = "integer|null|Unknown",
    birth_year_source = "string|null"
  )
  field_status <- list(
    birth_year = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    birth_year_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )
  tool_messages <- list(list(
    role = "tool",
    name = "Search",
    content = paste0(
      "__START_OF_SOURCE 1__ <CONTENT> Ramona Moye Camaconi 1982-01-07 legislative profile. </CONTENT> ",
      "<URL> https://www.example.gov/legislators/date-of-birth/428 </URL> __END_OF_SOURCE 1__"
    )
  ))

  snippet_result <- core$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = reticulate::py$`_asa_default_snippet_model`,
    expected_schema = schema,
    field_status = field_status,
    tool_messages = tool_messages,
    entity_tokens = list("Ramona", "Moye", "Camaconi"),
    extracted_urls = list(),
    max_sources = 1L,
    max_chars = 800L,
    extraction_engine = "deterministic_then_legacy",
    debug = FALSE
  )

  snippet_r <- reticulate::py_to_r(snippet_result)
  payloads <- snippet_r[[1]]
  urls <- snippet_r[[2]]
  meta <- snippet_r[[3]]

  expect_equal(length(payloads), 1L)
  expect_equal(as.integer(payloads[[1]]$payload$birth_year), 1982L)
  expect_match(as.character(payloads[[1]]$payload$birth_year_source), "date-of-birth/428", perl = TRUE)
  expect_equal(as.character(urls[[1]]), "https://www.example.gov/legislators/date-of-birth/428")
  expect_equal(as.integer(meta$selector_model_calls), 0L)
  expect_equal(as.integer(meta$langextract_calls), 0L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$`_asa_default_snippet_selector_calls`)), 0L)
})

test_that("default snippet engine falls back to selector model only after deterministic miss", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  reticulate::py_run_string(paste0(
    "import json\n",
    "_asa_fallback_snippet_selector_calls = 0\n",
    "class _ASAFallbackSnippetModel:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        global _asa_fallback_snippet_selector_calls\n",
    "        _asa_fallback_snippet_selector_calls += 1\n",
    "        class _Resp:\n",
    "            def __init__(self, content):\n",
    "                self.content = content\n",
    "        return _Resp(json.dumps({'prior_occupation': 'lawyer'}))\n",
    "_asa_fallback_snippet_model = _ASAFallbackSnippetModel()\n"
  ))

  schema <- list(
    prior_occupation = "string|Unknown",
    prior_occupation_source = "string|null"
  )
  field_status <- list(
    prior_occupation = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    prior_occupation_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )
  tool_messages <- list(list(
    role = "tool",
    name = "Search",
    content = paste0(
      "__START_OF_SOURCE 1__ <CONTENT> Ada Lovelace official profile overview. </CONTENT> ",
      "<URL> https://www.example.gov/profile </URL> __END_OF_SOURCE 1__"
    )
  ))

  snippet_result <- core$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = reticulate::py$`_asa_fallback_snippet_model`,
    expected_schema = schema,
    field_status = field_status,
    tool_messages = tool_messages,
    entity_tokens = list("Ada", "Lovelace"),
    extracted_urls = list(),
    max_sources = 1L,
    max_chars = 800L,
    extraction_engine = "deterministic_then_legacy",
    debug = FALSE
  )

  snippet_r <- reticulate::py_to_r(snippet_result)
  payloads <- snippet_r[[1]]
  meta <- snippet_r[[3]]

  expect_equal(length(payloads), 1L)
  expect_equal(as.character(payloads[[1]]$payload$prior_occupation), "lawyer")
  expect_match(
    as.character(payloads[[1]]$payload$prior_occupation_source),
    "example\\.gov",
    perl = TRUE
  )
  expect_equal(as.integer(meta$deterministic_fallback_calls), 1L)
  expect_equal(as.integer(meta$selector_model_calls), 1L)
  expect_equal(as.integer(meta$langextract_calls), 0L)
  expect_equal(as.integer(meta$snippet_candidates_attempted), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$`_asa_fallback_snippet_selector_calls`)), 1L)
})

test_that("search snippet field hints prioritize enriched birth-year snippets", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  reticulate::py_run_string(paste0(
    "import json\n",
    "class _ASAEmptySnippetModel:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        class _Resp:\n",
    "            def __init__(self, content):\n",
    "                self.content = content\n",
    "        return _Resp('{}')\n",
    "_asa_empty_snippet_model = _ASAEmptySnippetModel()\n"
  ))

  schema <- list(
    birth_year = "integer|null|Unknown",
    birth_year_source = "string|null"
  )

  field_status <- list(
    birth_year = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    birth_year_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )

  tool_text <- paste0(
    "__START_OF_SOURCE 1__ <CONTENT> Ramona Moye Camaconi legislative profile and committee summary. </CONTENT> ",
    "<URL> https://www.example.gov/legislators/profile/overview </URL> __END_OF_SOURCE 1__ ",
    "__START_OF_SOURCE 2__ <CONTENT> Ramona Moye Camaconi 1982-01-07 legislative profile. </CONTENT> ",
    "<URL> https://www.example.gov/legislators/date-of-birth/428 </URL> __END_OF_SOURCE 2__"
  )
  tool_messages <- list(list(role = "tool", name = "Search", content = tool_text))

  snippet_result <- core$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = reticulate::py$`_asa_empty_snippet_model`,
    expected_schema = schema,
    field_status = field_status,
    tool_messages = tool_messages,
    entity_tokens = list("Ramona", "Moye", "Camaconi"),
    extracted_urls = list(),
    max_sources = 1L,
    max_chars = 800L,
    extraction_engine = "legacy",
    debug = FALSE
  )

  snippet_r <- reticulate::py_to_r(snippet_result)
  payloads <- snippet_r[[1]]
  urls <- snippet_r[[2]]
  meta <- snippet_r[[3]]

  expect_equal(length(payloads), 1L)
  expect_equal(as.integer(payloads[[1]]$payload$birth_year), 1982L)
  expect_match(
    as.character(payloads[[1]]$payload$birth_year_source),
    "date-of-birth/428",
    perl = TRUE
  )
  expect_equal(as.character(urls[[1]]), "https://www.example.gov/legislators/date-of-birth/428")
  expect_equal(as.integer(meta$snippet_candidates_total), 2L)
  expect_equal(as.integer(meta$snippet_candidates_selected), 1L)
  expect_equal(as.integer(meta$snippet_candidates_selected_with_field_hints), 1L)
  expect_equal(as.integer(meta$snippet_openwebpage_escalation_events), 0L)
})

test_that("search snippet langextract no_payload is classified as fallback, not provider error", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  original_bridge <- core$`_langextract_extract_schema_with_fallback`
  on.exit(
    reticulate::py_set_attr(core, "_langextract_extract_schema_with_fallback", original_bridge),
    add = TRUE
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "def _asa_langextract_no_payload(**kwargs):\n",
    "    return {'ok': False, 'payload': {}, 'provider': 'gemini', 'model_id': 'gemini-2.5-flash', 'error': 'no_payload'}\n\n",
    "class _ASAEmptySnippetFallbackModel:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        return AIMessage(content='{}')\n\n",
    "_asa_langextract_no_payload = _asa_langextract_no_payload\n",
    "_asa_empty_snippet_fallback_model = _ASAEmptySnippetFallbackModel()\n"
  ))
  reticulate::py_set_attr(
    core,
    "_langextract_extract_schema_with_fallback",
    reticulate::py$`_asa_langextract_no_payload`
  )

  schema <- list(
    birth_place = "string|Unknown",
    birth_place_source = "string|null"
  )
  field_status <- list(
    birth_place = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    birth_place_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )
  tool_messages <- list(list(
    role = "tool",
    name = "Search",
    content = paste0(
      "__START_OF_SOURCE 1__ <CONTENT> Ada Lovelace was born in London. </CONTENT> ",
      "<URL> https://www.example.gov/profile </URL> __END_OF_SOURCE 1__"
    )
  ))

  snippet_result <- core$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = reticulate::py$`_asa_empty_snippet_fallback_model`,
    expected_schema = schema,
    field_status = field_status,
    tool_messages = tool_messages,
    entity_tokens = list("Ada", "Lovelace"),
    extracted_urls = list(),
    max_sources = 1L,
    max_chars = 800L,
    extraction_engine = "langextract",
    debug = FALSE
  )

  meta <- reticulate::py_to_r(snippet_result[[3]])
  expect_equal(as.integer(meta$langextract_calls), 1L)
  expect_equal(as.integer(meta$langextract_fallback_calls), 1L)
  expect_equal(as.integer(meta$langextract_errors), 0L)
  expect_equal(as.integer(meta$selector_model_calls), 0L)
  expect_identical(as.character(meta$langextract_last_error), "")
  expect_true(as.integer(meta$deterministic_fallback_calls) >= 1L)
})

test_that("openwebpage langextract provider failures remain classified as provider errors", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  original_bridge <- core$`_langextract_extract_schema_with_fallback`
  on.exit(
    reticulate::py_set_attr(core, "_langextract_extract_schema_with_fallback", original_bridge),
    add = TRUE
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n\n",
    "def _asa_langextract_timeout(**kwargs):\n",
    "    return {'ok': False, 'payload': {}, 'provider': 'gemini', 'model_id': 'gemini-2.5-flash', 'error': 'langextract_timeout:18.0s'}\n\n",
    "class _ASAEmptyOpenWebpageFallbackModel:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        return AIMessage(content='{}')\n\n",
    "_asa_langextract_timeout = _asa_langextract_timeout\n",
    "_asa_empty_openwebpage_fallback_model = _ASAEmptyOpenWebpageFallbackModel()\n"
  ))
  reticulate::py_set_attr(
    core,
    "_langextract_extract_schema_with_fallback",
    reticulate::py$`_asa_langextract_timeout`
  )

  schema <- list(
    birth_place = "string|Unknown",
    birth_place_source = "string|null"
  )
  field_status <- list(
    birth_place = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    birth_place_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )
  tool_messages <- list(list(
    role = "tool",
    name = "OpenWebpage",
    content = paste(
      "URL: https://www.example.gov/profile",
      "Final URL: https://www.example.gov/profile",
      "Title: Example profile",
      "Relevant excerpts:",
      "Ada Lovelace was born in London.",
      sep = "\n"
    )
  ))

  openwebpage_result <- core$`_llm_extract_schema_payloads_from_openwebpages`(
    selector_model = reticulate::py$`_asa_empty_openwebpage_fallback_model`,
    expected_schema = schema,
    field_status = field_status,
    tool_messages = tool_messages,
    entity_tokens = list("Ada", "Lovelace"),
    extracted_urls = list(),
    max_pages = 1L,
    max_chars = 1200L,
    extraction_engine = "langextract",
    debug = FALSE
  )

  meta <- reticulate::py_to_r(openwebpage_result[[3]])
  expect_equal(as.integer(meta$langextract_calls), 1L)
  expect_equal(as.integer(meta$langextract_fallback_calls), 1L)
  expect_equal(as.integer(meta$langextract_errors), 1L)
  expect_match(as.character(meta$langextract_last_error), "langextract_timeout", fixed = TRUE)
})


test_that("deterministic numeric recovery requires a keyword hit when configured", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  schema <- list(
    birth_year = "integer|null|Unknown",
    birth_year_source = "string|null"
  )
  source_url <- "https://www.flickr.com/photos/194792615@N03/52180793916"
  source_text <- paste(
    "Ramona Moye Camaconi, diputada titular por Beni.",
    "Uploaded on June 29, 2022.",
    "Taken on June 29, 2022.",
    "13 views 0 faves 0 comments"
  )

  out <- core$`_recover_unknown_fields_from_tool_evidence`(
    field_status = list(),
    expected_schema = schema,
    finalization_policy = list(
      field_recovery_enabled = TRUE,
      field_recovery_mode = "balanced",
      field_recovery_require_keyword_hit_for_numbers = TRUE
    ),
    allowed_source_urls = list(source_url),
    source_text_index = stats::setNames(list(source_text), source_url),
    entity_name_tokens = list("Ramona", "Moye", "Camaconi")
  )

  out_r <- reticulate::py_to_r(out)
  expect_false(identical(as.character(out_r$birth_year$status), "found"))
  expect_false(identical(out_r$birth_year$value, 2022L))
})


test_that("deterministic recovery reports entity mismatch when candidates exist but are off-target", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  schema <- list(
    birth_year = "integer|null|Unknown",
    birth_year_source = "string|null"
  )
  source_url <- "https://www.example.gov/profile"
  source_text <- paste(
    "Uploaded on June 29, 2022.",
    "Taken on June 29, 2022.",
    "13 views 0 faves 0 comments.",
    "This text does not mention the target entity."
  )

  out <- core$`_recover_unknown_fields_from_tool_evidence`(
    field_status = list(),
    expected_schema = schema,
    finalization_policy = list(
      field_recovery_enabled = TRUE,
      field_recovery_mode = "balanced",
      # Allow numeric extraction so we can observe entity mismatch behavior.
      field_recovery_require_keyword_hit_for_numbers = FALSE
    ),
    allowed_source_urls = list(source_url),
    source_text_index = stats::setNames(list(source_text), source_url),
    entity_name_tokens = list("Ada", "Lovelace")
  )

  out_r <- reticulate::py_to_r(out)
  expect_equal(as.character(out_r$birth_year$evidence), "recovery_blocked_entity_mismatch")
  expect_equal(as.character(out_r$birth_year$evidence_reason), "recovery_blocked_entity_mismatch")
})

test_that("deterministic recovery blocks near-miss entity/value mismatches", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  schema <- list(
    birth_year = "integer|null|Unknown",
    birth_year_source = "string|null"
  )
  source_url <- "https://www.example.gov/profile/12345"
  source_text <- paste(
    "Ramona profile card.",
    "Date of birth: 1982-01-07.",
    "Legislative profile summary."
  )

  out <- core$`_recover_unknown_fields_from_tool_evidence`(
    field_status = list(),
    expected_schema = schema,
    finalization_policy = list(
      field_recovery_enabled = TRUE,
      field_recovery_mode = "balanced",
      field_recovery_require_keyword_hit_for_numbers = FALSE,
      field_recovery_entity_tolerance_enabled = TRUE,
      field_recovery_entity_tolerance_hit_slack = 1L,
      field_recovery_entity_tolerance_ratio_slack = 0.20,
      field_recovery_entity_tolerance_penalty = 0.10
    ),
    allowed_source_urls = list(source_url),
    source_text_index = stats::setNames(list(source_text), source_url),
    entity_name_tokens = list("Ramona", "Moye", "Camaconi")
  )

  out_r <- reticulate::py_to_r(out)
  expect_false(identical(as.character(out_r$birth_year$status), "found"))
  expect_equal(
    as.character(out_r$birth_year$evidence),
    "recovery_blocked_entity_value_mismatch"
  )
  expect_equal(
    as.character(out_r$birth_year$evidence_reason),
    "recovery_blocked_entity_value_mismatch"
  )
  expect_false(identical(as.character(out_r$birth_year_source$status), "found"))
})

test_that("field-status extraction rejects candidates with entity/value mismatch", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  schema <- list(
    prior_occupation = "string|Unknown",
    prior_occupation_source = "string|null"
  )
  field_status <- list(
    prior_occupation = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    prior_occupation_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )
  profile_url <- "https://example.gov/profile/ramona"
  separator <- paste(rep("archival snippet without target identity", 80), collapse = " ")
  source_text <- paste(
    "Ramona Moye Camaconi is a Bolivian legislator.",
    separator,
    "Juan Perez worked as a teacher before politics.",
    "Profile registry with mixed person snippets."
  )
  payload <- list(
    prior_occupation = "teacher",
    prior_occupation_source = profile_url
  )
  extra_payloads <- list(list(
    tool_name = "search_snippet_extract",
    text = source_text,
    payload = payload,
    source_blocks = list(),
    source_payloads = list(payload),
    has_structured_payload = TRUE,
    urls = list(profile_url)
  ))

  updates <- core$`_extract_field_status_updates`(
    existing_field_status = field_status,
    expected_schema = schema,
    tool_messages = list(),
    extra_payloads = extra_payloads,
    tool_calls_delta = 1L,
    unknown_after_searches = 3L,
    entity_name_tokens = list("Ramona", "Moye", "Camaconi"),
    evidence_enabled = TRUE
  )
  updates_r <- reticulate::py_to_r(updates)
  updated_fs <- updates_r[[1]]

  expect_false(identical(as.character(updated_fs$prior_occupation$status), "found"))
  expect_equal(
    as.character(updated_fs$prior_occupation$evidence),
    "grounding_blocked_entity_value_mismatch"
  )
})


# --- Context-aware anchoring tests ---

test_that("anchor confidence increases with context tokens from structured prompts", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  reticulate::py_run_string(paste0(
    "from langchain_core.messages import HumanMessage\n",
    "_asa_msg_name_only = [HumanMessage(content='",
      'Find info about \"Ramona Moye Camaconi\".',
    "')]\n",
    "_asa_msg_with_ctx = [HumanMessage(content='",
      'Find info about \"Ramona Moye Camaconi\".\\ncountry: Bolivia\\nparty: MAS\\nregion: Beni',
    "')]\n"
  ))

  anchor_name <- reticulate::py_to_r(
    core$`_task_target_anchor_from_messages`(reticulate::py$`_asa_msg_name_only`)
  )
  anchor_ctx <- reticulate::py_to_r(
    core$`_task_target_anchor_from_messages`(reticulate::py$`_asa_msg_with_ctx`)
  )

  # Context-rich prompt should have higher confidence.
  expect_true(anchor_ctx$confidence > anchor_name$confidence)
  # Context tokens should be populated.
  expect_true(length(anchor_ctx$context_tokens) > 0L)
  # Context labels should be populated (non-name hint lines).
  expect_true(length(anchor_ctx$context_labels) > 0L)
})


test_that("anchor overlap includes context_hits and context_ratio", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  anchor <- list(
    tokens = list("ramona", "moye", "camaconi"),
    phrases = list("ramona moye camaconi"),
    id_signals = list(),
    context_tokens = list("bolivia", "beni"),
    context_labels = list("country:Bolivia", "region:Beni"),
    confidence = 0.6,
    strength = "moderate",
    provenance = list("profile_hints"),
    mode = "adaptive"
  )

  overlap_match <- reticulate::py_to_r(core$`_anchor_overlap_for_candidate`(
    target_anchor = anchor,
    candidate_url = "https://example.bo/ramona-moye",
    candidate_text = "Ramona Moye Camaconi es diputada en Beni, Bolivia."
  ))

  expect_true(overlap_match$context_hits > 0L)
  expect_true(overlap_match$context_ratio > 0)

  overlap_no_ctx <- reticulate::py_to_r(core$`_anchor_overlap_for_candidate`(
    target_anchor = anchor,
    candidate_url = "https://example.com/generic",
    candidate_text = "Some unrelated content about politics."
  ))

  expect_equal(as.integer(overlap_no_ctx$context_hits), 0L)
})


test_that("context-contradiction hard blocks candidates from wrong country", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  anchor <- list(
    tokens = list("ramona"),
    phrases = list(),
    id_signals = list(),
    context_tokens = list("bolivia", "beni"),
    context_labels = list("country:Bolivia", "region:Beni"),
    confidence = 0.5,
    strength = "moderate",
    provenance = list("profile_hints"),
    mode = "adaptive"
  )

  # Candidate from Chile (contradicts Bolivia) with no name overlap
  mismatch <- reticulate::py_to_r(core$`_anchor_mismatch_state`(
    target_anchor = anchor,
    candidate_url = "https://www.example.cl/ramona-parra",
    candidate_text = "Ramona Parra fue una militante chilena."
  ))

  expect_true(mismatch$hard_block)
  expect_false(mismatch$pass)
  expect_equal(as.character(mismatch$reason), "anchor_context_contradiction_hard_block")
  expect_true(mismatch$context_contradiction)
})


test_that("context-contradiction does not fire when candidate matches anchor country", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  anchor <- list(
    tokens = list("ramona", "moye"),
    phrases = list("ramona moye"),
    id_signals = list(),
    context_tokens = list("bolivia"),
    context_labels = list("country:Bolivia"),
    confidence = 0.5,
    strength = "moderate",
    provenance = list("profile_hints"),
    mode = "adaptive"
  )

  # Candidate from Bolivia (matches anchor country)
  result <- reticulate::py_to_r(core$`_anchor_mismatch_state`(
    target_anchor = anchor,
    candidate_url = "https://www.example.bo/ramona-moye",
    candidate_text = "Ramona Moye es diputada de Bolivia."
  ))

  expect_false(result$context_contradiction)
  expect_true(result$pass)
})


test_that("soft penalty scales inversely with anchor strength when in soft mode", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  # Weak anchor (low confidence, soft mode)
  anchor_weak <- list(
    tokens = list("john"),
    phrases = list(),
    id_signals = list(),
    context_tokens = list(),
    context_labels = list(),
    confidence = 0.15,
    strength = "weak",
    provenance = list(),
    mode = "adaptive"
  )

  # Strong anchor (high confidence)
  anchor_strong <- list(
    tokens = list("john", "smith", "jones"),
    phrases = list("john smith jones"),
    id_signals = list(),
    context_tokens = list(),
    context_labels = list(),
    confidence = 0.85,
    strength = "strong",
    provenance = list(),
    mode = "adaptive"
  )

  # Both should produce mismatch (candidate text has no entity overlap)
  result_weak <- reticulate::py_to_r(core$`_anchor_mismatch_state`(
    target_anchor = anchor_weak,
    candidate_url = "https://example.com/page",
    candidate_text = "This page has no relevant names."
  ))

  result_strong <- reticulate::py_to_r(core$`_anchor_mismatch_state`(
    target_anchor = anchor_strong,
    candidate_url = "https://example.com/page",
    candidate_text = "This page has no relevant names."
  ))

  # Weak anchor should get higher penalty than strong anchor.
  expect_true(result_weak$penalty > result_strong$penalty)
})
