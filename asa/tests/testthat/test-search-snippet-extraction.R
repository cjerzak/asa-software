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

test_that("triage_then_structured skips low-information snippets and carries webpage follow-up", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  original_supports <- core$`_search_snippet_supports_direct_extraction`
  on.exit(
    reticulate::py_set_attr(core, "_search_snippet_supports_direct_extraction", original_supports),
    add = TRUE
  )

  reticulate::py_run_string(paste0(
    "def _asa_force_low_information(*args, **kwargs):\n",
    "    return (False, 'low_information')\n",
    "\n",
    "_asa_triage_skip_with_calls = 0\n",
    "_asa_triage_skip_legacy_calls = 0\n",
    "class _ASATriageSkipInvoker:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        return {'prior_occupation': 'mathematician'}\n",
    "class _ASATriageSkipModel:\n",
    "    def with_structured_output(self, *args, **kwargs):\n",
    "        global _asa_triage_skip_with_calls\n",
    "        _asa_triage_skip_with_calls += 1\n",
    "        return _ASATriageSkipInvoker()\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        global _asa_triage_skip_legacy_calls\n",
    "        _asa_triage_skip_legacy_calls += 1\n",
    "        class _Resp:\n",
    "            def __init__(self, content):\n",
    "                self.content = content\n",
    "        return _Resp('{}')\n",
    "_asa_triage_skip_model = _ASATriageSkipModel()\n"
  ))
  reticulate::py_set_attr(
    core,
    "_search_snippet_supports_direct_extraction",
    reticulate::py$`_asa_force_low_information`
  )

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
      "__START_OF_SOURCE 1__ <CONTENT> General biography overview. </CONTENT> ",
      "<URL> https://profiles.example.net/ada </URL> __END_OF_SOURCE 1__"
    )
  ))

  snippet_result <- core$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = reticulate::py$`_asa_triage_skip_model`,
    expected_schema = schema,
    field_status = field_status,
    tool_messages = tool_messages,
    entity_tokens = list("Ada", "Lovelace"),
    extracted_urls = list(),
    max_sources = 1L,
    max_chars = 800L,
    extraction_engine = "triage_then_structured",
    debug = FALSE
  )

  snippet_r <- reticulate::py_to_r(snippet_result)
  payloads <- snippet_r[[1]]
  urls <- snippet_r[[2]]
  meta <- snippet_r[[3]]

  expect_equal(length(payloads), 0L)
  expect_equal(length(urls), 0L)
  expect_equal(as.integer(meta$skipped_low_information), 1L)
  expect_equal(as.integer(meta$structured_output_calls), 0L)
  expect_equal(as.integer(meta$selector_model_calls), 0L)
  expect_equal(as.integer(meta$snippet_candidates_attempted), 0L)
  expect_equal(as.integer(meta$snippet_extraction_attempts), 0L)
  expect_equal(as.character(meta$preferred_openwebpage_url), "https://profiles.example.net/ada")
  expect_equal(as.character(meta$preferred_openwebpage_urls[[1]]), "https://profiles.example.net/ada")
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$`_asa_triage_skip_with_calls`)), 0L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$`_asa_triage_skip_legacy_calls`)), 0L)
})

test_that("entity overlap alone does not permit direct structured snippet extraction", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  out <- core$`_search_snippet_supports_direct_extraction`(
    candidate = list(
      url = "https://profiles.example.net/ada",
      source_tier = 2L,
      url_score = 0.70,
      authority_score = 0.60,
      entity_ratio = 0.95,
      field_hint_score = 0,
      query_intent_quoted_phrase_hits = 0L,
      extraction_text = "Ada Lovelace biography overview."
    ),
    expected_schema = list(
      prior_occupation = "string|Unknown",
      prior_occupation_source = "string|null"
    ),
    field_status = list(
      prior_occupation = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
      prior_occupation_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
    )
  )

  out_r <- reticulate::py_to_r(out)
  expect_false(isTRUE(out_r[[1]]))
  expect_equal(as.character(out_r[[2]]), "low_information")
})

test_that("date-only snippets do not count as direct extraction evidence for non-date fields", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  out <- core$`_search_snippet_supports_direct_extraction`(
    candidate = list(
      url = "https://profiles.example.net/ada",
      source_tier = 2L,
      url_score = 0.70,
      authority_score = 0.60,
      entity_ratio = 0.95,
      field_hint_score = 0,
      query_intent_quoted_phrase_hits = 0L,
      extraction_text = "Ada Lovelace 1815-12-10 biography overview."
    ),
    expected_schema = list(
      prior_occupation = "string|Unknown",
      prior_occupation_source = "string|null"
    ),
    field_status = list(
      prior_occupation = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
      prior_occupation_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
    )
  )

  out_r <- reticulate::py_to_r(out)
  expect_false(isTRUE(out_r[[1]]))
  expect_equal(as.character(out_r[[2]]), "low_information")
})

test_that("structured date evidence still permits direct extraction when date-like fields remain unresolved", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  out <- core$`_search_snippet_supports_direct_extraction`(
    candidate = list(
      url = "https://www.example.gov/profile/ada-lovelace",
      source_tier = 3L,
      url_score = 0.92,
      authority_score = 0.91,
      entity_ratio = 0.62,
      field_hint_score = 0,
      query_intent_quoted_phrase_hits = 0L,
      extraction_text = "Ada Lovelace was born in London on December 10, 1815. Occupation: mathematician."
    ),
    expected_schema = list(
      birth_year = "integer|null|Unknown",
      birth_year_source = "string|null",
      prior_occupation = "string|Unknown",
      prior_occupation_source = "string|null"
    ),
    field_status = list(
      birth_year = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
      birth_year_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L),
      prior_occupation = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
      prior_occupation_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
    )
  )

  out_r <- reticulate::py_to_r(out)
  expect_true(isTRUE(out_r[[1]]))
  expect_equal(as.character(out_r[[2]]), "eligible")
})

test_that("snippet escalation gate rejects weak generic candidates", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(ASA_TEST_LANGGRAPH_MODULES)
  reticulate::py_run_string("import sys\nsys.modules.pop('asa_backend.graph.agent_graph_core', None)\n")
  core <- reticulate::import_from_path(
    "asa_backend.graph.agent_graph_core",
    path = python_path
  )

  allowed <- core$`_search_snippet_candidate_supports_openwebpage_escalation`(
    candidate = list(
      url = "https://prezi.com/p/84pdg0dsextr/una-biografia",
      source_tier = 1L,
      url_score = 0.20,
      authority_score = 0.10,
      entity_ratio = 0.10,
      field_hint_score = 0,
      query_intent_quoted_phrase_hits = 0L,
      openwebpage_followup_score = 0.80
    ),
    webpage_policy = list(
      enabled = TRUE,
      open_only_if_score_ge = 0.40
    )
  )

  expect_false(isTRUE(reticulate::py_to_r(allowed)))
})

test_that("snippet escalation gate keeps strong official candidates above threshold", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(ASA_TEST_LANGGRAPH_MODULES)
  reticulate::py_run_string("import sys\nsys.modules.pop('asa_backend.graph.agent_graph_core', None)\n")
  core <- reticulate::import_from_path(
    "asa_backend.graph.agent_graph_core",
    path = python_path
  )

  allowed <- core$`_search_snippet_candidate_supports_openwebpage_escalation`(
    candidate = list(
      url = "https://www.example.gov/profile/ada-lovelace",
      source_tier = 3L,
      url_score = 0.75,
      authority_score = 0.60,
      entity_ratio = 0.45,
      field_hint_score = 1,
      query_intent_quoted_phrase_hits = 1L,
      openwebpage_followup_score = 0.65
    ),
    webpage_policy = list(
      enabled = TRUE,
      open_only_if_score_ge = 0.40
    )
  )
  blocked <- core$`_search_snippet_candidate_supports_openwebpage_escalation`(
    candidate = list(
      url = "https://www.example.gov/profile/ada-lovelace",
      source_tier = 3L,
      url_score = 0.75,
      authority_score = 0.60,
      entity_ratio = 0.45,
      field_hint_score = 1,
      query_intent_quoted_phrase_hits = 1L,
      openwebpage_followup_score = 0.30
    ),
    webpage_policy = list(
      enabled = TRUE,
      open_only_if_score_ge = 0.40
    )
  )

  expect_true(isTRUE(reticulate::py_to_r(allowed)))
  expect_false(isTRUE(reticulate::py_to_r(blocked)))
})

test_that("snippet escalation gate admits sparse official profile URLs via authority lane", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(ASA_TEST_LANGGRAPH_MODULES)
  reticulate::py_run_string("import sys\nsys.modules.pop('asa_backend.graph.agent_graph_core', None)\n")
  core <- reticulate::import_from_path(
    "asa_backend.graph.agent_graph_core",
    path = python_path
  )

  allowed <- core$`_search_snippet_candidate_supports_openwebpage_escalation`(
    candidate = list(
      url = "https://official.example/parliamentarian?id=428",
      source_tier = 3L,
      url_score = 0.80,
      authority_score = 0.98,
      preferred_domain_score = 1,
      entity_ratio = 0,
      field_hint_score = 0,
      query_intent_quoted_phrase_hits = 0L,
      openwebpage_followup_score = 0.72
    ),
    webpage_policy = list(
      enabled = TRUE,
      open_only_if_score_ge = 0.40
    )
  )

  expect_true(isTRUE(reticulate::py_to_r(allowed)))
})

test_that("snippet escalation gate rejects generic authority docs without target support", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(ASA_TEST_LANGGRAPH_MODULES)
  reticulate::py_run_string("import sys\nsys.modules.pop('asa_backend.graph.agent_graph_core', None)\n")
  core <- reticulate::import_from_path(
    "asa_backend.graph.agent_graph_core",
    path = python_path
  )

  blocked <- core$`_search_snippet_candidate_supports_openwebpage_escalation`(
    candidate = list(
      url = "https://www.electoral.gob.ar/nuevo/paginas/btn/tyf_f003.php",
      source_tier = 3L,
      url_score = 0.95,
      authority_score = 0.98,
      detail_score = 0.60,
      preferred_domain_score = 0,
      entity_ratio = 0,
      field_hint_score = 0,
      query_intent_quoted_phrase_hits = 2L,
      openwebpage_followup_score = 0.90
    ),
    webpage_policy = list(
      enabled = TRUE,
      open_only_if_score_ge = 0.40
    )
  )

  expect_false(isTRUE(reticulate::py_to_r(blocked)))
})

test_that("search snippet field hints ignore publication dates without matching keywords", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  score <- core$`_search_snippet_field_hint_score`(
    snippet_text = paste(
      "Registro Civil de Camarzana de Tera.",
      "December 30, 2023.",
      "Contiene información crucial como el nombre completo y los nombres de los padres."
    ),
    field_hints = list(list(
      field = "birth_year",
      keywords = list("birth year", "year of birth", "born")
    ))
  )

  expect_equal(as.numeric(reticulate::py_to_r(score)), 0)
})

test_that("snippet escalation gate rejects field-hint-only snippets without target support", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/agent_graph_core.py",
    initialize = TRUE
  )
  asa_test_require_langgraph_stack(ASA_TEST_LANGGRAPH_MODULES)
  reticulate::py_run_string("import sys\nsys.modules.pop('asa_backend.graph.agent_graph_core', None)\n")
  core <- reticulate::import_from_path(
    "asa_backend.graph.agent_graph_core",
    path = python_path
  )

  candidate <- list(
    url = "https://www.regciv.es/registro-civil-de-camarzana-de-tera",
    source_tier = 1L,
    url_score = 0.90,
    authority_score = 0.50,
    entity_ratio = 0,
    field_hint_score = 0.85,
    query_intent_quoted_phrase_hits = 1L,
    openwebpage_followup_score = 1.09
  )

  allowed <- core$`_search_snippet_candidate_supports_openwebpage_escalation`(
    candidate = candidate,
    webpage_policy = list(
      enabled = TRUE,
      open_only_if_score_ge = 0.40
    )
  )
  relevant <- core$`_candidate_is_relevant_followup_target`(
    candidate = candidate,
    webpage_policy = list(
      enabled = TRUE,
      open_only_if_score_ge = 0.40
    )
  )

  expect_false(isTRUE(reticulate::py_to_r(allowed)))
  expect_false(isTRUE(reticulate::py_to_r(relevant)))
})

test_that("triage_then_structured uses structured output after deterministic miss", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  original_supports <- core$`_search_snippet_supports_direct_extraction`
  on.exit(
    reticulate::py_set_attr(core, "_search_snippet_supports_direct_extraction", original_supports),
    add = TRUE
  )

  reticulate::py_run_string(paste0(
    "def _asa_force_direct_extract(*args, **kwargs):\n",
    "    return (True, 'eligible')\n",
    "\n",
    "_asa_triage_structured_with_calls = 0\n",
    "_asa_triage_structured_invoke_calls = 0\n",
    "_asa_triage_structured_legacy_calls = 0\n",
    "class _ASATriageStructuredInvoker:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        global _asa_triage_structured_invoke_calls\n",
    "        _asa_triage_structured_invoke_calls += 1\n",
    "        return {'prior_occupation': 'mathematician'}\n",
    "class _ASATriageStructuredModel:\n",
    "    def with_structured_output(self, *args, **kwargs):\n",
    "        global _asa_triage_structured_with_calls\n",
    "        _asa_triage_structured_with_calls += 1\n",
    "        return _ASATriageStructuredInvoker()\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        global _asa_triage_structured_legacy_calls\n",
    "        _asa_triage_structured_legacy_calls += 1\n",
    "        class _Resp:\n",
    "            def __init__(self, content):\n",
    "                self.content = content\n",
    "        return _Resp('{}')\n",
    "_asa_triage_structured_model = _ASATriageStructuredModel()\n"
  ))
  reticulate::py_set_attr(
    core,
    "_search_snippet_supports_direct_extraction",
    reticulate::py$`_asa_force_direct_extract`
  )

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
      "__START_OF_SOURCE 1__ <CONTENT> Ada Lovelace official biography and profile. </CONTENT> ",
      "<URL> https://www.example.gov/profile/ada-lovelace </URL> __END_OF_SOURCE 1__"
    )
  ))

  snippet_result <- core$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = reticulate::py$`_asa_triage_structured_model`,
    expected_schema = schema,
    field_status = field_status,
    tool_messages = tool_messages,
    entity_tokens = list("Ada", "Lovelace"),
    extracted_urls = list(),
    max_sources = 1L,
    max_chars = 800L,
    extraction_engine = "triage_then_structured",
    debug = FALSE
  )

  snippet_r <- reticulate::py_to_r(snippet_result)
  payloads <- snippet_r[[1]]
  urls <- snippet_r[[2]]
  meta <- snippet_r[[3]]

  expect_equal(length(payloads), 1L)
  expect_equal(as.character(payloads[[1]]$payload$prior_occupation), "mathematician")
  expect_match(
    as.character(payloads[[1]]$payload$prior_occupation_source),
    "example\\.gov/profile/ada-lovelace",
    perl = TRUE
  )
  expect_equal(as.character(urls[[1]]), "https://www.example.gov/profile/ada-lovelace")
  expect_equal(as.integer(meta$deterministic_fallback_calls), 1L)
  expect_equal(as.integer(meta$structured_output_calls), 1L)
  expect_gte(as.integer(meta$structured_output_elapsed_ms_total), 0L)
  expect_gte(as.integer(meta$structured_output_elapsed_ms_max), 0L)
  expect_equal(as.integer(meta$selector_model_calls), 0L)
  expect_equal(as.integer(meta$snippet_candidates_attempted), 1L)
  expect_equal(as.integer(meta$snippet_extraction_attempts), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$`_asa_triage_structured_with_calls`)), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$`_asa_triage_structured_invoke_calls`)), 1L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$`_asa_triage_structured_legacy_calls`)), 0L)
})

test_that("openai structured helper passes a named schema instead of the bare inner schema", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  reticulate::py_run_string(paste0(
    "_asa_structured_schema_calls = []\n",
    "class _ASAStructuredSchemaInvoker:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        return {'birth_year': 1815}\n",
    "class _ASAStructuredSchemaModel:\n",
    "    def with_structured_output(self, *args, **kwargs):\n",
    "        schema = kwargs.get('schema', args[0] if args else None)\n",
    "        method = kwargs.get('method')\n",
    "        _asa_structured_schema_calls.append({'schema': schema, 'method': method})\n",
    "        return _ASAStructuredSchemaInvoker()\n",
    "_asa_structured_schema_model = _ASAStructuredSchemaModel()\n"
  ))

  out <- core$`_invoke_structured_selector_model_with_timeout`(
    model = reticulate::py$`_asa_structured_schema_model`,
    messages = list(
      list(role = "system", content = "Return a JSON object only."),
      list(role = "user", content = "{\"page_text\":\"Ada Lovelace was born in 1815.\"}")
    ),
    expected_schema = list(
      birth_year = "integer|null|Unknown",
      birth_year_source = "string|null"
    ),
    allowed_keys = list("birth_year", "birth_year_source"),
    page_url = "https://www.example.gov/profile/ada-lovelace",
    backend_hint = "openai"
  )

  out_r <- reticulate::py_to_r(out)
  calls <- reticulate::py_to_r(reticulate::py$`_asa_structured_schema_calls`)

  expect_true(isTRUE(out_r$ok))
  expect_equal(as.integer(out_r$payload$birth_year), 1815L)
  expect_equal(length(calls), 1L)
  expect_equal(as.character(calls[[1]]$method), "json_schema")
  expect_equal(as.character(calls[[1]]$schema$name), "asa_schema_extract")
  expect_true(is.list(calls[[1]]$schema$schema))
  expect_false("type" %in% names(calls[[1]]$schema))
})

test_that("triage_then_structured skips structured calls when snippet timeout circuit is open", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  original_supports <- core$`_search_snippet_supports_direct_extraction`
  on.exit(
    reticulate::py_set_attr(core, "_search_snippet_supports_direct_extraction", original_supports),
    add = TRUE
  )

  reticulate::py_run_string(paste0(
    "def _asa_force_direct_extract_circuit(*args, **kwargs):\n",
    "    return (True, 'eligible')\n",
    "\n",
    "_asa_triage_circuit_with_calls = 0\n",
    "_asa_triage_circuit_invoke_calls = 0\n",
    "class _ASATriageCircuitInvoker:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        global _asa_triage_circuit_invoke_calls\n",
    "        _asa_triage_circuit_invoke_calls += 1\n",
    "        return {'prior_occupation': 'mathematician'}\n",
    "class _ASATriageCircuitModel:\n",
    "    def with_structured_output(self, *args, **kwargs):\n",
    "        global _asa_triage_circuit_with_calls\n",
    "        _asa_triage_circuit_with_calls += 1\n",
    "        return _ASATriageCircuitInvoker()\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        raise RuntimeError('legacy path should not be used')\n",
    "_asa_triage_circuit_model = _ASATriageCircuitModel()\n"
  ))
  reticulate::py_set_attr(
    core,
    "_search_snippet_supports_direct_extraction",
    reticulate::py$`_asa_force_direct_extract_circuit`
  )

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
      "__START_OF_SOURCE 1__ <CONTENT> Ada Lovelace official biography and profile. </CONTENT> ",
      "<URL> https://www.example.gov/profile/ada-lovelace </URL> __END_OF_SOURCE 1__"
    )
  ))

  snippet_result <- core$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = reticulate::py$`_asa_triage_circuit_model`,
    expected_schema = schema,
    field_status = field_status,
    tool_messages = tool_messages,
    entity_tokens = list("Ada", "Lovelace"),
    extracted_urls = list(),
    max_sources = 1L,
    max_chars = 800L,
    extraction_engine = "triage_then_structured",
    structured_output_timeout_circuit_open = TRUE,
    debug = FALSE
  )

  snippet_r <- reticulate::py_to_r(snippet_result)
  payloads <- snippet_r[[1]]
  urls <- snippet_r[[2]]
  meta <- snippet_r[[3]]

  expect_equal(length(payloads), 0L)
  expect_equal(length(urls), 1L)
  expect_equal(as.integer(meta$structured_output_calls), 0L)
  expect_equal(as.integer(meta$deterministic_fallback_calls), 1L)
  expect_equal(as.integer(meta$deterministic_fallback_no_payload), 1L)
  expect_equal(as.integer(meta$snippet_openwebpage_escalation_events), 1L)
  expect_equal(as.character(urls[[1]]), "https://www.example.gov/profile/ada-lovelace")
  expect_equal(as.character(meta$preferred_openwebpage_urls[[1]]), "https://www.example.gov/profile/ada-lovelace")
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$`_asa_triage_circuit_with_calls`)), 0L)
  expect_equal(as.integer(reticulate::py_to_r(reticulate::py$`_asa_triage_circuit_invoke_calls`)), 0L)
})

test_that("snippet escalation ranking keeps sparse official URLs ahead of noisy named pages", {
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
    "def __asa_sparse_official_score(url):\n",
    "    text = str(url or '')\n",
    "    if 'news.example.net' in text:\n",
    "        return 1.15\n",
    "    if 'official.example' in text:\n",
    "        return 0.95\n",
    "    return 0.0\n"
  ))
  reticulate::py_set_attr(
    graph,
    "_score_primary_source_url",
    reticulate::py_eval("__asa_sparse_official_score", convert = FALSE)
  )

  search_msg <- list(
    role = "tool",
    name = "Search",
    content = paste(
      "__START_OF_SOURCE 1__ <CONTENT>Ada Lovelace biography article.</CONTENT>",
      "<URL>https://news.example.net/article/ada-lovelace</URL> __END_OF_SOURCE 1__",
      "__START_OF_SOURCE 2__ <CONTENT>Official parliamentary profile page.</CONTENT>",
      "<URL>https://official.example/parliamentarian?id=428</URL> __END_OF_SOURCE 2__"
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
    search_queries = list("Ada Lovelace prior occupation"),
    max_sources = 2L,
    extraction_engine = "legacy",
    debug = FALSE
  )

  meta <- reticulate::py_to_r(out[[3]])
  expect_gte(as.integer(meta$snippet_openwebpage_escalation_events), 1L)
  expect_equal(
    as.character(meta$preferred_openwebpage_url),
    "https://official.example/parliamentarian?id=428"
  )
})

test_that("search snippet field hints prioritize enriched birth-year snippets", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  reticulate::py_run_string(paste0(
    "import json\n",
    "class _ASAFieldHintSnippetModel:\n",
    "    def invoke(self, messages, **kwargs):\n",
    "        class _Resp:\n",
    "            def __init__(self, content):\n",
    "                self.content = content\n",
    "        return _Resp(json.dumps({'birth_year': 1982}))\n",
    "_asa_empty_snippet_model = _ASAFieldHintSnippetModel()\n"
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
  expect_equal(as.integer(meta$selector_model_calls), 1L)
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


test_that("deterministic recovery keeps weakly aligned candidates visible with scored diagnostics", {
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
  expect_false(identical(as.character(out_r$birth_year$status), "found"))
  expect_equal(as.character(out_r$birth_year$evidence_source_url), source_url)
  expect_true(as.numeric(out_r$birth_year$candidate_score) > 0)
  expect_true(as.numeric(out_r$birth_year$target_consistency) < 0.35)
  expect_true(as.character(out_r$birth_year$evidence_category) %in% c(
    "high_conflict_demotion",
    "soft_anchor_penalty",
    "insufficient_support"
  ))
  expect_false(identical(as.character(out_r$birth_year$evidence), "recovery_blocked_entity_mismatch"))
})

test_that("deterministic recovery soft-penalizes near-miss alignment when source evidence is otherwise strong", {
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
  expect_equal(as.character(out_r$birth_year$evidence_category), "soft_anchor_penalty")
  expect_equal(as.character(out_r$birth_year$contradiction_state), "soft_anchor_penalty")
  expect_true(as.numeric(out_r$birth_year$candidate_score) > 0)
  expect_false(identical(as.character(out_r$birth_year_source$status), "found"))
})

test_that("candidate consistency hard-blocks explicit context contradiction", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  state <- reticulate::py_to_r(core$`_candidate_target_consistency_state`(
    value = "Teacher",
    source_url = "https://www.example.cl/ramona-parra",
    source_text = "Ramona Parra fue una militante chilena.",
    target_anchor = list(
      tokens = list("ramona"),
      phrases = list(),
      id_signals = list(),
      context_tokens = list("bolivia"),
      context_labels = list("country:Bolivia"),
      confidence = 0.7,
      strength = "moderate",
      provenance = list("test"),
      mode = "adaptive"
    )
  ))

  expect_true(isTRUE(state$hard_block))
  expect_false(isTRUE(state$promotion_block))
  expect_equal(as.character(state$contradiction_state), "explicit_contradiction")
})

test_that("candidate consistency strongly demotes medium-confidence target conflict", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  state <- reticulate::py_to_r(core$`_candidate_target_consistency_state`(
    value = "alternate",
    source_url = "https://www.example.gov/profile/12345",
    source_text = "Bob Jones serves as alternate member of the committee.",
    target_anchor = list(
      tokens = list("alice", "smith"),
      phrases = list("alice smith"),
      id_signals = list(),
      context_tokens = list(),
      context_labels = list(),
      confidence = 0.9,
      strength = "strong",
      provenance = list("test"),
      mode = "strict"
    )
  ))

  expect_false(isTRUE(state$hard_block))
  expect_true(isTRUE(state$promotion_block))
  expect_equal(as.character(state$contradiction_state), "medium_conflict")
  expect_true(as.numeric(state$demotion_penalty) > 0.30)
})

test_that("deterministic recovery softens hard anchor mismatch only for allowlisted specific source-backed evidence", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  schema <- list(
    birth_year = "integer|null|Unknown",
    birth_year_source = "string|null"
  )
  source_url <- "https://www.example.gov/legislators/profile/428"
  source_text <- paste(
    "Camaconi legislative profile.",
    "Date of birth: 1982-01-07.",
    "Official profile registry."
  )

  out <- core$`_recover_unknown_fields_from_tool_evidence`(
    field_status = list(),
    expected_schema = schema,
    finalization_policy = list(
      field_recovery_enabled = TRUE,
      field_recovery_mode = "recall",
      field_recovery_require_keyword_hit_for_numbers = FALSE,
      anchor_mode = "strict",
      anchor_mismatch_penalty = 0.05,
      field_recovery_entity_tolerance_enabled = FALSE
    ),
    allowed_source_urls = list(source_url),
    source_text_index = stats::setNames(list(source_text), source_url),
    target_anchor = list(
      tokens = list("ramona", "moye", "camaconi"),
      phrases = list("ramona moye camaconi"),
      id_signals = list(),
      context_tokens = list(),
      context_labels = list(),
      confidence = 0.9,
      strength = "strong",
      provenance = list("test"),
      mode = "strict"
    )
  )

  out_r <- reticulate::py_to_r(out)
  expect_equal(as.character(out_r$birth_year$status), "found")
  expect_equal(as.integer(out_r$birth_year$value), 1982L)
  expect_equal(as.character(out_r$birth_year$evidence), "recovery_source_backed")
  expect_equal(as.character(out_r$birth_year$evidence_reason), "recovery_promoted_anchor_soft_penalty")
  expect_equal(as.character(out_r$birth_year_source$status), "found")
  expect_equal(as.character(out_r$birth_year_source$value), source_url)
})

test_that("field-status extraction rejects candidates with entity/value mismatch", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  schema <- list(
    office_title = "string|Unknown",
    office_title_source = "string|null"
  )
  field_status <- list(
    office_title = list(status = "unknown", value = "Unknown", source_url = NULL, attempts = 0L),
    office_title_source = list(status = "pending", value = NULL, source_url = NULL, attempts = 0L)
  )
  profile_url <- "https://example.gov/profile/alice"
  source_text <- "Directory snippet: Bob Jones serves as alternate member of the committee."
  payload <- list(
    office_title = "alternate",
    office_title_source = profile_url
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
    entity_name_tokens = list("Alice", "Smith"),
    evidence_enabled = TRUE
  )
  updates_r <- reticulate::py_to_r(updates)
  updated_fs <- updates_r[[1]]

  expect_false(identical(as.character(updated_fs$office_title$status), "found"))
  expect_equal(as.character(updated_fs$office_title$evidence_category), "high_conflict_demotion")
  expect_true(as.character(updated_fs$office_title$contradiction_state) %in% c(
    "medium_conflict",
    "high_conflict"
  ))
  expect_true(as.numeric(updated_fs$office_title$candidate_score) > 0)
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
    provenance = list("entity_hints"),
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
    provenance = list("entity_hints"),
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
    provenance = list("entity_hints"),
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


test_that("default disambiguation query template is task-agnostic", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  templates <- reticulate::py_to_r(core$`_normalize_query_templates`(NULL))

  expect_equal(
    as.character(templates$disambiguation_query),
    "{entity} {field} exact match"
  )
})


test_that("retry rewrite message uses neutral default disambiguation wording", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  msg <- core$`_build_retry_rewrite_message`(
    state = reticulate::dict(),
    pending_fields = list("prior_occupation"),
    query_context = "Ada Lovelace"
  )
  msg <- as.character(reticulate::py_to_r(msg))

  expect_match(msg, "disambiguation='\\{entity\\} \\{field\\} exact match'", perl = TRUE)
  expect_false(grepl("\\bbiography\\b|\\bprofile\\b", msg, ignore.case = TRUE, perl = TRUE))
})


test_that("retry rewrite message preserves caller disambiguation template override", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py"
  )

  msg <- core$`_build_retry_rewrite_message`(
    state = reticulate::dict(
      query_templates = reticulate::dict(
        disambiguation_query = "{entity} {field} official source"
      )
    ),
    pending_fields = list("prior_occupation"),
    query_context = "Ada Lovelace"
  )
  msg <- as.character(reticulate::py_to_r(msg))

  expect_match(msg, "disambiguation='\\{entity\\} \\{field\\} official source'", perl = TRUE)
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
