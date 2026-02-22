# test-search-snippet-extraction.R

test_that("search snippet extraction produces structured payloads promotable into field_status", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
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


test_that("deterministic numeric recovery requires a keyword hit when configured", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
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
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
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
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
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
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
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
