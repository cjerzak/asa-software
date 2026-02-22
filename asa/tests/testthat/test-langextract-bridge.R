# test-langextract-bridge.R

test_that("field_resolver defaults use langextract engine and compatibility alias", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
  )

  defaults <- reticulate::py_to_r(core$`_normalize_orchestration_options`(list()))
  fr <- defaults$field_resolver

  expect_equal(fr$webpage_extraction_engine, "langextract")
  expect_true(isTRUE(fr$webpage_extraction_enabled))
  expect_true(isTRUE(fr$llm_webpage_extraction))
  expect_equal(as.integer(fr$llm_webpage_extraction_max_chars), 9000L)
  expect_equal(as.integer(fr$langextract_extraction_passes), 2L)
  expect_equal(as.integer(fr$langextract_max_char_buffer), 2000L)

  legacy_off <- reticulate::py_to_r(core$`_normalize_orchestration_options`(list(
    field_resolver = list(
      llm_webpage_extraction = FALSE
    )
  )))

  fr_off <- legacy_off$field_resolver
  expect_false(isTRUE(fr_off$webpage_extraction_enabled))
  expect_false(isTRUE(fr_off$llm_webpage_extraction))
})

test_that("source URL normalization handles multiline and escaped-final-url tails", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
  )

  raw_multiline <- paste(
    "https://www.flickr.com/photos/194792615@N03/52180793916",
    "Final URL: https://www.flickr.com/photos/194792615@N03/52180793916",
    sep = "\n"
  )
  raw_escaped <- "https://www.flickr.com/photos/194792615@N03/52180793916\\nFinal"

  normalized_multiline <- as.character(core$`_normalize_url_match`(raw_multiline))
  normalized_escaped <- as.character(core$`_normalize_url_match`(raw_escaped))

  expect_equal(normalized_multiline, "https://www.flickr.com/photos/194792615@N03/52180793916")
  expect_equal(normalized_escaped, "https://www.flickr.com/photos/194792615@N03/52180793916")
})

test_that("openwebpage text cleaner removes boilerplate and keeps evidence lines", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
  )

  raw_text <- paste(
    "URL: https://www.flickr.com/photos/194792615@N03/52180793916",
    "Final URL: https://www.flickr.com/photos/194792615@N03/52180793916",
    "Title: Ramona Moye Camaconi | Flickr",
    "Bytes read: 511523",
    "",
    "Relevant excerpts:",
    "",
    "[1]",
    "Explore",
    "[link: https://www.flickr.com/explore]",
    "Ramona Moye Camaconi es diputada electa por el partido MAS en Beni.",
    sep = "\n"
  )

  cleaned <- as.character(core$`_prepare_openwebpage_text_for_extraction`(
    raw_text,
    entity_tokens = list("Ramona", "Moye"),
    max_chars = 1200L
  ))

  expect_match(cleaned, "Ramona Moye Camaconi", fixed = TRUE)
  expect_no_match(cleaned, "URL:", fixed = TRUE)
  expect_no_match(cleaned, "\\[link:")
  expect_no_match(cleaned, "Explore", fixed = TRUE)
})

test_that("openwebpage hard-failure classifier catches blocked fetch outputs", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
  )

  blocked_text <- paste(
    "URL: https://ftierra.org/example",
    "Bytes read: 0",
    "Blocked fetch detected.",
    "Reason: http_403_bot_marker (status=403)",
    "Markers: enable javascript and cookies, title:just a moment",
    sep = "\n"
  )

  reason <- as.character(core$`_openwebpage_hard_failure_reason`(blocked_text))
  expect_equal(reason, "blocked_fetch")
})

test_that("openwebpage hard-failure classifier catches tool error JSON payloads", {
  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.core",
    required_files = "asa_backend/graph/core.py"
  )

  tool_error_json <- paste0(
    "{\"ok\":false,\"tool\":\"OpenWebpage\",\"error_type\":\"request_exception\",",
    "\"retryable\":true,\"url\":\"https://example.com\",\"error\":\"curl: (56)\"}"
  )

  reason <- as.character(core$`_openwebpage_hard_failure_reason`(tool_error_json))
  expect_equal(reason, "tool_error:request_exception")
})


test_that("langextract bridge normalizes extracted keys and source siblings", {
  bridge <- asa_test_import_module(
    "asa_backend.extraction.langextract_bridge",
    required_file = "asa_backend/extraction/langextract_bridge.py",
    required_modules = character(0)
  )

  reticulate::py_run_string(paste0(
    "class _ASAExtraction:\n",
    "    def __init__(self, extraction_class, extraction_text, attributes=None):\n",
    "        self.extraction_class = extraction_class\n",
    "        self.extraction_text = extraction_text\n",
    "        self.attributes = attributes or {}\n",
    "_asa_ex1 = _ASAExtraction('full_name', 'Ada Lovelace')\n"
  ))

  out <- reticulate::py_to_r(bridge$normalize_langextract_output_to_allowed_keys(
    extractions = list(reticulate::py$`_asa_ex1`),
    allowed_keys = list("full_name", "full_name_source"),
    page_url = "https://example.com/profile"
  ))

  expect_equal(as.character(out$full_name), "Ada Lovelace")
  expect_equal(as.character(out$full_name_source), "https://example.com/profile")
})


test_that("langextract bridge normalizes gemini models/* route ids", {
  bridge <- asa_test_import_module(
    "asa_backend.extraction.langextract_bridge",
    required_file = "asa_backend/extraction/langextract_bridge.py",
    required_modules = character(0)
  )

  reticulate::py_run_string(paste0(
    "class _ASAGeminiModel:\n",
    "    def __init__(self, model_name):\n",
    "        self.model_name = model_name\n",
    "_asa_gemini_model = _ASAGeminiModel('models/gemini-3-flash-preview')\n"
  ))

  route_pair <- bridge$`_resolve_route`(
    selector_model = reticulate::py$`_asa_gemini_model`,
    backend_hint = "gemini"
  )

  expect_null(route_pair[[2]])
  expect_equal(as.character(route_pair[[1]]$provider), "gemini")
  expect_equal(as.character(route_pair[[1]]$model_id), "gemini-3-flash-preview")
})


test_that("langextract bridge returns unsupported backend without importing provider runtime", {
  bridge <- asa_test_import_module(
    "asa_backend.extraction.langextract_bridge",
    required_file = "asa_backend/extraction/langextract_bridge.py",
    required_modules = character(0)
  )

  reticulate::py_run_string(paste0(
    "class _ASADummyModel:\n",
    "    pass\n",
    "_asa_dummy_model = _ASADummyModel()\n"
  ))

  result <- reticulate::py_to_r(bridge$extract_schema_from_openwebpage_text(
    page_url = "https://example.com/profile",
    page_text = "name: Ada Lovelace",
    allowed_keys = list("full_name", "full_name_source"),
    schema_keys = list("full_name", "full_name_source"),
    selector_model = reticulate::py$`_asa_dummy_model`,
    backend_hint = "anthropic"
  ))

  expect_false(isTRUE(result$ok))
  expect_match(as.character(result$error), "unsupported_backend", fixed = TRUE)
})
