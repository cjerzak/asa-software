# Live OpenAI structured-output regression tests.
# These tests require a valid OPENAI_API_KEY and network access.

test_that("OpenAI structured helper succeeds with the live provider", {
  asa_test_skip_api_tests()
  api_key <- asa_test_require_openai_key()

  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py",
    required_modules = c(ASA_TEST_LANGGRAPH_MODULES, "langchain_openai")
  )
  chat_models <- reticulate::import("langchain_openai")

  openai_model <- Sys.getenv("ASA_TEST_OPENAI_MODEL", unset = "")
  if (!nzchar(openai_model)) {
    openai_model <- "gpt-4.1-mini"
  }

  llm <- chat_models$ChatOpenAI(
    model = openai_model,
    temperature = 0,
    api_key = api_key
  )

  allowed_keys <- c(
    "birth_year",
    "birth_year_source",
    "prior_occupation",
    "prior_occupation_source"
  )
  messages <- list(
    list(
      role = "system",
      content = paste(
        "Extract structured schema field values from the provided text snippet.",
        "Return a JSON object using only the allowed keys and only explicit concrete values."
      )
    ),
    list(
      role = "user",
      content = jsonlite::toJSON(
        list(
          page_url = "https://www.example.gov/profile/ada-lovelace",
          allowed_keys = allowed_keys,
          page_text = "Ada Lovelace was born in London in 1815. Occupation: mathematician."
        ),
        auto_unbox = TRUE
      )
    )
  )

  out <- core$`_invoke_structured_selector_model_with_timeout`(
    model = llm,
    messages = messages,
    expected_schema = list(
      birth_year = "integer|null|Unknown",
      birth_year_source = "string|null",
      prior_occupation = "string|Unknown",
      prior_occupation_source = "string|null"
    ),
    allowed_keys = allowed_keys,
    page_url = "https://www.example.gov/profile/ada-lovelace",
    timeout_s = 20,
    backend_hint = "openai"
  )

  out_r <- reticulate::py_to_r(out)
  payload_value <- out_r$payload
  if (is.null(payload_value)) {
    payload_value <- list()
  }
  payload_names <- names(payload_value)
  error_text <- if (is.null(out_r$error)) "" else as.character(out_r$error)

  expect_true(isTRUE(out_r$ok))
  expect_equal(as.character(out_r$backend), "openai")
  expect_true(nzchar(as.character(out_r$schema_mode)))
  expect_true(length(payload_names) >= 1L)
  expect_true(all(payload_names %in% allowed_keys))
  expect_false(grepl("^structured_output_error", error_text))
})

test_that("OpenAI snippet triage path reaches structured output without schema failure", {
  asa_test_skip_api_tests()
  api_key <- asa_test_require_openai_key()

  core <- asa_test_import_langgraph_module(
    "asa_backend.graph.agent_graph_core",
    required_files = "asa_backend/graph/agent_graph_core.py",
    required_modules = c(ASA_TEST_LANGGRAPH_MODULES, "langchain_openai")
  )
  chat_models <- reticulate::import("langchain_openai")

  openai_model <- Sys.getenv("ASA_TEST_OPENAI_MODEL", unset = "")
  if (!nzchar(openai_model)) {
    openai_model <- "gpt-4.1-mini"
  }

  llm <- chat_models$ChatOpenAI(
    model = openai_model,
    temperature = 0,
    api_key = api_key
  )

  original_deterministic <- core$`_deterministic_schema_payload_from_source_text`
  on.exit(
    reticulate::py_set_attr(core, "_deterministic_schema_payload_from_source_text", original_deterministic),
    add = TRUE
  )
  reticulate::py_run_string(paste0(
    "def _asa_force_empty_deterministic(**kwargs):\n",
    "    return {}\n",
    "_asa_force_empty_deterministic = _asa_force_empty_deterministic\n"
  ))
  reticulate::py_set_attr(
    core,
    "_deterministic_schema_payload_from_source_text",
    reticulate::py$`_asa_force_empty_deterministic`
  )

  allowed_keys <- c(
    "birth_year",
    "birth_year_source",
    "prior_occupation",
    "prior_occupation_source"
  )
  tool_messages <- list(list(
    role = "tool",
    name = "Search",
    content = paste0(
      "__START_OF_SOURCE 1__ <CONTENT> Ada Lovelace was born in London in 1815. ",
      "Occupation: mathematician. </CONTENT> ",
      "<URL> https://www.example.gov/profile/ada-lovelace </URL> __END_OF_SOURCE 1__"
    )
  ))

  snippet_result <- core$`_llm_extract_schema_payloads_from_search_snippets`(
    selector_model = llm,
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
    ),
    tool_messages = tool_messages,
    entity_tokens = list("Ada", "Lovelace"),
    extracted_urls = list(),
    max_sources = 1L,
    max_chars = 800L,
    timeout_s = 20L,
    extraction_engine = "triage_then_structured",
    debug = FALSE
  )

  snippet_r <- reticulate::py_to_r(snippet_result)
  payloads <- snippet_r[[1]]
  urls <- snippet_r[[2]]
  meta <- snippet_r[[3]]
  payload_names <- character(0)
  if (length(payloads) >= 1L) {
    payload_value <- payloads[[1]]$payload
    if (is.null(payload_value)) {
      payload_value <- list()
    }
    payload_names <- names(payload_value)
  }

  expect_true(length(payloads) >= 1L)
  expect_true(length(urls) >= 1L)
  expect_equal(as.integer(meta$structured_output_calls), 1L)
  expect_equal(as.integer(meta$structured_output_errors), 0L)
  expect_equal(as.character(meta$structured_output_backend), "openai")
  expect_true(nzchar(as.character(meta$structured_output_schema_mode)))
  expect_true(length(payload_names) >= 1L)
  expect_true(all(payload_names %in% allowed_keys))
  expect_true(any(payload_names %in% c("birth_year", "prior_occupation")))
})
