# Focused reliability tests for shared JSON parse/repair diagnostics and
# graph-level propagation via json_repair events.

.as_r_value <- function(value) {
  tryCatch(reticulate::py_to_r(value), error = function(e) value)
}

test_that("parse_llm_json returns structured diagnostics for malformed JSON", {
  utils <- asa_test_import_module(
    "shared.state_graph_utils",
    required_file = "shared/state_graph_utils.py",
    required_modules = c("pydantic"),
    initialize = TRUE
  )

  diag <- .as_r_value(utils$parse_llm_json("{bad json", return_diagnostics = TRUE))

  expect_true(is.list(diag))
  expect_false(isTRUE(diag$ok))
  expect_equal(as.character(diag$error_reason), "json_decode_failed")
  expect_true(is.character(diag$exception_type) || is.null(diag$exception_type))
  expect_true(is.list(diag$context))
  attempts <- diag$context$attempt_count
  if (is.null(attempts) || is.na(attempts)) {
    attempts <- 0L
  }
  expect_true(as.integer(attempts) >= 1L)
})

test_that("repair_json_output_to_schema reports repair failure diagnostics", {
  utils <- asa_test_import_module(
    "shared.state_graph_utils",
    required_file = "shared/state_graph_utils.py",
    required_modules = c("pydantic"),
    initialize = TRUE
  )

  # Force top-level shape mismatch (schema expects array, parse fallback is object).
  schema <- list("string")
  diag <- .as_r_value(utils$repair_json_output_to_schema(
    "{broken",
    schema,
    fallback_on_failure = FALSE,
    return_diagnostics = TRUE
  ))

  expect_true(is.list(diag))
  expect_false(isTRUE(diag$ok))
  expect_equal(as.character(diag$error_reason), "type_mismatch")
  expect_true(is.list(diag$parse_diagnostics))
  expect_false(isTRUE(diag$parse_diagnostics$ok))
  expect_equal(as.character(diag$parse_diagnostics$error_reason), "json_decode_failed")
})

test_that("_repair_best_effort_json emits bridge-visible parse/repair diagnostics on failure", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  py <- reticulate::py
  py$prod_module_diag <- prod
  reticulate::py_run_string(paste0(
    "import asa_backend.api.agent_api as _agent_api_diag\n",
    "from langchain_core.messages import AIMessage\n",
    "_orig_repair_diag = _agent_api_diag.repair_json_output_to_schema\n",
    "def _always_fail_repair_diag(text, schema, fallback_on_failure=False):\n",
    "    return None\n",
    "_agent_api_diag.repair_json_output_to_schema = _always_fail_repair_diag\n",
    "_diag_msg = AIMessage(content='{\"status\":')\n",
    "_diag_repaired_msg, _diag_event = prod_module_diag._repair_best_effort_json(\n",
    "    {'status': 'string'},\n",
    "    _diag_msg,\n",
    "    fallback_on_failure=True,\n",
    "    schema_source='explicit',\n",
    "    context='unit_test',\n",
    "    debug=False,\n",
    ")\n"
  ))
  on.exit(
    reticulate::py_run_string(
      "import asa_backend.api.agent_api as _agent_api_diag\nif '_orig_repair_diag' in globals():\n    _agent_api_diag.repair_json_output_to_schema = _orig_repair_diag\n"
    ),
    add = TRUE
  )

  event <- .as_r_value(py$`_diag_event`)

  expect_true(is.list(event))
  expect_false(isTRUE(event$repair_applied))
  expect_equal(as.character(event$repair_reason), "repair_failed")
  expect_true(is.list(event$parse_diagnostics))
  expect_true(is.list(event$repair_diagnostics))
  expect_false(isTRUE(event$parse_diagnostics$ok))
  expect_false(isTRUE(event$repair_diagnostics$ok))
})
