test_that("custom_ddg_production shim forwards required backend symbols", {
  python_path <- asa_test_skip_if_no_python(
    required_files = c("custom_ddg_production.py", "asa_backend/agent_api.py")
  )
  asa_test_require_langgraph_stack(ASA_TEST_LANGGRAPH_MODULES)

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)

  required_symbols <- c(
    "configure_search",
    "configure_logging",
    "configure_tor",
    "configure_tor_registry",
    "configure_anti_detection",
    "create_standard_agent",
    "create_memory_folding_agent",
    "create_memory_folding_agent_with_checkpointer",
    "_repair_best_effort_json",
    "_sanitize_memory_dict"
  )

  has_symbol <- function(name) {
    tryCatch({
      !is.null(reticulate::py_get_attr(prod, name))
    }, error = function(e) FALSE)
  }

  missing <- required_symbols[!vapply(required_symbols, has_symbol, logical(1))]
  expect_equal(
    length(missing),
    0L,
    info = paste("Missing compatibility symbols:", paste(missing, collapse = ", "))
  )
})


test_that("custom_ddg_production no longer aliases agent_graph module identity", {
  python_path <- asa_test_skip_if_no_python(
    required_files = c("custom_ddg_production.py", "asa_backend/agent_api.py")
  )
  asa_test_require_langgraph_stack(ASA_TEST_LANGGRAPH_MODULES)

  prod <- reticulate::import_from_path("custom_ddg_production", path = python_path)
  py <- reticulate::py
  py$prod_module_for_test <- prod

  reticulate::py_run_string(paste0(
    "import asa_backend.agent_graph as _agent_graph_for_test\n",
    "import asa_backend.agent_api as _agent_api_for_test\n",
    "_asa_same_graph = (prod_module_for_test is _agent_graph_for_test)\n",
    "_asa_same_api = (prod_module_for_test is _agent_api_for_test)\n",
    "_asa_fn_forwarded = (prod_module_for_test.create_standard_agent is _agent_api_for_test.create_standard_agent)\n"
  ))

  expect_false(isTRUE(reticulate::py_to_r(py$`_asa_same_graph`)))
  expect_false(isTRUE(reticulate::py_to_r(py$`_asa_same_api`)))
  expect_true(isTRUE(reticulate::py_to_r(py$`_asa_fn_forwarded`)))
})
