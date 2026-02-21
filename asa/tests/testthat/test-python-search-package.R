test_that("asa_backend.search aligns with agent_api public search surface", {
  python_path <- asa_test_skip_if_no_python(
    required_files = c(
      "asa_backend/search/__init__.py",
      "asa_backend/agent_api.py"
    )
  )

  # Importing the search subsystem requires additional Python deps beyond core langgraph.
  asa_test_skip_if_missing_python_modules(
    c("langchain_community", "pydantic", "requests", "ddgs", "bs4", "selenium", "primp"),
    method = "py_module_available"
  )

  search_pkg <- reticulate::import_from_path("asa_backend.search", path = python_path)
  api <- reticulate::import_from_path("asa_backend.agent_api", path = python_path)

  required <- c(
    "SearchConfig",
    "configure_search",
    "configure_logging",
    "configure_tor",
    "configure_tor_registry",
    "configure_anti_detection",
    "PatchedDuckDuckGoSearchAPIWrapper",
    "PatchedDuckDuckGoSearchRun"
  )

  missing_search <- required[!vapply(required, reticulate::py_has_attr, logical(1), x = search_pkg)]
  expect_equal(length(missing_search), 0L, info = paste("Missing in search package:", paste(missing_search, collapse = ", ")))

  missing_api <- required[!vapply(required, reticulate::py_has_attr, logical(1), x = api)]
  expect_equal(length(missing_api), 0L, info = paste("Missing in agent_api:", paste(missing_api, collapse = ", ")))

  py <- reticulate::py
  py$`_asa_search_pkg` <- search_pkg
  py$`_asa_api` <- api

  reticulate::py_run_string(paste0(
    "_asa_same_run = (_asa_search_pkg.PatchedDuckDuckGoSearchRun is _asa_api.PatchedDuckDuckGoSearchRun)\n",
    "_asa_same_cfg = (_asa_search_pkg.SearchConfig is _asa_api.SearchConfig)\n"
  ))

  expect_true(isTRUE(reticulate::py_to_r(py$`_asa_same_run`)))
  expect_true(isTRUE(reticulate::py_to_r(py$`_asa_same_cfg`)))
})
