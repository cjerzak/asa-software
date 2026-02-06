# Ensure tests use the standard asa conda environment when available.
options(asa.default_conda_env = "asa_env")

if (requireNamespace("reticulate", quietly = TRUE)) {
  try(
    suppressWarnings(reticulate::use_condaenv("asa_env", required = FALSE)),
    silent = TRUE
  )
}

# ---------------------------------------------------------------------------
# Shared Python / prompt helpers (avoid duplication across test files)
# ---------------------------------------------------------------------------

asa_test_python_path <- function(required_files = character()) {
  candidates <- c(
    # Common dev/test entrypoints:
    file.path(getwd(), "inst", "python"),
    file.path(getwd(), "asa", "inst", "python"),
    file.path(getwd(), "..", "inst", "python"),
    file.path(getwd(), "..", "..", "inst", "python"),
    # Installed package path:
    system.file("python", package = "asa")
  )

  candidates <- unique(normalizePath(candidates, winslash = "/", mustWork = FALSE))
  candidates <- candidates[nzchar(candidates)]

  if (is.null(required_files)) {
    required_files <- character(0)
  }
  required_files <- as.character(required_files)
  required_files <- required_files[nzchar(required_files)]

  for (path in candidates) {
    if (!dir.exists(path)) next
    ok <- TRUE
    for (rf in required_files) {
      if (!file.exists(file.path(path, rf))) {
        ok <- FALSE
        break
      }
    }
    if (ok) return(path)
  }

  for (path in candidates) {
    if (dir.exists(path)) return(path)
  }

  ""
}

asa_test_skip_if_no_python <- function(required_files = character(),
                                      initialize = TRUE,
                                      conda_env = NULL) {
  if (!requireNamespace("testthat", quietly = TRUE)) {
    stop("testthat is required for asa_test_skip_if_no_python()", call. = FALSE)
  }
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    testthat::skip("reticulate not installed")
  }

  python_path <- asa_test_python_path(required_files = required_files)
  if (!nzchar(python_path) || !dir.exists(python_path)) {
    testthat::skip("Python modules not found")
  }

  if (is.null(conda_env)) {
    conda_env <- tryCatch(asa:::.get_default_conda_env(), error = function(e) NULL)
  }
  if (!is.null(conda_env) && is.character(conda_env) && nzchar(conda_env)) {
    try(reticulate::use_condaenv(conda_env, required = FALSE), silent = TRUE)
  }

  if (!reticulate::py_available(initialize = isTRUE(initialize))) {
    testthat::skip("Python not available")
  }

  python_path
}

asa_test_skip_if_missing_python_modules <- function(modules,
                                                   method = c("import", "py_module_available")) {
  if (!requireNamespace("testthat", quietly = TRUE)) {
    stop("testthat is required for asa_test_skip_if_missing_python_modules()", call. = FALSE)
  }
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    testthat::skip("reticulate not installed")
  }

  if (is.null(modules)) {
    modules <- character(0)
  }
  modules <- as.character(modules)
  modules <- modules[nzchar(modules)]
  if (length(modules) == 0) {
    return(invisible(TRUE))
  }

  method <- match.arg(method)

  for (module in modules) {
    ok <- if (method == "py_module_available") {
      isTRUE(reticulate::py_module_available(module))
    } else {
      tryCatch({
        reticulate::import(module, convert = FALSE)
        TRUE
      }, error = function(e) FALSE)
    }

    if (!ok) {
      testthat::skip(paste0("Python module not available: ", module))
    }
  }

  invisible(TRUE)
}

asa_test_import_from_path_or_skip <- function(module_name, python_path) {
  if (!requireNamespace("testthat", quietly = TRUE)) {
    stop("testthat is required for asa_test_import_from_path_or_skip()", call. = FALSE)
  }
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    testthat::skip("reticulate not installed")
  }

  tryCatch(
    reticulate::import_from_path(module_name, path = python_path),
    error = function(e) {
      testthat::skip(paste0("Failed to import ", module_name, ": ", conditionMessage(e)))
    }
  )
}

asa_test_import_module <- function(module_name,
                                   required_file = paste0(module_name, ".py"),
                                   required_modules = NULL,
                                   initialize = FALSE) {
  python_path <- asa_test_skip_if_no_python(
    required_files = required_file, initialize = initialize
  )
  if (!is.null(required_modules)) {
    asa_test_skip_if_missing_python_modules(required_modules)
  }
  reticulate::import_from_path(module_name, path = python_path)
}

asa_test_recursion_limit_prompt <- function() {
  paste0(
    "You are doing a multi-step research task.\n",
    "1) You MUST call the Search tool first with the query: \"asa recursion limit test data\".\n",
    "   Do NOT output the final JSON until after you have tool output.\n",
    "2) After you get tool output, produce ONLY valid JSON with this exact schema:\n",
    "{\n",
    "  \"status\": \"complete\"|\"partial\",\n",
    "  \"items\": [\n",
    "    {\"name\": string, \"birth_year\": integer, \"field\": string, \"key_contribution\": string|null}\n",
    "  ],\n",
    "  \"missing\": [string],\n",
    "  \"notes\": string\n",
    "}\n",
    "Missing-data rules:\n",
    "- If you could not run Search or did not get tool output, set status=\"partial\".\n",
    "- Use null for unknown key_contribution.\n",
    "- List any unknown fields in missing.\n",
    "- Do NOT speculate.\n",
    "Known seed data (you may use this even without Search):\n",
    "- Ada Lovelace (birth_year=1815, field=\"mathematics\")\n",
    "- Alan Turing (birth_year=1912, field=\"computer science\")\n",
    "- Grace Hopper (birth_year=1906, field=\"computer science\")\n",
    "Your items MUST include these three people at minimum.\n"
  )
}
