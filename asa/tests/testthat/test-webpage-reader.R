# test-webpage-reader.R
# Tests for the optional webpage reader tool (allow_read_webpages gate)

.get_python_path_webpage_reader <- function() {
  python_path <- system.file("python", package = "asa")
  if (python_path == "" || !dir.exists(python_path)) {
    # Development path fallback
    python_path <- file.path(getwd(), "inst/python")
    if (!dir.exists(python_path)) {
      python_path <- file.path(dirname(getwd()), "asa/inst/python")
    }
  }
  python_path
}

.skip_if_no_python_webpage_reader <- function() {
  python_path <- .get_python_path_webpage_reader()
  if (!dir.exists(python_path)) {
    skip("Python modules not found")
  }
  # Prefer the package's default conda env if available (this is where the
  # backend dependencies like bs4/langchain are typically installed).
  conda_env <- tryCatch(asa:::.get_default_conda_env(), error = function(e) NULL)
  if (!is.null(conda_env) && is.character(conda_env) && nzchar(conda_env)) {
    try(reticulate::use_condaenv(conda_env, required = FALSE), silent = TRUE)
  }
  if (!reticulate::py_available(initialize = TRUE)) {
    skip("Python not available")
  }
  python_path
}

.skip_if_missing_python_modules_webpage_reader <- function(modules) {
  if (is.null(modules) || length(modules) == 0) {
    return(invisible(TRUE))
  }

  for (module in modules) {
    if (!reticulate::py_module_available(module)) {
      skip(paste0("Python module not available: ", module))
    }
  }

  invisible(TRUE)
}

.import_webpage_tool <- function(python_path) {
  tryCatch(
    reticulate::import_from_path("webpage_tool", path = python_path),
    error = function(e) {
      skip(paste0("Failed to import webpage_tool: ", conditionMessage(e)))
    }
  )
}

.extract_first_excerpt <- function(open_webpage_output) {
  if (!grepl("[1]", open_webpage_output, fixed = TRUE)) {
    return("")
  }
  excerpt <- sub("(?s).*\\[1\\]\\n", "", open_webpage_output, perl = TRUE)
  excerpt <- sub("(?s)\\n\\[2\\].*$", "", excerpt, perl = TRUE)
  excerpt
}

test_that("OpenWebpage tool is gated by allow_read_webpages", {
  python_path <- .skip_if_no_python_webpage_reader()
  .skip_if_missing_python_modules_webpage_reader(
    c("requests", "bs4", "pydantic", "langchain_core")
  )

  webpage_tool <- .import_webpage_tool(python_path)
  cfg_prev <- webpage_tool$configure_webpage_reader()

  on.exit({
    try(webpage_tool$configure_webpage_reader(
      allow_read_webpages = cfg_prev$allow_read_webpages,
      relevance_mode = cfg_prev$relevance_mode,
      embedding_provider = cfg_prev$embedding_provider,
      embedding_model = cfg_prev$embedding_model,
      prefilter_k = cfg_prev$prefilter_k,
      use_mmr = cfg_prev$use_mmr,
      mmr_lambda = cfg_prev$mmr_lambda,
      cache_enabled = cfg_prev$cache_enabled
    ), silent = TRUE)
    try(webpage_tool$clear_webpage_reader_cache(), silent = TRUE)
  }, add = TRUE)

  tool <- webpage_tool$create_webpage_reader_tool()

  webpage_tool$configure_webpage_reader(
    allow_read_webpages = FALSE,
    relevance_mode = "lexical",
    cache_enabled = FALSE
  )
  out_disabled <- tool$`_run`(
    url = "https://connorjerzak.com/collaborators/",
    query = "Europe"
  )
  expect_match(out_disabled, "Webpage reading is disabled", fixed = TRUE)
  expect_match(out_disabled, "allow_read_webpages=FALSE", fixed = TRUE)

  webpage_tool$configure_webpage_reader(
    allow_read_webpages = TRUE,
    relevance_mode = "lexical",
    cache_enabled = FALSE
  )
  out_enabled <- tool$`_run`(
    url = "http://localhost/",
    query = "test"
  )
  expect_match(out_enabled, "Refusing to open URL", fixed = TRUE)
})

test_that(".with_webpage_reader_config toggles Python allow_read_webpages", {
  python_path <- .skip_if_no_python_webpage_reader()
  .skip_if_missing_python_modules_webpage_reader(
    c("requests", "bs4", "pydantic", "langchain_core")
  )

  conda_env <- tryCatch(asa:::.get_default_conda_env(), error = function(e) NULL)
  if (is.null(conda_env) || !is.character(conda_env) || !nzchar(conda_env)) {
    skip("Default conda env not available")
  }

  webpage_tool <- .import_webpage_tool(python_path)
  cfg_prev <- webpage_tool$configure_webpage_reader()

  inside <- tryCatch(
    asa:::.with_webpage_reader_config(
      allow_read_webpages = TRUE,
      relevance_mode = "lexical",
      conda_env = conda_env,
      fn = function() webpage_tool$configure_webpage_reader()$allow_read_webpages
    ),
    error = function(e) {
      skip(paste0("Webpage reader config wrapper unavailable: ", conditionMessage(e)))
    }
  )

  expect_true(isTRUE(inside))
  expect_equal(
    webpage_tool$configure_webpage_reader()$allow_read_webpages,
    cfg_prev$allow_read_webpages
  )
})

test_that("OpenWebpage can read collaborators page (live network)", {
  skip_on_cran()
  skip_if(Sys.getenv("ASA_TEST_LIVE_WEBPAGE_READER") != "1",
          "Set ASA_TEST_LIVE_WEBPAGE_READER=1 to run this live-network test")

  python_path <- .skip_if_no_python_webpage_reader()
  .skip_if_missing_python_modules_webpage_reader(
    c("requests", "bs4", "pydantic", "langchain_core")
  )

  webpage_tool <- .import_webpage_tool(python_path)
  cfg_prev <- webpage_tool$configure_webpage_reader()

  on.exit({
    try(webpage_tool$configure_webpage_reader(
      allow_read_webpages = cfg_prev$allow_read_webpages,
      relevance_mode = cfg_prev$relevance_mode,
      embedding_provider = cfg_prev$embedding_provider,
      embedding_model = cfg_prev$embedding_model,
      prefilter_k = cfg_prev$prefilter_k,
      use_mmr = cfg_prev$use_mmr,
      mmr_lambda = cfg_prev$mmr_lambda,
      cache_enabled = cfg_prev$cache_enabled
    ), silent = TRUE)
    try(webpage_tool$clear_webpage_reader_cache(), silent = TRUE)
  }, add = TRUE)

  webpage_tool$configure_webpage_reader(
    allow_read_webpages = TRUE,
    relevance_mode = "lexical",
    chunk_chars = 2000,
    max_chunks = 2,
    cache_enabled = FALSE
  )

  tool <- webpage_tool$create_webpage_reader_tool()
  out <- tool$`_run`(
    url = "https://connorjerzak.com/collaborators/",
    query = "Europe:"
  )

  excerpt1 <- .extract_first_excerpt(out)
  expect_true(nchar(excerpt1) > 0)

  pos_europe <- regexpr("Europe:", excerpt1, fixed = TRUE)
  expect_true(pos_europe[1] > -1)

  after <- substring(excerpt1, pos_europe[1] + attr(pos_europe, "match.length"))
  lines <- trimws(unlist(strsplit(after, "\n", fixed = TRUE)))
  lines <- lines[lines != ""]
  lines <- lines[!grepl("^[\\-–—]+$", lines)]

  expect_true(length(lines) >= 1)
  expect_true(grepl("Adel Daoud", lines[1], fixed = TRUE))
})
