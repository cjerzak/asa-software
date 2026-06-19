# Live manual test for Wayback fallback in the OpenCode-era research backend.
#
# Run from the repository root:
#   ASA_RUN_WAYBACK_LIVE_TESTS=true \
#     conda run -n asa_env Rscript --vanilla asa/tests/manual/test_opencode_wayback_research_fallback_live.R
#
# Optional overrides:
#   ASA_WAYBACK_TEST_URL=https://www.wikipedia.org
#   ASA_WAYBACK_TEST_AFTER=2020-01-01
#   ASA_WAYBACK_TEST_BEFORE=2020-12-31

truthy <- function(value) {
  tolower(trimws(value %||% "")) %in% c("1", "true", "t", "yes", "y", "on")
}

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0L) {
    return(y)
  }
  if (length(x) == 1L && is.na(x)) {
    return(y)
  }
  x
}

if (!truthy(Sys.getenv("ASA_RUN_WAYBACK_LIVE_TESTS", unset = ""))) {
  message("Skipping live Wayback fallback test; set ASA_RUN_WAYBACK_LIVE_TESTS=true to run it.")
  quit(status = 0L)
}

fail <- function(...) {
  message("[FAIL] ", paste0(..., collapse = ""))
  quit(status = 1L)
}

check <- function(condition, ...) {
  if (!isTRUE(condition)) {
    fail(...)
  }
}

cat("=== OpenCode Wayback Research Fallback Live Test ===\n\n")

loaded_dev <- FALSE
for (path in c("asa", ".", file.path(getwd(), "asa"), file.path(dirname(getwd()), "asa"))) {
  desc <- file.path(path, "DESCRIPTION")
  if (file.exists(desc)) {
    tryCatch({
      devtools::load_all(path, quiet = TRUE)
      cat(sprintf("Loaded development package from: %s\n", normalizePath(path, winslash = "/")))
      loaded_dev <- TRUE
    }, error = function(e) {
      NULL
    })
    if (loaded_dev) {
      break
    }
  }
}
if (!loaded_dev) {
  suppressPackageStartupMessages(library(asa))
  cat("Loaded installed asa package\n")
}

if (!requireNamespace("reticulate", quietly = TRUE)) {
  fail("reticulate is required for this manual test.")
}

suppressWarnings(
  try(reticulate::use_condaenv("asa_env", required = FALSE), silent = TRUE)
)
if (!reticulate::py_available(initialize = TRUE)) {
  fail("Python is not available through reticulate. Run this from the asa_env conda environment.")
}

python_candidates <- c(
  file.path(getwd(), "asa", "inst", "python"),
  file.path(getwd(), "inst", "python"),
  file.path(dirname(getwd()), "asa", "inst", "python"),
  system.file("python", package = "asa")
)
python_candidates <- unique(normalizePath(python_candidates, winslash = "/", mustWork = FALSE))
python_path <- ""
for (path in python_candidates) {
  if (
    dir.exists(path) &&
      file.exists(file.path(path, "workflows", "research_graph_workflow.py")) &&
      file.exists(file.path(path, "tools", "archive_wayback_tool.py"))
  ) {
    python_path <- path
    break
  }
}
if (!nzchar(python_path)) {
  fail("Could not locate ASA Python sources. Tried: ", paste(python_candidates, collapse = ", "))
}
cat(sprintf("Python path: %s\n", python_path))

for (module in c("langchain_core", "langgraph", "pydantic", "requests")) {
  if (!reticulate::py_module_available(module)) {
    fail("Missing Python module in active environment: ", module)
  }
}

research_graph <- reticulate::import_from_path(
  "workflows.research_graph_workflow",
  path = python_path,
  convert = FALSE
)
wayback_tool <- reticulate::import_from_path(
  "tools.archive_wayback_tool",
  path = python_path,
  convert = FALSE
)

date_after <- Sys.getenv("ASA_WAYBACK_TEST_AFTER", unset = "2020-01-01")
date_before <- Sys.getenv("ASA_WAYBACK_TEST_BEFORE", unset = "2020-12-31")
override_url <- Sys.getenv("ASA_WAYBACK_TEST_URL", unset = "")
candidate_urls <- if (nzchar(trimws(override_url))) {
  trimws(strsplit(override_url, ",", fixed = TRUE)[[1]])
} else {
  c("https://www.wikipedia.org", "https://www.wikipedia.org/", "https://example.com/")
}
candidate_urls <- candidate_urls[nzchar(candidate_urls)]

within_range <- function(date_text) {
  dt <- as.Date(date_text)
  if (is.na(dt)) {
    return(FALSE)
  }
  after <- as.Date(date_after)
  before <- as.Date(date_before)
  if (!is.na(after) && dt < after) {
    return(FALSE)
  }
  if (!is.na(before) && dt >= before) {
    return(FALSE)
  }
  TRUE
}

cat(sprintf("Wayback date range: after=%s before=%s\n", date_after, date_before))
selected_url <- ""
selected_snapshot <- NULL
for (url in candidate_urls) {
  cat(sprintf("Checking Wayback CDX snapshots for %s ...\n", url))
  snapshots <- tryCatch(
    reticulate::py_to_r(wayback_tool$find_snapshots_in_range(
      url = url,
      after_date = date_after,
      before_date = date_before,
      limit = 5L
    )),
    error = function(e) {
      cat(sprintf("  CDX lookup failed: %s\n", conditionMessage(e)))
      list()
    }
  )
  if (length(snapshots) == 0L) {
    next
  }
  for (snapshot in snapshots) {
    snap_date <- as.character(snapshot$date %||% "")
    snap_url <- as.character(snapshot$wayback_url %||% "")
    if (within_range(snap_date) && grepl("^https://web\\.archive\\.org/web/", snap_url)) {
      selected_url <- url
      selected_snapshot <- snapshot
      break
    }
  }
  if (nzchar(selected_url)) {
    break
  }
}

if (!nzchar(selected_url) || is.null(selected_snapshot)) {
  fail(
    "No usable live Wayback snapshot found for candidates ",
    paste(candidate_urls, collapse = ", "),
    " in range [", date_after, ", ", date_before, ")."
  )
}

cat(sprintf("Selected URL: %s\n", selected_url))
cat(sprintf("Selected snapshot: %s %s\n\n", selected_snapshot$date, selected_snapshot$wayback_url))

py <- reticulate::py
py$asa_wayback_test_url <- selected_url
py$asa_wayback_test_json <- as.character(jsonlite::toJSON(
  list(list(name = "Wayback Fixture", source_url = selected_url)),
  auto_unbox = TRUE
))
py$rg_mod <- research_graph
reticulate::py_run_string("
from langchain_core.messages import AIMessage
from langchain_core.tools import tool

wayback_search_calls = 0

@tool('Search')
def wayback_live_search(query: str) -> str:
    \"\"\"Deterministic search tool for the live Wayback fallback test.\"\"\"
    global wayback_search_calls
    wayback_search_calls += 1
    return (
        'Title: Wayback fallback fixture\\n'
        f'URL: {asa_wayback_test_url}\\n'
        'Snippet: This fixture intentionally relies on the source URL. '
        'The date verifier is forced to return undetermined.'
    )

class _WaybackFallbackLLM:
    def __init__(self):
        self.n = 0

    def bind_tools(self, tools):
        return self

    def invoke(self, messages):
        self.n += 1
        if self.n == 1:
            return AIMessage(
                content='calling search',
                tool_calls=[{
                    'name': 'Search',
                    'args': {'query': 'wayback fallback fixture'},
                    'id': 'call_wayback_live_1',
                }],
            )
        return AIMessage(content=asa_wayback_test_json)

def make_wayback_live_llm():
    return _WaybackFallbackLLM()

def _asa_wayback_live_undetermined(url, date_after=None, date_before=None, html_content=None, config=None):
    return {
        'url': url,
        'passes': None,
        'reason': 'forced_undetermined_for_wayback_live_test',
    }

rg_mod.verify_date_constraint = _asa_wayback_live_undetermined
")

make_config <- function(use_wayback) {
  research_graph$ResearchConfig(
    max_workers = 1L,
    max_rounds = 1L,
    budget_queries = 3L,
    budget_tokens = 10000L,
    budget_time_sec = 60L,
    target_items = 1L,
    use_wikidata = FALSE,
    use_web = TRUE,
    use_wikipedia = FALSE,
    max_tool_calls_per_round = 1L,
    date_after = date_after,
    date_before = date_before,
    temporal_strictness = "strict",
    use_wayback = isTRUE(use_wayback)
  )
}

make_state <- function(use_wayback) {
  list(
    wikidata_type = NULL,
    query = "Wayback fallback live fixture",
    schema = list(name = "character"),
    config = list(
      max_workers = 1L,
      target_items = 1L,
      use_web = TRUE,
      use_wikidata = FALSE,
      use_wikipedia = FALSE,
      date_after = date_after,
      date_before = date_before,
      temporal_strictness = "strict",
      use_wayback = isTRUE(use_wayback)
    ),
    seen_hashes = list(),
    plan = list(search_queries = list("wayback fallback fixture")),
    results = list(),
    novelty_history = list(),
    round_number = 0L,
    queries_used = 0L,
    tokens_used = 0L,
    input_tokens = 0L,
    output_tokens = 0L,
    start_time = as.numeric(Sys.time()),
    elapsed_carry_sec = 0
  )
}

rows_from <- function(output) {
  rows <- output$new_results %||% list()
  if (is.data.frame(rows)) {
    return(split(rows, seq_len(nrow(rows))))
  }
  rows
}

cat("Running positive fallback case with use_wayback=TRUE ...\n")
positive_searcher <- research_graph$create_searcher_node(
  llm = py$make_wayback_live_llm(),
  tools = list(py$wayback_live_search),
  wikidata_tool = NULL,
  research_config = make_config(TRUE)
)
positive <- reticulate::py_to_r(positive_searcher(make_state(TRUE)))
positive_rows <- rows_from(positive)

check(length(positive_rows) == 1L, "Expected exactly one positive result, got ", length(positive_rows), ".")
positive_url <- as.character(positive_rows[[1]]$source_url %||% "")
positive_confidence <- suppressWarnings(as.numeric(positive_rows[[1]]$confidence %||% NA_real_))
check(
  grepl("^https://web\\.archive\\.org/web/", positive_url),
  "Expected positive result source_url to be rewritten to a Wayback URL, got: ",
  positive_url
)
check(
  !is.na(positive_confidence) && positive_confidence > 0.6 && positive_confidence <= 0.9,
  "Expected positive confidence to be boosted above 0.6 and capped at 0.9, got: ",
  positive_confidence
)
cat(sprintf("Positive source_url: %s\n", positive_url))
cat(sprintf("Positive confidence: %.2f\n\n", positive_confidence))

cat("Running negative control with use_wayback=FALSE ...\n")
negative_searcher <- research_graph$create_searcher_node(
  llm = py$make_wayback_live_llm(),
  tools = list(py$wayback_live_search),
  wikidata_tool = NULL,
  research_config = make_config(FALSE)
)
negative <- reticulate::py_to_r(negative_searcher(make_state(FALSE)))
negative_rows <- rows_from(negative)
check(length(negative_rows) == 0L, "Expected negative control to drop the undetermined web result.")

cat("\n[PASS] Live Wayback fallback accepted an archived snapshot only when use_wayback=TRUE.\n")
quit(status = 0L)
