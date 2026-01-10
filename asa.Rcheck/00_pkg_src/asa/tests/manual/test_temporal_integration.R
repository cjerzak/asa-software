# test_temporal_integration.R
# Manual integration tests for temporal filtering functionality
#
# Run with: Rscript tests/manual/test_temporal_integration.R
#
# Prerequisites:
# - conda environment "asa_env" with required packages
# - OPENAI_API_KEY environment variable (for LLM-based tests)
# - Internet connection (for Wikidata/Wayback API tests)

# Try to load development version first, fall back to installed
loaded_dev <- FALSE
dev_paths <- c(
  ".",
  "..",
  "asa",
  file.path(getwd(), "asa"),
  file.path(dirname(getwd()), "asa")
)
for (p in dev_paths) {
  if (file.exists(file.path(p, "DESCRIPTION"))) {
    tryCatch({
      devtools::load_all(p)
      cat(sprintf("  Loaded development version from: %s\n", normalizePath(p)))
      loaded_dev <- TRUE
      break
    }, error = function(e) NULL)
  }
}
if (!loaded_dev) {
  library(asa)
  cat("  Loaded installed version of asa\n")
}
library(reticulate)

# Null coalescing helper
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x)) y else x

cat("=== Temporal Filtering Integration Tests ===\n\n")

# Track test results
test_results <- list(passed = 0, failed = 0, skipped = 0)

run_test <- function(name, test_fn) {
  cat(sprintf("\n[TEST] %s\n", name))
  tryCatch({
    result <- test_fn()
    if (isTRUE(result)) {
      cat("  [PASS]\n")
      test_results$passed <<- test_results$passed + 1
    } else if (is.null(result)) {
      cat("  [SKIP]\n")
      test_results$skipped <<- test_results$skipped + 1
    } else {
      cat(sprintf("  [FAIL] %s\n", result %||% "Unknown error"))
      test_results$failed <<- test_results$failed + 1
    }
  }, error = function(e) {
    cat(sprintf("  [ERROR] %s\n", conditionMessage(e)))
    test_results$failed <<- test_results$failed + 1
  })
}

# ────────────────────────────────────────────────────────────────────────
# Setup
# ────────────────────────────────────────────────────────────────────────
cat("Setting up environment...\n")

tryCatch({
  use_condaenv("asa_env", required = TRUE)
  cat("  Conda environment: asa_env\n")
}, error = function(e) {
  cat(sprintf("  [WARN] Could not activate conda env: %s\n", conditionMessage(e)))
  cat("  Attempting to continue with current Python...\n")
})

# Check multiple locations for Python modules
python_paths <- c(
  # Development paths (run from asa/ or asa-software/)
  file.path(getwd(), "inst/python"),
  file.path(dirname(getwd()), "asa/inst/python"),
  file.path(getwd(), "asa/inst/python"),
  # Installed package path
  system.file("python", package = "asa")
)

python_path <- NULL
for (p in python_paths) {
  if (dir.exists(p) && file.exists(file.path(p, "date_extractor.py"))) {
    python_path <- p
    break
  }
}

if (is.null(python_path)) {
  stop("Python modules not found. Tried: ", paste(python_paths, collapse = ", "))
}
cat(sprintf("  Python path: %s\n", python_path))

# Import modules
date_extractor <- import_from_path("date_extractor", path = python_path)
wayback_tool <- import_from_path("wayback_tool", path = python_path)
wikidata_tool <- import_from_path("wikidata_tool", path = python_path)

cat("  Modules loaded successfully.\n")

# ────────────────────────────────────────────────────────────────────────
# Test 1: Date Extraction from Mock HTML (JSON-LD)
# ────────────────────────────────────────────────────────────────────────
run_test("Date extraction from JSON-LD structured data", function() {
  mock_html <- '
    <html>
    <head>
      <script type="application/ld+json">
        {"@type": "NewsArticle", "datePublished": "2023-08-15T10:30:00Z", "headline": "Test Article"}
      </script>
    </head>
    <body><article>Test content</article></body>
    </html>
  '

  result <- date_extractor$extract_publication_date(
    url = "https://example.com/article",
    html_content = mock_html
  )

  cat(sprintf("  Extracted date: %s\n", result$date %||% "None"))
  cat(sprintf("  Confidence: %.2f\n", result$confidence))
  cat(sprintf("  Source: %s\n", result$source))

  if (result$date == "2023-08-15" && result$confidence >= 0.9) {
    return(TRUE)
  }
  return(sprintf("Expected 2023-08-15 with high confidence, got %s (%.2f)",
                 result$date, result$confidence))
})

# ────────────────────────────────────────────────────────────────────────
# Test 2: Date Extraction from Mock HTML (Meta Tags)
# ────────────────────────────────────────────────────────────────────────
run_test("Date extraction from meta tags", function() {
  mock_html <- '
    <html>
    <head>
      <meta property="article:published_time" content="2023-05-20T14:00:00+00:00">
      <meta property="og:title" content="Test Page">
    </head>
    <body><p>Content</p></body>
    </html>
  '

  result <- date_extractor$extract_publication_date(
    url = "https://example.com/page",
    html_content = mock_html
  )

  cat(sprintf("  Extracted date: %s\n", result$date %||% "None"))
  cat(sprintf("  Confidence: %.2f\n", result$confidence))
  cat(sprintf("  Source: %s\n", result$source))

  if (result$date == "2023-05-20" && result$confidence >= 0.85) {
    return(TRUE)
  }
  return(sprintf("Expected 2023-05-20 with >= 0.85 confidence, got %s (%.2f)",
                 result$date, result$confidence))
})

# ────────────────────────────────────────────────────────────────────────
# Test 3: Date Constraint Verification
# ────────────────────────────────────────────────────────────────────────
run_test("Date constraint verification (pass case)", function() {
  mock_html <- '
    <html>
    <head>
      <script type="application/ld+json">
        {"@type": "Article", "datePublished": "2023-06-15"}
      </script>
    </head>
    </html>
  '

  result <- date_extractor$verify_date_constraint(
    url = "https://example.com/article",
    date_after = "2023-01-01",
    date_before = "2024-01-01",
    html_content = mock_html
  )

  cat(sprintf("  Passes: %s\n", result$passes))
  cat(sprintf("  Reason: %s\n", result$reason))

  if (isTRUE(result$passes)) {
    return(TRUE)
  }
  return(sprintf("Expected passes=TRUE, got %s (%s)", result$passes, result$reason))
})

run_test("Date constraint verification (fail case)", function() {
  mock_html <- '
    <html>
    <head>
      <script type="application/ld+json">
        {"@type": "Article", "datePublished": "2022-06-15"}
      </script>
    </head>
    </html>
  '

  result <- date_extractor$verify_date_constraint(
    url = "https://example.com/old-article",
    date_after = "2023-01-01",
    date_before = NULL,
    html_content = mock_html
  )

  cat(sprintf("  Passes: %s\n", result$passes))
  cat(sprintf("  Reason: %s\n", result$reason))

  if (isFALSE(result$passes)) {
    return(TRUE)
  }
  return(sprintf("Expected passes=FALSE for old article, got %s", result$passes))
})

# ────────────────────────────────────────────────────────────────────────
# Test 4: Wayback Machine Availability Check (Live API)
# ────────────────────────────────────────────────────────────────────────
run_test("Wayback Machine availability check (live API)", function() {
  cat("  Checking archive availability for google.com...\n")

  result <- wayback_tool$check_availability("https://www.google.com")

  if (!is.null(result)) {
    cat(sprintf("  Archived: YES\n"))
    cat(sprintf("  Snapshot timestamp: %s\n", result$timestamp %||% "N/A"))
    cat(sprintf("  Wayback URL: %s\n", substr(result$url %||% "", 1, 60)))
    return(TRUE)
  }
  return("No snapshot found (API may be unavailable)")
})

# ────────────────────────────────────────────────────────────────────────
# Test 5: Wayback CDX Search with Date Range (Live API)
# ────────────────────────────────────────────────────────────────────────
run_test("Wayback CDX search with date range (live API)", function() {
  cat("  Searching for wikipedia.org snapshots in 2020...\n")

  snapshots <- wayback_tool$find_snapshots_in_range(
    url = "https://www.wikipedia.org",
    after_date = "2020-01-01",
    before_date = "2020-12-31",
    limit = 5L
  )

  cat(sprintf("  Snapshots found: %d\n", length(snapshots)))

  if (length(snapshots) > 0) {
    cat("  Sample snapshots:\n")
    for (i in seq_len(min(3, length(snapshots)))) {
      snap <- snapshots[[i]]
      cat(sprintf("    %d. %s\n", i, snap$date %||% "unknown date"))
    }
    return(TRUE)
  }
  return("No snapshots found in date range")
})

# ────────────────────────────────────────────────────────────────────────
# Test 6: Wayback find_snapshots_before (Live API)
# ────────────────────────────────────────────────────────────────────────
run_test("Wayback find_snapshots_before (live API)", function() {
  cat("  Finding snapshots before 2015-01-01...\n")

  snapshots <- wayback_tool$find_snapshots_before(
    url = "https://www.nytimes.com",
    before_date = "2015-01-01",
    limit = 5L
  )

  cat(sprintf("  Snapshots found: %d\n", length(snapshots)))

  if (length(snapshots) > 0) {
    cat(sprintf("  Most recent pre-2015 snapshot: %s\n",
                snapshots[[1]]$date %||% "unknown"))
    return(TRUE)
  }
  return("No pre-2015 snapshots found")
})

# ────────────────────────────────────────────────────────────────────────
# Test 7: Wikidata SPARQL Filter Generation
# ────────────────────────────────────────────────────────────────────────
run_test("Wikidata SPARQL temporal filter generation", function() {
  filter <- wikidata_tool$`_build_temporal_filter`("2020-01-01", "2024-01-01")

  cat(sprintf("  Filter length: %d chars\n", nchar(filter)))
  cat(sprintf("  Contains FILTER: %s\n", grepl("FILTER", filter)))
  cat(sprintf("  Contains xsd:dateTime: %s\n", grepl("xsd:dateTime", filter)))

  if (grepl("FILTER", filter) && grepl("2020-01-01", filter) && grepl("2024-01-01", filter)) {
    return(TRUE)
  }
  return("Filter missing expected components")
})

# ────────────────────────────────────────────────────────────────────────
# Test 8: Wikidata Tool with Temporal Parameters (Live API)
# ────────────────────────────────────────────────────────────────────────
run_test("Wikidata tool with temporal parameters (live API)", function() {
  cat("  Creating tool with date_after=2023-01-01...\n")

  tool <- wikidata_tool$create_wikidata_tool(
    date_after = "2023-01-01",
    date_before = NULL
  )

  cat(sprintf("  Tool name: %s\n", tool$name))
  cat(sprintf("  date_after: %s\n", tool$date_after %||% "NULL"))

  if (tool$date_after == "2023-01-01") {
    return(TRUE)
  }
  return(sprintf("date_after not set correctly: %s", tool$date_after))
})

# ────────────────────────────────────────────────────────────────────────
# Test 9: Wikidata Query Execution (Live API)
# ────────────────────────────────────────────────────────────────────────
run_test("Wikidata entity query execution (live API)", function() {
  cat("  Querying US states from Wikidata...\n")

  tool <- wikidata_tool$WikidataSearchTool()
  result <- tool$`_run`("entity_type:us_states")

  # Count result lines
  lines <- strsplit(result, "\n")[[1]]
  result_lines <- grep("^\\d+\\.", lines, value = TRUE)

  cat(sprintf("  Results: %d items\n", length(result_lines)))

  if (length(result_lines) >= 45) {  # Should have ~50 states
    cat("  Sample results:\n")
    for (i in seq_len(min(3, length(result_lines)))) {
      cat(sprintf("    %s\n", result_lines[i]))
    }
    return(TRUE)
  }
  return(sprintf("Expected >= 45 states, got %d", length(result_lines)))
})

# ────────────────────────────────────────────────────────────────────────
# Test 10: Full asa_enumerate with Temporal (requires OPENAI_API_KEY)
# ────────────────────────────────────────────────────────────────────────
run_test("asa_enumerate with temporal filtering (requires API key)", function() {
  if (Sys.getenv("OPENAI_API_KEY") == "") {
    cat("  Skipping - OPENAI_API_KEY not set\n")
    return(NULL)  # Skip
  }

  cat("  Initializing agent...\n")
  agent <- tryCatch({
    initialize_agent(
      backend = "openai",
      model = "gpt-4.1-mini",
      conda_env = "asa_env",
      verbose = FALSE
    )
  }, error = function(e) {
    cat(sprintf("  Agent init failed: %s\n", conditionMessage(e)))
    return(NULL)
  })

  if (is.null(agent)) {
    return("Failed to initialize agent")
  }

  cat("  Running enumeration with temporal filter...\n")
  result <- tryCatch({
    asa_enumerate(
      query = "Find 3 US states",
      schema = c(name = "character", capital = "character"),
      temporal = list(
        time_filter = "y"  # past year
      ),
      max_workers = 1L,
      max_rounds = 2L,
      budget = list(queries = 5L, tokens = 10000L, time_sec = 60L),
      stop_policy = list(target_items = 3L),
      sources = list(web = FALSE, wikipedia = TRUE, wikidata = TRUE),
      progress = FALSE,
      checkpoint = FALSE,
      verbose = FALSE,
      agent = agent
    )
  }, error = function(e) {
    cat(sprintf("  Enumeration failed: %s\n", conditionMessage(e)))
    return(NULL)
  })

  if (is.null(result)) {
    return("Enumeration returned NULL")
  }

  cat(sprintf("  Status: %s\n", result$status))
  cat(sprintf("  Items found: %d\n", nrow(result$data)))
  cat(sprintf("  Stop reason: %s\n", result$stop_reason %||% "N/A"))

  if (nrow(result$data) > 0) {
    cat("  Results:\n")
    print(head(result$data, 3))
    return(TRUE)
  }
  return(sprintf("No results returned (status: %s)", result$status))
})

# ────────────────────────────────────────────────────────────────────────
# Test 11: R Validation - Valid temporal parameters
# ────────────────────────────────────────────────────────────────────────
run_test("R validation accepts valid temporal parameters", function() {
  # This should NOT error during config creation
  config <- tryCatch({
    asa:::.create_research_config(
      max_workers = 2,
      max_rounds = 5,
      budget = list(queries = 10),
      stop_policy = list(),
      sources = list(web = TRUE),
      temporal = list(
        time_filter = "m",
        after = "2023-01-01",
        before = "2024-06-15",
        strictness = "strict",
        use_wayback = TRUE
      )
    )
  }, error = function(e) {
    return(list(error = conditionMessage(e)))
  })

  if (!is.null(config$error)) {
    return(sprintf("Unexpected error: %s", config$error))
  }

  cat(sprintf("  time_filter: %s\n", config$time_filter))
  cat(sprintf("  date_after: %s\n", config$date_after))
  cat(sprintf("  date_before: %s\n", config$date_before))
  cat(sprintf("  temporal_strictness: %s\n", config$temporal_strictness))
  cat(sprintf("  use_wayback: %s\n", config$use_wayback))

  if (config$time_filter == "m" &&
      config$date_after == "2023-01-01" &&
      config$date_before == "2024-06-15" &&
      config$temporal_strictness == "strict" &&
      config$use_wayback == TRUE) {
    return(TRUE)
  }
  return("Config values don't match expected")
})

# ────────────────────────────────────────────────────────────────────────
# Test 12: R Validation - Invalid temporal parameters
# ────────────────────────────────────────────────────────────────────────
run_test("R validation rejects invalid temporal parameters", function() {
  # Test invalid time_filter
  err1 <- tryCatch({
    asa:::.create_research_config(
      max_workers = 2, max_rounds = 5,
      budget = list(), stop_policy = list(), sources = list(),
      temporal = list(time_filter = "x")
    )
    return(NULL)  # Should have errored
  }, error = function(e) conditionMessage(e))

  cat(sprintf("  Invalid time_filter error: %s\n",
              if(is.null(err1)) "MISSING" else "caught"))

  # Test invalid date format
  err2 <- tryCatch({
    asa:::.create_research_config(
      max_workers = 2, max_rounds = 5,
      budget = list(), stop_policy = list(), sources = list(),
      temporal = list(after = "not-a-date")
    )
    return(NULL)
  }, error = function(e) conditionMessage(e))

  cat(sprintf("  Invalid date error: %s\n",
              if(is.null(err2)) "MISSING" else "caught"))

  # Test invalid strictness
  err3 <- tryCatch({
    asa:::.create_research_config(
      max_workers = 2, max_rounds = 5,
      budget = list(), stop_policy = list(), sources = list(),
      temporal = list(strictness = "very_strict")
    )
    return(NULL)
  }, error = function(e) conditionMessage(e))

  cat(sprintf("  Invalid strictness error: %s\n",
              if(is.null(err3)) "MISSING" else "caught"))

  if (!is.null(err1) && !is.null(err2) && !is.null(err3)) {
    return(TRUE)
  }
  return("Some validation errors were not caught")
})

# ────────────────────────────────────────────────────────────────────────
# Summary
# ────────────────────────────────────────────────────────────────────────
cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""))
cat("\n")
cat(sprintf("SUMMARY: %d passed, %d failed, %d skipped\n",
            test_results$passed, test_results$failed, test_results$skipped))
cat("=" |> rep(60) |> paste(collapse = ""))
cat("\n")

if (test_results$failed > 0) {
  cat("\n[!] Some tests failed. Review output above for details.\n")
  quit(status = 1)
} else {
  cat("\n[OK] All tests passed!\n")
  quit(status = 0)
}
