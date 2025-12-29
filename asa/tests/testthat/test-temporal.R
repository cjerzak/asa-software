# test-temporal.R
# Tests for temporal filtering functionality across Python modules and R interface

# ============================================================================
# Helper: Load Python Modules
# ============================================================================

.get_python_path <- function() {
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

.skip_if_no_python <- function() {
  python_path <- .get_python_path()
  if (!dir.exists(python_path)) {
    skip("Python modules not found")
  }
  if (!reticulate::py_available(initialize = FALSE)) {
    skip("Python not available")
  }
  python_path
}

# ============================================================================
# R Interface Validation Tests (research.R)
# ============================================================================

test_that("asa_enumerate validates temporal$time_filter", {

  # These tests require API key because asa_enumerate validates API key before temporal params

  skip_if(Sys.getenv("OPENAI_API_KEY") == "", "OPENAI_API_KEY not set")

  # Invalid time filter values
  expect_error(
    asa_enumerate(query = "test", temporal = list(time_filter = "x")),
    regexp = "time_filter"
  )

  expect_error(
    asa_enumerate(query = "test", temporal = list(time_filter = "day")),
    regexp = "time_filter"
  )

  expect_error(
    asa_enumerate(query = "test", temporal = list(time_filter = "year")),
    regexp = "time_filter"
  )

  expect_error(
    asa_enumerate(query = "test", temporal = list(time_filter = 123)),
    regexp = "time_filter"
  )
})

test_that("asa_enumerate validates temporal$after date format", {
  skip_if(Sys.getenv("OPENAI_API_KEY") == "", "OPENAI_API_KEY not set")

  # Invalid month
  expect_error(
    asa_enumerate(query = "test", temporal = list(after = "2024-13-01")),
    regexp = "after.*valid|after.*ISO"
  )

  # Invalid format (not ISO 8601)
  expect_error(
    asa_enumerate(query = "test", temporal = list(after = "January 1, 2024")),
    regexp = "after.*valid|after.*ISO"
  )

  # Random string
  expect_error(
    asa_enumerate(query = "test", temporal = list(after = "not-a-date")),
    regexp = "after.*valid|after.*ISO"
  )

  # Note: Slash format "2024/01/15" is accepted by R's as.Date(), so no error
})

test_that("asa_enumerate validates temporal$before date format", {
  skip_if(Sys.getenv("OPENAI_API_KEY") == "", "OPENAI_API_KEY not set")

  # Invalid day
  expect_error(
    asa_enumerate(query = "test", temporal = list(before = "2024-02-30")),
    regexp = "before.*valid|before.*ISO"
  )

  # Invalid month
  expect_error(
    asa_enumerate(query = "test", temporal = list(before = "2024-00-15")),
    regexp = "before.*valid|before.*ISO"
  )

  # Note: Slash format "2024/01/15" is accepted by R's as.Date(), so no error
})

test_that("asa_enumerate validates temporal$strictness", {
  skip_if(Sys.getenv("OPENAI_API_KEY") == "", "OPENAI_API_KEY not set")

  expect_error(
    asa_enumerate(query = "test", temporal = list(strictness = "very_strict")),
    regexp = "strictness"
  )

  expect_error(
    asa_enumerate(query = "test", temporal = list(strictness = "relaxed")),
    regexp = "strictness"
  )

  expect_error(
    asa_enumerate(query = "test", temporal = list(strictness = "none")),
    regexp = "strictness"
  )
})

test_that(".create_research_config includes temporal parameters", {
  config <- asa:::.create_research_config(
    workers = 2,
    max_rounds = 5,
    budget = list(queries = 25),
    stop_policy = list(),
    sources = list(web = TRUE),
    temporal = list(
      time_filter = "w",
      after = "2023-06-01",
      before = "2024-01-01",
      strictness = "strict",
      use_wayback = TRUE
    )
  )

  expect_equal(config$time_filter, "w")
  expect_equal(config$date_after, "2023-06-01")
  expect_equal(config$date_before, "2024-01-01")
  expect_equal(config$temporal_strictness, "strict")
  expect_true(config$use_wayback)
})

test_that(".create_research_config handles NULL temporal", {
  config <- asa:::.create_research_config(
    workers = 2,
    max_rounds = 5,
    budget = list(),
    stop_policy = list(),
    sources = list(),
    temporal = NULL
  )

  expect_null(config$time_filter)
  expect_null(config$date_after)
  expect_null(config$date_before)
})

test_that(".create_research_config handles partial temporal", {
  # Only time_filter
  config1 <- asa:::.create_research_config(
    workers = 2, max_rounds = 5,
    budget = list(), stop_policy = list(), sources = list(),
    temporal = list(time_filter = "m")
  )
  expect_equal(config1$time_filter, "m")
  expect_null(config1$date_after)

  # Only after date
  config2 <- asa:::.create_research_config(
    workers = 2, max_rounds = 5,
    budget = list(), stop_policy = list(), sources = list(),
    temporal = list(after = "2020-01-01")
  )
  expect_null(config2$time_filter)
  expect_equal(config2$date_after, "2020-01-01")
})

test_that(".create_research_config defaults strictness to best_effort", {
  config <- asa:::.create_research_config(
    workers = 2, max_rounds = 5,
    budget = list(), stop_policy = list(), sources = list(),
    temporal = list(after = "2020-01-01")  # No strictness specified
  )

  expect_equal(config$temporal_strictness, "best_effort")
})

test_that(".create_research_config defaults use_wayback to FALSE", {
  config <- asa:::.create_research_config(
    workers = 2, max_rounds = 5,
    budget = list(), stop_policy = list(), sources = list(),
    temporal = list(after = "2020-01-01")  # No use_wayback specified
  )

  expect_false(config$use_wayback)
})

# ============================================================================
# Date Extractor Tests (date_extractor.py)
# ============================================================================

test_that("_parse_date_string handles ISO 8601 formats", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  # ISO 8601 with Z suffix
  expect_equal(date_extractor$`_parse_date_string`("2024-01-15T10:30:00Z"), "2024-01-15")

  # Simple ISO date
  expect_equal(date_extractor$`_parse_date_string`("2024-01-15"), "2024-01-15")

  # ISO 8601 without timezone
  expect_equal(date_extractor$`_parse_date_string`("2024-06-20T14:30:00"), "2024-06-20")
})

test_that("_parse_date_string handles human-readable formats", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  # Full month name
  expect_equal(date_extractor$`_parse_date_string`("January 15, 2024"), "2024-01-15")

  # Abbreviated month
  expect_equal(date_extractor$`_parse_date_string`("Jan 15, 2024"), "2024-01-15")

  # Day-first format
  expect_equal(date_extractor$`_parse_date_string`("15 January 2024"), "2024-01-15")
})

test_that("_parse_date_string handles edge cases", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  # Empty string
  result <- date_extractor$`_parse_date_string`("")
  expect_true(is.null(result) || is.na(result))

  # Invalid dates should return NULL
  result <- date_extractor$`_parse_date_string`("not a date")
  expect_true(is.null(result) || is.na(result))

  # Year-only extraction (returns YYYY-01-01)
  result <- date_extractor$`_parse_date_string`("Published in 2023")
  expect_equal(result, "2023-01-01")
})

test_that("_extract_from_url extracts dates from URL patterns", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  # YYYY/MM/DD pattern
  result <- date_extractor$`_extract_from_url`("https://example.com/2024/01/15/article")
  expect_true(length(result) > 0)
  expect_equal(result[[1]]$date, "2024-01-15")
  expect_equal(result[[1]]$source, "url_pattern_ymd")

  # YYYY/MM pattern (day defaults to 01)
  result <- date_extractor$`_extract_from_url`("https://example.com/2024/03/news")
  expect_true(length(result) > 0)
  expect_equal(result[[1]]$date, "2024-03-01")

  # No date in URL
  result <- date_extractor$`_extract_from_url`("https://example.com/about")
  expect_equal(length(result), 0)
})

test_that("_extract_from_url handles ISO date pattern in URL", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  result <- date_extractor$`_extract_from_url`("https://example.com/post-2024-01-15-title")
  expect_true(length(result) > 0)
  expect_equal(result[[1]]$date, "2024-01-15")
})

test_that("_extract_from_url handles boundary dates", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  # Leap year February 29 (valid)
  result <- date_extractor$`_extract_from_url`("https://example.com/2024/02/29/leap")
  expect_true(length(result) > 0)
  expect_equal(result[[1]]$date, "2024-02-29")

  # December 31 (valid)
  result <- date_extractor$`_extract_from_url`("https://example.com/2023/12/31/news")
  expect_true(length(result) > 0)
  expect_equal(result[[1]]$date, "2023-12-31")
})

test_that("verify_date_constraint validates after constraint", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  mock_html <- '
    <html>
    <head>
      <script type="application/ld+json">
        {"@type": "Article", "datePublished": "2023-06-15"}
      </script>
    </head>
    <body></body>
    </html>
  '

  # Date passes "after" constraint (2023-06-15 > 2023-01-01)
  result <- date_extractor$verify_date_constraint(
    url = "https://example.com/article",
    date_after = "2023-01-01",
    date_before = NULL,
    html_content = mock_html
  )
  expect_true(result$passes)
  expect_equal(result$extracted_date, "2023-06-15")

  # Date fails "after" constraint (2023-06-15 < 2024-01-01)
  result <- date_extractor$verify_date_constraint(
    url = "https://example.com/article",
    date_after = "2024-01-01",
    date_before = NULL,
    html_content = mock_html
  )
  expect_false(result$passes)
})

test_that("verify_date_constraint validates before constraint", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  mock_html <- '
    <html>
    <head>
      <script type="application/ld+json">
        {"@type": "Article", "datePublished": "2023-06-15"}
      </script>
    </head>
    <body></body>
    </html>
  '

  # Date passes "before" constraint (2023-06-15 < 2024-01-01)
  result <- date_extractor$verify_date_constraint(
    url = "https://example.com/article",
    date_after = NULL,
    date_before = "2024-01-01",
    html_content = mock_html
  )
  expect_true(result$passes)

  # Date fails "before" constraint (2023-06-15 >= 2023-01-01)
  result <- date_extractor$verify_date_constraint(
    url = "https://example.com/article",
    date_after = NULL,
    date_before = "2023-01-01",
    html_content = mock_html
  )
  expect_false(result$passes)
})

test_that("verify_date_constraint handles no date found", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  mock_html <- '<html><body><p>No date here</p></body></html>'

  result <- date_extractor$verify_date_constraint(
    url = "https://example.com/nodates",
    date_after = "2023-01-01",
    html_content = mock_html
  )

  # Should be undetermined (NULL passes)
  expect_true(is.null(result$passes) || is.na(result$passes))
  expect_equal(result$reason, "no_date_found")
})

test_that("_extract_from_json_ld extracts datePublished", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)
  bs4 <- reticulate::import("bs4")

  html <- '
    <html>
    <head>
      <script type="application/ld+json">
        {"@type": "NewsArticle", "datePublished": "2023-08-20T10:00:00Z", "dateModified": "2023-08-21T14:00:00Z"}
      </script>
    </head>
    </html>
  '
  soup <- bs4$BeautifulSoup(html, "html.parser")
  results <- date_extractor$`_extract_from_json_ld`(soup)

  expect_true(length(results) >= 1)
  # Find the datePublished result
  dates <- sapply(results, function(r) r$date)
  expect_true("2023-08-20" %in% dates)
})

test_that("_extract_from_meta_tags extracts article:published_time", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)
  bs4 <- reticulate::import("bs4")

  html <- '
    <html>
    <head>
      <meta property="article:published_time" content="2023-05-10T09:00:00+00:00">
    </head>
    </html>
  '
  soup <- bs4$BeautifulSoup(html, "html.parser")
  results <- date_extractor$`_extract_from_meta_tags`(soup)

  expect_true(length(results) >= 1)
  expect_equal(results[[1]]$date, "2023-05-10")
  expect_true(results[[1]]$confidence >= 0.85)
})

test_that("DateExtractionConfig has correct defaults", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  config <- date_extractor$DateExtractionConfig()

  expect_equal(config$timeout, 10.0)
  expect_equal(config$max_retries, 2L)
  expect_equal(config$min_confidence, 0.3)
})

test_that("filter_results_by_date handles empty list", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  result <- date_extractor$filter_results_by_date(
    results = list(),
    date_after = "2023-01-01",
    date_before = NULL
  )

  expect_equal(length(result[[1]]), 0)  # passed
  expect_equal(length(result[[2]]), 0)  # failed
  expect_equal(length(result[[3]]), 0)  # undetermined
})

# ============================================================================
# Wikidata Tool Tests (wikidata_tool.py)
# ============================================================================

test_that("_build_temporal_filter generates correct SPARQL", {
  python_path <- .skip_if_no_python()
  wikidata <- reticulate::import_from_path("wikidata_tool", path = python_path)

  # No dates - empty filter
  result <- wikidata$`_build_temporal_filter`(NULL, NULL)
  expect_equal(result, "")

  # After date only
  result <- wikidata$`_build_temporal_filter`("2020-01-01", NULL)
  expect_match(result, "FILTER")
  expect_match(result, "2020-01-01")
  expect_match(result, "xsd:dateTime")

  # Before date only
  result <- wikidata$`_build_temporal_filter`(NULL, "2024-01-01")
  expect_match(result, "FILTER")
  expect_match(result, "2024-01-01")

  # Both dates
  result <- wikidata$`_build_temporal_filter`("2020-01-01", "2024-01-01")
  expect_match(result, "2020-01-01")
  expect_match(result, "2024-01-01")
})

test_that("_inject_temporal_filter modifies query correctly", {
  python_path <- .skip_if_no_python()
  wikidata <- reticulate::import_from_path("wikidata_tool", path = python_path)

  sample_query <- '
    SELECT ?person WHERE {
      ?person wdt:P39 wd:Q4416090 .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
  '

  # No dates - query unchanged
  result <- wikidata$`_inject_temporal_filter`(sample_query, NULL, NULL)
  expect_equal(result, sample_query)

  # With dates - filter injected
  result <- wikidata$`_inject_temporal_filter`(sample_query, "2020-01-01", NULL)
  expect_match(result, "FILTER")
  expect_match(result, "SERVICE wikibase:label")  # SERVICE still there
})

test_that("WikidataSearchTool._parse_temporal_from_query parses dates", {
  python_path <- .skip_if_no_python()
  wikidata <- reticulate::import_from_path("wikidata_tool", path = python_path)

  tool <- wikidata$WikidataSearchTool()

  # After date only
  result <- tool$`_parse_temporal_from_query`("entity_type:us_senators after:2020-01-01")
  expect_equal(result[[2]], "2020-01-01")  # date_after

  # Before date only
  result <- tool$`_parse_temporal_from_query`("entity_type:countries before:2024-06-01")
  expect_equal(result[[3]], "2024-06-01")  # date_before

  # Both dates
  result <- tool$`_parse_temporal_from_query`("entity_type:fortune500 after:2020-01-01 before:2023-12-31")
  expect_equal(result[[2]], "2020-01-01")
  expect_equal(result[[3]], "2023-12-31")
})

test_that("create_wikidata_tool accepts temporal parameters", {
  python_path <- .skip_if_no_python()
  wikidata <- reticulate::import_from_path("wikidata_tool", path = python_path)

  tool <- wikidata$create_wikidata_tool(
    date_after = "2020-01-01",
    date_before = "2024-01-01"
  )

  expect_equal(tool$date_after, "2020-01-01")
  expect_equal(tool$date_before, "2024-01-01")
})

test_that("create_wikidata_tool handles NULL temporal parameters", {
  python_path <- .skip_if_no_python()
  wikidata <- reticulate::import_from_path("wikidata_tool", path = python_path)

  tool <- wikidata$create_wikidata_tool()

  expect_true(is.null(tool$date_after) || tool$date_after == "")
  expect_true(is.null(tool$date_before) || tool$date_before == "")
})

test_that("WikidataConfig has correct defaults", {
  python_path <- .skip_if_no_python()
  wikidata <- reticulate::import_from_path("wikidata_tool", path = python_path)

  config <- wikidata$WikidataConfig()

  expect_equal(config$timeout, 60.0)
  expect_equal(config$max_results, 500L)
  expect_equal(config$retry_count, 3L)
  expect_equal(config$retry_delay, 2.0)
})

# ============================================================================
# Wayback Tool Tests (wayback_tool.py)
# ============================================================================

test_that("WaybackSearchTool._parse_query extracts URL and dates", {
  python_path <- .skip_if_no_python()
  wayback <- reticulate::import_from_path("wayback_tool", path = python_path)

  tool <- wayback$WaybackSearchTool()

  # URL with before date
  result <- tool$`_parse_query`("url:https://example.com before:2020-01-01")
  expect_equal(result[[1]], "https://example.com")
  expect_equal(result[[3]], "2020-01-01")  # before_date

  # URL with after date
  result <- tool$`_parse_query`("url:https://example.com/page after:2018-06-15")
  expect_equal(result[[1]], "https://example.com/page")
  expect_equal(result[[2]], "2018-06-15")  # after_date

  # Both dates
  result <- tool$`_parse_query`("url:https://test.org after:2019-01-01 before:2022-12-31")
  expect_equal(result[[1]], "https://test.org")
  expect_equal(result[[2]], "2019-01-01")
  expect_equal(result[[3]], "2022-12-31")
})

test_that("WaybackSearchTool._parse_query handles URL without prefix", {
  python_path <- .skip_if_no_python()
  wayback <- reticulate::import_from_path("wayback_tool", path = python_path)

  tool <- wayback$WaybackSearchTool()

  result <- tool$`_parse_query`("https://example.com before:2023-01-01")
  expect_equal(result[[1]], "https://example.com")
  expect_equal(result[[3]], "2023-01-01")
})

test_that("WaybackConfig has correct defaults", {
  python_path <- .skip_if_no_python()
  wayback <- reticulate::import_from_path("wayback_tool", path = python_path)

  config <- wayback$WaybackConfig()

  expect_equal(config$timeout, 30.0)
  expect_equal(config$max_results, 100L)
  expect_equal(config$retry_count, 3L)
  expect_equal(config$retry_delay, 2.0)
  expect_equal(config$request_delay, 0.5)
})

test_that("create_wayback_tool returns configured tool", {
  python_path <- .skip_if_no_python()
  wayback <- reticulate::import_from_path("wayback_tool", path = python_path)

  custom_config <- wayback$WaybackConfig(timeout = 60.0, max_results = 50L)
  tool <- wayback$create_wayback_tool(config = custom_config)

  expect_equal(tool$name, "wayback_search")
  expect_equal(tool$config$timeout, 60.0)
  expect_equal(tool$config$max_results, 50L)
})

test_that("create_wayback_tool uses default config when not specified", {
  python_path <- .skip_if_no_python()
  wayback <- reticulate::import_from_path("wayback_tool", path = python_path)

  tool <- wayback$create_wayback_tool()

  expect_equal(tool$name, "wayback_search")
  expect_equal(tool$config$timeout, 30.0)
})

# ============================================================================
# ResearchConfig Tests (research_graph.py)
# ============================================================================

test_that("ResearchConfig has temporal field defaults", {
  python_path <- .skip_if_no_python()
  research <- reticulate::import_from_path("research_graph", path = python_path)

  config <- research$ResearchConfig()

  expect_true(is.null(config$time_filter))
  expect_true(is.null(config$date_after))
  expect_true(is.null(config$date_before))
  expect_equal(config$temporal_strictness, "best_effort")
  expect_false(config$use_wayback)
})

test_that("ResearchConfig accepts temporal parameters", {
  python_path <- .skip_if_no_python()
  research <- reticulate::import_from_path("research_graph", path = python_path)

  config <- research$ResearchConfig(
    time_filter = "m",
    date_after = "2023-01-01",
    date_before = "2024-01-01",
    temporal_strictness = "strict",
    use_wayback = TRUE
  )

  expect_equal(config$time_filter, "m")
  expect_equal(config$date_after, "2023-01-01")
  expect_equal(config$date_before, "2024-01-01")
  expect_equal(config$temporal_strictness, "strict")
  expect_true(config$use_wayback)
})

test_that("ResearchConfig accepts all valid time_filter values", {
  python_path <- .skip_if_no_python()
  research <- reticulate::import_from_path("research_graph", path = python_path)

  for (filter in c("d", "w", "m", "y")) {
    config <- research$ResearchConfig(time_filter = filter)
    expect_equal(config$time_filter, filter)
  }
})

# ============================================================================
# ExtractedDate Dataclass Tests (date_extractor.py)
# ============================================================================

test_that("ExtractedDate dataclass works correctly", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  extracted <- date_extractor$ExtractedDate(
    date = "2023-06-15",
    confidence = 0.95,
    source = "json_ld.datePublished",
    raw_value = "2023-06-15T10:00:00Z"
  )

  expect_equal(extracted$date, "2023-06-15")
  expect_equal(extracted$confidence, 0.95)
  expect_equal(extracted$source, "json_ld.datePublished")
  expect_equal(extracted$raw_value, "2023-06-15T10:00:00Z")
})

# ============================================================================
# Edge Case Tests
# ============================================================================

test_that("Date parsing handles ambiguous MM/DD vs DD/MM formats", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  # MM/DD/YYYY format (US style) is tried first
  result <- date_extractor$`_parse_date_string`("01/15/2024")
  expect_equal(result, "2024-01-15")

  # Unambiguous date
  result <- date_extractor$`_parse_date_string`("12/31/2024")
  expect_equal(result, "2024-12-31")
})

test_that("SPARQL filter injection preserves query structure", {
  python_path <- .skip_if_no_python()
  wikidata <- reticulate::import_from_path("wikidata_tool", path = python_path)

  query_with_service <- '
    SELECT ?x WHERE {
      ?x wdt:P31 wd:Q5 .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
  '

  result <- wikidata$`_inject_temporal_filter`(query_with_service, "2020-01-01", "2024-01-01")

  # FILTER should come before SERVICE
  filter_pos <- regexpr("FILTER", result)
  service_pos <- regexpr("SERVICE wikibase:label", result)
  expect_true(filter_pos[[1]] < service_pos[[1]])

  # Query should still be syntactically valid (has SELECT, WHERE, closing braces)
  expect_match(result, "SELECT")
  expect_match(result, "WHERE")
})

test_that("URL extraction handles edge cases in paths", {
  python_path <- .skip_if_no_python()
  date_extractor <- reticulate::import_from_path("date_extractor", path = python_path)

  # Date at end of URL
  result <- date_extractor$`_extract_from_url`("https://example.com/news/2024/06/15")
  expect_true(length(result) > 0)

  # Date with trailing slash
  result <- date_extractor$`_extract_from_url`("https://example.com/2024/06/15/")
  expect_true(length(result) > 0)

  # Multiple date-like patterns (should extract first valid one)
  result <- date_extractor$`_extract_from_url`("https://example.com/archive/2023/2024/06/15")
  expect_true(length(result) >= 1)
})
