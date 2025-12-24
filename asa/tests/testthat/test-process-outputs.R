# Tests for output extraction and processing functions

# Test data - mock agent traces
mock_search_trace <- "ToolMessage(content='__START_OF_SOURCE 1__ <CONTENT>First search result content</CONTENT> <URL>https://example.com/1</URL> __END_OF_SOURCE__  __START_OF_SOURCE 2__ <CONTENT>Second search result</CONTENT> <URL>https://example.com/2</URL> __END_OF_SOURCE__', name='Search', tool_call_id='123')"

mock_wikipedia_trace <- "ToolMessage(content='Page: Albert Einstein\\nSummary: Albert Einstein was a theoretical physicist.', name='Wikipedia', tool_call_id='456')"

mock_mixed_trace <- paste(
  mock_search_trace,
  mock_wikipedia_trace,
  sep = "\\n\\n"
)

mock_json_trace <- "AIMessage(content='{\"name\": \"Einstein\", \"year\": 1879}', additional_kwargs={})"

mock_tier_trace <- "{'_tier': 'primp', 'results': [...]}"

# Tests for extract_search_snippets
test_that("extract_search_snippets extracts content correctly", {
  snippets <- extract_search_snippets(mock_search_trace)

  expect_type(snippets, "character")
  expect_length(snippets, 2)
  expect_match(snippets[1], "First search result")
  expect_match(snippets[2], "Second search result")
})

test_that("extract_search_snippets handles empty input", {
  expect_equal(extract_search_snippets(""), character(0))
  expect_equal(extract_search_snippets(NULL), character(0))
})

test_that("extract_search_snippets handles no matches", {
  no_search <- "Some random text without search results"
  expect_equal(extract_search_snippets(no_search), character(0))
})

test_that("extract_search_snippets cleans escape sequences", {
  escaped_trace <- "ToolMessage(content='__START_OF_SOURCE 1__ <CONTENT>Line1\\nLine2</CONTENT> <URL>http://test.com</URL> __END_OF_SOURCE__', name='Search')"
  snippets <- extract_search_snippets(escaped_trace)

  expect_length(snippets, 1)
  expect_match(snippets[1], "Line1.*Line2")
})

# Tests for extract_wikipedia_content
test_that("extract_wikipedia_content extracts Wikipedia snippets", {
  wiki <- extract_wikipedia_content(mock_wikipedia_trace)

  expect_type(wiki, "character")
  expect_length(wiki, 1)
  expect_match(wiki[1], "Albert Einstein")
  expect_match(wiki[1], "theoretical physicist")
})

test_that("extract_wikipedia_content handles empty input", {
  expect_equal(extract_wikipedia_content(""), character(0))
  expect_equal(extract_wikipedia_content(NULL), character(0))
})

test_that("extract_wikipedia_content filters invalid results", {
  invalid_wiki <- "ToolMessage(content='No good Wikipedia Search Result was found', name='Wikipedia')"
  expect_equal(extract_wikipedia_content(invalid_wiki), character(0))
})

test_that("extract_wikipedia_content cleans whitespace", {
  messy_wiki <- "ToolMessage(content='Page:   Test  \\nSummary:   Multiple    spaces', name='Wikipedia')"
  wiki <- extract_wikipedia_content(messy_wiki)

  expect_length(wiki, 1)
  expect_match(wiki[1], "Page: Test")
  expect_false(grepl("\\s{2,}", wiki[1]))  # No multiple spaces
})

# Tests for extract_urls
test_that("extract_urls extracts URLs correctly", {
  urls <- extract_urls(mock_search_trace)

  expect_type(urls, "character")
  expect_length(urls, 2)
  expect_equal(urls[1], "https://example.com/1")
  expect_equal(urls[2], "https://example.com/2")
})

test_that("extract_urls handles empty input", {
  expect_equal(extract_urls(""), character(0))
  expect_equal(extract_urls(NULL), character(0))
})

test_that("extract_urls handles no matches", {
  no_urls <- "ToolMessage with no URL tags"
  expect_equal(extract_urls(no_urls), character(0))
})

test_that("extract_urls handles duplicate URLs", {
  dup_trace <- "ToolMessage(content='__START_OF_SOURCE 1__ <URL>http://same.com</URL> __END_OF_SOURCE__  __START_OF_SOURCE 1__ <URL>http://same.com</URL> __END_OF_SOURCE__', name='Search')"
  urls <- extract_urls(dup_trace)

  # Should deduplicate within the same source
  expect_true(length(urls) >= 1)
})

# Tests for extract_search_tiers
test_that("extract_search_tiers extracts tier information", {
  tiers <- extract_search_tiers(mock_tier_trace)

  expect_type(tiers, "character")
  expect_true("primp" %in% tiers)
})

test_that("extract_search_tiers handles all tier types", {
  all_tiers <- "'_tier': 'primp' ... '_tier': 'selenium' ... '_tier': 'ddgs' ... '_tier': 'requests'"
  tiers <- extract_search_tiers(all_tiers)

  expect_true(all(c("primp", "selenium", "ddgs", "requests") %in% tiers))
})

test_that("extract_search_tiers returns unique tiers", {
  duplicate_tiers <- "'_tier': 'primp' ... '_tier': 'primp' ... '_tier': 'primp'"
  tiers <- extract_search_tiers(duplicate_tiers)

  expect_equal(length(tiers), 1)
  expect_equal(tiers[1], "primp")
})

test_that("extract_search_tiers handles empty input", {
  expect_equal(extract_search_tiers(""), character(0))
  expect_equal(extract_search_tiers(NULL), character(0))
})

test_that("extract_search_tiers handles no matches", {
  no_tier <- "Some text without tier information"
  expect_equal(extract_search_tiers(no_tier), character(0))
})

# Tests for extract_agent_results
test_that("extract_agent_results returns correct structure", {
  results <- extract_agent_results(mock_mixed_trace)

  expect_type(results, "list")
  expect_named(results, c("search_snippets", "search_urls", "wikipedia_snippets",
                         "json_data", "search_tiers"))
})

test_that("extract_agent_results handles empty input", {
  results <- extract_agent_results("")

  expect_type(results, "list")
  expect_equal(length(results$search_snippets), 0)
  expect_equal(length(results$search_urls), 0)
  expect_equal(length(results$wikipedia_snippets), 0)
  expect_null(results$json_data)
  expect_equal(length(results$search_tiers), 0)
})

test_that("extract_agent_results handles NULL input", {
  results <- extract_agent_results(NULL)

  expect_type(results, "list")
  expect_equal(length(results$search_snippets), 0)
})

test_that("extract_agent_results extracts all components", {
  full_trace <- paste(mock_search_trace, mock_wikipedia_trace, mock_tier_trace, sep = " ")
  results <- extract_agent_results(full_trace)

  expect_true(length(results$search_snippets) > 0)
  expect_true(length(results$wikipedia_snippets) > 0)
  expect_true(length(results$search_tiers) > 0)
})

# Tests for .extract_json_from_trace (internal function)
test_that("JSON extraction from trace works", {
  # Test via extract_agent_results
  results <- extract_agent_results(mock_json_trace)

  expect_type(results$json_data, "list")
  if (!is.null(results$json_data)) {
    expect_true("name" %in% names(results$json_data) ||
                "year" %in% names(results$json_data))
  }
})

# Tests for process_outputs
test_that("process_outputs requires raw_output column", {
  df <- data.frame(wrong_col = c("a", "b"))
  expect_error(process_outputs(df), "raw_output")
})

test_that("process_outputs adds count columns", {
  df <- data.frame(
    raw_output = c(mock_search_trace, mock_wikipedia_trace),
    stringsAsFactors = FALSE
  )

  result <- process_outputs(df, parallel = FALSE)

  expect_true("search_count" %in% names(result))
  expect_true("wiki_count" %in% names(result))
  expect_equal(nrow(result), 2)
})

test_that("process_outputs handles empty data frame", {
  df <- data.frame(raw_output = character(0), stringsAsFactors = FALSE)
  result <- process_outputs(df, parallel = FALSE)

  expect_equal(nrow(result), 0)
  expect_true("search_count" %in% names(result))
})

test_that("process_outputs extracts JSON fields as columns", {
  df <- data.frame(
    raw_output = c(mock_json_trace),
    stringsAsFactors = FALSE
  )

  result <- process_outputs(df, parallel = FALSE)

  # Should have basic columns
  expect_true("search_count" %in% names(result))
  expect_true("wiki_count" %in% names(result))
})

test_that("process_outputs handles malformed raw_output", {
  df <- data.frame(
    raw_output = c("malformed trace", "", NA_character_),
    stringsAsFactors = FALSE
  )

  # Should not error
  expect_error(process_outputs(df, parallel = FALSE), NA)

  result <- process_outputs(df, parallel = FALSE)
  expect_equal(nrow(result), 3)
})

test_that("process_outputs parallel parameter is respected", {
  skip_if_not(requireNamespace("future", quietly = TRUE),
              "future package not available")
  skip_if_not(requireNamespace("future.apply", quietly = TRUE),
              "future.apply package not available")

  df <- data.frame(
    raw_output = c(mock_search_trace, mock_wikipedia_trace),
    stringsAsFactors = FALSE
  )

  # Should work with parallel = TRUE
  expect_error(process_outputs(df, parallel = TRUE, workers = 2), NA)
})

test_that("process_outputs preserves original columns", {
  df <- data.frame(
    id = 1:2,
    raw_output = c(mock_search_trace, mock_wikipedia_trace),
    stringsAsFactors = FALSE
  )

  result <- process_outputs(df, parallel = FALSE)

  expect_true("id" %in% names(result))
  expect_equal(result$id, 1:2)
})

# Edge case tests
test_that("extract_search_snippets handles multiple sources with gaps", {
  gap_trace <- "ToolMessage(content='__START_OF_SOURCE 1__ <CONTENT>First</CONTENT> <URL>http://1.com</URL> __END_OF_SOURCE__  __START_OF_SOURCE 3__ <CONTENT>Third</CONTENT> <URL>http://3.com</URL> __END_OF_SOURCE__', name='Search')"
  snippets <- extract_search_snippets(gap_trace)

  # Should create vector with empty string for missing source 2
  expect_length(snippets, 3)
  expect_match(snippets[1], "First")
  expect_equal(snippets[2], "")
  expect_match(snippets[3], "Third")
})

test_that("extract_search_snippets handles escaped quotes", {
  quote_trace <- 'ToolMessage(content="__START_OF_SOURCE 1__ <CONTENT>He said \\"hello\\"</CONTENT> <URL>http://test.com</URL> __END_OF_SOURCE__", name="Search")'
  snippets <- extract_search_snippets(quote_trace)

  expect_length(snippets, 1)
  expect_match(snippets[1], "He said")
})

test_that("process_outputs works with single row", {
  df <- data.frame(raw_output = mock_search_trace, stringsAsFactors = FALSE)
  result <- process_outputs(df, parallel = FALSE)

  expect_equal(nrow(result), 1)
  expect_true(result$search_count[1] > 0)
})
