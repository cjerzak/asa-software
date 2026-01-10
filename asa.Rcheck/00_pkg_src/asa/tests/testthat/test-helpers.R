# Tests for helper functions

test_that("null coalescing operator works", {
  expect_equal(NULL %||% "default", "default")
  expect_equal("value" %||% "default", "value")
  expect_equal(NA %||% "default", NA)  # NA is not NULL
})

test_that("json_escape handles special characters", {
  expect_equal(json_escape("hello"), "hello")
  expect_equal(json_escape('say "hi"'), 'say \\"hi\\"')
  expect_equal(json_escape("line1\nline2"), "line1\\nline2")
  expect_equal(json_escape(NULL), "")
  expect_equal(json_escape(NA), "")
})

test_that("decode_html converts entities", {
  expect_equal(decode_html("&amp;"), "&")
  expect_equal(decode_html("&lt;tag&gt;"), "<tag>")
  expect_equal(decode_html("&quot;quoted&quot;"), '"quoted"')
  expect_equal(decode_html(NULL), NULL)
})

test_that("clean_whitespace normalizes spaces", {
  expect_equal(clean_whitespace("  multiple   spaces  "), "multiple spaces")
  expect_equal(clean_whitespace("normal text"), "normal text")
  expect_equal(clean_whitespace(NULL), NULL)
})

test_that("truncate_string works correctly", {
  expect_equal(truncate_string("short", 10), "short")
  expect_equal(truncate_string("this is a long string", 10), "this is...")
  expect_equal(nchar(truncate_string("very long text here", 10)), 10)
})

test_that("format_duration formats time correctly", {
  expect_match(format_duration(0.5), "seconds")
  expect_match(format_duration(5), "minutes")
  expect_match(format_duration(90), "hour")
  expect_equal(format_duration(NA), "N/A")
})

# ============================================================================
# configure_temporal() Tests
# ============================================================================

test_that("configure_temporal rejects invalid time_filter values", {
  # These tests only run when agent is initialized
  skip_if(!asa:::.is_initialized(), "Agent not initialized")

  expect_error(configure_temporal("invalid"), "one of: 'd', 'w', 'm', 'y'")
  expect_error(configure_temporal("day"), "one of: 'd', 'w', 'm', 'y'")
  expect_error(configure_temporal(123), "one of: 'd', 'w', 'm', 'y'")
})

test_that("configure_temporal normalizes NULL/NA/none to 'none'", {
  skip_if(!asa:::.is_initialized(), "Agent not initialized")

  search_tool <- asa:::asa_env$tools[[2]]

  # All these should set time to "none"
  suppressMessages(configure_temporal(NULL))
  expect_equal(search_tool$api_wrapper$time, "none")

  suppressMessages(configure_temporal(NA))
  expect_equal(search_tool$api_wrapper$time, "none")

  suppressMessages(configure_temporal("none"))
  expect_equal(search_tool$api_wrapper$time, "none")
})

test_that("configure_temporal returns previous filter value", {
  skip_if(!asa:::.is_initialized(), "Agent not initialized")

  # Set to known value
  suppressMessages(configure_temporal("m"))

  # Set to new value, should return old
  old <- suppressMessages(configure_temporal("y"))
  expect_equal(old, "m")

  # Clean up
  suppressMessages(configure_temporal(NULL))
})
