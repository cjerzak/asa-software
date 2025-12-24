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

# Additional tests for safe_json_parse
test_that("safe_json_parse parses valid JSON", {
  result <- safe_json_parse('{"name": "test", "value": 42}')
  expect_type(result, "list")
  expect_equal(result$name, "test")
  expect_equal(result$value, 42)
})

test_that("safe_json_parse returns NULL for invalid JSON", {
  expect_null(safe_json_parse("not json"))
  expect_null(safe_json_parse("{incomplete"))
  expect_null(safe_json_parse('{"bad": }'))
})

test_that("safe_json_parse handles edge cases", {
  expect_null(safe_json_parse(NULL))
  expect_null(safe_json_parse(NA))
  expect_null(safe_json_parse(""))
  expect_null(safe_json_parse("  "))
})

test_that("safe_json_parse handles arrays", {
  result <- safe_json_parse('[1, 2, 3]')
  expect_type(result, "integer")
  expect_equal(result, c(1, 2, 3))
})

test_that("safe_json_parse handles nested structures", {
  json <- '{"outer": {"inner": "value"}}'
  result <- safe_json_parse(json)
  expect_type(result, "list")
  expect_equal(result$outer$inner, "value")
})

# Additional tests for print2
test_that("print2 outputs correctly", {
  expect_output(print2("test"), "test")
  expect_output(print2("hello", "world"), "helloworld")
})

test_that("print2 adds newline", {
  output <- capture.output(print2("test"))
  expect_equal(output, "test")
})

# Additional tests for JSON escape
test_that("json_escape handles all special characters", {
  input <- "line1\nline2\rline3\ttab\"quote\\backslash"
  escaped <- json_escape(input)

  expect_match(escaped, "\\\\n")
  expect_match(escaped, "\\\\r")
  expect_match(escaped, "\\\\t")
  expect_match(escaped, '\\\\"')
  expect_match(escaped, "\\\\\\\\")
})

test_that("json_escape is reversible", {
  original <- "Hello \"world\" \n with newline"
  escaped <- json_escape(original)

  # Should be different after escaping
  expect_false(identical(original, escaped))

  # Should contain escaped sequences
  expect_match(escaped, '\\\\"')
  expect_match(escaped, "\\\\n")
})

# Additional tests for decode_html
test_that("decode_html handles multiple entities in one string", {
  input <- "&lt;div&gt;Hello &amp; goodbye&lt;/div&gt;"
  output <- decode_html(input)

  expect_equal(output, "<div>Hello & goodbye</div>")
})

test_that("decode_html handles numeric entities", {
  expect_equal(decode_html("&#39;single&#39;"), "'single'")
  expect_equal(decode_html("&#x27;quote&#x27;"), "'quote'")
  expect_equal(decode_html("slash&#x2F;here"), "slash/here")
})

test_that("decode_html handles no entities", {
  input <- "plain text with no entities"
  expect_equal(decode_html(input), input)
})

# Additional tests for clean_whitespace
test_that("clean_whitespace handles tabs and newlines", {
  input <- "text\twith\ttabs\nand\nnewlines"
  output <- clean_whitespace(input)

  expect_false(grepl("\t", output))
  expect_false(grepl("\n", output))
  expect_match(output, "^text with tabs and newlines$")
})

test_that("clean_whitespace handles leading/trailing spaces", {
  expect_equal(clean_whitespace("  text  "), "text")
  expect_equal(clean_whitespace("\n\ntext\n\n"), "text")
})

test_that("clean_whitespace preserves single spaces", {
  expect_equal(clean_whitespace("a b c"), "a b c")
})

# Additional tests for truncate_string
test_that("truncate_string custom ellipsis works", {
  result <- truncate_string("very long text here", 10, ellipsis = "...")
  expect_equal(nchar(result), 10)
  expect_match(result, "\\.\\.\\.$")
})

test_that("truncate_string with different ellipsis", {
  result <- truncate_string("long text", 8, ellipsis = "..")
  expect_equal(nchar(result), 8)
  expect_match(result, "\\.\\.$")
})

test_that("truncate_string doesn't truncate short strings", {
  short <- "short"
  expect_equal(truncate_string(short, 100), short)
})

test_that("truncate_string handles exact length", {
  text <- "1234567890"
  expect_equal(truncate_string(text, 10), text)

  # One char over should truncate
  result <- truncate_string(text, 9)
  expect_equal(nchar(result), 9)
})

# Additional tests for format_duration
test_that("format_duration handles zero", {
  expect_match(format_duration(0), "seconds")
})

test_that("format_duration handles fractional minutes", {
  result <- format_duration(1.5)
  expect_match(result, "1.5 minutes")
})

test_that("format_duration handles hours correctly", {
  result <- format_duration(125)  # 2 hours 5 minutes
  expect_match(result, "2 hours")
  expect_match(result, "5")
})

test_that("format_duration handles exactly 1 hour", {
  result <- format_duration(60)
  expect_match(result, "1 hours 0")
})

# Tests for helper combinations
test_that("helpers can be chained", {
  text <- "&lt;div&gt;  Multiple   spaces  &amp; entities &lt;/div&gt;"
  result <- clean_whitespace(decode_html(text))

  expect_equal(result, "<div> Multiple spaces & entities </div>")
})

test_that("truncate after cleaning works", {
  text <- "  lots   of   spaces   here  "
  cleaned <- clean_whitespace(text)
  truncated <- truncate_string(cleaned, 10)

  expect_equal(nchar(truncated), 10)
})

# Edge case tests
test_that("json_escape handles empty string", {
  expect_equal(json_escape(""), "")
})

test_that("decode_html handles unknown entities", {
  # Unknown entities should be left as-is
  input <- "&unknown;entity"
  output <- decode_html(input)
  expect_equal(output, input)
})

test_that("clean_whitespace handles empty string", {
  expect_equal(clean_whitespace(""), "")
  expect_equal(clean_whitespace("   "), "")
})

test_that("truncate_string with max_length < ellipsis length", {
  # Edge case: max_length smaller than ellipsis
  result <- truncate_string("long text", 2, ellipsis = "...")
  # Should still work, might produce odd result but shouldn't error
  expect_type(result, "character")
})

test_that("format_duration with very large values", {
  result <- format_duration(1440)  # 24 hours
  expect_match(result, "24 hours")
})

test_that("format_duration with very small values", {
  result <- format_duration(0.01)  # 0.6 seconds
  expect_match(result, "seconds")
})

# Test NULL coalescing with different types
test_that("null coalescing works with different types", {
  expect_equal(NULL %||% 123, 123)
  expect_equal(NULL %||% list(a = 1), list(a = 1))
  expect_equal(NULL %||% TRUE, TRUE)

  expect_equal(0 %||% 1, 0)  # 0 is not NULL
  expect_equal(FALSE %||% TRUE, FALSE)  # FALSE is not NULL
  expect_equal("" %||% "default", "")  # Empty string is not NULL
})
