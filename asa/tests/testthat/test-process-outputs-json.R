# JSON extraction tests for process_outputs.R

escape_python_double_quotes <- function(s) {
  # Simulate Python string escaping for content="...": escape backslashes and
  # double quotes.
  s <- gsub("\\\\", "\\\\\\\\", s)
  quote_escape <- "\\\""
  gsub("\"", quote_escape, s, fixed = TRUE)
}

test_that(".extract_json_from_trace parses trace-escaped JSON (escaped quotes)", {
  json_text <- jsonlite::toJSON(
    list(note = 'The "Great" Wall'),
    auto_unbox = TRUE
  )
  trace <- paste0(
    "AIMessage(content=\"",
    escape_python_double_quotes(json_text),
    "\", additional_kwargs={})"
  )

  parsed <- asa:::.extract_json_from_trace(trace)
  expect_true(is.list(parsed) && length(parsed) > 0)
  expect_equal(parsed$note, 'The "Great" Wall')
})

test_that(".extract_json_from_trace parses trace-escaped JSON (preserves backslashes)", {
  json_text <- jsonlite::toJSON(
    list(path = "C:\\Windows"),
    auto_unbox = TRUE
  )
  trace <- paste0(
    "AIMessage(content=\"",
    escape_python_double_quotes(json_text),
    "\", additional_kwargs={})"
  )

  parsed <- asa:::.extract_json_from_trace(trace)
  expect_true(is.list(parsed) && length(parsed) > 0)
  expect_equal(parsed$path, "C:\\Windows")
})
