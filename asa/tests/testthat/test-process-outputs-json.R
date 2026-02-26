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

test_that(".extract_json_from_trace returns NULL for structured traces without terminal AI JSON", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", content = "Find person", tool_calls = NULL),
        list(
          message_type = "AIMessage",
          content = "",
          tool_calls = list(list(name = "Search", args = list(query = "person"), id = "call_1"))
        ),
        list(message_type = "ToolMessage", name = "Search", content = "__START_OF_SOURCE 1__ ...", tool_calls = NULL)
      )
    ),
    auto_unbox = TRUE,
    pretty = FALSE,
    null = "null"
  )

  parsed <- asa:::.extract_json_from_trace(trace_json)
  expect_null(parsed)
})

test_that(".extract_json_from_trace parses structured traces with terminal AI JSON", {
  terminal <- list(
    birth_year = 1982L,
    birth_place = "Beni",
    confidence = "Low"
  )
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", content = "Find person", tool_calls = NULL),
        list(
          message_type = "AIMessage",
          content = "",
          tool_calls = list(list(name = "Search", args = list(query = "person"), id = "call_1"))
        ),
        list(message_type = "ToolMessage", name = "Search", content = "__START_OF_SOURCE 1__ ...", tool_calls = NULL),
        list(
          message_type = "AIMessage",
          content = jsonlite::toJSON(terminal, auto_unbox = TRUE, null = "null"),
          tool_calls = list()
        )
      )
    ),
    auto_unbox = TRUE,
    pretty = FALSE,
    null = "null"
  )

  parsed <- asa:::.extract_json_from_trace(trace_json)
  expect_true(is.list(parsed) && length(parsed) > 0)
  expect_equal(as.integer(parsed$birth_year), 1982L)
  expect_equal(as.character(parsed$birth_place), "Beni")
  expect_equal(as.character(parsed$confidence), "Low")

  extracted <- extract_agent_results(trace_json)
  expect_true(is.list(extracted$json_data_canonical))
  expect_equal(as.integer(extracted$json_data_canonical$birth_year), 1982L)
})

test_that(".extract_json_from_trace ignores ToolMessage envelope payloads", {
  legacy_trace <- paste(
    "ToolMessage(content=",
    deparse('{"message_type":"ToolMessage","name":"OpenWebpage","content":"URL: https://example.com","tool_calls":null}'),
    ")",
    sep = ""
  )

  parsed <- asa:::.extract_json_from_trace(legacy_trace)
  expect_null(parsed)
})

test_that(".count_unknown_ratio ignores sibling confidence fields", {
  parsed <- list(
    birth_place = "Unknown",
    birth_place_source = NULL,
    birth_place_confidence = NULL,
    education_level = "Bachelor's",
    education_level_source = "https://example.com/profile",
    education_level_confidence = NULL,
    confidence = "Low",
    justification = "limited evidence"
  )

  ratio <- asa:::.count_unknown_ratio(parsed)
  expect_equal(as.numeric(ratio), 0.5)
})
