# Tests for high-level action extraction and ASCII visualization

test_that(".extract_action_trace builds action steps from structured trace", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(
          message_type = "HumanMessage",
          name = NULL,
          content = "Find the population of Tokyo",
          tool_calls = NULL
        ),
        list(
          message_type = "AIMessage",
          name = NULL,
          content = "",
          tool_calls = list(
            list(name = "Search", args = list(query = "Tokyo population"), id = "call_1", type = "tool_call"),
            list(name = "update_plan", args = list(step_id = 1, status = "in_progress"), id = "call_2", type = "tool_call")
          )
        ),
        list(
          message_type = "ToolMessage",
          name = "Search",
          content = "Source says Tokyo has around 14 million residents.",
          tool_calls = NULL
        ),
        list(
          message_type = "ToolMessage",
          name = "update_plan",
          content = "Plan step 1 updated to 'in_progress'",
          tool_calls = NULL
        ),
        list(
          message_type = "AIMessage",
          name = NULL,
          content = "{\"population\":\"14M\"}",
          tool_calls = list()
        )
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )

  action <- asa:::.extract_action_trace(
    trace_json = trace_json,
    plan_history = list(
      list(
        steps = list(
          list(step = "Collect data", status = "completed")
        )
      )
    )
  )

  expect_equal(action$step_count, 5L)
  expect_equal(length(action$steps), 5L)
  expect_match(action$ascii, "AGENT ACTION MAP")
  expect_match(action$ascii, "Requested 2 tool calls")
  expect_match(action$ascii, "Tool Search")
  expect_match(action$ascii, "Plan steps: 1 total")
})

test_that(".extract_action_trace truncates long previews", {
  long_prompt <- paste(rep("longtext", 40), collapse = " ")
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", name = NULL, content = long_prompt, tool_calls = NULL)
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )

  action <- asa:::.extract_action_trace(trace_json = trace_json, max_preview_chars = 40L)
  preview <- action$steps[[1]]$preview

  expect_true(nchar(preview) <= 40L)
  expect_true(endsWith(preview, "..."))
})

test_that(".extract_action_trace falls back to raw trace parsing", {
  raw_trace <- paste(
    "[human]",
    "Find one fact.",
    "",
    "[ai]",
    "  -> Search(query='tokyo population')",
    "",
    "[tool: Search]",
    "Tokyo population is about 14 million.",
    "",
    "[ai]",
    "{\"answer\":\"14M\"}",
    sep = "\n"
  )

  action <- asa:::.extract_action_trace(trace_json = "", raw_trace = raw_trace)

  expect_equal(action$step_count, 4L)
  expect_match(action$ascii, "Tool Search")
})

test_that(".extract_action_trace reports omitted steps when capped", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", name = NULL, content = "Task", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "", tool_calls = list(list(name = "Search", args = list(query = "a")))),
        list(message_type = "ToolMessage", name = "Search", content = "result a", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "", tool_calls = list(list(name = "Search", args = list(query = "b")))),
        list(message_type = "ToolMessage", name = "Search", content = "result b", tool_calls = NULL)
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )

  action <- asa:::.extract_action_trace(trace_json = trace_json, max_steps = 2L)

  expect_equal(length(action$steps), 2L)
  expect_equal(action$omitted_steps, 3L)
  expect_match(action$ascii, "additional step\\(s\\) omitted")
})

test_that(".extract_action_trace collapses consecutive tool results from same actor", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", name = NULL, content = "Task", tool_calls = NULL),
        list(
          message_type = "AIMessage",
          name = NULL,
          content = "",
          tool_calls = list(
            list(name = "save_finding", args = list(finding = "a", category = "fact"), id = "c1")
          )
        ),
        list(message_type = "ToolMessage", name = "save_finding", content = "Saved finding A", tool_calls = NULL),
        list(message_type = "ToolMessage", name = "save_finding", content = "Saved finding B", tool_calls = NULL),
        list(message_type = "ToolMessage", name = "save_finding", content = "Saved finding C", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "{\"ok\":true}", tool_calls = list())
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )

  action <- asa:::.extract_action_trace(trace_json = trace_json)

  expect_equal(action$step_count, 4L)
  expect_true(any(vapply(action$steps, function(s) identical(s$summary, "Returned 3 results"), logical(1))))
  expect_match(action$ascii, "Returned 3 results")
})

test_that(".summarize_plan_history uses clear wording for unspecified statuses", {
  summary <- asa:::.summarize_plan_history(
    list(
      list(
        steps = list(
          list(step = "Locate profile", status = "foo"),
          list(step = "Extract fields", status = "")
        )
      )
    )
  )

  expect_true(length(summary) >= 1L)
  expect_match(summary[[1]], "status metadata not yet populated")
})
