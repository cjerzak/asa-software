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
  expect_match(action$ascii, "WHAT HAPPENED OVERALL")
  expect_match(action$ascii, "Requested 2 tool calls")
  expect_match(action$ascii, "Tool Search")
  expect_match(action$ascii, "by: AI")
  expect_match(action$ascii, "tok: in=0 out=0 total=0")
  expect_match(action$ascii, "LangGraph timed steps:")
  expect_false(grepl("\\] AI:", action$ascii))
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

test_that(".clip_action_text preserves URL tail identifiers for disambiguation", {
  txt <- "OpenWebpage(url=http://www.vicepresidencia.gob.bo/spip.php?page=parlamentario&id_parlamentario=428)"
  clipped <- asa:::.clip_action_text(txt, max_chars = 64L)

  expect_true(nchar(clipped) <= 64L)
  expect_true(grepl("\\.\\.\\.", clipped))
  expect_true(grepl("id_parlamentario=428\\)?$", clipped) || grepl("428", clipped))
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

test_that(".summarize_plan_history falls back to current plan when history is empty", {
  summary <- asa:::.summarize_plan_history(
    plan_history = list(),
    plan = list(
      steps = list(
        list(step = "Locate profile", status = "completed"),
        list(step = "Extract fields", status = "pending")
      )
    )
  )

  expect_true(length(summary) >= 1L)
  expect_match(summary[[1]], "2 total \\(1 completed, 0 in_progress, 1 pending\\)")
})

test_that(".summarize_action_overall returns a compact header summary", {
  raw_steps <- list(
    list(type = "human", actor = "Human", summary = "Submitted task prompt", preview = "Prompt"),
    list(type = "ai_tool_calls", actor = "AI", summary = "Requested 1 tool call (Search)", preview = "Search(query=a)"),
    list(type = "tool_result", actor = "Tool Search", summary = "Returned result", preview = "Result"),
    list(type = "ai_response", actor = "AI", summary = "Produced structured answer", preview = "{\"ok\":true}")
  )

  overall <- asa:::.summarize_action_overall(
    raw_steps,
    plan_summary = c("Plan steps: 2 total (1 completed, 1 pending)")
  )

  expect_true(length(overall) >= 4L)
  expect_true(any(grepl("Human prompts:", overall)))
  expect_true(any(grepl("Tool results:", overall)))
  expect_true(any(grepl("Structured terminal answer emitted", overall)))
})

test_that(".extract_action_trace annotates anomalies and investigator summary", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", name = NULL, content = "Find bio fields for Alice Brown", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "", tool_calls = list(
          list(name = "Search", args = list(query = "Alice Brown parliament profile 2014"))
        )),
        list(message_type = "ToolMessage", name = "Search", content = "Ramona is an 1884 American novel set in California.", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "", tool_calls = list(
          list(name = "Search", args = list(query = "Alice Brown birth year"))
        )),
        list(message_type = "ToolMessage", name = "Search", content = "", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "{\"birth_year\":\"Unknown\"}", tool_calls = list())
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )

  action <- asa:::.extract_action_trace(
    trace_json = trace_json,
    field_status = list(
      birth_year = list(status = "unknown"),
      birth_place = list(status = "unknown"),
      education_level = list(status = "pending")
    )
  )

  expect_match(action$ascii, "INVESTIGATOR SIGNALS")
  expect_match(action$ascii, "Field resolution:")
  expect_match(action$ascii, "flag: OFF_TOPIC_RESULT")
  expect_match(action$ascii, "flag: EMPTY_RESULT")
  expect_match(action$ascii, "LOW_EVIDENCE_FINAL_ANSWER")
  expect_true(any(grepl("Anomalies:", action$overall_summary)))
})

test_that(".extract_action_trace uses tool_quality_events for off-topic flags", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", name = NULL, content = "Find profile fields", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "", tool_calls = list(
          list(name = "Search", args = list(query = "Alice Brown profile"))
        )),
        list(message_type = "ToolMessage", name = "Search", content = "Completely unrelated page about old novels.", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "", tool_calls = list(
          list(name = "Search", args = list(query = "Alice Brown biography"))
        )),
        list(message_type = "ToolMessage", name = "Search", content = "Another unrelated page about restaurant menus.", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "{\"ok\":true}", tool_calls = list())
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )

  action <- asa:::.extract_action_trace(
    trace_json = trace_json,
    tool_quality_events = list(
      list(message_index_in_round = 1L, tool_name = "Search", is_empty = FALSE, is_off_target = FALSE),
      list(message_index_in_round = 1L, tool_name = "Search", is_empty = FALSE, is_off_target = TRUE)
    ),
    diagnostics = list(off_target_tool_results_count = 1L, empty_tool_results_count = 0L),
    max_preview_chars = 220L
  )

  tool_steps <- which(vapply(action$steps, function(step) identical(step$type, "tool_result"), logical(1)))
  expect_true(length(tool_steps) >= 2L)
  first_flags <- action$steps[[tool_steps[[1]]]]$flags
  second_flags <- action$steps[[tool_steps[[2]]]]$flags
  if (is.null(first_flags)) first_flags <- character(0)
  if (is.null(second_flags)) second_flags <- character(0)
  expect_false("OFF_TOPIC_RESULT" %in% first_flags)
  expect_true("OFF_TOPIC_RESULT" %in% second_flags)
  expect_true(any(grepl("diagnostics\\(EMPTY_RESULT=0, OFF_TOPIC_RESULT=1\\)", action$overall_summary)))
})

test_that(".extract_action_trace maps grouped tool_quality_events onto collapsed tool results", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", name = NULL, content = "Task", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "", tool_calls = list(
          list(name = "Search", args = list(query = "alice brown"))
        )),
        list(message_type = "ToolMessage", name = "Search", content = "result one", tool_calls = NULL),
        list(message_type = "ToolMessage", name = "Search", content = "", tool_calls = NULL),
        list(message_type = "ToolMessage", name = "Search", content = "result three", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "{\"ok\":true}", tool_calls = list())
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )

  action <- asa:::.extract_action_trace(
    trace_json = trace_json,
    tool_quality_events = list(
      list(message_index_in_round = 1L, tool_name = "Search", is_empty = FALSE, is_off_target = FALSE),
      list(message_index_in_round = 2L, tool_name = "Search", is_empty = TRUE, is_off_target = FALSE),
      list(message_index_in_round = 3L, tool_name = "Search", is_empty = FALSE, is_off_target = TRUE)
    ),
    diagnostics = list(off_target_tool_results_count = 1L, empty_tool_results_count = 1L)
  )

  tool_steps <- which(vapply(action$steps, function(step) identical(step$type, "tool_result"), logical(1)))
  expect_true(length(tool_steps) >= 1L)
  flags <- action$steps[[tool_steps[[1]]]]$flags
  if (is.null(flags)) flags <- character(0)
  expect_true("EMPTY_RESULT" %in% flags)
  expect_true("OFF_TOPIC_RESULT" %in% flags)
})

test_that(".extract_action_trace maps token_trace entries onto visible AI steps", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", name = NULL, content = "Task", tool_calls = NULL),
        list(
          message_type = "AIMessage",
          name = NULL,
          content = "",
          tool_calls = list(list(name = "Search", args = list(query = "tokyo"), id = "c1"))
        ),
        list(message_type = "ToolMessage", name = "Search", content = "Tokyo result", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "{\"ok\":true}", tool_calls = list())
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )

  action <- asa:::.extract_action_trace(
    trace_json = trace_json,
    token_trace = list(
      list(node = "agent", input_tokens = 50L, output_tokens = 10L, total_tokens = 60L, elapsed_minutes = 0.12),
      list(node = "finalize", input_tokens = 30L, output_tokens = 5L, total_tokens = 35L, elapsed_minutes = 0.04)
    )
  )

  expect_match(action$ascii, "tok: in=50 out=10 total=60")
  expect_match(action$ascii, "tok: in=30 out=5 total=35")
  expect_match(action$ascii, "min: 0.1200m")
  expect_match(action$ascii, "min: 0.0400m")
  expect_equal(length(action$langgraph_step_timings), 2L)
  expect_equal(action$langgraph_step_timings[[1]]$node, "agent")
  expect_equal(action$langgraph_step_timings[[2]]$node, "finalize")
})

# --- Timing helper tests ---

test_that(".as_scalar_minutes handles valid numeric input", {
  expect_equal(asa:::.as_scalar_minutes(1.5), 1.5)
  expect_equal(asa:::.as_scalar_minutes(0), 0)
  expect_equal(asa:::.as_scalar_minutes(0.001), 0.001)
})

test_that(".as_scalar_minutes returns default for NULL/NA/negative", {
  expect_true(is.na(asa:::.as_scalar_minutes(NULL)))
  expect_true(is.na(asa:::.as_scalar_minutes(NA)))
  expect_true(is.na(asa:::.as_scalar_minutes(-1)))
  expect_true(is.na(asa:::.as_scalar_minutes(character(0))))
  expect_equal(asa:::.as_scalar_minutes(NULL, default = 0), 0)
})

test_that(".as_scalar_minutes handles string input", {
  expect_equal(asa:::.as_scalar_minutes("0.1234"), 0.1234)
  expect_true(is.na(asa:::.as_scalar_minutes("not_a_number")))
})

test_that(".normalize_token_trace_entry reads elapsed_minutes correctly", {
  entry <- list(node = "agent", input_tokens = 100L, output_tokens = 50L,
                total_tokens = 150L, elapsed_minutes = 0.25)
  norm <- asa:::.normalize_token_trace_entry(entry)

  expect_equal(norm$node, "agent")
  expect_equal(norm$input_tokens, 100L)
  expect_equal(norm$output_tokens, 50L)
  expect_equal(norm$total_tokens, 150L)
  expect_equal(norm$elapsed_minutes, 0.25)
})

test_that(".normalize_token_trace_entry falls back to elapsed_seconds", {
  entry <- list(node = "tools", elapsed_seconds = 6.0)
  norm <- asa:::.normalize_token_trace_entry(entry)
  expect_equal(norm$elapsed_minutes, 0.1)
})

test_that(".normalize_token_trace_entry handles missing timing gracefully", {
  entry <- list(node = "agent", total_tokens = 100L)
  norm <- asa:::.normalize_token_trace_entry(entry)
  expect_true(is.na(norm$elapsed_minutes))
})

test_that(".extract_langgraph_step_timings aggregates multiple entries", {
  trace <- list(
    list(node = "agent", input_tokens = 50L, output_tokens = 10L,
         total_tokens = 60L, elapsed_minutes = 0.10),
    list(node = "tools", input_tokens = 0L, output_tokens = 0L,
         total_tokens = 0L, elapsed_minutes = 0.30),
    list(node = "agent", input_tokens = 40L, output_tokens = 8L,
         total_tokens = 48L, elapsed_minutes = 0.08),
    list(node = "finalize", input_tokens = 30L, output_tokens = 5L,
         total_tokens = 35L, elapsed_minutes = 0.04)
  )

  timings <- asa:::.extract_langgraph_step_timings(trace)
  expect_equal(length(timings), 4L)
  expect_equal(timings[[1]]$node, "agent")
  expect_equal(timings[[2]]$node, "tools")
  expect_equal(timings[[3]]$node, "agent")
  expect_equal(timings[[4]]$node, "finalize")
  expect_equal(timings[[1]]$elapsed_minutes, 0.10)
  expect_equal(timings[[2]]$elapsed_minutes, 0.30)
})

test_that(".extract_langgraph_step_timings returns empty list for empty input", {
  expect_equal(asa:::.extract_langgraph_step_timings(list()), list())
  expect_equal(asa:::.extract_langgraph_step_timings(NULL), list())
})

test_that(".format_step_minutes formats correctly", {
  expect_equal(asa:::.format_step_minutes(0.1234), "0.1234")
  expect_equal(asa:::.format_step_minutes(0.1234, digits = 2L), "0.12")
  expect_equal(asa:::.format_step_minutes(NULL), "n/a")
  expect_equal(asa:::.format_step_minutes(NA), "n/a")
  expect_equal(asa:::.format_step_minutes(NA, na_label = "---"), "---")
})

test_that(".collapse_action_events carries elapsed_minutes through collapsed groups", {
  events <- list(
    list(type = "tool_result", actor = "Tool Search", summary = "Returned result",
         preview = "result A", elapsed_minutes = 0.05),
    list(type = "tool_result", actor = "Tool Search", summary = "Returned result",
         preview = "result B", elapsed_minutes = 0.03),
    list(type = "tool_result", actor = "Tool Search", summary = "Returned result",
         preview = "result C", elapsed_minutes = NA_real_)
  )
  collapsed <- asa:::.collapse_action_events(events)

  expect_equal(length(collapsed), 1L)
  expect_equal(collapsed[[1]]$summary, "Returned 3 results")
  expect_equal(collapsed[[1]]$elapsed_minutes, 0.05)
})

test_that(".collapse_action_events returns NA when all group members lack timing", {
  events <- list(
    list(type = "tool_result", actor = "Tool Search", summary = "Returned result", preview = "a"),
    list(type = "tool_result", actor = "Tool Search", summary = "Returned result", preview = "b")
  )
  collapsed <- asa:::.collapse_action_events(events)

  expect_equal(length(collapsed), 1L)
  expect_true(is.na(collapsed[[1]]$elapsed_minutes))
})

test_that(".render_action_ascii includes LANGGRAPH aggregate and slowest sections", {
  action_trace <- list(
    steps = list(),
    step_count = 0L,
    omitted_steps = 0L,
    langgraph_step_timings = list(
      list(node = "agent", elapsed_minutes = 0.15, total_tokens = 100L),
      list(node = "tools", elapsed_minutes = 0.32, total_tokens = 0L)
    ),
    plan_summary = character(0),
    overall_summary = character(0),
    wall_time_minutes = NA_real_
  )

  ascii <- asa:::.render_action_ascii(action_trace)
  expect_match(ascii, "\\[LANGGRAPH\\] Node aggregates")
  expect_match(ascii, "\\[LANGGRAPH\\] Slowest timed invocations")
  expect_match(ascii, "agent: 0\\.1500m \\(tok=100, n=1\\)")
  expect_match(ascii, "tools: 0\\.3200m \\(tok=0, n=1\\)")
})

test_that(".render_action_ascii shows TIMING sanity check when wall_time_minutes provided", {
  action_trace <- list(
    steps = list(),
    step_count = 0L,
    omitted_steps = 0L,
    langgraph_step_timings = list(
      list(node = "agent", elapsed_minutes = 0.10, total_tokens = 50L),
      list(node = "tools", elapsed_minutes = 0.30, total_tokens = 0L),
      list(node = "finalize", elapsed_minutes = 0.05, total_tokens = 20L)
    ),
    plan_summary = character(0),
    overall_summary = character(0),
    wall_time_minutes = 0.55
  )

  ascii <- asa:::.render_action_ascii(action_trace)
  expect_match(ascii, "\\[TIMING\\]")
  expect_match(ascii, "wall=0\\.5500m")
  expect_match(ascii, "nodes=0\\.4500m")
  expect_match(ascii, "delta=\\+0\\.1000m")
  expect_match(ascii, "\\(overhead\\)")
})

test_that(".render_action_ascii omits TIMING line when wall_time_minutes is NA", {
  action_trace <- list(
    steps = list(),
    step_count = 0L,
    omitted_steps = 0L,
    langgraph_step_timings = list(
      list(node = "agent", elapsed_minutes = 0.10, total_tokens = 50L)
    ),
    plan_summary = character(0),
    overall_summary = character(0),
    wall_time_minutes = NA_real_
  )

  ascii <- asa:::.render_action_ascii(action_trace)
  expect_false(grepl("\\[TIMING\\]", ascii))
})

test_that(".extract_action_trace passes wall_time_minutes through to output", {
  action <- asa:::.extract_action_trace(
    trace_json = "",
    raw_trace = "",
    wall_time_minutes = 1.23
  )
  expect_equal(action$wall_time_minutes, 1.23)
})

test_that("min: timing lines appear on AI steps in ASCII output", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(message_type = "HumanMessage", name = NULL, content = "Task", tool_calls = NULL),
        list(message_type = "AIMessage", name = NULL, content = "{\"ok\":true}", tool_calls = list())
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )

  action <- asa:::.extract_action_trace(
    trace_json = trace_json,
    token_trace = list(
      list(node = "agent", input_tokens = 20L, output_tokens = 5L,
           total_tokens = 25L, elapsed_minutes = 0.0777)
    )
  )

  expect_match(action$ascii, "min: 0\\.0777m")
})

test_that(".render_action_ascii does not append 'm' to n/a minute labels", {
  action_trace <- list(
    steps = list(
      list(
        type = "tool_result",
        actor = "Tool Search",
        summary = "Returned result",
        preview = "Result",
        input_tokens = 0L,
        output_tokens = 0L,
        total_tokens = 0L,
        elapsed_minutes = NA_real_
      )
    ),
    step_count = 1L,
    omitted_steps = 0L,
    langgraph_step_timings = list(),
    plan_summary = character(0),
    investigator_summary = character(0),
    overall_summary = character(0),
    wall_time_minutes = NA_real_
  )

  ascii <- asa:::.render_action_ascii(action_trace)
  expect_false(grepl("n/am", ascii, fixed = TRUE))
  expect_match(ascii, "min: n/a")
})

# --- .try_or_warn tests ---

test_that(".try_or_warn emits warning and returns default on error", {
  warns <- character(0)
  result <- withCallingHandlers(
    asa:::.try_or_warn(stop("boom"), default = "fallback", context = "test_ctx"),
    warning = function(w) {
      warns[[length(warns) + 1L]] <<- conditionMessage(w)
      invokeRestart("muffleWarning")
    }
  )
  expect_equal(result, "fallback")
  expect_true(any(grepl("\\[test_ctx\\]", warns)))
  expect_true(any(grepl("boom", warns)))
})

test_that(".try_or_warn passes through result on success", {
  result <- asa:::.try_or_warn(42L, default = 0L, context = "test_ctx")
  expect_equal(result, 42L)
})

test_that(".try_or_warn works without context label", {
  warns <- character(0)
  result <- withCallingHandlers(
    asa:::.try_or_warn(stop("oops"), default = NULL),
    warning = function(w) {
      warns[[length(warns) + 1L]] <<- conditionMessage(w)
      invokeRestart("muffleWarning")
    }
  )
  expect_null(result)
  expect_true(any(grepl("oops", warns)))
  expect_false(any(grepl("^\\[", warns)))
})

# --- Real trace data test ---

test_that(".extract_action_trace succeeds on real trace_real.txt data", {
  trace_path <- file.path(
    testthat::test_path(), "..", "..", "..", "tracked_reports", "trace_real.txt"
  )
  trace_path <- normalizePath(trace_path, mustWork = FALSE)
  skip_if_not(file.exists(trace_path), "trace_real.txt not available")
  skip_if(file.size(trace_path) < 100L, "trace_real.txt too small")

  trace_json <- readLines(trace_path, warn = FALSE)
  trace_json <- paste(trace_json, collapse = "\n")

  action <- asa:::.extract_action_trace(trace_json = trace_json)

  expect_match(action$ascii, "AGENT ACTION MAP")
  expect_true(nchar(action$ascii) > 50L)
  expect_true(action$step_count > 0L)
})
