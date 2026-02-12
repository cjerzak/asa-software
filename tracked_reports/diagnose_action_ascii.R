# Diagnostic script: isolate where the action_ascii extraction pipeline fails.
# Usage: Rscript tracked_reports/diagnose_action_ascii.R

options(error = NULL)

# --- Load package --------------------------------------------------------
script_file <- commandArgs(trailingOnly = FALSE)
script_path <- sub("^--file=", "", script_file[grep("^--file=", script_file)])
if (length(script_path) == 0L || !nzchar(script_path[[1]])) {
  script_path <- file.path(getwd(), "tracked_reports", "diagnose_action_ascii.R")
}
script_dir <- dirname(normalizePath(script_path[[1]], winslash = "/", mustWork = FALSE))
local_asa_path <- normalizePath(file.path(script_dir, "..", "asa"), winslash = "/", mustWork = FALSE)

if (requireNamespace("devtools", quietly = TRUE)) {
  devtools::load_all(local_asa_path, quiet = TRUE)
}
cat("=== Diagnostic: action_ascii extraction pipeline ===\n\n")

# --- Read saved trace JSON -----------------------------------------------
trace_file <- file.path(script_dir, "trace_real.txt")
if (!file.exists(trace_file)) {
  stop("trace_real.txt not found at: ", trace_file)
}
trace_json_raw <- readr::read_file(trace_file)
cat("trace_real.txt size: ", nchar(trace_json_raw), " chars\n")
cat("trace_real.txt class: ", paste(class(trace_json_raw), collapse = ", "), "\n")
cat("starts with '{': ", startsWith(trimws(trace_json_raw), "{"), "\n")
cat("contains asa_trace_v1: ", grepl("asa_trace_v1", trace_json_raw), "\n\n")

# --- Step 0: Test as.character() coercion --------------------------------
cat("--- Step 0: as.character() coercion ---\n")
tryCatch({
  coerced <- as.character(trace_json_raw)
  cat("  as.character() class: ", paste(class(coerced), collapse = ", "), "\n")
  cat("  length: ", length(coerced), "\n")
  cat("  nchar: ", nchar(coerced), "\n")
  cat("  identical to original: ", identical(coerced, trace_json_raw), "\n")
  cat("  PASS\n\n")
}, error = function(e) {
  cat("  ERROR: ", conditionMessage(e), "\n\n")
})

# --- Step 1: .parse_trace_json_messages() --------------------------------
cat("--- Step 1: .parse_trace_json_messages() ---\n")
messages <- tryCatch({
  msgs <- asa:::.parse_trace_json_messages(trace_json_raw)
  if (is.null(msgs)) {
    cat("  Result: NULL (parse returned NULL)\n")
    cat("  Possible reasons: empty text, not asa_trace_v1, JSON parse failure\n")
  } else {
    cat("  Result: list of ", length(msgs), " messages\n")
    # Show message types
    types <- vapply(msgs, function(m) {
      as.character(m$message_type %||% "unknown")
    }, character(1))
    cat("  Message types: ", paste(table(types), names(table(types)), sep = "x ", collapse = ", "), "\n")
  }
  cat("  PASS\n\n")
  msgs
}, error = function(e) {
  cat("  ERROR: ", conditionMessage(e), "\n")
  cat("  Call: ", deparse(conditionCall(e)), "\n\n")
  NULL
})

# --- Step 2: .trace_messages_to_action_events() --------------------------
cat("--- Step 2: .trace_messages_to_action_events() ---\n")
events <- tryCatch({
  if (is.null(messages)) {
    cat("  Skipping (no messages from step 1)\n\n")
    list()
  } else {
    evts <- asa:::.trace_messages_to_action_events(messages, max_preview_chars = 88L)
    cat("  Result: ", length(evts), " events\n")
    if (length(evts) > 0) {
      event_types <- vapply(evts, function(e) as.character(e$type %||% "?"), character(1))
      cat("  Event types: ", paste(table(event_types), names(table(event_types)), sep = "x ", collapse = ", "), "\n")
    }
    cat("  PASS\n\n")
    evts
  }
}, error = function(e) {
  cat("  ERROR: ", conditionMessage(e), "\n")
  cat("  Call: ", deparse(conditionCall(e)), "\n\n")
  list()
})

# --- Step 3: .collapse_action_events() -----------------------------------
cat("--- Step 3: .collapse_action_events() ---\n")
collapsed <- tryCatch({
  coll <- asa:::.collapse_action_events(events, max_preview_chars = 88L)
  cat("  Result: ", length(coll), " collapsed events (from ", length(events), " raw)\n")
  cat("  PASS\n\n")
  coll
}, error = function(e) {
  cat("  ERROR: ", conditionMessage(e), "\n")
  cat("  Call: ", deparse(conditionCall(e)), "\n\n")
  events
})

# --- Step 4: .render_action_ascii() --------------------------------------
cat("--- Step 4: .render_action_ascii() ---\n")
tryCatch({
  # Build the action_trace structure that .render_action_ascii expects
  out <- list(
    step_count = length(collapsed),
    omitted_steps = 0L,
    steps = collapsed,
    langgraph_step_timings = list(),
    plan_summary = character(0),
    overall_summary = character(0),
    wall_time_minutes = NA_real_
  )
  ascii <- asa:::.render_action_ascii(out)
  cat("  Result: ", nchar(ascii), " chars\n")
  cat("  Non-empty: ", nzchar(ascii), "\n")
  cat("  First 200 chars:\n")
  cat("  ", substr(ascii, 1, 200), "\n")
  cat("  PASS\n\n")
}, error = function(e) {
  cat("  ERROR: ", conditionMessage(e), "\n")
  cat("  Call: ", deparse(conditionCall(e)), "\n\n")
})

# --- Step 5: Full .extract_action_trace() pipeline -----------------------
cat("--- Step 5: Full .extract_action_trace() pipeline ---\n")
full_result <- tryCatch({
  result <- asa:::.extract_action_trace(
    trace_json = trace_json_raw,
    raw_trace = "",
    plan_history = list(),
    token_trace = list(),
    wall_time_minutes = NA_real_
  )
  cat("  step_count: ", result$step_count, "\n")
  cat("  omitted_steps: ", result$omitted_steps, "\n")
  cat("  ascii nchar: ", nchar(result$ascii), "\n")
  cat("  ascii non-empty: ", nzchar(result$ascii), "\n")
  cat("  PASS\n\n")
  result
}, error = function(e) {
  cat("  ERROR: ", conditionMessage(e), "\n")
  cat("  Call: ", deparse(conditionCall(e)), "\n\n")
  NULL
})

# --- Step 6: Simulate run_task.R tryCatch wrapper ------------------------
cat("--- Step 6: Simulate run_task.R outer tryCatch ---\n")
tryCatch({
  # This mirrors run_task.R:347-366 exactly
  # Test each argument evaluation separately
  cat("  Testing argument evals:\n")

  arg_trace_json <- tryCatch({
    val <- as.character(trace_json_raw %||% "")
    cat("    trace_json: OK (", nchar(val), " chars)\n")
    val
  }, error = function(e) {
    cat("    trace_json: ERROR - ", conditionMessage(e), "\n")
    ""
  })

  arg_raw_trace <- tryCatch({
    val <- as.character("" %||% "")
    cat("    raw_trace: OK\n")
    val
  }, error = function(e) {
    cat("    raw_trace: ERROR - ", conditionMessage(e), "\n")
    ""
  })

  arg_plan_history <- tryCatch({
    val <- list() %||% list()
    cat("    plan_history: OK\n")
    val
  }, error = function(e) {
    cat("    plan_history: ERROR - ", conditionMessage(e), "\n")
    list()
  })

  arg_token_trace <- tryCatch({
    val <- list() %||% list()
    cat("    token_trace: OK\n")
    val
  }, error = function(e) {
    cat("    token_trace: ERROR - ", conditionMessage(e), "\n")
    list()
  })

  arg_wall_time <- tryCatch({
    val <- NA_real_ %||% NA_real_
    cat("    wall_time_minutes: OK\n")
    val
  }, error = function(e) {
    cat("    wall_time_minutes: ERROR - ", conditionMessage(e), "\n")
    NA_real_
  })

  cat("  Calling .extract_action_trace() with evaluated args...\n")
  result <- asa:::.extract_action_trace(
    trace_json = arg_trace_json,
    raw_trace = arg_raw_trace,
    plan_history = arg_plan_history,
    token_trace = arg_token_trace,
    wall_time_minutes = arg_wall_time
  )
  cat("  ascii nchar: ", nchar(result$ascii), "\n")
  cat("  PASS\n\n")
}, error = function(e) {
  cat("  ERROR: ", conditionMessage(e), "\n")
  cat("  Call: ", deparse(conditionCall(e)), "\n")
  cat("  Full traceback:\n")
  cat("  ", paste(capture.output(traceback()), collapse = "\n  "), "\n\n")
})

# --- Step 7: Test .build_trace_json silent failure path ------------------
cat("--- Step 7: .build_trace_json() error reporting check ---\n")
cat("  Note: .build_trace_json silently returns '' on any error.\n")
cat("  The trace_json saved to disk was produced by this function.\n")
cat("  Since trace_real.txt has valid content, .build_trace_json\n")
cat("  succeeded for this run.\n\n")

# --- Summary -------------------------------------------------------------
cat("=== DIAGNOSTIC SUMMARY ===\n")
if (!is.null(full_result) && nzchar(full_result$ascii)) {
  cat("Pipeline works correctly on saved trace_json.\n")
  cat("The issue is likely:\n")
  cat("  1. response$trace_json at run_task.R:349 has a different type/value\n")
  cat("     than what was written to disk (e.g., reticulate wrapper vs string)\n")
  cat("  2. The as.character() coercion on a reticulate object fails silently\n")
  cat("  3. Or response$trace_json is actually '' during the run\n")
  cat("     (the fallback in trace_test_real.R re-extracted successfully\n")
  cat("     because attempt$trace_json was written to disk as the raw value)\n")
  cat("\nRecommendation: Add diagnostic logging to the outer tryCatch\n")
  cat("at run_task.R:355 to capture the actual error + argument types.\n")
} else {
  cat("Pipeline FAILS on saved trace_json.\n")
  cat("Review the step-by-step output above to identify the failure point.\n")
}
