test_that("heartbeat start/stop scopes progress env to invocation", {
  skip_on_os("windows")

  original_progress <- Sys.getenv("ASA_PROGRESS_STATE_FILE", unset = "")
  on.exit({
    if (is.character(original_progress) && nzchar(original_progress)) {
      Sys.setenv(ASA_PROGRESS_STATE_FILE = original_progress)
    } else {
      Sys.unsetenv("ASA_PROGRESS_STATE_FILE")
    }
  }, add = TRUE)

  Sys.setenv(ASA_PROGRESS_STATE_FILE = "seed_progress_file")

  hb <- asa:::.heartbeat_start(
    label = "test-heartbeat-lifecycle",
    interval_sec = 10L,
    stall_after_sec = 60L
  )
  on.exit(asa:::.heartbeat_stop(hb), add = TRUE)

  expect_true(is.list(hb))
  expect_true(is.character(hb$progress_file) && nzchar(hb$progress_file))
  expect_identical(Sys.getenv("ASA_PROGRESS_STATE_FILE", unset = ""), hb$progress_file)

  expect_true(!is.na(suppressWarnings(as.integer(hb$pid)[1])))
  expect_true(suppressWarnings(as.integer(hb$pid)[1]) > 0L)
  expect_true(asa:::.heartbeat_process_alive(hb$pid))

  asa:::.heartbeat_set_phase(hb, phase = "backend_invoke", detail = "unit_test")
  asa:::.heartbeat_stop(hb)

  expect_false(asa:::.heartbeat_process_alive(hb$pid))
  expect_identical(Sys.getenv("ASA_PROGRESS_STATE_FILE", unset = ""), "seed_progress_file")
})


test_that("heartbeat stop is idempotent", {
  skip_on_os("windows")

  hb <- asa:::.heartbeat_start(label = "test-heartbeat-idempotent", interval_sec = 10L, stall_after_sec = 60L)
  asa:::.heartbeat_stop(hb)
  expect_silent(asa:::.heartbeat_stop(hb))
})


test_that(".with_heartbeat restores env and cleans temporary files", {
  skip_on_os("windows")

  original_progress <- Sys.getenv("ASA_PROGRESS_STATE_FILE", unset = "")
  on.exit({
    if (is.character(original_progress) && nzchar(original_progress)) {
      Sys.setenv(ASA_PROGRESS_STATE_FILE = original_progress)
    } else {
      Sys.unsetenv("ASA_PROGRESS_STATE_FILE")
    }
  }, add = TRUE)

  Sys.setenv(ASA_PROGRESS_STATE_FILE = "seed_progress_wrapper")
  before_files <- list.files(
    tempdir(),
    pattern = "^asa_heartbeat_(stop|phase|progress|script|pid)_",
    full.names = TRUE
  )

  inside_progress <- ""
  result <- asa:::.with_heartbeat(
    fn = function() {
      inside_progress <<- Sys.getenv("ASA_PROGRESS_STATE_FILE", unset = "")
      42L
    },
    label = "test-wrapper-success",
    interval_sec = 10L,
    stall_after_sec = 60L
  )

  expect_identical(result, 42L)
  expect_true(nzchar(inside_progress))
  expect_false(identical(inside_progress, "seed_progress_wrapper"))
  expect_identical(Sys.getenv("ASA_PROGRESS_STATE_FILE", unset = ""), "seed_progress_wrapper")

  after_success_files <- list.files(
    tempdir(),
    pattern = "^asa_heartbeat_(stop|phase|progress|script|pid)_",
    full.names = TRUE
  )
  expect_setequal(after_success_files, before_files)

  expect_error(
    asa:::.with_heartbeat(
      fn = function() stop("boom"),
      label = "test-wrapper-error",
      interval_sec = 10L,
      stall_after_sec = 60L
    ),
    "boom"
  )
  expect_identical(Sys.getenv("ASA_PROGRESS_STATE_FILE", unset = ""), "seed_progress_wrapper")

  after_error_files <- list.files(
    tempdir(),
    pattern = "^asa_heartbeat_(stop|phase|progress|script|pid)_",
    full.names = TRUE
  )
  expect_setequal(after_error_files, before_files)
})


.heartbeat_script_fixture <- function() {
  asa:::.heartbeat_build_script(
    stopfile = tempfile("hb_stop_"),
    phasefile = tempfile("hb_phase_"),
    progress_file = tempfile("hb_progress_"),
    pidfile = tempfile("hb_pid_"),
    label = "heartbeat-test",
    interval_sec = 20L,
    stall_after_sec = 180L,
    r_pid = 12345L
  )
}


test_that("heartbeat script parses backend progress fields for reason classification", {
  script <- .heartbeat_script_fixture()

  expect_true(grepl("PROGRESS_PHASE=$(get_kv phase \"$PROGRESS_LINE\")", script, fixed = TRUE))
  expect_true(grepl("PENDING_TOOL_CALLS=$(get_kv pending_tool_calls \"$PROGRESS_LINE\")", script, fixed = TRUE))
  expect_true(grepl("PENDING_TOOL=$(get_kv pending_tool \"$PROGRESS_LINE\")", script, fixed = TRUE))
  expect_true(grepl("STOP_REASON=$(get_kv stop_reason \"$PROGRESS_LINE\")", script, fixed = TRUE))
  expect_true(grepl("BACKEND_ERROR=$(get_kv error \"$PROGRESS_LINE\")", script, fixed = TRUE))
  expect_true(
    grepl(
      "printf \"  stage    phase=%s    status=%s    reason=%s    node=%s\\n\"",
      script,
      fixed = TRUE
    )
  )
})


test_that("heartbeat reason ladder prioritizes backend state before telemetry", {
  script <- .heartbeat_script_fixture()

  priority_markers <- c(
    "if [ \"$BACKEND_ERROR\" != \"none\" ]; then",
    "elif [ \"$STOP_REASON\" != \"none\" ]; then",
    "elif [ \"$PROGRESS_PHASE\" = \"recursion_limit\" ]; then",
    "elif [ \"$PROGRESS_PHASE\" = \"complete\" ]; then",
    "elif [ \"$PENDING_TOOL_CALLS\" = \"1\" ]; then",
    "elif [ \"$TOOL_LIMIT_NUM\" -gt 0 ] && [ \"$TOOL_REMAINING_NUM\" -eq 0 ]; then",
    "elif [ \"$FIELDS_TOTAL_NUM\" -gt 0 ] && [ \"$FIELDS_RESOLVED_NUM\" -ge \"$FIELDS_TOTAL_NUM\" ]; then",
    "elif [ \"$TCP_COUNT\" -gt 0 ]; then",
    "elif [ \"$CPU_ACTIVE\" -eq 1 ]; then"
  )
  marker_positions <- vapply(
    priority_markers,
    function(marker) {
      as.integer(regexpr(marker, script, fixed = TRUE)[1])
    },
    integer(1)
  )

  expect_true(all(marker_positions > 0L))
  expect_true(all(diff(marker_positions) > 0L))

  reason_tokens <- c(
    "WAIT_REASON=backend_error",
    "WAIT_REASON=stopping_${STOP_REASON}",
    "WAIT_REASON=recursion_limit_finalize",
    "WAIT_REASON=finalizing_output",
    "WAIT_REASON=waiting_for_tool_results",
    "WAIT_REASON=\"waiting_for_tool_results[$PENDING_TOOL]\"",
    "WAIT_REASON=tool_budget_exhausted",
    "WAIT_REASON=all_fields_resolved",
    "WAIT_REASON=waiting_on_network",
    "WAIT_REASON=local_compute",
    "WAIT_REASON=backend_processing"
  )
  reason_present <- vapply(
    reason_tokens,
    function(token) grepl(token, script, fixed = TRUE),
    logical(1)
  )
  expect_true(all(reason_present))
})
