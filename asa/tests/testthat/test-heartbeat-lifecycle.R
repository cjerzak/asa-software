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
