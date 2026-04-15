local_trace_15units_env <- function() {
  script_path <- normalizePath(
    testthat::test_path("..", "..", "..", "tracked_reports", "trace_test_real_15units.R"),
    mustWork = TRUE
  )
  env <- new.env(parent = globalenv())
  sys.source(script_path, envir = env)
  env
}

local_trace_15units_shell_path <- function() {
  normalizePath(
    testthat::test_path("..", "..", "..", "tracked_reports", "trace_test_real_15units.sh"),
    mustWork = TRUE
  )
}

local_trace_15units_repo_root <- function() {
  normalizePath(
    testthat::test_path("..", "..", ".."),
    mustWork = TRUE
  )
}

run_trace_15units_shell <- function(shell_cmd, script_arg, workdir) {
  old_wd <- setwd(workdir)
  on.exit(setwd(old_wd), add = TRUE)

  stdout_file <- tempfile("trace15_shell_stdout_")
  stderr_file <- tempfile("trace15_shell_stderr_")
  on.exit(unlink(c(stdout_file, stderr_file), force = TRUE), add = TRUE)

  status <- suppressWarnings(system2(
    command = shell_cmd,
    args = c(script_arg, "--help"),
    stdout = stdout_file,
    stderr = stderr_file
  ))
  status_int <- suppressWarnings(as.integer(status))
  if (is.na(status_int)) {
    status_int <- 0L
  }

  list(
    status = status_int,
    stdout = paste(readLines(stdout_file, warn = FALSE), collapse = "\n"),
    stderr = paste(readLines(stderr_file, warn = FALSE), collapse = "\n")
  )
}

run_trace_15units_rscript_wrapper <- function(script_lines, script_name = "documentPackage.R") {
  repo_root <- local_trace_15units_repo_root()
  wrapper_dir <- file.path(
    repo_root,
    "Tmp",
    sprintf("trace15-wrapper-%s-%s", Sys.getpid(), as.integer(Sys.time()))
  )
  dir.create(wrapper_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(wrapper_dir, recursive = TRUE, force = TRUE), add = TRUE)

  wrapper_path <- file.path(wrapper_dir, script_name)
  wrapper_rel_path <- file.path("Tmp", basename(wrapper_dir), script_name)
  writeLines(script_lines, wrapper_path)

  old_wd <- setwd(repo_root)
  on.exit(setwd(old_wd), add = TRUE)

  stdout_file <- tempfile("trace15_rscript_stdout_")
  stderr_file <- tempfile("trace15_rscript_stderr_")
  on.exit(unlink(c(stdout_file, stderr_file), force = TRUE), add = TRUE)

  status <- suppressWarnings(system2(
    command = "Rscript",
    args = wrapper_rel_path,
    stdout = stdout_file,
    stderr = stderr_file
  ))
  status_int <- suppressWarnings(as.integer(status))
  if (is.na(status_int)) {
    status_int <- 0L
  }

  list(
    status = status_int,
    stdout = paste(readLines(stdout_file, warn = FALSE), collapse = "\n"),
    stderr = paste(readLines(stderr_file, warn = FALSE), collapse = "\n")
  )
}

restore_envvar <- function(name, value) {
  if (is.na(value)) {
    Sys.unsetenv(name)
    return(invisible(NULL))
  }

  do.call(Sys.setenv, stats::setNames(list(value), name))
  invisible(NULL)
}

create_fake_trace_repo_root <- function() {
  root <- tempfile("trace-root-")
  dir.create(file.path(root, "asa"), recursive = TRUE, showWarnings = FALSE)
  writeLines(
    c(
      "Package: asa",
      "Version: 0.0.0"
    ),
    file.path(root, "asa", "DESCRIPTION")
  )
  root
}

create_trace_15units_input_db <- function(path) {
  countries <- c(
    "Argentina", "Bolivia", "Canada", "Denmark", "Estonia",
    "Finland", "Greece", "Hungary", "India", "Japan",
    "Kenya", "Latvia", "Mexico", "Nepal", "Oman"
  )

  payload <- data.frame(
    country = countries,
    row_id = seq(101L, 115L),
    person_name = sprintf("Person %02d", seq_along(countries)),
    election_year = seq(2001L, 2015L),
    pol_party = sprintf("Party %02d", seq_along(countries)),
    region = sprintf("Region %02d", seq_along(countries)),
    person_bday = sprintf("1970-01-%02d", seq_along(countries)),
    gender_character = rep("Unknown", length(countries)),
    ethnic = rep("Unknown", length(countries)),
    stringsAsFactors = FALSE
  )

  conn <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)
  DBI::dbWriteTable(conn, "InputMESData", payload, overwrite = TRUE)
}

test_that("trace shell launcher supports invocation from different locations", {
  shell_path <- local_trace_15units_shell_path()
  temp_wd <- tempfile("trace15-shell-wd-")
  dir.create(temp_wd, recursive = TRUE, showWarnings = FALSE)

  bash_result <- run_trace_15units_shell(
    shell_cmd = "bash",
    script_arg = shell_path,
    workdir = temp_wd
  )
  expect_equal(bash_result$status, 0L)
  expect_match(bash_result$stdout, "Usage:")
  expect_match(bash_result$stdout, "trace_test_real_15units\\.sh")

  sh_result <- run_trace_15units_shell(
    shell_cmd = "sh",
    script_arg = shell_path,
    workdir = temp_wd
  )
  expect_equal(sh_result$status, 0L)
  expect_match(sh_result$stdout, "Usage:")

  relative_result <- run_trace_15units_shell(
    shell_cmd = "sh",
    script_arg = basename(shell_path),
    workdir = dirname(shell_path)
  )
  expect_equal(relative_result$status, 0L)
  expect_match(relative_result$stdout, "Usage:")
})

test_that("trace shell launcher uses canonical script path for supervisor relaunch", {
  shell_path <- local_trace_15units_shell_path()
  script_lines <- readLines(shell_path, warn = FALSE)
  script_text <- paste(script_lines, collapse = "\n")

  expect_match(script_text, 'SCRIPT_PATH="\\$\\{SCRIPT_DIR\\}/\\$\\(basename "\\$\\{SCRIPT_SOURCE\\}"\\)"')
  expect_match(script_text, 'supervisor_cmd=\\(bash "\\$\\{SCRIPT_PATH\\}" --supervisor --run-id "\\$\\{RUN_ID\\}"\\)')
})

test_that("trace shell launcher passes bash syntax check", {
  shell_path <- local_trace_15units_shell_path()
  status <- suppressWarnings(system2("bash", c("-n", shell_path)))
  status_int <- suppressWarnings(as.integer(status))
  if (is.na(status_int)) {
    status_int <- 0L
  }

  expect_equal(status_int, 0L)
})

test_that("sourcing the trace script is hermetic", {
  original_error <- getOption("error")
  original_ignore <- Sys.getenv("ASA_IGNORE_PATH_CHROMEDRIVER", unset = NA_character_)
  original_disable <- Sys.getenv("ASA_DISABLE_UC", unset = NA_character_)
  on.exit({
    options(error = original_error)
    restore_envvar("ASA_IGNORE_PATH_CHROMEDRIVER", original_ignore)
    restore_envvar("ASA_DISABLE_UC", original_disable)
  }, add = TRUE)

  sentinel_error <- function() "keep-existing-error-handler"
  options(error = sentinel_error)
  expected_error <- getOption("error")
  Sys.setenv(
    ASA_IGNORE_PATH_CHROMEDRIVER = "keep-ignore",
    ASA_DISABLE_UC = "keep-disable"
  )

  env <- local_trace_15units_env()

  expect_identical(getOption("error"), expected_error)
  expect_identical(Sys.getenv("ASA_IGNORE_PATH_CHROMEDRIVER"), "keep-ignore")
  expect_identical(Sys.getenv("ASA_DISABLE_UC"), "keep-disable")
  expect_false(isTRUE(env$.trace_runtime_bootstrapped))
})

test_that("trace script resolves itself when launched from a wrapper that changes directories", {
  repo_root <- local_trace_15units_repo_root()
  wrapper_result <- run_trace_15units_rscript_wrapper(c(
    "setwd(\"asa\")",
    "trace_script <- normalizePath(\"../tracked_reports/trace_test_real_15units.R\", mustWork = TRUE)",
    "env <- new.env(parent = globalenv())",
    "tryCatch(",
    "  sys.source(trace_script, envir = env),",
    "  error = function(e) {",
    "    message(\"SOURCE_ERR: \", conditionMessage(e))",
    "    quit(status = 1L)",
    "  }",
    ")",
    "cat(\"TRACE_SCRIPT_PATH=\", env$TRACE_SCRIPT_PATH, \"\\n\", sep = \"\")",
    "cat(\"TRACE_REPO_ROOT=\", env$TRACE_REPO_ROOT, \"\\n\", sep = \"\")"
  ))

  expect_equal(wrapper_result$status, 0L)
  expect_match(
    wrapper_result$stdout,
    "TRACE_SCRIPT_PATH=.*/tracked_reports/trace_test_real_15units\\.R"
  )
  expect_match(
    wrapper_result$stdout,
    paste0(
      "TRACE_REPO_ROOT=",
      gsub("([.|()\\^{}+$*?\\[\\]\\\\])", "\\\\\\1", repo_root)
    )
  )
  expect_false(grepl("SOURCE_ERR:", wrapper_result$stderr, fixed = TRUE))
})

test_that("ASA_TRACE_REPO_ROOT override takes precedence when sourcing", {
  fake_root <- create_fake_trace_repo_root()
  original_root <- Sys.getenv("ASA_TRACE_REPO_ROOT", unset = NA_character_)
  on.exit(restore_envvar("ASA_TRACE_REPO_ROOT", original_root), add = TRUE)

  Sys.setenv(ASA_TRACE_REPO_ROOT = fake_root)
  env <- local_trace_15units_env()

  expect_identical(env$TRACE_REPO_ROOT, normalizePath(fake_root, mustWork = TRUE))
  expect_identical(
    env$TRACE_ARTIFACT_BASE_DIR,
    file.path(normalizePath(fake_root, mustWork = TRUE), "tracked_reports", "trace_real_15units")
  )
  expect_false(isTRUE(env$.trace_runtime_bootstrapped))
})

test_that("resolve_trace_repo_root fails clearly when no root can be inferred", {
  env <- local_trace_15units_env()
  original_root <- Sys.getenv("ASA_TRACE_REPO_ROOT", unset = NA_character_)
  original_current_script_path <- env$current_script_path
  on.exit({
    restore_envvar("ASA_TRACE_REPO_ROOT", original_root)
    env$current_script_path <- original_current_script_path
  }, add = TRUE)

  Sys.unsetenv("ASA_TRACE_REPO_ROOT")
  env$current_script_path <- function() NA_character_

  expect_error(
    env$resolve_trace_repo_root(),
    "Cannot resolve trace repo root automatically\\. Set ASA_TRACE_REPO_ROOT"
  )
})

test_that("setup_trace_run creates a frozen 15-unit manifest", {
  env <- local_trace_15units_env()
  artifact_dir <- tempfile("trace-real-15units-artifacts-")
  dir.create(artifact_dir, recursive = TRUE, showWarnings = FALSE)
  db_path <- file.path(tempdir(), paste0("trace-15units-", as.integer(Sys.time()), ".sqlite"))
  create_trace_15units_input_db(db_path)

  run_context <- env$setup_trace_run(
    artifact_base_dir = artifact_dir,
    input_db_path = db_path,
    sampling_seed = 99L,
    require_existing_manifest = FALSE
  )

  manifest <- env$read_sample_manifest(
    run_context$paths$sample_manifest_path,
    expected_unit_count = env$TRACE_EXPECTED_UNIT_COUNT
  )

  expect_equal(nrow(manifest), 15L)
  expect_identical(manifest$unit_index, 1:15)
  expect_true(file.exists(run_context$paths$run_context_path))

  first_key <- env$build_unit_key(manifest[1, , drop = FALSE], 1L)
  expect_match(first_key, "^01_[a-z0-9_]+_row00101$")
})

test_that("aggregate_trace_run preserves order and flags missing bundles", {
  env <- local_trace_15units_env()
  artifact_dir <- tempfile("trace-real-15units-artifacts-")
  dir.create(artifact_dir, recursive = TRUE, showWarnings = FALSE)
  db_path <- file.path(tempdir(), paste0("trace-15units-", as.integer(Sys.time()) + 1L, ".sqlite"))
  create_trace_15units_input_db(db_path)

  run_context <- env$setup_trace_run(
    artifact_base_dir = artifact_dir,
    input_db_path = db_path,
    sampling_seed = 101L,
    require_existing_manifest = FALSE
  )
  manifest <- env$read_sample_manifest(
    run_context$paths$sample_manifest_path,
    expected_unit_count = env$TRACE_EXPECTED_UNIT_COUNT
  )

  row1 <- manifest[1, , drop = FALSE]
  meta1 <- env$build_unit_meta(row1, 1L)
  attempt <- list(
    status = "success",
    trace_json = "",
    trace = "",
    message = jsonlite::toJSON(
      list(
        education_level = "Unknown",
        education_institution = "Unknown",
        education_field = "Unknown",
        prior_occupation = "Unknown",
        class_background = "Unknown",
        disability_status = "Unknown",
        birth_place = "Unknown",
        birth_year = "Unknown",
        lgbtq_status = "Unknown",
        lgbtq_details = NULL,
        confidence = 0.5,
        justification = "test payload"
      ),
      auto_unbox = TRUE,
      null = "null"
    ),
    elapsed_time = 1.25,
    token_stats = list(tokens_used = 42),
    plan = list(step = "test"),
    fold_stats = list(),
    raw_response = NULL,
    execution = list(
      diagnostics = list(),
      completion_gate = list(),
      json_repair = list(),
      field_status = list(),
      payload_integrity = list(),
      tool_quality_events = list(list(tool_name = "search_query"))
    )
  )
  env$write_unit_bundle(
    unit_dir = file.path(run_context$paths$units_root_dir, meta1$unit_key),
    unit_key = meta1$unit_key,
    unit_meta = meta1,
    prompt = env$build_prompt_for_row(row1),
    started_at_utc = env$format_utc(),
    finished_at_utc = env$format_utc(),
    attempt = attempt,
    unit_error = NULL
  )

  row2 <- manifest[2, , drop = FALSE]
  meta2 <- env$build_unit_meta(row2, 2L)
  env$write_unit_bundle(
    unit_dir = file.path(run_context$paths$units_root_dir, meta2$unit_key),
    unit_key = meta2$unit_key,
    unit_meta = meta2,
    prompt = env$build_prompt_for_row(row2),
    started_at_utc = env$format_utc(),
    finished_at_utc = env$format_utc(),
    attempt = NULL,
    unit_error = simpleError("boom")
  )

  result <- env$aggregate_trace_run(
    run_id = run_context$run_id,
    artifact_base_dir = artifact_dir,
    verbose = FALSE
  )

  expect_equal(result$run_summary$success_count, 1L)
  expect_equal(result$run_summary$error_count, 14L)
  expect_identical(result$run_summary$units[[1]]$unit_key, meta1$unit_key)
  expect_identical(result$run_summary$units[[1]]$status, "success")
  expect_identical(result$run_summary$units[[2]]$status, "error")
  expect_match(result$run_summary$units[[2]]$error_message, "boom")
  expect_identical(result$run_summary$units[[3]]$status, "error")
  expect_match(result$run_summary$units[[3]]$error_message, "Missing unit artifacts")
})
