local_trace_15units_env <- function() {
  script_path <- normalizePath(
    testthat::test_path("..", "..", "..", "tracked_reports", "trace_test_real_15units.R"),
    mustWork = TRUE
  )
  env <- new.env(parent = globalenv())
  sys.source(script_path, envir = env)
  env
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
