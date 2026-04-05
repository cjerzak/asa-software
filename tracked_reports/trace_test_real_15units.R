# Rscript: trace_test_real_15units.R

options(error = NULL)
Sys.setenv(ASA_IGNORE_PATH_CHROMEDRIVER = "1")
Sys.setenv(ASA_DISABLE_UC = "1")
readRenviron("~/.Renviron")

current_script_path <- function() {
  file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  if (length(file_arg) > 0L) {
    return(normalizePath(sub("^--file=", "", file_arg[[1]]), mustWork = FALSE))
  }
  normalizePath("tracked_reports/trace_test_real_15units.R", mustWork = FALSE)
}

resolve_trace_repo_root <- function() {
  env_root <- trimws(Sys.getenv("ASA_TRACE_REPO_ROOT", unset = ""))
  if (nzchar(env_root)) {
    return(normalizePath(path.expand(env_root), mustWork = FALSE))
  }
  normalizePath(file.path(dirname(current_script_path()), ".."), mustWork = FALSE)
}

TRACE_REPO_ROOT <- resolve_trace_repo_root()
TRACE_SCRIPT_PATH <- current_script_path()

# Prefer local package source so trace runs validate current repo code.
devtools::load_all(file.path(TRACE_REPO_ROOT, "asa"))

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(jsonlite)
  library(readr)
  library(asa)
})

TRACE_SOURCE_TABLE <- "InputMESData"
TRACE_INPUT_DB_PATH <- {
  env_path <- trimws(Sys.getenv("ASA_TRACE_INPUT_DB_PATH", unset = ""))
  if (nzchar(env_path)) {
    path.expand(env_path)
  } else {
    "/Users/cjerzak/Library/CloudStorage/Dropbox/GLP/Electoral systems/InputMESData_6aa27952.sqlite"
  }
}
TRACE_ARTIFACT_BASE_DIR <- {
  env_dir <- trimws(Sys.getenv("ASA_TRACE_ARTIFACT_BASE_DIR", unset = ""))
  if (nzchar(env_dir)) {
    path.expand(env_dir)
  } else {
    file.path(TRACE_REPO_ROOT, "tracked_reports", "trace_real_15units")
  }
}
TRACE_SAMPLING_SEED <- 12344321L
TRACE_EXPECTED_UNIT_COUNT <- 15L
TRACE_PARALLEL_JOBS <- 15L
TRACE_TOR_SOCKS_PORT_START <- 9050L
TRACE_TOR_CONTROL_PORT_START <- 9150L
TRACE_CONDA_ENV <- Sys.getenv("ASA_CONDA_ENV", unset = "asa_env")

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

format_utc <- function(x = Sys.time()) {
  format(x, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

trim_or_na <- function(x) {
  if (is.null(x) || length(x) == 0L) {
    return(NA_character_)
  }
  value <- trimws(enc2utf8(as.character(x[[1]])))
  value <- gsub("\\s+", " ", value)
  if (!nzchar(value) || identical(value, "NA")) {
    return(NA_character_)
  }
  value
}

value_or_default <- function(x, default) {
  value <- trim_or_na(x)
  if (is.na(value)) default else value
}

format_year_value <- function(x) {
  if (is.null(x) || length(x) == 0L || is.na(x[[1]])) {
    return("Unknown")
  }
  numeric_year <- suppressWarnings(as.integer(round(as.numeric(x[[1]]))))
  if (!is.na(numeric_year)) {
    return(as.character(numeric_year))
  }
  value_or_default(x, "Unknown")
}

make_slug <- function(x) {
  value <- value_or_default(x, "unknown")
  value <- iconv(value, from = "", to = "ASCII//TRANSLIT", sub = "")
  value <- tolower(value)
  value <- gsub("[^a-z0-9]+", "_", value)
  value <- gsub("^_+|_+$", "", value)
  if (!nzchar(value)) "unknown" else value
}

write_json_pretty <- function(x, path) {
  readr::write_file(
    jsonlite::toJSON(x, auto_unbox = TRUE, pretty = TRUE, null = "null"),
    path
  )
}

read_json_if_exists <- function(path, simplify_vector = TRUE) {
  if (!is.character(path) || length(path) != 1L || !file.exists(path)) {
    return(NULL)
  }
  tryCatch(
    jsonlite::fromJSON(path, simplifyVector = simplify_vector),
    error = function(e) NULL
  )
}

to_character_vector <- function(x) {
  if (is.null(x) || length(x) == 0L) {
    return(character(0))
  }
  unique(as.character(unlist(x, use.names = FALSE)))
}

artifact_sentinel <- function(artifact, reason, unit_key, generated_at_utc, details = NULL) {
  payload <- list(
    status = "empty",
    artifact = artifact,
    reason = reason,
    expected_empty = TRUE,
    unit_key = unit_key,
    generated_at_utc = generated_at_utc
  )
  if (!is.null(details)) {
    payload$details <- details
  }
  payload
}

artifact_or_sentinel <- function(value, artifact, reason, unit_key, generated_at_utc) {
  is_empty <- FALSE
  if (is.null(value)) {
    is_empty <- TRUE
  } else if (is.list(value) && length(value) == 0L) {
    is_empty <- TRUE
  } else if (is.atomic(value) && length(value) == 0L) {
    is_empty <- TRUE
  } else if (
    is.character(value) &&
      length(value) == 1L &&
      !is.na(value[[1]]) &&
      !nzchar(value[[1]])
  ) {
    is_empty <- TRUE
  }

  if (isTRUE(is_empty)) {
    return(artifact_sentinel(
      artifact = artifact,
      reason = reason,
      unit_key = unit_key,
      generated_at_utc = generated_at_utc
    ))
  }

  value
}

to_json_scalar <- function(x) {
  txt <- tryCatch(
    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"),
    error = function(e) NA_character_
  )
  if (is.character(txt) && length(txt) > 1L) {
    txt <- paste0(txt, collapse = "")
  }
  if (!is.character(txt) || length(txt) == 0L) {
    return(NA_character_)
  }
  txt[[1]]
}

extract_final_answer <- function(attempt) {
  extracted <- list()
  if (is.character(attempt$trace_json) && length(attempt$trace_json) == 1L && nzchar(attempt$trace_json)) {
    extracted <- tryCatch(
      asa::extract_agent_results(attempt$trace_json),
      error = function(e) list()
    )
  }

  final_answer <- extracted[["json_data_canonical"]] %||% extracted[["json_data"]]
  if (is.null(final_answer) && is.character(attempt$message) && length(attempt$message) == 1L && nzchar(attempt$message)) {
    final_answer <- tryCatch(
      jsonlite::fromJSON(attempt$message, simplifyVector = FALSE),
      error = function(e) NULL
    )
  }

  list(
    extracted = extracted,
    final_answer = final_answer
  )
}

collect_tool_names <- function(attempt) {
  tool_quality_events <- attempt$execution$tool_quality_events %||% list()
  tool_names <- unique(
    unlist(
      lapply(tool_quality_events, function(x) {
        if (!is.list(x)) {
          return(NA_character_)
        }
        as.character(x$tool_name %||% NA_character_)
      }),
      use.names = FALSE
    )
  )
  tool_names[!is.na(tool_names) & nzchar(tool_names)]
}

extract_action_ascii <- function(attempt) {
  action_ascii_text <- attempt$action_ascii %||% attempt$execution$action_ascii %||% ""
  if (
    !nzchar(action_ascii_text) &&
      is.character(attempt$trace_json) &&
      length(attempt$trace_json) == 1L &&
      nzchar(attempt$trace_json) &&
      exists(".extract_action_trace", envir = asNamespace("asa"), inherits = FALSE)
  ) {
    action_ascii_text <- tryCatch(
      asa:::.extract_action_trace(
        trace_json = attempt$trace_json,
        raw_trace = attempt$trace %||% "",
        plan_history = attempt$plan_history %||% list(),
        tool_quality_events = attempt$execution$tool_quality_events %||% list(),
        diagnostics = attempt$execution$diagnostics %||% list(),
        token_trace = attempt$token_stats$token_trace %||% list(),
        wall_time_minutes = attempt$elapsed_time %||% NA_real_
      )$ascii %||% "",
      error = function(e) ""
    )
  }
  action_ascii_text
}

build_payload_release_audit <- function(attempt, final_answer, unit_key, generated_at_utc) {
  payload_integrity <- attempt$execution$payload_integrity %||% list()
  canonical_text <- to_json_scalar(final_answer)
  released_text <- if (is.character(attempt$message) && length(attempt$message) == 1L) {
    attempt$message
  } else {
    to_json_scalar(attempt$message)
  }

  canonical_byte_hash <- asa:::.text_sha256(canonical_text)
  released_byte_hash <- asa:::.text_sha256(released_text)
  canonical_semantic_hash <- asa:::.semantic_json_sha256(canonical_text)
  released_semantic_hash <- asa:::.semantic_json_sha256(released_text)

  byte_hash_matches <- (
    !is.na(canonical_byte_hash) &&
      !is.na(released_byte_hash) &&
      identical(canonical_byte_hash, released_byte_hash)
  )
  semantic_hash_matches <- (
    !is.na(canonical_semantic_hash) &&
      !is.na(released_semantic_hash) &&
      identical(canonical_semantic_hash, released_semantic_hash)
  )

  hash_mismatch_type <- NA_character_
  if (
    !is.na(canonical_byte_hash) &&
      !is.na(released_byte_hash) &&
      !is.na(canonical_semantic_hash) &&
      !is.na(released_semantic_hash)
  ) {
    if (!byte_hash_matches && semantic_hash_matches) {
      hash_mismatch_type <- "byte_only"
    } else if (!byte_hash_matches && !semantic_hash_matches) {
      hash_mismatch_type <- "byte_and_semantic"
    } else if (byte_hash_matches && !semantic_hash_matches) {
      hash_mismatch_type <- "semantic_only"
    } else {
      hash_mismatch_type <- "none"
    }
  }

  list(
    unit_key = unit_key,
    generated_at_utc = payload_integrity$generated_at_utc %||% generated_at_utc,
    canonical_artifact_id = payload_integrity$canonical_artifact_id %||% canonical_byte_hash,
    released_artifact_id = payload_integrity$released_artifact_id %||% released_byte_hash,
    released_from = payload_integrity$released_from %||% NA_character_,
    canonical_available = isTRUE(payload_integrity$canonical_available),
    canonical_matches_message = isTRUE(payload_integrity$canonical_matches_message),
    canonical_byte_hash = canonical_byte_hash,
    released_byte_hash = released_byte_hash,
    byte_hash_matches = isTRUE(byte_hash_matches),
    canonical_semantic_hash = canonical_semantic_hash,
    released_semantic_hash = released_semantic_hash,
    semantic_hash_matches = isTRUE(semantic_hash_matches),
    hash_mismatch_type = hash_mismatch_type,
    truncation_signals = payload_integrity$truncation_signals %||% character(0),
    json_repair_reasons = payload_integrity$json_repair_reasons %||% character(0)
  )
}

sample_real_units <- function(input_db_path, sampling_seed) {
  conn <- DBI::dbConnect(RSQLite::SQLite(), input_db_path)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  sampled <- DBI::dbReadTable(conn, TRACE_SOURCE_TABLE)
  sampled <- sampled[, c(
    "country", "row_id", "person_name", "election_year", "pol_party",
    "region", "person_bday", "gender_character", "ethnic"
  )]
  sampled <- sampled[order(sampled$country, sampled$row_id), , drop = FALSE]

  set.seed(sampling_seed)
  sampled <- do.call(
    rbind,
    lapply(split(sampled, sampled$country), function(country_rows) {
      country_rows[sample.int(nrow(country_rows), 1L), , drop = FALSE]
    })
  )

  sampled <- sampled[order(sampled$country, sampled$row_id), , drop = FALSE]
  sampled$unit_index <- seq_len(nrow(sampled))
  rownames(sampled) <- NULL

  country_count <- length(unique(sampled$country))
  if (country_count != TRACE_EXPECTED_UNIT_COUNT || nrow(sampled) != TRACE_EXPECTED_UNIT_COUNT) {
    stop(sprintf(
      "Expected %d sampled units across %d countries, got %d rows across %d countries.",
      TRACE_EXPECTED_UNIT_COUNT,
      TRACE_EXPECTED_UNIT_COUNT,
      nrow(sampled),
      country_count
    ))
  }

  sampled
}

validate_sample_manifest <- function(sampled_units, expected_unit_count = TRACE_EXPECTED_UNIT_COUNT) {
  required_cols <- c(
    "country", "row_id", "person_name", "election_year", "pol_party",
    "region", "person_bday", "gender_character", "ethnic", "unit_index"
  )
  missing_cols <- setdiff(required_cols, names(sampled_units))
  if (length(missing_cols) > 0L) {
    stop(sprintf(
      "Sample manifest is missing required columns: %s",
      paste(missing_cols, collapse = ", ")
    ))
  }

  sampled_units <- as.data.frame(sampled_units, stringsAsFactors = FALSE)
  sampled_units$unit_index <- suppressWarnings(as.integer(sampled_units$unit_index))
  sampled_units$row_id <- suppressWarnings(as.integer(sampled_units$row_id))
  sampled_units <- sampled_units[order(sampled_units$unit_index), , drop = FALSE]
  rownames(sampled_units) <- NULL

  if (nrow(sampled_units) != expected_unit_count) {
    stop(sprintf(
      "Expected %d rows in sampled_units.csv, found %d.",
      expected_unit_count,
      nrow(sampled_units)
    ))
  }
  if (!identical(sampled_units$unit_index, seq_len(expected_unit_count))) {
    stop("Sample manifest unit_index values must be the contiguous sequence 1..n.")
  }
  if (length(unique(sampled_units$country)) != expected_unit_count) {
    stop("Sample manifest must contain exactly one row per country.")
  }

  sampled_units
}

read_sample_manifest <- function(path, expected_unit_count = TRACE_EXPECTED_UNIT_COUNT) {
  sampled_units <- readr::read_csv(path, show_col_types = FALSE)
  validate_sample_manifest(sampled_units, expected_unit_count = expected_unit_count)
}

build_prompt_for_row <- function(row) {
  asa::build_prompt(
    PROMPT_TEMPLATE,
    person_name = value_or_default(row$person_name, "Unknown"),
    country = value_or_default(row$country, "Unknown"),
    year = format_year_value(row$election_year),
    party = value_or_default(row$pol_party, "Unknown"),
    region = value_or_default(row$region, "National level"),
    bday = value_or_default(row$person_bday, "Not available"),
    gender = value_or_default(row$gender_character, "Unknown"),
    ethnicity = value_or_default(row$ethnic, "Unknown")
  )
}

build_unit_key <- function(row, unit_index) {
  row_id <- as.integer(row$row_id[[1]])
  sprintf(
    "%02d_%s_row%05d",
    as.integer(unit_index),
    make_slug(row$country[[1]]),
    row_id
  )
}

build_unit_meta <- function(row, unit_index) {
  list(
    unit_index = as.integer(unit_index),
    unit_key = build_unit_key(row, unit_index),
    country = value_or_default(row$country, "Unknown"),
    row_id = as.integer(row$row_id[[1]]),
    person_name = value_or_default(row$person_name, "Unknown"),
    election_year = format_year_value(row$election_year),
    pol_party = value_or_default(row$pol_party, "Unknown"),
    region = value_or_default(row$region, "National level"),
    person_bday = value_or_default(row$person_bday, "Not available"),
    gender_character = value_or_default(row$gender_character, "Unknown"),
    ethnic = value_or_default(row$ethnic, "Unknown")
  )
}

unit_artifact_paths <- function(unit_dir) {
  list(
    prompt = file.path(unit_dir, "prompt_example.txt"),
    trace = file.path(unit_dir, "trace.txt"),
    token_stats = file.path(unit_dir, "token_stats.txt"),
    plan_output = file.path(unit_dir, "plan_output.txt"),
    fold_stats = file.path(unit_dir, "fold_stats.txt"),
    fold_summary = file.path(unit_dir, "fold_summary.txt"),
    fold_archive = file.path(unit_dir, "fold_archive.txt"),
    execution_summary = file.path(unit_dir, "execution_summary.txt"),
    diagnostics = file.path(unit_dir, "diagnostics.txt"),
    completion_gate = file.path(unit_dir, "completion_gate.txt"),
    json_repair = file.path(unit_dir, "json_repair.txt"),
    invoke_error = file.path(unit_dir, "invoke_error.txt"),
    action_ascii = file.path(unit_dir, "action_ascii.txt"),
    answer_pred = file.path(unit_dir, "answer_pred.txt"),
    payload_release_audit = file.path(unit_dir, "payload_release_audit.txt")
  )
}

write_unit_bundle <- function(
  unit_dir,
  unit_key,
  unit_meta,
  prompt,
  started_at_utc,
  finished_at_utc,
  attempt = NULL,
  unit_error = NULL
) {
  dir.create(unit_dir, recursive = TRUE, showWarnings = FALSE)
  unit_paths <- unit_artifact_paths(unit_dir)

  readr::write_file(prompt, unit_paths$prompt)

  if (is.null(attempt)) {
    error_message <- conditionMessage(unit_error %||% simpleError("Unknown unit error"))
    error_payload <- list(
      status = "error",
      unit = unit_meta,
      started_at_utc = started_at_utc,
      finished_at_utc = finished_at_utc,
      elapsed_time = NA_real_,
      error_message = error_message,
      tool_names = character(0),
      unit_dir = normalizePath(unit_dir, mustWork = FALSE)
    )

    readr::write_file("", unit_paths$trace)
    write_json_pretty(artifact_sentinel("token_stats", "run_task_failed", unit_key, finished_at_utc), unit_paths$token_stats)
    write_json_pretty(artifact_sentinel("plan_output", "run_task_failed", unit_key, finished_at_utc), unit_paths$plan_output)
    write_json_pretty(artifact_sentinel("fold_stats", "run_task_failed", unit_key, finished_at_utc), unit_paths$fold_stats)
    write_json_pretty(artifact_sentinel("fold_summary", "run_task_failed", unit_key, finished_at_utc), unit_paths$fold_summary)
    write_json_pretty(artifact_sentinel("fold_archive", "run_task_failed", unit_key, finished_at_utc), unit_paths$fold_archive)
    write_json_pretty(error_payload, unit_paths$execution_summary)
    write_json_pretty(error_payload, unit_paths$diagnostics)
    write_json_pretty(artifact_sentinel("completion_gate", "run_task_failed", unit_key, finished_at_utc), unit_paths$completion_gate)
    write_json_pretty(artifact_sentinel("json_repair", "run_task_failed", unit_key, finished_at_utc), unit_paths$json_repair)
    write_json_pretty(error_payload, unit_paths$invoke_error)
    readr::write_file("", unit_paths$action_ascii)
    write_json_pretty(artifact_sentinel("answer_pred", "run_task_failed", unit_key, finished_at_utc), unit_paths$answer_pred)
    write_json_pretty(error_payload, unit_paths$payload_release_audit)

    return(list(
      summary = list(
        unit_key = unit_key,
        country = unit_meta$country,
        row_id = unit_meta$row_id,
        person_name = unit_meta$person_name,
        status = "error",
        elapsed_time = NA_real_,
        tokens_used = NA_real_,
        error_message = error_message,
        unit_dir = normalizePath(unit_dir, mustWork = FALSE)
      ),
      tool_names = character(0),
      files_written = unname(unlist(unit_paths))
    ))
  }

  trace_text <- attempt$trace_json %||% attempt$trace %||% ""
  readr::write_file(as.character(trace_text), unit_paths$trace)

  extraction <- extract_final_answer(attempt)
  attempt_status <- as.character(attempt$status %||% "success")
  if (!length(attempt_status) || is.na(attempt_status[[1]]) || !nzchar(attempt_status[[1]])) {
    attempt_status <- "success"
  } else {
    attempt_status <- attempt_status[[1]]
  }
  final_answer <- artifact_or_sentinel(
    extraction$final_answer,
    artifact = "answer_pred",
    reason = "answer_not_available",
    unit_key = unit_key,
    generated_at_utc = finished_at_utc
  )

  plan_output <- artifact_or_sentinel(
    attempt$plan,
    artifact = "plan_output",
    reason = "plan_not_generated",
    unit_key = unit_key,
    generated_at_utc = finished_at_utc
  )

  fold_stats_output <- artifact_or_sentinel(
    attempt$fold_stats,
    artifact = "fold_stats",
    reason = "fold_stats_not_available",
    unit_key = unit_key,
    generated_at_utc = finished_at_utc
  )

  summary_state <- tryCatch(
    if (!is.null(attempt$raw_response)) reticulate::py_to_r(attempt$raw_response$summary) else NULL,
    error = function(e) NULL
  )
  archive_state <- tryCatch(
    if (!is.null(attempt$raw_response)) reticulate::py_to_r(attempt$raw_response$archive) else NULL,
    error = function(e) NULL
  )

  fold_summary_output <- artifact_or_sentinel(
    summary_state %||% list(),
    artifact = "fold_summary",
    reason = "fold_summary_not_available",
    unit_key = unit_key,
    generated_at_utc = finished_at_utc
  )
  fold_archive_output <- artifact_or_sentinel(
    archive_state %||% list(),
    artifact = "fold_archive",
    reason = "fold_archive_not_available",
    unit_key = unit_key,
    generated_at_utc = finished_at_utc
  )

  diagnostics_output <- attempt$execution$diagnostics %||% list()
  completion_gate_output <- artifact_or_sentinel(
    attempt$execution$completion_gate %||% list(),
    artifact = "completion_gate",
    reason = "completion_gate_not_available",
    unit_key = unit_key,
    generated_at_utc = finished_at_utc
  )

  json_repair_output <- tryCatch(
    as.list(attempt$execution$json_repair %||% list()),
    error = function(e) list()
  )

  invoke_error_output <- list()
  if (length(json_repair_output) > 0L) {
    fallback_event <- NULL
    for (event in json_repair_output) {
      if (is.list(event) && identical(event$repair_reason %||% "", "invoke_exception_fallback")) {
        fallback_event <- event
        break
      }
    }
    if (!is.null(fallback_event)) {
      invoke_error_output <- list(
        error_type = fallback_event$error_type %||% NA_character_,
        error_message = fallback_event$error_message %||% NA_character_,
        retry_attempts = fallback_event$retry_attempts %||% NA_integer_,
        retry_max_attempts = fallback_event$retry_max_attempts %||% NA_integer_,
        retryable_error = fallback_event$retryable_error %||% NA
      )
    }
  }
  invoke_error_output <- artifact_or_sentinel(
    invoke_error_output,
    artifact = "invoke_error",
    reason = "invoke_exception_absent",
    unit_key = unit_key,
    generated_at_utc = finished_at_utc
  )

  tool_names <- collect_tool_names(attempt)
  error_message <- if (identical(attempt_status, "success")) NA_character_ else trim_or_na(attempt$message)
  execution_summary_output <- list(
    status = attempt_status,
    unit = unit_meta,
    started_at_utc = started_at_utc,
    finished_at_utc = finished_at_utc,
    elapsed_time = attempt$elapsed_time %||% NA_real_,
    error_message = error_message,
    tool_names = tool_names,
    unit_dir = normalizePath(unit_dir, mustWork = FALSE),
    field_status = attempt$execution$field_status %||% list(),
    diagnostics = diagnostics_output,
    completion_gate = attempt$execution$completion_gate %||% list()
  )

  readr::write_file(
    jsonlite::toJSON(attempt$token_stats %||% list(), auto_unbox = TRUE, pretty = TRUE, null = "null"),
    unit_paths$token_stats
  )
  write_json_pretty(plan_output, unit_paths$plan_output)
  write_json_pretty(fold_stats_output, unit_paths$fold_stats)
  write_json_pretty(fold_summary_output, unit_paths$fold_summary)
  write_json_pretty(fold_archive_output, unit_paths$fold_archive)
  write_json_pretty(execution_summary_output, unit_paths$execution_summary)
  write_json_pretty(diagnostics_output, unit_paths$diagnostics)
  write_json_pretty(completion_gate_output, unit_paths$completion_gate)
  write_json_pretty(json_repair_output, unit_paths$json_repair)
  write_json_pretty(invoke_error_output, unit_paths$invoke_error)
  readr::write_file(extract_action_ascii(attempt), unit_paths$action_ascii)
  write_json_pretty(final_answer, unit_paths$answer_pred)
  write_json_pretty(
    build_payload_release_audit(
      attempt = attempt,
      final_answer = final_answer,
      unit_key = unit_key,
      generated_at_utc = finished_at_utc
    ),
    unit_paths$payload_release_audit
  )

  list(
    summary = list(
      unit_key = unit_key,
      country = unit_meta$country,
      row_id = unit_meta$row_id,
      person_name = unit_meta$person_name,
      status = attempt_status,
      elapsed_time = attempt$elapsed_time %||% NA_real_,
      tokens_used = as.numeric((attempt$token_stats %||% list())$tokens_used %||% NA_real_),
      error_message = error_message,
      unit_dir = normalizePath(unit_dir, mustWork = FALSE)
    ),
    tool_names = tool_names,
    files_written = unname(unlist(unit_paths))
  )
}

emulation_profile <- "fast"
use_fast_emulation <- identical(emulation_profile, "fast")

search_budget_limit <- if (use_fast_emulation) 8L else 14L
unknown_after_limit <- if (use_fast_emulation) 3L else 5L
agent_recursion_limit <- if (use_fast_emulation) 40L else 64L

search_timeout_s <- if (use_fast_emulation) 15.0 else 30.0
search_max_retries <- if (use_fast_emulation) 2L else 3L
search_retry_delay_s <- if (use_fast_emulation) 0.8 else 2.0
search_backoff_multiplier <- if (use_fast_emulation) 1.25 else 1.5
search_inter_delay_s <- if (use_fast_emulation) 0.6 else 1.5
search_jitter_factor <- if (use_fast_emulation) 0.25 else 0.5
search_max_results <- if (use_fast_emulation) 8L else 10L

webpage_timeout_s <- if (use_fast_emulation) 20.0 else 45.0
auto_openwebpage_policy <- "auto"

orchestration_options_override <- if (use_fast_emulation) {
  list(
    retrieval_controller = list(
      enabled = TRUE,
      mode = "enforce",
      max_empty_round_streak = 2L,
      adaptive_budget_enabled = TRUE,
      adaptive_patience_steps = 2L,
      adaptive_low_value_threshold = 0.08
    ),
    field_resolver = list(
      webpage_extraction_enabled = TRUE,
      search_snippet_extraction_enabled = TRUE,
      search_snippet_extraction_engine = "triage_then_structured",
      search_snippet_extraction_max_sources_per_round = 2L,
      search_snippet_extraction_max_total_sources = 6L
    ),
    finalizer = list(
      enabled = TRUE,
      mode = "enforce"
    )
  )
} else {
  list(
    field_resolver = list(
      search_snippet_extraction_engine = "triage_then_structured"
    )
  )
}

source_policy_override <- list(
  min_candidate_score = 0.62,
  min_source_quality = 0.32,
  min_source_specificity = 0.28,
  min_source_quality_textual_nonfree = 0.40,
  min_source_tier_for_non_unknown = "secondary",
  min_source_tier_for_sensitive_fields = "secondary",
  allow_tertiary_sources = FALSE,
  sensitive_fields_require_explicit_disclosure = TRUE,
  deny_host_fragments = c(
    "idcrawl.",
    "spokeo.",
    "truthfinder.",
    "whitepages.",
    "fastpeoplesearch.",
    "radaris.",
    "peekyou.",
    "mylife.",
    "beenverified.",
    "peoplefinders.",
    "searchpeoplefree."
  )
)

retry_policy_override <- list(
  rewrite_after_streak = 2L,
  stop_after_streak = 3L,
  no_new_evidence_replan_after_streak = 2L,
  no_new_evidence_stop_after_streak = 3L,
  no_new_evidence_high_quality_score = 0.72,
  no_new_evidence_min_tier = "secondary"
)

finalization_policy_override <- list(
  field_recovery_mode = "precision",
  field_recovery_min_source_quality = 0.34,
  diagnostics_auto_recovery_enabled = TRUE,
  quality_gate_enforce = TRUE,
  quality_gate_unknown_ratio_max = 0.85,
  quality_gate_min_global_confidence = 0.55,
  quality_gate_min_found_fields = 2L,
  quality_gate_require_invariant_ok = TRUE
)

PROMPT_TEMPLATE <- r"(TASK OVERVIEW:
You are a search-enabled research agent specializing in biographical information about political elites.

Research the following fields for the target person:
- educational attainment
- prior occupation
- birth year
- birth place
- disability status
- sexual orientation (publicly disclosed LGBTQ identity)

SEARCH STRATEGY:
- Start with Wikipedia, then use official biographies, government/parliament profiles, and credible news if needed.
- Cross-reference sources when signals conflict.

SOURCE REQUIREMENT:
- Provide exact, full URLs for every source field (no domain-only citations).

IMPORTANT GUIDELINES:
- Use explicit statements or reliable sources only.
- If information is not publicly disclosed, return Unknown.
- Do not guess or fabricate information.

CLASS BACKGROUND RULES:
- Identify the primary occupation before entering politics.
- Then map to class_background using:
  * Working class = manual labor, service, agriculture, trades, clerical.
  * Middle class/professional = teachers, lawyers, engineers, civil servants, managers.
  * Upper/elite = large business owners, top executives, aristocracy, major landowners.
  * Unknown = insufficient information.

TARGET INDIVIDUAL:
- Name: {{person_name}}
- Country: {{country}}
- Election Year: {{year}}
- Political Party: {{party}}
- Region/Constituency: {{region}}
- Known Birthday: {{bday}}
- Known Gender: {{gender}}
- Known Ethnicity: {{ethnicity}}

DISAMBIGUATION:
If multiple people share the same name, identify the correct person by matching (where possible):
- Country, Political party, Time period, Gender, Region

OUTPUT:
- Return strict JSON only.
)"

EXPECTED_SCHEMA <- list(
  education_level = "High School|Some College|Associate|Bachelor's|Master's/Professional|PhD|Unknown",
  education_institution = "string|Unknown",
  education_field = "string|Unknown",
  prior_occupation = "string|Unknown",
  class_background = "Working class|Middle class/professional|Upper/elite|Unknown",
  disability_status = "No disability|Some disability|Unknown",
  birth_place = "string|Unknown",
  birth_year = "integer|null|Unknown",
  lgbtq_status = "Non-LGBTQ|Openly LGBTQ|Unknown",
  lgbtq_details = "string|null",
  confidence = "number",
  justification = "string"
)

build_agent_config_summary <- function(tor_registry_path) {
  list(
    backend = "gemini",
    model = "gemini-3-flash-preview",
    proxy = "ASA_PROXY (per-worker)",
    proxy_env_var = "ASA_PROXY",
    tor_control_port_env_var = "TOR_CONTROL_PORT",
    tor_registry_path = normalizePath(tor_registry_path, mustWork = FALSE),
    conda_env = TRACE_CONDA_ENV,
    use_browser = FALSE,
    use_memory_folding = TRUE,
    recursion_limit = agent_recursion_limit,
    memory_threshold = 32L,
    memory_keep_recent = 16L,
    fold_char_budget = 5L * 10000L,
    rate_limit = 0.3,
    timeout = 180L,
    plan_mode_enabled = TRUE,
    search_budget_limit = search_budget_limit,
    unknown_after_limit = unknown_after_limit,
    search_max_results = search_max_results,
    search_timeout_s = search_timeout_s,
    search_max_retries = search_max_retries,
    search_retry_delay_s = search_retry_delay_s,
    search_backoff_multiplier = search_backoff_multiplier,
    search_inter_delay_s = search_inter_delay_s,
    search_jitter_factor = search_jitter_factor,
    webpage_timeout_s = webpage_timeout_s,
    auto_openwebpage_policy = auto_openwebpage_policy
  )
}

trace_paths <- function(run_id, artifact_base_dir = TRACE_ARTIFACT_BASE_DIR) {
  run_dir <- file.path(path.expand(artifact_base_dir), run_id)
  logs_dir <- file.path(run_dir, "logs")
  list(
    run_id = run_id,
    artifact_base_dir = path.expand(artifact_base_dir),
    run_dir = run_dir,
    units_root_dir = file.path(run_dir, "units"),
    logs_dir = logs_dir,
    tor_dir = file.path(run_dir, "tor"),
    sample_manifest_path = file.path(run_dir, "sampled_units.csv"),
    run_summary_path = file.path(run_dir, "run_summary.json"),
    run_provenance_path = file.path(run_dir, "run_provenance.json"),
    run_context_path = file.path(run_dir, "run_context.json"),
    parallel_indices_path = file.path(logs_dir, "parallel_indices.txt"),
    parallel_joblog_path = file.path(logs_dir, "parallel_joblog.tsv"),
    parallel_stdout_path = file.path(logs_dir, "parallel_stdout.log"),
    parallel_stderr_path = file.path(logs_dir, "parallel_stderr.log"),
    supervisor_stdout_path = file.path(logs_dir, "supervisor_stdout.log"),
    supervisor_stderr_path = file.path(logs_dir, "supervisor_stderr.log"),
    supervisor_pid_path = file.path(logs_dir, "supervisor.pid"),
    tor_registry_path = file.path(run_dir, "tor", "tor_exit_registry.sqlite")
  )
}

ensure_trace_directories <- function(paths) {
  dir.create(paths$run_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(paths$units_root_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(paths$logs_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(paths$tor_dir, recursive = TRUE, showWarnings = FALSE)
}

create_run_context <- function(run_id,
                               paths,
                               input_db_path,
                               sampling_seed,
                               run_started_at_utc = format_utc(),
                               parallel_jobs = TRACE_PARALLEL_JOBS,
                               expected_unit_count = TRACE_EXPECTED_UNIT_COUNT,
                               tor_socks_port_start = TRACE_TOR_SOCKS_PORT_START,
                               tor_control_port_start = TRACE_TOR_CONTROL_PORT_START) {
  list(
    run_id = run_id,
    run_started_at_utc = run_started_at_utc,
    output_root = normalizePath(paths$run_dir, mustWork = FALSE),
    units_root = normalizePath(paths$units_root_dir, mustWork = FALSE),
    logs_root = normalizePath(paths$logs_dir, mustWork = FALSE),
    tor_root = normalizePath(paths$tor_dir, mustWork = FALSE),
    run_context_path = normalizePath(paths$run_context_path, mustWork = FALSE),
    source_db_path = input_db_path,
    source_table = TRACE_SOURCE_TABLE,
    sampling_seed = as.integer(sampling_seed),
    sampled_units_path = normalizePath(paths$sample_manifest_path, mustWork = FALSE),
    expected_unit_count = as.integer(expected_unit_count),
    parallel_jobs = as.integer(parallel_jobs),
    tor_socks_port_start = as.integer(tor_socks_port_start),
    tor_control_port_start = as.integer(tor_control_port_start),
    tor_registry_path = normalizePath(paths$tor_registry_path, mustWork = FALSE),
    launcher = list(
      joblog_path = normalizePath(paths$parallel_joblog_path, mustWork = FALSE),
      parallel_stdout_path = normalizePath(paths$parallel_stdout_path, mustWork = FALSE),
      parallel_stderr_path = normalizePath(paths$parallel_stderr_path, mustWork = FALSE),
      supervisor_stdout_path = normalizePath(paths$supervisor_stdout_path, mustWork = FALSE),
      supervisor_stderr_path = normalizePath(paths$supervisor_stderr_path, mustWork = FALSE),
      supervisor_pid_path = normalizePath(paths$supervisor_pid_path, mustWork = FALSE)
    ),
    agent_config = build_agent_config_summary(paths$tor_registry_path)
  )
}

load_run_context <- function(run_id, artifact_base_dir = TRACE_ARTIFACT_BASE_DIR) {
  paths <- trace_paths(run_id, artifact_base_dir = artifact_base_dir)
  context <- read_json_if_exists(paths$run_context_path, simplify_vector = TRUE)
  if (is.null(context)) {
    stop(sprintf("Run context not found: %s", paths$run_context_path))
  }
  context$paths <- paths
  context$expected_unit_count <- as.integer(context$expected_unit_count %||% TRACE_EXPECTED_UNIT_COUNT)
  context$parallel_jobs <- as.integer(context$parallel_jobs %||% TRACE_PARALLEL_JOBS)
  context$tor_socks_port_start <- as.integer(context$tor_socks_port_start %||% TRACE_TOR_SOCKS_PORT_START)
  context$tor_control_port_start <- as.integer(context$tor_control_port_start %||% TRACE_TOR_CONTROL_PORT_START)
  context
}

setup_trace_run <- function(run_id = NULL,
                            artifact_base_dir = TRACE_ARTIFACT_BASE_DIR,
                            input_db_path = TRACE_INPUT_DB_PATH,
                            sampling_seed = TRACE_SAMPLING_SEED,
                            require_existing_manifest = FALSE) {
  if (is.null(run_id) || !nzchar(run_id)) {
    run_id <- paste0("trace_real_15units_", format(Sys.time(), "%Y%m%dT%H%M%S"))
  }

  paths <- trace_paths(run_id, artifact_base_dir = artifact_base_dir)
  ensure_trace_directories(paths)

  existing_context <- read_json_if_exists(paths$run_context_path, simplify_vector = TRUE)
  run_started_at_utc <- existing_context$run_started_at_utc %||% format_utc()

  if (file.exists(paths$sample_manifest_path)) {
    sampled_units <- read_sample_manifest(
      paths$sample_manifest_path,
      expected_unit_count = TRACE_EXPECTED_UNIT_COUNT
    )
  } else {
    if (isTRUE(require_existing_manifest)) {
      stop(sprintf(
        "Restart requested but sampled_units.csv is missing for run %s",
        run_id
      ))
    }
    message(sprintf("Sampling %d real units from %s", TRACE_EXPECTED_UNIT_COUNT, input_db_path))
    sampled_units <- sample_real_units(
      input_db_path = input_db_path,
      sampling_seed = sampling_seed
    )
    readr::write_csv(sampled_units, paths$sample_manifest_path)
  }

  run_context <- create_run_context(
    run_id = run_id,
    paths = paths,
    input_db_path = input_db_path,
    sampling_seed = sampling_seed,
    run_started_at_utc = run_started_at_utc
  )
  write_json_pretty(run_context, paths$run_context_path)

  run_context$paths <- paths
  run_context$sampled_units <- sampled_units
  run_context
}

emit_setup_env <- function(run_context) {
  paths <- run_context$paths
  cat(sprintf("RUN_ID=%s\n", run_context$run_id))
  cat(sprintf("RUN_DIR=%s\n", normalizePath(paths$run_dir, mustWork = FALSE)))
  cat(sprintf("UNITS_ROOT_DIR=%s\n", normalizePath(paths$units_root_dir, mustWork = FALSE)))
  cat(sprintf("LOGS_DIR=%s\n", normalizePath(paths$logs_dir, mustWork = FALSE)))
  cat(sprintf("SAMPLE_MANIFEST_PATH=%s\n", normalizePath(paths$sample_manifest_path, mustWork = FALSE)))
  cat(sprintf("RUN_CONTEXT_PATH=%s\n", normalizePath(paths$run_context_path, mustWork = FALSE)))
  cat(sprintf("RUN_STARTED_AT_UTC=%s\n", run_context$run_started_at_utc))
  cat(sprintf("PARALLEL_JOBLOG_PATH=%s\n", normalizePath(paths$parallel_joblog_path, mustWork = FALSE)))
  cat(sprintf("PARALLEL_STDOUT_PATH=%s\n", normalizePath(paths$parallel_stdout_path, mustWork = FALSE)))
  cat(sprintf("PARALLEL_STDERR_PATH=%s\n", normalizePath(paths$parallel_stderr_path, mustWork = FALSE)))
  cat(sprintf("SUPERVISOR_STDOUT_PATH=%s\n", normalizePath(paths$supervisor_stdout_path, mustWork = FALSE)))
  cat(sprintf("SUPERVISOR_STDERR_PATH=%s\n", normalizePath(paths$supervisor_stderr_path, mustWork = FALSE)))
  cat(sprintf("SUPERVISOR_PID_PATH=%s\n", normalizePath(paths$supervisor_pid_path, mustWork = FALSE)))
}

initialize_trace_agent <- function(run_context, verbose = TRUE) {
  message(sprintf(
    "Trace profile=%s | budget=%d | unknown_after=%d | search_timeout=%.1fs | search_retries=%d | webpage_timeout=%.1fs | auto_openwebpage=%s",
    emulation_profile,
    search_budget_limit,
    unknown_after_limit,
    search_timeout_s,
    search_max_retries,
    webpage_timeout_s,
    auto_openwebpage_policy
  ))

  initialize_agent(
    backend = "gemini",
    model = "gemini-3-flash-preview",
    conda_env = TRACE_CONDA_ENV,
    proxy = NA,
    tor = tor_options(registry_path = run_context$tor_registry_path),
    use_browser = FALSE,
    use_memory_folding = TRUE,
    recursion_limit = agent_recursion_limit,
    memory_threshold = 32L,
    memory_keep_recent = 16L,
    fold_char_budget = 5L * 10000L,
    rate_limit = 0.3,
    timeout = 180L,
    verbose = verbose,
    search = search_options(
      max_results = search_max_results,
      timeout = search_timeout_s,
      max_retries = search_max_retries,
      retry_delay = search_retry_delay_s,
      backoff_multiplier = search_backoff_multiplier,
      inter_search_delay = search_inter_delay_s,
      humanize_timing = TRUE,
      jitter_factor = search_jitter_factor,
      auto_openwebpage_policy = auto_openwebpage_policy,
      wiki_top_k_results = 3L,
      wiki_doc_content_chars_max = 1200L,
      search_doc_content_chars_max = 1200L,
      allow_read_webpages = TRUE,
      webpage_timeout = webpage_timeout_s,
      webpage_pdf_timeout = webpage_timeout_s,
      webpage_max_chars = 12000L,
      webpage_max_chunks = 8L,
      webpage_chunk_chars = 1200L
    )
  )
}

run_trace_worker <- function(run_id,
                             unit_index,
                             artifact_base_dir = TRACE_ARTIFACT_BASE_DIR,
                             verbose = TRUE) {
  run_context <- load_run_context(run_id, artifact_base_dir = artifact_base_dir)
  paths <- run_context$paths
  sampled_units <- read_sample_manifest(
    paths$sample_manifest_path,
    expected_unit_count = run_context$expected_unit_count
  )

  unit_index <- as.integer(unit_index)
  if (is.na(unit_index) || unit_index < 1L || unit_index > nrow(sampled_units)) {
    stop(sprintf(
      "unit_index must be between 1 and %d; received %s",
      nrow(sampled_units),
      as.character(unit_index)
    ))
  }

  row <- sampled_units[sampled_units$unit_index == unit_index, , drop = FALSE]
  if (nrow(row) != 1L) {
    stop(sprintf(
      "Expected exactly one manifest row for unit_index=%d, found %d",
      unit_index,
      nrow(row)
    ))
  }

  unit_meta <- build_unit_meta(row, unit_index = unit_index)
  unit_dir <- file.path(paths$units_root_dir, unit_meta$unit_key)
  prompt <- build_prompt_for_row(row)
  unit_started_at_utc <- format_utc()

  message(sprintf(
    "[%02d/%02d] %s | %s | row_id=%d | proxy=%s | control_port=%s",
    unit_index,
    nrow(sampled_units),
    unit_meta$country,
    unit_meta$person_name,
    unit_meta$row_id,
    Sys.getenv("ASA_PROXY", unset = ""),
    Sys.getenv("TOR_CONTROL_PORT", unset = "")
  ))

  agent <- initialize_trace_agent(run_context, verbose = verbose)

  attempt <- NULL
  unit_error <- NULL
  tryCatch({
    attempt <- asa:::.with_heartbeat(
      fn = function() {
        run_task(
          prompt = prompt,
          output_format = "raw",
          expected_fields = NULL,
          expected_schema = EXPECTED_SCHEMA,
          verbose = verbose,
          use_plan_mode = TRUE,
          search_budget_limit = search_budget_limit,
          unknown_after_searches = unknown_after_limit,
          source_policy = source_policy_override,
          retry_policy = retry_policy_override,
          finalization_policy = finalization_policy_override,
          query_templates = NULL,
          orchestration_options = orchestration_options_override,
          agent = agent
        )
      },
      label = sprintf("trace_test_real_15units[%s][%s]", emulation_profile, unit_meta$unit_key),
      interval_sec = if (use_fast_emulation) 15L else 25L,
      start_phase = "backend_invoke",
      start_detail = "run_task",
      complete_phase = "backend_complete",
      complete_detail = "run_task_returned",
      error_phase = "backend_error",
      verbose = verbose
    )
  }, error = function(e) {
    unit_error <<- e
  })

  unit_finished_at_utc <- format_utc()
  write_unit_bundle(
    unit_dir = unit_dir,
    unit_key = unit_meta$unit_key,
    unit_meta = unit_meta,
    prompt = prompt,
    started_at_utc = unit_started_at_utc,
    finished_at_utc = unit_finished_at_utc,
    attempt = attempt,
    unit_error = unit_error
  )
}

rebuild_unit_output_from_artifacts <- function(row, unit_index, units_root_dir) {
  unit_meta <- build_unit_meta(row, unit_index = unit_index)
  unit_dir <- file.path(units_root_dir, unit_meta$unit_key)
  unit_paths <- unit_artifact_paths(unit_dir)
  missing_files <- unlist(unit_paths[!file.exists(unlist(unit_paths))], use.names = FALSE)
  existing_files <- if (dir.exists(unit_dir)) {
    normalizePath(list.files(unit_dir, full.names = TRUE), mustWork = FALSE)
  } else {
    character(0)
  }

  if (length(missing_files) > 0L) {
    return(list(
      summary = list(
        unit_key = unit_meta$unit_key,
        country = unit_meta$country,
        row_id = unit_meta$row_id,
        person_name = unit_meta$person_name,
        status = "error",
        elapsed_time = NA_real_,
        tokens_used = NA_real_,
        error_message = sprintf(
          "Missing unit artifacts: %s",
          paste(basename(missing_files), collapse = ", ")
        ),
        unit_dir = normalizePath(unit_dir, mustWork = FALSE)
      ),
      tool_names = character(0),
      files_written = existing_files
    ))
  }

  execution_summary <- read_json_if_exists(unit_paths$execution_summary, simplify_vector = TRUE) %||% list()
  token_stats <- read_json_if_exists(unit_paths$token_stats, simplify_vector = TRUE) %||% list()
  status <- trim_or_na(execution_summary$status)
  if (is.na(status)) {
    status <- "error"
  }
  error_message <- trim_or_na(execution_summary$error_message)
  if (identical(status, "success")) {
    error_message <- NA_character_
  }

  list(
    summary = list(
      unit_key = unit_meta$unit_key,
      country = unit_meta$country,
      row_id = unit_meta$row_id,
      person_name = unit_meta$person_name,
      status = status,
      elapsed_time = suppressWarnings(as.numeric(execution_summary$elapsed_time %||% NA_real_)),
      tokens_used = suppressWarnings(as.numeric(token_stats$tokens_used %||% NA_real_)),
      error_message = error_message,
      unit_dir = normalizePath(unit_dir, mustWork = FALSE)
    ),
    tool_names = to_character_vector(execution_summary$tool_names %||% character(0)),
    files_written = normalizePath(unlist(unit_paths), mustWork = FALSE)
  )
}

collect_run_file_inventory <- function(paths) {
  existing_files <- if (dir.exists(paths$run_dir)) {
    normalizePath(
      list.files(paths$run_dir, recursive = TRUE, full.names = TRUE),
      mustWork = FALSE
    )
  } else {
    character(0)
  }
  sort(unique(existing_files))
}

aggregate_trace_run <- function(run_id,
                                artifact_base_dir = TRACE_ARTIFACT_BASE_DIR,
                                verbose = TRUE) {
  run_context <- load_run_context(run_id, artifact_base_dir = artifact_base_dir)
  paths <- run_context$paths
  sampled_units <- read_sample_manifest(
    paths$sample_manifest_path,
    expected_unit_count = run_context$expected_unit_count
  )

  unit_results <- vector("list", nrow(sampled_units))
  all_tool_names <- character(0)

  for (idx in seq_len(nrow(sampled_units))) {
    row <- sampled_units[idx, , drop = FALSE]
    unit_output <- rebuild_unit_output_from_artifacts(
      row = row,
      unit_index = idx,
      units_root_dir = paths$units_root_dir
    )
    unit_results[[idx]] <- unit_output$summary
    all_tool_names <- unique(c(all_tool_names, unit_output$tool_names))
  }

  run_finished_at_utc <- format_utc()
  success_count <- sum(vapply(unit_results, function(x) identical(x$status, "success"), logical(1)))
  error_count <- sum(vapply(unit_results, function(x) identical(x$status, "error"), logical(1)))

  run_summary <- list(
    run_id = run_context$run_id,
    generated_at_utc = run_finished_at_utc,
    n_units = length(unit_results),
    success_count = success_count,
    error_count = error_count,
    units = unit_results
  )
  write_json_pretty(run_summary, paths$run_summary_path)

  run_provenance <- list(
    run_id = run_context$run_id,
    generated_at_utc = run_finished_at_utc,
    run_started_at_utc = run_context$run_started_at_utc,
    run_finished_at_utc = run_finished_at_utc,
    output_root = normalizePath(paths$run_dir, mustWork = FALSE),
    units_root = normalizePath(paths$units_root_dir, mustWork = FALSE),
    logs_root = normalizePath(paths$logs_dir, mustWork = FALSE),
    tor_root = normalizePath(paths$tor_dir, mustWork = FALSE),
    source_db_path = run_context$source_db_path,
    source_table = run_context$source_table %||% TRACE_SOURCE_TABLE,
    sampling_seed = as.integer(run_context$sampling_seed %||% TRACE_SAMPLING_SEED),
    sampled_units_path = normalizePath(paths$sample_manifest_path, mustWork = FALSE),
    run_context_path = normalizePath(paths$run_context_path, mustWork = FALSE),
    distinct_countries = as.list(unique(sampled_units$country)),
    launcher = list(
      mode = "background_supervisor",
      parallel_jobs = as.integer(run_context$parallel_jobs %||% TRACE_PARALLEL_JOBS),
      tor_socks_port_start = as.integer(run_context$tor_socks_port_start %||% TRACE_TOR_SOCKS_PORT_START),
      tor_control_port_start = as.integer(run_context$tor_control_port_start %||% TRACE_TOR_CONTROL_PORT_START),
      joblog_path = normalizePath(paths$parallel_joblog_path, mustWork = FALSE),
      parallel_stdout_path = normalizePath(paths$parallel_stdout_path, mustWork = FALSE),
      parallel_stderr_path = normalizePath(paths$parallel_stderr_path, mustWork = FALSE),
      supervisor_stdout_path = normalizePath(paths$supervisor_stdout_path, mustWork = FALSE),
      supervisor_stderr_path = normalizePath(paths$supervisor_stderr_path, mustWork = FALSE),
      supervisor_pid_path = normalizePath(paths$supervisor_pid_path, mustWork = FALSE)
    ),
    agent_config = run_context$agent_config %||% build_agent_config_summary(paths$tor_registry_path),
    files_written = as.list(unique(c(
      collect_run_file_inventory(paths),
      normalizePath(paths$run_summary_path, mustWork = FALSE),
      normalizePath(paths$run_provenance_path, mustWork = FALSE)
    ))),
    tools_called = as.list(sort(unique(all_tool_names)))
  )
  write_json_pretty(run_provenance, paths$run_provenance_path)

  if (isTRUE(verbose)) {
    message(sprintf(
      "Trace run complete: %d success, %d error. Summary: %s",
      success_count,
      error_count,
      paths$run_summary_path
    ))
  }

  list(
    run_summary = run_summary,
    run_provenance = run_provenance
  )
}

run_trace_sequential <- function(artifact_base_dir = TRACE_ARTIFACT_BASE_DIR,
                                 input_db_path = TRACE_INPUT_DB_PATH,
                                 sampling_seed = TRACE_SAMPLING_SEED,
                                 verbose = TRUE) {
  if (Sys.info()[["sysname"]] == "Darwin") {
    try(system("brew services start tor", ignore.stdout = TRUE, ignore.stderr = TRUE), silent = TRUE)
  }
  if (!nzchar(Sys.getenv("ASA_PROXY", unset = ""))) {
    Sys.setenv(ASA_PROXY = sprintf("socks5h://127.0.0.1:%d", TRACE_TOR_SOCKS_PORT_START))
  }
  if (!nzchar(Sys.getenv("TOR_CONTROL_PORT", unset = ""))) {
    Sys.setenv(TOR_CONTROL_PORT = as.character(TRACE_TOR_CONTROL_PORT_START))
  }

  run_context <- setup_trace_run(
    artifact_base_dir = artifact_base_dir,
    input_db_path = input_db_path,
    sampling_seed = sampling_seed,
    require_existing_manifest = FALSE
  )

  sampled_units <- run_context$sampled_units %||% read_sample_manifest(
    run_context$paths$sample_manifest_path,
    expected_unit_count = TRACE_EXPECTED_UNIT_COUNT
  )
  for (idx in seq_len(nrow(sampled_units))) {
    run_trace_worker(
      run_id = run_context$run_id,
      unit_index = idx,
      artifact_base_dir = artifact_base_dir,
      verbose = verbose
    )
  }

  aggregate_trace_run(
    run_id = run_context$run_id,
    artifact_base_dir = artifact_base_dir,
    verbose = verbose
  )
}

parse_trace_args <- function(args) {
  parsed <- list(mode = "sequential")
  if (length(args) == 0L) {
    return(parsed)
  }

  i <- 1L
  while (i <= length(args)) {
    arg <- args[[i]]
    if (!startsWith(arg, "--")) {
      stop(sprintf("Unexpected positional argument: %s", arg), call. = FALSE)
    }

    key <- sub("^--", "", arg)
    if (identical(key, "help")) {
      parsed$help <- TRUE
      i <- i + 1L
      next
    }

    if (i == length(args)) {
      stop(sprintf("Missing value for --%s", key), call. = FALSE)
    }

    value <- args[[i + 1L]]
    parsed[[gsub("-", "_", key)]] <- value
    i <- i + 2L
  }

  parsed
}

print_trace_usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript trace_test_real_15units.R",
    "  Rscript trace_test_real_15units.R --mode setup [--run-id <trace_real_15units_...>]",
    "  Rscript trace_test_real_15units.R --mode worker --run-id <trace_real_15units_...> --unit-index <1-15>",
    "  Rscript trace_test_real_15units.R --mode aggregate --run-id <trace_real_15units_...>",
    sep = "\n"
  ))
  cat("\n")
}

main <- function() {
  args <- parse_trace_args(commandArgs(trailingOnly = TRUE))
  if (isTRUE(args$help)) {
    print_trace_usage()
    return(invisible(NULL))
  }

  mode <- args$mode %||% "sequential"
  run_id <- trim_or_na(args$run_id)
  unit_index <- trim_or_na(args$unit_index)

  if (identical(mode, "setup")) {
    run_context <- setup_trace_run(
      run_id = if (is.na(run_id)) NULL else run_id,
      require_existing_manifest = !is.na(run_id)
    )
    emit_setup_env(run_context)
    return(invisible(run_context))
  }

  if (identical(mode, "worker")) {
    if (is.na(run_id) || is.na(unit_index)) {
      stop("--mode worker requires --run-id and --unit-index", call. = FALSE)
    }
    return(invisible(run_trace_worker(
      run_id = run_id,
      unit_index = as.integer(unit_index)
    )))
  }

  if (identical(mode, "aggregate")) {
    if (is.na(run_id)) {
      stop("--mode aggregate requires --run-id", call. = FALSE)
    }
    return(invisible(aggregate_trace_run(run_id = run_id)))
  }

  if (!identical(mode, "sequential")) {
    stop(sprintf("Unknown mode: %s", mode), call. = FALSE)
  }

  invisible(run_trace_sequential())
}

if (sys.nframe() == 0L) {
  main()
}
