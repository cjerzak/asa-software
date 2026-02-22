# Rscript: trace_test_real.R

# outputs:
#./asa-software/tracked_reports/prompt_example_real.txt
#./asa-software/tracked_reports/trace_real.txt        
#./asa-software/tracked_reports/token_stats_real.txt
#./asa-software/tracked_reports/plan_output_real.txt
#./asa-software/tracked_reports/fold_stats_real.txt
#./asa-software/tracked_reports/fold_summary_real.txt
#./asa-software/tracked_reports/fold_archive_real.txt
#./asa-software/tracked_reports/execution_summary_real.txt
#./asa-software/tracked_reports/diagnostics_real.txt
#./asa-software/tracked_reports/json_repair_real.txt
#./asa-software/tracked_reports/invoke_error_real.txt
#./asa-software/tracked_reports/action_ascii_real.txt
#./asa-software/tracked_reports/answer_pred_real.txt
#./asa-software/tracked_reports/payload_release_audit_real.txt
options(error=NULL)
# install.packages( "~/Documents/asa-software/asa",repos = NULL, type = "source",force = F);
# devtools::load_all('~/Documents/asa-software/asa')
# devtools::install_github( 'cjerzak/asa-software/asa' )
# asa::build_backend(force = TRUE, fix_browser = TRUE)

# Prefer local package source so trace runs validate current repo code.
devtools::load_all('~/Documents/asa-software/asa')

trace_run_timestamp <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
trace_run_id <- paste0("trace_real_", format(Sys.time(), "%Y%m%dT%H%M%S"))

artifact_sentinel <- function(artifact, reason = "artifact_not_available") {
  list(
    status = "empty",
    artifact = artifact,
    reason = reason,
    run_id = trace_run_id,
    generated_at = trace_run_timestamp
  )
}

artifact_or_sentinel <- function(value,
                                 artifact,
                                 reason = "artifact_not_available") {
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
    return(artifact_sentinel(artifact = artifact, reason = reason))
  }
  value
}

plan_mode_enabled <- FALSE

prompt <- r"(TASK OVERVIEW:
You are a search-enabled research agent specializing in biographical information about political elites.

Research the following fields for the target person:
- educational attainment
- prior occupation (for class background)
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
- Name: Ramona Moye Camaconi
- Country: Bolivia
- Election Year: 2014
- Political Party: Movimiento Al Socialismo - MAS
- Region/Constituency: Beni
- Known Birthday: Not available
- Known Gender: Female
- Known Ethnicity: Indigenous

DISAMBIGUATION:
If multiple people share the same name, identify the correct person by matching:
- Country: Bolivia
- Political party: Movimiento Al Socialismo - MAS
- Time period: Active around 2014
- Gender: Female
- Region: Beni

OUTPUT:
- Return strict JSON only.
)"

EXPECTED_SCHEMA <- list(
  education_level = "High School|Some College|Associate|Bachelor's|Master's/Professional|PhD|Unknown",
  education_institution = "string|Unknown",
  education_field = "string|Unknown",
  education_source = "string|null",
  
  prior_occupation = "string|Unknown",
  class_background = "Working class|Middle class/professional|Upper/elite|Unknown",
  prior_occupation_source = "string|null",
  
  disability_status = "No disability|Some disability|Unknown",
  disability_source = "string|null",
  
  birth_place = "string|Unknown",
  birth_place_source = "string|null",
  
  birth_year = "integer|null|Unknown",
  birth_year_source = "string|null",
  
  lgbtq_status = "Non-LGBTQ|Openly LGBTQ|Unknown",
  lgbtq_details = "string|null",
  lgbtq_source = "string|null",
  
  confidence = "Low|Medium|High",
  justification = "string"
)

system("brew services start tor")
message(sprintf("Starting run_task at %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
attempt <- run_task(
    prompt = prompt,
    #output_format = "json",
    output_format = "raw",
    expected_fields = NULL,
    expected_schema = EXPECTED_SCHEMA,
    verbose = TRUE,
    use_plan_mode = plan_mode_enabled,
    search_budget_limit = 14L,
    unknown_after_searches = 5L,
    retry_policy = list(
      rewrite_after_streak = 2L,
      stop_after_streak = 3L
    ),
    agent = initialize_agent(
      #backend = "gemini", model = "gemini-2.5-pro",
      backend = "gemini", model = "gemini-3-pro-preview",
      #backend = "gemini", model = "gemini-3-flash-preview",
      #backend = "openai", model = "gpt-5-mini-2025-08-07",
      #backend = "openai", model = "gpt-5-nano-2025-08-07",
      proxy = "socks5h://127.0.0.1:9050",
      use_browser = FALSE, 
      use_memory_folding = TRUE,
      #recursion_limit = 32L, memory_threshold = 8L, memory_keep_recent = 4L, # production
      #recursion_limit = 32L, memory_threshold = 16L, memory_keep_recent = 8L, # production
      recursion_limit = 64L, memory_threshold = 32L, memory_keep_recent = 16L, # production
      #fold_char_budget = 5L * (10000L), # default is 30000L
      fold_char_budget = 5L * (10000L), # default is 30000L
      rate_limit = 0.3,
      timeout = 180L,
      verbose = TRUE,
      search = search_options(
        # Wikipedia tool output
        wiki_top_k_results = 3L,
        wiki_doc_content_chars_max = 1200L,
        
        # DuckDuckGo Search tool output (snippet chars returned to the LLM)
        search_doc_content_chars_max = 1200L, # (chars per word)*words
        
        # Full webpage reader (OpenWebpage tool output)
        allow_read_webpages = TRUE,
        webpage_max_chars = 12000L,
        webpage_max_chunks = 8L,
        webpage_chunk_chars = 1200L
      )
    )
)

# write to disk for further investigations.
readr::write_file(prompt, "~/Documents/asa-software/tracked_reports/prompt_example_real.txt")

# Structured trace (asa_trace_v1 format — each message is a distinct JSON object)
readr::write_file(
  attempt$trace_json,
  "~/Documents/asa-software/tracked_reports/trace_real.txt"
)

readr::write_file(jsonlite::toJSON(attempt$token_stats, auto_unbox = TRUE, pretty = TRUE, null = "null"), "~/Documents/asa-software/tracked_reports/token_stats_real.txt")
plan_output <- artifact_or_sentinel(
  attempt$plan,
  artifact = "plan_output",
  reason = if (isTRUE(plan_mode_enabled)) "plan_not_generated" else "plan_mode_disabled"
)
readr::write_file(
  jsonlite::toJSON(plan_output, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/plan_output_real.txt"
)

# fold_stats
fold_stats_output <- artifact_or_sentinel(
  attempt$fold_stats,
  artifact = "fold_stats",
  reason = "fold_stats_not_available"
)
readr::write_file(
  jsonlite::toJSON(fold_stats_output, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/fold_stats_real.txt"
)

# summary/archive from memory folding state (if present)
archive_state <- ummary_state <- NULL
if (!is.null(attempt$raw_response)) {
  summary_state <- tryCatch(reticulate::py_to_r(attempt$raw_response$summary), error = function(e) NULL)
  archive_state <- tryCatch(reticulate::py_to_r(attempt$raw_response$archive), error = function(e) NULL)
}
if (is.null(summary_state)) summary_state <- list()
if (is.null(archive_state)) archive_state <- list()

fold_summary_output <- artifact_or_sentinel(
  summary_state,
  artifact = "fold_summary",
  reason = "fold_summary_not_available"
)
fold_archive_output <- artifact_or_sentinel(
  archive_state,
  artifact = "fold_archive",
  reason = "fold_archive_not_available"
)

readr::write_file(
  jsonlite::toJSON(fold_summary_output, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/fold_summary_real.txt"
)
readr::write_file(
  jsonlite::toJSON(fold_archive_output, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/fold_archive_real.txt"
)

# execution summary (elapsed_time, field_status)
readr::write_file(
  jsonlite::toJSON(
    list(
      elapsed_time = attempt$elapsed_time,
      field_status = attempt$execution$field_status,
      diagnostics = attempt$execution$diagnostics
    ),
    auto_unbox = TRUE, pretty = TRUE, null = "null"
  ),
  "~/Documents/asa-software/tracked_reports/execution_summary_real.txt"
)

readr::write_file(
  jsonlite::toJSON(attempt$execution$diagnostics, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/diagnostics_real.txt"
)

# json repair events (includes invoke_exception_fallback error details when present)
json_repair <- attempt$execution$json_repair %||% list()
json_repair <- tryCatch(as.list(json_repair), error = function(e) list())
readr::write_file(
  jsonlite::toJSON(json_repair, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/json_repair_real.txt"
)

invoke_error <- NULL
if (is.list(json_repair) && length(json_repair) > 0) {
  fallback_ev <- NULL
  for (ev in json_repair) {
    if (is.list(ev) && identical(ev$repair_reason %||% "", "invoke_exception_fallback")) {
      fallback_ev <- ev
      break
    }
  }
  if (!is.null(fallback_ev)) {
    invoke_error <- list(
      error_type = fallback_ev$error_type %||% NA_character_,
      error_message = fallback_ev$error_message %||% NA_character_,
      retry_attempts = fallback_ev$retry_attempts %||% NA_integer_,
      retry_max_attempts = fallback_ev$retry_max_attempts %||% NA_integer_,
      retryable_error = fallback_ev$retryable_error %||% NA
    )
  }
}
if (is.null(invoke_error)) invoke_error <- list()
invoke_error <- artifact_or_sentinel(
  invoke_error,
  artifact = "invoke_error",
  reason = "invoke_exception_absent"
)

readr::write_file(
  jsonlite::toJSON(invoke_error, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/invoke_error_real.txt"
)

has_invoke_error <- (
  is.list(invoke_error) &&
    !is.null(invoke_error$error_type) &&
    !all(is.na(invoke_error$error_type))
)
if (isTRUE(has_invoke_error)) {
  message(
    sprintf(
      "Model invoke failed (%s): %s",
      invoke_error$error_type %||% "UnknownError",
      invoke_error$error_message %||% ""
    )
  )
}

# high-level action visualization (ASCII)
action_ascii_text <- attempt$action_ascii %||% attempt$execution$action_ascii %||% ""
if (!nzchar(action_ascii_text) &&
    is.character(attempt$trace_json) &&
    length(attempt$trace_json) == 1L &&
    nzchar(attempt$trace_json) &&
    exists(".extract_action_trace", envir = asNamespace("asa"), inherits = FALSE)) {
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

readr::write_file(
  action_ascii_text,
  "~/Documents/asa-software/tracked_reports/action_ascii_real.txt"
)

cat("Token stats:\n")
cat("  tokens_used:", attempt$token_stats$tokens_used, "\n")
cat("  input_tokens:", attempt$token_stats$input_tokens, "\n")
cat("  output_tokens:", attempt$token_stats$output_tokens, "\n")
cat("  elapsed_time:", attempt$elapsed_time, "\n")
cat("  fold_stats:", jsonlite::toJSON(attempt$fold_stats, auto_unbox = TRUE), "\n")

# save final answer (extract from structured trace on disk)
tmp <- readr::read_file("~/Documents/asa-software/tracked_reports/trace_real.txt")
extracted <- extract_agent_results(tmp)
final_answer <- extracted[["json_data_canonical"]] %||% extracted[["json_data"]]
if (is.null(final_answer) && is.character(attempt$message) && length(attempt$message) == 1L && nzchar(attempt$message)) {
  final_answer <- tryCatch(
    jsonlite::fromJSON(attempt$message, simplifyVector = FALSE),
    error = function(e) NULL
  )
}
message("Trace test complete")

jsonlite::write_json(
  final_answer,
  "~/Documents/asa-software/tracked_reports/answer_pred_real.txt",
  auto_unbox = TRUE,
  pretty = TRUE,
  null = "null"
)

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

payload_integrity <- attempt$execution$payload_integrity %||% list()
canonical_text <- to_json_scalar(extracted[["json_data_canonical"]])
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

payload_release_audit <- list(
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

readr::write_file(
  jsonlite::toJSON(payload_release_audit, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/payload_release_audit_real.txt"
)
cat(jsonlite::toJSON(final_answer, pretty = TRUE, auto_unbox = TRUE, null = "null"))
