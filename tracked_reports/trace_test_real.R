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
#./asa-software/tracked_reports/completion_gate_real.txt
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
trace_inference_started_at <- NA_character_
trace_inference_finished_at <- NA_character_
blind_benchmark_mode <- TRUE

artifact_dir <- normalizePath("~/Documents/asa-software/tracked_reports", mustWork = TRUE)
path_prompt <- file.path(artifact_dir, "prompt_example_real.txt")
path_trace <- file.path(artifact_dir, "trace_real.txt")
path_token_stats <- file.path(artifact_dir, "token_stats_real.txt")
path_plan_output <- file.path(artifact_dir, "plan_output_real.txt")
path_fold_stats <- file.path(artifact_dir, "fold_stats_real.txt")
path_fold_summary <- file.path(artifact_dir, "fold_summary_real.txt")
path_fold_archive <- file.path(artifact_dir, "fold_archive_real.txt")
path_execution_summary <- file.path(artifact_dir, "execution_summary_real.txt")
path_diagnostics <- file.path(artifact_dir, "diagnostics_real.txt")
path_completion_gate <- file.path(artifact_dir, "completion_gate_real.txt")
path_json_repair <- file.path(artifact_dir, "json_repair_real.txt")
path_invoke_error <- file.path(artifact_dir, "invoke_error_real.txt")
path_action_ascii <- file.path(artifact_dir, "action_ascii_real.txt")
path_answer_pred <- file.path(artifact_dir, "answer_pred_real.txt")
path_answer_gold <- file.path(artifact_dir, "answer_gold_real.txt")
path_payload_release_audit <- file.path(artifact_dir, "payload_release_audit_real.txt")
path_run_provenance <- file.path(artifact_dir, "run_provenance_real.txt")
path_eval_metrics <- file.path(artifact_dir, "evaluation_metrics_real.txt")
path_regression_gate <- file.path(artifact_dir, "regression_gate_real.txt")

read_json_if_exists <- function(path) {
  if (!is.character(path) || length(path) != 1L || !file.exists(path)) {
    return(NULL)
  }
  tryCatch(
    jsonlite::fromJSON(path, simplifyVector = FALSE),
    error = function(e) NULL
  )
}

baseline_snapshot <- list(
  answer_pred = read_json_if_exists(path_answer_pred),
  token_stats = read_json_if_exists(path_token_stats),
  diagnostics = read_json_if_exists(path_diagnostics),
  completion_gate = read_json_if_exists(path_completion_gate)
)

artifact_sentinel <- function(artifact,
                              reason = "artifact_not_available",
                              mode = NA_character_,
                              upstream_dependency = NA_character_) {
  resolved_mode <- as.character(mode %||% NA_character_)
  if (length(resolved_mode) == 0L || is.na(resolved_mode[[1]]) || !nzchar(resolved_mode[[1]])) {
    if (identical(reason, "plan_mode_disabled")) {
      resolved_mode <- "plan_disabled"
    } else if (grepl("not_available|absent|disabled", reason)) {
      resolved_mode <- "not_generated"
    } else {
      resolved_mode <- "error"
    }
  } else {
    resolved_mode <- resolved_mode[[1]]
  }

  resolved_dependency <- as.character(upstream_dependency %||% NA_character_)
  if (length(resolved_dependency) == 0L || is.na(resolved_dependency[[1]]) || !nzchar(resolved_dependency[[1]])) {
    resolved_dependency <- switch(
      artifact,
      plan_output = "plan_mode",
      fold_summary = "raw_response$summary",
      fold_archive = "raw_response$archive",
      invoke_error = "execution$json_repair",
      "upstream_pipeline_state"
    )
  } else {
    resolved_dependency <- resolved_dependency[[1]]
  }

  list(
    status = "empty",
    artifact = artifact,
    reason = reason,
    expected_empty = TRUE,
    mode = resolved_mode,
    upstream_dependency = resolved_dependency,
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

# Keep emulation on (humanized timing) while avoiding long stalls.
# Switch to "full" to restore slower, maximum-stealth pacing.
emulation_profile <- "fast"
#emulation_profile <- "full"
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
      search_snippet_extraction_max_sources_per_round = 2L,
      search_snippet_extraction_max_total_sources = 6L
    ),
    finalizer = list(
      enabled = TRUE,
      mode = "enforce"
    ),
    policy_version = "2026-02-24-trace-benchmark-v2"
  )
} else {
  NULL
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
  preferred_domains = c(
    "vicepresidencia.gob.bo",
    "oep.org.bo",
    "tse.org.bo",
    "bolivia.gob.bo"
  ),
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

query_templates_override <- NULL
#query_templates_override <- list(
#  focused_field_query = "\"{entity}\" {field} Bolivia MAS Beni 2014",
#  source_constrained_query = "site:{domain} \"{entity}\" {field} Bolivia",
#  disambiguation_query = "\"{entity}\" MAS Beni Bolivia diputado 2014"
#)

# Heartbeat lifecycle is managed by package internals:
# asa:::.heartbeat_start/.heartbeat_set_phase/.heartbeat_stop/.with_heartbeat

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

system("brew services start tor")
if (isTRUE(blind_benchmark_mode)) {
  message("Blind benchmark mode enabled: gold labels are not read during inference.")
}
message(sprintf("Starting run_task at %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
message(sprintf(
  paste0(
    "Trace profile=%s | budget=%d | unknown_after=%d | ",
    "search_timeout=%.1fs | search_retries=%d | webpage_timeout=%.1fs | auto_openwebpage=%s"
  ),
  emulation_profile,
  search_budget_limit,
  unknown_after_limit,
  search_timeout_s,
  search_max_retries,
  webpage_timeout_s,
  auto_openwebpage_policy
))
trace_inference_started_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
plan_mode_enabled <- FALSE
attempt <- asa:::.with_heartbeat(
  fn = function() {
    run_task(
      prompt = prompt,
      #output_format = "json",
      output_format = "raw",
      expected_fields = NULL,
      expected_schema = EXPECTED_SCHEMA,
      verbose = TRUE,
      use_plan_mode = plan_mode_enabled,
      search_budget_limit = search_budget_limit,
      unknown_after_searches = unknown_after_limit,
      source_policy = source_policy_override,
      retry_policy = retry_policy_override,
      finalization_policy = finalization_policy_override,
      query_templates = query_templates_override,
      orchestration_options = orchestration_options_override,
      agent = initialize_agent(
        #backend = "gemini", model = "gemini-2.5-pro",
        #backend = "gemini", model = "gemini-3-pro-preview",
        backend = "gemini", model = "gemini-3-flash-preview",
        #backend = "openai", model = "gpt-5-mini-2025-08-07",
        #backend = "openai", model = "gpt-5-nano-2025-08-07",
        proxy = "socks5h://127.0.0.1:9050",
        use_browser = FALSE,
        use_memory_folding = TRUE,
        #recursion_limit = 32L, memory_threshold = 8L, memory_keep_recent = 4L, # production
        #recursion_limit = 32L, memory_threshold = 16L, memory_keep_recent = 8L, # production
        recursion_limit = agent_recursion_limit, memory_threshold = 32L, memory_keep_recent = 16L, # production
        #fold_char_budget = 5L * (10000L), # default is 30000L
        fold_char_budget = 5L * (10000L), # default is 30000L
        rate_limit = 0.3,
        timeout = 180L,
        verbose = TRUE,
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
          # Wikipedia tool output
          wiki_top_k_results = 3L,
          wiki_doc_content_chars_max = 1200L,

          # DuckDuckGo Search tool output (snippet chars returned to the LLM)
          search_doc_content_chars_max = 1200L, # (chars per word)*words

          # Full webpage reader (OpenWebpage tool output)
          allow_read_webpages = TRUE,
          webpage_timeout = webpage_timeout_s,
          webpage_pdf_timeout = webpage_timeout_s,
          webpage_max_chars = 12000L,
          webpage_max_chunks = 8L,
          webpage_chunk_chars = 1200L
        )
      )
    )
  },
  label = sprintf("trace_test_real[%s]", emulation_profile),
  interval_sec = if (use_fast_emulation) 15L else 25L,
  start_phase = "backend_invoke",
  start_detail = "run_task",
  complete_phase = "backend_complete",
  complete_detail = "run_task_returned",
  error_phase = "backend_error",
  verbose = TRUE
)
trace_inference_finished_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

# write to disk for further investigations.
readr::write_file(prompt, path_prompt)

# Structured trace (asa_trace_v1 format — each message is a distinct JSON object)
readr::write_file(
  attempt$trace_json,
  path_trace
)

readr::write_file(
  jsonlite::toJSON(attempt$token_stats, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_token_stats
)
plan_output <- artifact_or_sentinel(
  attempt$plan,
  artifact = "plan_output",
  reason = if (isTRUE(plan_mode_enabled)) "plan_not_generated" else "plan_mode_disabled"
)
readr::write_file(
  jsonlite::toJSON(plan_output, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_plan_output
)

# fold_stats
fold_stats_output <- artifact_or_sentinel(
  attempt$fold_stats,
  artifact = "fold_stats",
  reason = "fold_stats_not_available"
)
readr::write_file(
  jsonlite::toJSON(fold_stats_output, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_fold_stats
)

# summary/archive from memory folding state (if present)
archive_state <- summary_state <- NULL
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
  path_fold_summary
)
readr::write_file(
  jsonlite::toJSON(fold_archive_output, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_fold_archive
)

# execution summary (elapsed_time, field_status)
readr::write_file(
  jsonlite::toJSON(
    list(
      elapsed_time = attempt$elapsed_time,
      field_status = attempt$execution$field_status,
      diagnostics = attempt$execution$diagnostics,
      completion_gate = attempt$execution$completion_gate
    ),
    auto_unbox = TRUE, pretty = TRUE, null = "null"
  ),
  path_execution_summary
)

readr::write_file(
  jsonlite::toJSON(attempt$execution$diagnostics, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_diagnostics
)

completion_gate_output <- artifact_or_sentinel(
  attempt$execution$completion_gate %||% list(),
  artifact = "completion_gate",
  reason = "completion_gate_not_available"
)
readr::write_file(
  jsonlite::toJSON(completion_gate_output, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_completion_gate
)

# json repair events (includes invoke_exception_fallback error details when present)
json_repair <- attempt$execution$json_repair %||% list()
json_repair <- tryCatch(as.list(json_repair), error = function(e) list())
readr::write_file(
  jsonlite::toJSON(json_repair, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_json_repair
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
  path_invoke_error
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
  path_action_ascii
)

cat("Token stats:\n")
cat("  tokens_used:", attempt$token_stats$tokens_used, "\n")
cat("  input_tokens:", attempt$token_stats$input_tokens, "\n")
cat("  output_tokens:", attempt$token_stats$output_tokens, "\n")
cat("  elapsed_time:", attempt$elapsed_time, "\n")
cat("  fold_stats:", jsonlite::toJSON(attempt$fold_stats, auto_unbox = TRUE), "\n")

# save final answer (extract from structured trace on disk)
tmp <- readr::read_file(path_trace)
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
  path_answer_pred,
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
  run_id = payload_integrity$run_id %||% trace_run_id,
  generated_at_utc = payload_integrity$generated_at_utc %||% trace_run_timestamp,
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
readr::write_file(
  jsonlite::toJSON(payload_release_audit, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_payload_release_audit
)

`%||%` <- function(x, y) if (is.null(x)) y else x

canonicalize_url <- function(url) {
  if (!is.character(url) || length(url) != 1L || is.na(url) || !nzchar(url)) {
    return(NA_character_)
  }
  x <- trimws(url)
  x <- sub("#.*$", "", x)
  scheme_split <- strsplit(x, "://", fixed = TRUE)[[1]]
  if (length(scheme_split) != 2L) {
    return(tolower(x))
  }
  scheme <- tolower(scheme_split[[1]])
  rest <- scheme_split[[2]]
  path_split <- strsplit(rest, "\\?", perl = TRUE)[[1]]
  host_path <- path_split[[1]]
  query <- if (length(path_split) > 1L) paste(path_split[-1], collapse = "?") else ""
  host <- tolower(sub("/.*$", "", host_path))
  path <- sub("^[^/]+", "", host_path)
  if (!nzchar(path)) path <- "/"
  params <- character(0)
  if (nzchar(query)) {
    raw_params <- strsplit(query, "&", fixed = TRUE)[[1]]
    raw_params <- raw_params[nzchar(raw_params)]
    if (length(raw_params) > 0L) {
      params <- sort(raw_params)
    }
  }
  normalized_query <- if (length(params) > 0L) paste(params, collapse = "&") else ""
  paste0(scheme, "://", host, path, if (nzchar(normalized_query)) paste0("?", normalized_query) else "")
}

normalize_scalar <- function(x) {
  if (is.null(x)) return(NULL)
  if (is.logical(x)) return(as.logical(x[[1]]))
  if (is.numeric(x)) return(as.numeric(x[[1]]))
  if (!is.character(x)) return(x)
  val <- trimws(as.character(x[[1]]))
  if (!nzchar(val)) return("")
  if (grepl("^https?://", val, ignore.case = TRUE)) return(canonicalize_url(val))
  tolower(gsub("\\s+", " ", val))
}

is_exact_equal <- function(a, b) {
  if (is.null(a) && is.null(b)) return(TRUE)
  if (is.null(a) || is.null(b)) return(FALSE)
  if (is.numeric(a) && is.numeric(b)) return(isTRUE(all.equal(as.numeric(a[[1]]), as.numeric(b[[1]]))))
  if (is.character(a) && is.character(b)) return(identical(as.character(a[[1]]), as.character(b[[1]])))
  identical(a, b)
}

is_semantic_equal <- function(a, b) {
  if (is.null(a) && is.null(b)) return(TRUE)
  if (is.null(a) || is.null(b)) return(FALSE)
  a_norm <- normalize_scalar(a)
  b_norm <- normalize_scalar(b)
  identical(a_norm, b_norm)
}

is_unknown_like <- function(x) {
  if (is.null(x)) return(TRUE)
  if (!is.character(x)) return(FALSE)
  token <- tolower(trimws(x[[1]] %||% ""))
  token %in% c("", "unknown", "n/a", "na", "none", "null", "not available", "undisclosed")
}

source_tier_score <- function(url) {
  if (is.null(url) || !is.character(url) || !nzchar(url[[1]] %||% "")) return(0)
  host <- tolower(sub("^[a-z]+://", "", canonicalize_url(url[[1]])))
  host <- sub("/.*$", "", host)
  if (grepl("\\.gov\\b|\\.gob\\b|\\.edu\\b|parliament|assembly|senate|official|vicepresidencia", host)) return(1.0)
  if (grepl("wiki|\\.org\\b|news|press|journal|eldeber|la-razon|boletin", host)) return(0.7)
  0.3
}

confidence_to_numeric <- function(x) {
  if (is.null(x)) return(NA_real_)
  if (is.numeric(x) && length(x) > 0L) {
    return(as.numeric(x[[1]]))
  }
  if (!is.character(x) || length(x) == 0L) return(NA_real_)
  token <- tolower(trimws(x[[1]]))
  if (!nzchar(token)) return(NA_real_)
  if (token %in% c("low", "l")) return(0.33)
  if (token %in% c("medium", "med", "m")) return(0.66)
  if (token %in% c("high", "h")) return(0.90)
  suppressWarnings(as.numeric(token))
}

extract_metrics <- function(answer_obj, token_obj, diagnostics_obj, completion_gate_obj, gold_obj, schema_names) {
  keys <- as.character(schema_names)
  exact <- 0L
  semantic <- 0L
  for (k in keys) {
    pred_val <- if (is.list(answer_obj) && !is.null(answer_obj[[k]])) answer_obj[[k]] else NULL
    gold_val <- if (is.list(gold_obj) && !is.null(gold_obj[[k]])) gold_obj[[k]] else NULL
    if (is_exact_equal(pred_val, gold_val)) exact <- exact + 1L
    if (is_semantic_equal(pred_val, gold_val)) semantic <- semantic + 1L
  }

  source_fields <- character(0)
  if (is.list(answer_obj)) {
    source_fields <- names(answer_obj)
    if (is.null(source_fields)) source_fields <- character(0)
  }
  source_fields <- source_fields[grepl("_source$", source_fields)]
  source_scores <- c()
  low_quality_domains <- c()
  for (k in source_fields) {
    val <- if (is.list(answer_obj) && !is.null(answer_obj[[k]])) answer_obj[[k]] else NULL
    if (is.null(val) || (is.character(val) && !nzchar(trimws(val[[1]] %||% "")))) next
    score <- source_tier_score(val)
    source_scores <- c(source_scores, score)
    if (score <= 0.3) {
      low_quality_domains <- unique(c(low_quality_domains, canonicalize_url(val[[1]])))
    }
  }
  evidence_quality <- if (length(source_scores) > 0L) mean(source_scores) else 0

  base_fields <- keys[
    !grepl("_source$", keys) &
      !grepl("_details$", keys) &
      !(keys %in% c("confidence", "justification"))
  ]
  grounded <- 0L
  for (k in base_fields) {
    val <- if (is.list(answer_obj) && !is.null(answer_obj[[k]])) answer_obj[[k]] else NULL
    if (!is_unknown_like(val)) grounded <- grounded + 1L
  }
  tokens_used <- as.numeric(token_obj$tokens_used %||% NA_real_)
  tokens_per_grounded <- if (!is.na(tokens_used) && grounded > 0L) tokens_used / grounded else NA_real_
  invariant_failures <- as.numeric(diagnostics_obj$finalization_invariant_failures %||% NA_real_)
  quality_gate_failures <- as.numeric(diagnostics_obj$quality_gate_failures %||% NA_real_)
  global_confidence_payload <- if (is.list(answer_obj)) {
    confidence_to_numeric(answer_obj$confidence %||% NA_real_)
  } else {
    NA_real_
  }
  global_confidence_gate_score <- as.numeric(completion_gate_obj$global_confidence_score %||% NA_real_)
  if (!is.finite(global_confidence_gate_score)) global_confidence_gate_score <- NA_real_
  global_confidence <- if (!is.na(global_confidence_gate_score)) {
    global_confidence_gate_score
  } else {
    global_confidence_payload
  }
  global_confidence_source <- if (!is.na(global_confidence_gate_score)) {
    "completion_gate"
  } else if (!is.na(global_confidence_payload)) {
    "payload_fallback"
  } else {
    "missing"
  }
  quality_gate_failed_current <- as.logical(completion_gate_obj$quality_gate_failed %||% NA)
  if (length(quality_gate_failed_current) == 0L) quality_gate_failed_current <- NA
  quality_gate_failed_current <- if (is.na(quality_gate_failed_current[[1]] %||% NA)) {
    NA
  } else {
    isTRUE(quality_gate_failed_current[[1]])
  }
  quality_gate_reason_current <- as.character(completion_gate_obj$quality_gate_reason %||% NA_character_)
  if (length(quality_gate_reason_current) == 0L) quality_gate_reason_current <- NA_character_
  quality_gate_reason_current <- quality_gate_reason_current[[1]]
  if (is.na(quality_gate_reason_current) || !nzchar(trimws(quality_gate_reason_current))) {
    quality_gate_reason_current <- NA_character_
  }
  confidence_min_threshold_current <- as.numeric(
    completion_gate_obj$quality_gate_min_global_confidence %||%
      finalization_policy_override$quality_gate_min_global_confidence %||%
      NA_real_
  )
  if (!is.finite(confidence_min_threshold_current)) confidence_min_threshold_current <- NA_real_

  list(
    exact_match = exact,
    semantic_match = semantic,
    total_fields = length(keys),
    evidence_quality = evidence_quality,
    low_quality_source_urls = low_quality_domains,
    grounded_fields = grounded,
    tokens_used = tokens_used,
    tokens_per_grounded_field = tokens_per_grounded,
    finalization_invariant_failures = invariant_failures,
    quality_gate_failures = quality_gate_failures,
    quality_gate_failed_current = quality_gate_failed_current,
    quality_gate_reason_current = quality_gate_reason_current,
    confidence_min_threshold_current = confidence_min_threshold_current,
    global_confidence_payload = global_confidence_payload,
    global_confidence_gate_score = global_confidence_gate_score,
    global_confidence_source = global_confidence_source,
    global_confidence = global_confidence
  )
}

gold_answer <- read_json_if_exists(path_answer_gold)
current_answer <- read_json_if_exists(path_answer_pred)
current_token_stats <- read_json_if_exists(path_token_stats)
current_diagnostics <- read_json_if_exists(path_diagnostics)
current_completion_gate <- read_json_if_exists(path_completion_gate)

current_metrics <- extract_metrics(
  answer_obj = current_answer,
  token_obj = current_token_stats %||% list(),
  diagnostics_obj = current_diagnostics %||% list(),
  completion_gate_obj = current_completion_gate %||% list(),
  gold_obj = gold_answer %||% list(),
  schema_names = names(EXPECTED_SCHEMA)
)
baseline_metrics <- NULL
if (!is.null(baseline_snapshot$answer_pred) && !is.null(baseline_snapshot$token_stats)) {
  baseline_metrics <- extract_metrics(
    answer_obj = baseline_snapshot$answer_pred,
    token_obj = baseline_snapshot$token_stats %||% list(),
    diagnostics_obj = baseline_snapshot$diagnostics %||% list(),
    completion_gate_obj = baseline_snapshot$completion_gate %||% list(),
    gold_obj = gold_answer %||% list(),
    schema_names = names(EXPECTED_SCHEMA)
  )
}

delta_metrics <- NULL
regression_gate <- list(
  baseline_available = !is.null(baseline_metrics),
  pass = NA
)
regression_gate$confidence_meets_minimum <- isTRUE(
  !is.na(current_metrics$global_confidence) &&
    !is.na(current_metrics$confidence_min_threshold_current) &&
    as.numeric(current_metrics$global_confidence) >= as.numeric(current_metrics$confidence_min_threshold_current)
)
regression_gate$quality_gate_not_failed_current <- isTRUE(
  !is.na(current_metrics$quality_gate_failed_current) &&
    !as.logical(current_metrics$quality_gate_failed_current)
)
if (!is.null(baseline_metrics)) {
  delta_metrics <- list(
    exact_match_delta = as.numeric(current_metrics$exact_match) - as.numeric(baseline_metrics$exact_match),
    semantic_match_delta = as.numeric(current_metrics$semantic_match) - as.numeric(baseline_metrics$semantic_match),
    evidence_quality_delta = as.numeric(current_metrics$evidence_quality) - as.numeric(baseline_metrics$evidence_quality),
    global_confidence_delta = as.numeric(current_metrics$global_confidence) - as.numeric(baseline_metrics$global_confidence),
    quality_gate_failures_delta = as.numeric(current_metrics$quality_gate_failures) - as.numeric(baseline_metrics$quality_gate_failures),
    tokens_used_delta = as.numeric(current_metrics$tokens_used) - as.numeric(baseline_metrics$tokens_used),
    tokens_per_grounded_field_delta = as.numeric(current_metrics$tokens_per_grounded_field) - as.numeric(baseline_metrics$tokens_per_grounded_field),
    invariant_failures_delta = as.numeric(current_metrics$finalization_invariant_failures) - as.numeric(baseline_metrics$finalization_invariant_failures)
  )

  regression_gate$exact_non_decreasing <- isTRUE(delta_metrics$exact_match_delta >= 0)
  regression_gate$semantic_non_decreasing <- isTRUE(delta_metrics$semantic_match_delta >= 0)
  regression_gate$evidence_quality_non_decreasing <- isTRUE(delta_metrics$evidence_quality_delta >= 0)
  regression_gate$confidence_non_decreasing <- isTRUE(delta_metrics$global_confidence_delta >= -0.02)
  regression_gate$quality_gate_failures_non_increasing <- isTRUE(delta_metrics$quality_gate_failures_delta <= 0)
  regression_gate$tokens_non_increasing <- isTRUE(delta_metrics$tokens_used_delta <= 0)
  regression_gate$token_efficiency_non_increasing <- isTRUE(delta_metrics$tokens_per_grounded_field_delta <= 0)
  regression_gate$invariant_failures_non_increasing <- isTRUE(delta_metrics$invariant_failures_delta <= 0)
  regression_gate$no_low_quality_sources <- length(current_metrics$low_quality_source_urls %||% character(0)) == 0L
  regression_gate$pass <- isTRUE(
    regression_gate$exact_non_decreasing &&
      regression_gate$semantic_non_decreasing &&
      regression_gate$evidence_quality_non_decreasing &&
      regression_gate$confidence_non_decreasing &&
      regression_gate$confidence_meets_minimum &&
      regression_gate$quality_gate_not_failed_current &&
      regression_gate$quality_gate_failures_non_increasing &&
      regression_gate$tokens_non_increasing &&
      regression_gate$token_efficiency_non_increasing &&
      regression_gate$invariant_failures_non_increasing &&
      regression_gate$no_low_quality_sources
  )
}

evaluation_metrics <- list(
  run_id = trace_run_id,
  generated_at_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  benchmark_mode = if (isTRUE(blind_benchmark_mode)) "blind" else "standard",
  current = current_metrics,
  baseline = baseline_metrics,
  delta = delta_metrics
)
readr::write_file(
  jsonlite::toJSON(evaluation_metrics, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_eval_metrics
)
readr::write_file(
  jsonlite::toJSON(regression_gate, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_regression_gate
)

tool_quality_events <- attempt$execution$tool_quality_events %||% list()
tool_names <- unique(
  unlist(
    lapply(tool_quality_events, function(x) {
      if (!is.list(x)) return(NA_character_)
      as.character(x$tool_name %||% NA_character_)
    }),
    use.names = FALSE
  )
)
tool_names <- tool_names[!is.na(tool_names) & nzchar(tool_names)]

run_provenance <- list(
  run_id = trace_run_id,
  generated_at_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  benchmark_mode = if (isTRUE(blind_benchmark_mode)) "blind" else "standard",
  inference_started_at_utc = trace_inference_started_at,
  inference_finished_at_utc = trace_inference_finished_at,
  disallowed_inference_reads = list(path_answer_gold),
  gold_read_post_inference = file.exists(path_answer_gold),
  files_written = list(
    path_prompt,
    path_trace,
    path_token_stats,
    path_plan_output,
    path_fold_stats,
    path_fold_summary,
    path_fold_archive,
    path_execution_summary,
    path_diagnostics,
    path_completion_gate,
    path_json_repair,
    path_invoke_error,
    path_action_ascii,
    path_answer_pred,
    path_payload_release_audit,
    path_eval_metrics,
    path_regression_gate
  ),
  tools_called = as.list(tool_names),
  baseline_snapshot_available = list(
    answer_pred = !is.null(baseline_snapshot$answer_pred),
    token_stats = !is.null(baseline_snapshot$token_stats),
    diagnostics = !is.null(baseline_snapshot$diagnostics),
    completion_gate = !is.null(baseline_snapshot$completion_gate)
  )
)
readr::write_file(
  jsonlite::toJSON(run_provenance, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  path_run_provenance
)
cat(jsonlite::toJSON(final_answer, pretty = TRUE, auto_unbox = TRUE, null = "null"))
