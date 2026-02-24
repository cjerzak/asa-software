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

plan_mode_enabled <- FALSE

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
auto_openwebpage_policy <- if (use_fast_emulation) "off" else "conservative"

orchestration_options_override <- if (use_fast_emulation) {
  list(
    field_resolver = list(
      webpage_extraction_enabled = FALSE,
      search_snippet_extraction_enabled = FALSE
    )
  )
} else {
  NULL
}

.heartbeat_token <- function(x, default = "na", max_chars = 72L) {
  txt <- as.character(x)
  if (length(txt) == 0L || is.na(txt[[1]]) || !nzchar(txt[[1]])) {
    txt <- default
  } else {
    txt <- txt[[1]]
  }
  txt <- gsub("[[:space:]]+", "_", txt)
  txt <- gsub("[|=]", "/", txt)
  txt <- gsub("[^A-Za-z0-9_./:-]", "", txt)
  if (!nzchar(txt)) {
    txt <- default
  }
  if (nchar(txt) > max_chars) {
    txt <- substr(txt, 1L, max_chars)
  }
  txt
}

set_heartbeat_phase <- function(hb, phase = "running", detail = NA_character_) {
  if (is.null(hb) || !is.list(hb)) {
    return(invisible(NULL))
  }
  phasefile <- hb$phasefile
  if (!is.character(phasefile) || length(phasefile) != 1L || !nzchar(phasefile)) {
    return(invisible(NULL))
  }
  phase_token <- .heartbeat_token(phase, default = "running")
  detail_token <- .heartbeat_token(detail, default = "none")
  ts_now <- as.integer(Sys.time())
  payload <- sprintf("%s|%s|%d", phase_token, detail_token, ts_now)
  suppressWarnings(try(writeLines(payload, con = phasefile, useBytes = TRUE), silent = TRUE))
  invisible(NULL)
}

start_heartbeat <- function(label = "run_task", interval_sec = 20L, stall_after_sec = 180L) {
  interval_sec <- max(10L, min(30L, as.integer(interval_sec)))
  stall_after_sec <- max(60L, as.integer(stall_after_sec))
  hb_label <- .heartbeat_token(label, default = "run_task", max_chars = 64L)

  stopfile <- tempfile("asa_heartbeat_stop_")
  phasefile <- tempfile("asa_heartbeat_phase_")
  progress_file <- tempfile("asa_heartbeat_progress_")
  hb_seed <- list(phasefile = phasefile)
  set_heartbeat_phase(hb_seed, phase = "bootstrap", detail = "init")

  r_pid <- as.integer(Sys.getpid())
  script <- paste(
    sprintf("STOPFILE=%s", shQuote(stopfile)),
    sprintf("PHASEFILE=%s", shQuote(phasefile)),
    sprintf("PROGRESS_FILE=%s", shQuote(progress_file)),
    sprintf("RPID=%d", r_pid),
    sprintf("LABEL=%s", shQuote(hb_label)),
    sprintf("MID_INTERVAL=%d", interval_sec),
    sprintf("STALL_AFTER=%d", stall_after_sec),
    "START_TS=$(date +%s)",
    "LAST_EVENT_TS=$START_TS",
    "HAS_LSOF=0",
    "if command -v lsof >/dev/null 2>&1; then HAS_LSOF=1; fi",
    "format_age() {",
    "  T=$1",
    "  H=$((T/3600))",
    "  M=$(((T%3600)/60))",
    "  S=$((T%60))",
    "  if [ \"$H\" -gt 0 ]; then",
    "    printf \"%dh%02dm%02ds\" \"$H\" \"$M\" \"$S\"",
    "  else",
    "    printf \"%dm%02ds\" \"$M\" \"$S\"",
    "  fi",
    "}",
    "get_kv() {",
    "  KEY=\"$1\"",
    "  LINE=\"$2\"",
    "  printf \"%s\\n\" \"$LINE\" | awk -v key=\"$KEY\" '{for (i=1; i<=NF; i++) {split($i, a, \"=\"); if (a[1]==key) {print a[2]; exit}}}'",
    "}",
    "while [ ! -f \"$STOPFILE\" ] && kill -0 \"$RPID\" 2>/dev/null; do",
    "  NOW_TS=$(date +%s)",
    "  ELAPSED=$((NOW_TS-START_TS))",
    "  PHASE=bootstrap",
    "  DETAIL=none",
    "  PHASE_TS=$START_TS",
    "  if [ -f \"$PHASEFILE\" ]; then",
    "    PHASE_LINE=$(cat \"$PHASEFILE\" 2>/dev/null)",
    "    if [ -n \"$PHASE_LINE\" ]; then",
    "      PHASE=$(printf \"%s\" \"$PHASE_LINE\" | awk -F'|' '{print $1}')",
    "      DETAIL=$(printf \"%s\" \"$PHASE_LINE\" | awk -F'|' '{print $2}')",
    "      PHASE_TS=$(printf \"%s\" \"$PHASE_LINE\" | awk -F'|' '{print $3}')",
    "    fi",
    "  fi",
    "  PROGRESS_LINE=",
    "  if [ -f \"$PROGRESS_FILE\" ]; then",
    "    PROGRESS_LINE=$(cat \"$PROGRESS_FILE\" 2>/dev/null)",
    "  fi",
    "  NODE=$(get_kv node \"$PROGRESS_LINE\")",
    "  TOOL_USED=$(get_kv tool_used \"$PROGRESS_LINE\")",
    "  TOOL_LIMIT=$(get_kv tool_limit \"$PROGRESS_LINE\")",
    "  TOOL_REMAINING=$(get_kv tool_rem \"$PROGRESS_LINE\")",
    "  FIELDS_RESOLVED=$(get_kv resolved \"$PROGRESS_LINE\")",
    "  FIELDS_TOTAL=$(get_kv total \"$PROGRESS_LINE\")",
    "  FIELDS_UNKNOWN=$(get_kv unknown \"$PROGRESS_LINE\")",
    "  EVENT_TS=$(get_kv ts \"$PROGRESS_LINE\")",
    "  if [ -z \"$NODE\" ]; then NODE=na; fi",
    "  if [ -z \"$TOOL_USED\" ]; then TOOL_USED=na; fi",
    "  if [ -z \"$TOOL_LIMIT\" ]; then TOOL_LIMIT=na; fi",
    "  if [ -z \"$TOOL_REMAINING\" ]; then TOOL_REMAINING=na; fi",
    "  if [ -z \"$FIELDS_RESOLVED\" ]; then FIELDS_RESOLVED=na; fi",
    "  if [ -z \"$FIELDS_TOTAL\" ]; then FIELDS_TOTAL=na; fi",
    "  if [ -z \"$FIELDS_UNKNOWN\" ]; then FIELDS_UNKNOWN=na; fi",
    "  if [ -z \"$EVENT_TS\" ]; then EVENT_TS=$START_TS; fi",
    "  if [ \"$PHASE_TS\" -gt \"$EVENT_TS\" ] 2>/dev/null; then EVENT_TS=$PHASE_TS; fi",
    "  if [ \"$EVENT_TS\" -gt \"$LAST_EVENT_TS\" ] 2>/dev/null; then LAST_EVENT_TS=$EVENT_TS; fi",
    "  PY_PID=$(pgrep -P \"$RPID\" -f \"python|reticulate|asa_backend\" 2>/dev/null | head -n 1)",
    "  METRIC_PID=$RPID",
    "  if [ -n \"$PY_PID\" ]; then METRIC_PID=$PY_PID; fi",
    "  PS_LINE=$(ps -p \"$METRIC_PID\" -o %cpu= -o rss= 2>/dev/null | awk 'NR==1 {print $1 \" \" $2}')",
    "  CPU=$(printf \"%s\" \"$PS_LINE\" | awk '{if (NF>=1) printf(\"%.1f\", ($1+0)); else printf(\"0.0\")}')",
    "  RSS_MB=$(printf \"%s\" \"$PS_LINE\" | awk '{if (NF>=2) printf(\"%d\", int(($2+0)/1024)); else printf(\"0\")}')",
    "  TCP_COUNT=0",
    "  HOSTS=none",
    "  if [ \"$HAS_LSOF\" -eq 1 ]; then",
    "    TCP_COUNT=$(lsof -nP -a -p \"$METRIC_PID\" -iTCP -sTCP:ESTABLISHED 2>/dev/null | awk 'NR>1 {n++} END {print n+0}')",
    "    HOSTS=$(lsof -nP -a -p \"$METRIC_PID\" -iTCP -sTCP:ESTABLISHED 2>/dev/null | awk 'NR>1 {split($9,a,\"->\"); if (length(a)>1) {split(a[2],b,\":\"); h=b[1]; if (h!=\"\") c[h]++}} END {for (h in c) print c[h],h}' | sort -rn | head -n 3 | awk '{printf(\"%s%s(%s)\", sep, $2, $1); sep=\",\"}')",
    "    if [ -z \"$HOSTS\" ]; then HOSTS=none; fi",
    "  fi",
    "  CPU_ACTIVE=$(awk -v c=\"$CPU\" 'BEGIN {print ((c+0)>=3.0) ? 1 : 0}')",
    "  if [ \"$CPU_ACTIVE\" -eq 1 ] || [ \"$TCP_COUNT\" -gt 0 ]; then LAST_EVENT_TS=$NOW_TS; fi",
    "  IDLE_FOR=$((NOW_TS-LAST_EVENT_TS))",
    "  STATUS=active",
    "  SLEEP_SECS=10",
    "  if [ \"$IDLE_FOR\" -ge \"$STALL_AFTER\" ]; then",
    "    STATUS=stalled",
    "    SLEEP_SECS=30",
    "  elif [ \"$IDLE_FOR\" -ge 45 ]; then",
    "    STATUS=slow",
    "    SLEEP_SECS=$MID_INTERVAL",
    "  fi",
    "  WAIT_REASON=backend_wait",
    "  if [ \"$TCP_COUNT\" -gt 0 ]; then",
    "    WAIT_REASON=network_io",
    "  elif [ \"$CPU_ACTIVE\" -eq 1 ]; then",
    "    WAIT_REASON=local_compute",
    "  fi",
    "  PHASE_DISPLAY=$PHASE",
    "  if [ -n \"$DETAIL\" ] && [ \"$DETAIL\" != \"none\" ]; then PHASE_DISPLAY=\"${PHASE}(${DETAIL})\"; fi",
    "  ELAPSED_TXT=$(format_age \"$ELAPSED\")",
    "  IDLE_TXT=$(format_age \"$IDLE_FOR\")",
    "  printf \"[heartbeat] %s\\n\" \"$LABEL\"",
    "  printf \"  runtime  elapsed=%s    idle=%s    cadence=%ss\\n\" \"$ELAPSED_TXT\" \"$IDLE_TXT\" \"$SLEEP_SECS\"",
    "  printf \"  stage    phase=%s    status=%s    reason=%s    node=%s\\n\" \"$PHASE_DISPLAY\" \"$STATUS\" \"$WAIT_REASON\" \"$NODE\"",
    "  printf \"  budget   tools=%s/%s(rem=%s)    fields=%s/%s(unknown=%s)\\n\" \"$TOOL_USED\" \"$TOOL_LIMIT\" \"$TOOL_REMAINING\" \"$FIELDS_RESOLVED\" \"$FIELDS_TOTAL\" \"$FIELDS_UNKNOWN\"",
    "  printf \"  system   cpu=%s%%    rss=%sMB    tcp=%s    hosts=%s\\n\\n\" \"$CPU\" \"$RSS_MB\" \"$TCP_COUNT\" \"$HOSTS\"",
    "  sleep \"$SLEEP_SECS\"",
    "done",
    sep = "\n"
  )

  hb_pid <- suppressWarnings(
    system2("sh", c("-c", script), wait = FALSE, stdout = "", stderr = "")
  )
  list(
    pid = as.integer(hb_pid),
    stopfile = stopfile,
    phasefile = phasefile,
    progress_file = progress_file,
    label = hb_label
  )
}

stop_heartbeat <- function(hb) {
  if (is.null(hb) || !is.list(hb)) {
    return(invisible(NULL))
  }
  stopfile <- hb$stopfile
  if (is.character(stopfile) && length(stopfile) == 1L && nzchar(stopfile)) {
    suppressWarnings(try(file.create(stopfile), silent = TRUE))
  }
  hb_pid <- suppressWarnings(as.integer(hb$pid))
  if (!is.na(hb_pid) && hb_pid > 0L) {
    suppressWarnings(try(tools::pskill(hb_pid), silent = TRUE))
  }
  phasefile <- hb$phasefile
  if (is.character(phasefile) && length(phasefile) == 1L && nzchar(phasefile)) {
    suppressWarnings(try(unlink(phasefile, force = TRUE), silent = TRUE))
  }
  progress_file <- hb$progress_file
  if (is.character(progress_file) && length(progress_file) == 1L && nzchar(progress_file)) {
    suppressWarnings(try(unlink(progress_file, force = TRUE), silent = TRUE))
  }
  if (is.character(stopfile) && length(stopfile) == 1L && nzchar(stopfile)) {
    suppressWarnings(try(unlink(stopfile, force = TRUE), silent = TRUE))
  }
  invisible(NULL)
}

set_progress_state_file <- function(path = "") {
  if (is.character(path) && length(path) == 1L && nzchar(path)) {
    Sys.setenv(ASA_PROGRESS_STATE_FILE = path)
  } else {
    Sys.unsetenv("ASA_PROGRESS_STATE_FILE")
  }

  if (isTRUE(reticulate::py_available(initialize = FALSE))) {
    suppressWarnings(try({
      py_os <- reticulate::import("os")
      if (is.character(path) && length(path) == 1L && nzchar(path)) {
        py_os$environ[["ASA_PROGRESS_STATE_FILE"]] <- path
      } else {
        py_os$environ$pop("ASA_PROGRESS_STATE_FILE", NULL)
      }
    }, silent = TRUE))
  }
  invisible(NULL)
}

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
hb <- start_heartbeat(
  label = sprintf("trace_test_real[%s]", emulation_profile),
  interval_sec = if (use_fast_emulation) 15L else 25L
)
set_heartbeat_phase(hb, phase = "backend_invoke", detail = "run_task")
old_progress_file <- Sys.getenv("ASA_PROGRESS_STATE_FILE", unset = "")
set_progress_state_file(hb$progress_file)
heartbeat_cleaned <- FALSE
cleanup_heartbeat <- function() {
  if (isTRUE(heartbeat_cleaned)) {
    return(invisible(NULL))
  }
  if (is.character(old_progress_file) && length(old_progress_file) == 1L && nzchar(old_progress_file)) {
    set_progress_state_file(old_progress_file)
  } else {
    set_progress_state_file("")
  }
  stop_heartbeat(hb)
  heartbeat_cleaned <<- TRUE
  invisible(NULL)
}
on.exit({
  cleanup_heartbeat()
}, add = TRUE)

attempt <- run_task(
  prompt = prompt,
  #output_format = "json",
  output_format = "raw",
  expected_fields = NULL,
  expected_schema = EXPECTED_SCHEMA,
  verbose = TRUE,
  use_plan_mode = plan_mode_enabled,
  search_budget_limit = search_budget_limit,
  unknown_after_searches = unknown_after_limit,
  retry_policy = list(
    rewrite_after_streak = 2L,
    stop_after_streak = 3L
  ),
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
set_heartbeat_phase(hb, phase = "backend_complete", detail = "run_task_returned")
cleanup_heartbeat()

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
  "~/Documents/asa-software/tracked_reports/payload_release_audit_real.txt"
)
cat(jsonlite::toJSON(final_answer, pretty = TRUE, auto_unbox = TRUE, null = "null"))
