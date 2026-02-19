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
#./asa-software/tracked_reports/action_ascii_real.txt
#./asa-software/tracked_reports/answer_pred_real.txt
options(error=NULL)
# install.packages( "~/Documents/asa-software/asa",repos = NULL, type = "source",force = F);
# devtools::load_all('~/Documents/asa-software/asa')
# devtools::install_github( 'cjerzak/asa-software/asa' )
# asa::build_backend(force = TRUE, fix_browser = TRUE)

# Prefer local package source so trace runs validate current repo code.
devtools::load_all('~/Documents/asa-software/asa')

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
attempt <- run_task(
    prompt = prompt,
    #output_format = "json",
    output_format = "raw",
    expected_fields = NULL,
    expected_schema = EXPECTED_SCHEMA,
    verbose = FALSE,
    use_plan_mode = TRUE, 
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
      recursion_limit = 32L, memory_threshold = 16L, memory_keep_recent = 8L, # production
      #recursion_limit = 64L, memory_threshold = 32L, memory_keep_recent = 16L, # production
      #fold_char_budget = 5L * (10000L), # default is 30000L
      fold_char_budget = 5L * (10000L), # default is 30000L
      rate_limit = 0.3,
      timeout = 180L,
      verbose = TRUE,
      search = search_options(
        # Wikipedia tool output
        wiki_top_k_results = 3L,
        wiki_doc_content_chars_max = (5L) * 500L,
        
        # DuckDuckGo Search tool output (snippet chars returned to the LLM)
        search_doc_content_chars_max = (5L) * 1000L, # (chars per word)*words
        
        # Full webpage reader (OpenWebpage tool output)
        allow_read_webpages = TRUE,
        webpage_max_chars = (5L) * 8000L,
        webpage_max_chunks = 20,
        webpage_chunk_chars = (5L) * 600L
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
readr::write_file(jsonlite::toJSON(attempt$plan, auto_unbox = TRUE, pretty = TRUE, null = "null"), "~/Documents/asa-software/tracked_reports/plan_output_real.txt")

# fold_stats
readr::write_file(
  jsonlite::toJSON(attempt$fold_stats, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/fold_stats_real.txt"
)

# summary/archive from memory folding state (if present)
summary_state <- NULL
archive_state <- NULL
if (!is.null(attempt$raw_response)) {
  summary_state <- tryCatch(reticulate::py_to_r(attempt$raw_response$summary), error = function(e) NULL)
  archive_state <- tryCatch(reticulate::py_to_r(attempt$raw_response$archive), error = function(e) NULL)
}
if (is.null(summary_state)) summary_state <- list()
if (is.null(archive_state)) archive_state <- list()

readr::write_file(
  jsonlite::toJSON(summary_state, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/fold_summary_real.txt"
)
readr::write_file(
  jsonlite::toJSON(archive_state, auto_unbox = TRUE, pretty = TRUE, null = "null"),
  "~/Documents/asa-software/tracked_reports/fold_archive_real.txt"
)

# execution summary (elapsed_time, field_status)
readr::write_file(
  jsonlite::toJSON(
    list(
      elapsed_time = attempt$elapsed_time,
      field_status = attempt$execution$field_status
    ),
    auto_unbox = TRUE, pretty = TRUE, null = "null"
  ),
  "~/Documents/asa-software/tracked_reports/execution_summary_real.txt"
)

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
cat(jsonlite::toJSON(final_answer, pretty = TRUE, auto_unbox = TRUE, null = "null"))
