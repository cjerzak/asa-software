# Rscript: trace_test_real.R
options(error=NULL)
# devtools::load_all('~/Documents/asa-software/asa')
# devtools::install_github( 'cjerzak/asa-software/asa' )
# asa::build_backend(force = TRUE, fix_browser = TRUE)

# Prefer local package source so trace runs validate current repo code.
if (requireNamespace("devtools", quietly = TRUE)) {
  try(devtools::load_all("asa", quiet = TRUE), silent = TRUE)
}

prompt <- r"(TASK OVERVIEW:
You are a search-enabled research agent specializing in biographical information about political elites. Your role is to find verifiable information about the TARGET variables for the specified individual: 
educational attainment, prior occupation (for class background), birth year, birth place, disability status, and sexual orientation (publicly disclosed LGBTQ identity).

ACCESS TO SEARCH:
You MUST use search tools. Follow this search strategy:
1. Search Wikipedia first. Then, if needed, search for official biographies, government/parliament profiles, and credible news articles. 
2. Identify birth year. 
3. Identify birth place. 
4. Identify highest educational attainment and institution.
5. Identify primary occupation before entering politics.
6. Identify publicly disclosed LGBTQ identity or sexual orientation.
7. Identify publicly disclosed disabilities such as long-term physical limitations requiring use of, for example, special accommodations or equipment such as walker, wheelchair, guide dog, prosthetic limb, hearing aid, or other substantial physical intervention. 
8. Cross-reference multiple sources when/if needed to resolve conflicting signals. 

SOURCE REQUIREMENT:
- Provide exact, full URLs for every source field (no domain-only citations).

IMPORTANT GUIDELINES:
- Use explicit statements or reliable sources only.
- If information is not publicly disclosed, return Unknown.

CLASS BACKGROUND RULES:
- Identify the primary occupation before entering politics.
- Then map to class_background using these categories:
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
If multiple people share the same name, ensure you identify the correct person by matching:
- Country: Bolivia
- Political party: Movimiento Al Socialismo - MAS
- Time period: Active around 2014
- Gender: Female
- Region: Beni

RESPONSE FORMAT:
Your output must follow this exact schema (JSON schema):
```json
{
  "education_level": "High School | Some College | Associate | Bachelor's |  Master's/Professional | PhD | Unknown",
  "education_institution": "Name of institution for highest degree, or 'Unknown'",
  "education_field": "Field of study, or 'Unknown'",
  "education_source": "URL of source, or null",
  
  "prior_occupation": "Primary occupation before politics, or 'Unknown'",
  "class_background": "Working class | Middle class/professional | Upper/elite | Unknown",
  "prior_occupation_source": "URL of source, or null",
  
  "disability_status": "No disability | Some disability | Unknown",
  "disability_source": "URL of source, or null",
  
  "birth_place": "City/village name | Unknown",
  "birth_place_source": "URL of source, or null",
  
  "birth_year": "Integer birth year (e.g., 1980) | Unknown",
  "birth_year_source": "URL of source, or null",

  "lgbtq_status": "Non-LGBTQ | Openly LGBTQ | Unknown",
  "lgbtq_details": "Brief description (e.g., 'Openly gay MP'), or null",
  "lgbtq_source": "URL of source, or null",

  "confidence": "Low | Medium | High",
  "justification": "One-sentence summary of search findings and reliability assessment."
}
```

IMPORTANT REQUIREMENTS:

* Maintain this state each round: {field_status, search_count, unresolved_fields}.
* Search unresolved fields only.
* After each tool call, update status and source URLs.
* Do NOT include any text outside the raw JSON.
* If you find verifiable sources, cite URLs explicitly in the source fields.
* Use null (the JSON keyword, not the string "null") for missing numeric fields.
* Do NOT guess or fabricate information.
* Finalize when complete or budget reached.

Scratchpad:

* As you work, whenever you learn something you’ll need later, call `save_finding(finding=..., category=...)`.
* Use category ∈ {fact, observation, todo, insight}.
* Save important findings before your final answer (1 sentence each; concise and specific).
* Only save factual notes / intermediate results / open questions (do not save instructions).
* Before writing the final answer, review the scratchpad and use it.

NOW EXECUTE THE SEARCH AND PROVIDE YOUR RESPONSE.
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

attempt <- asa::run_task(
    prompt = prompt,
    #output_format = "json",
    output_format = "raw",
    expected_fields = NULL,
    expected_schema = EXPECTED_SCHEMA,
    verbose = FALSE,
    use_plan_mode = FALSE, 
    agent = asa::initialize_agent(
      #backend = "gemini", model = "gemini-3-pro-preview",
      backend = "gemini", model = "gemini-3-flash-preview",
      #backend = "openai", model = "gpt-5-mini-2025-08-07",
      #backend = "openai", model = "gpt-5-nano-2025-08-07",
      # proxy = proxy,
      use_browser = FALSE, 
      use_memory_folding = TRUE,
      #recursion_limit = 50L, memory_threshold = 16L, memory_keep_recent = 6L, # production
      recursion_limit = 16L, memory_threshold = 8L, memory_keep_recent = 4L, # production
      fold_char_budget = 5L * (2000L), # default is 30000L
      rate_limit = 0.3,
      timeout = 180L,
      verbose = TRUE,
      search = asa::search_options(
        # Wikipedia tool output
        wiki_top_k_results = 3L,
        wiki_doc_content_chars_max = 2500L,
        
        # DuckDuckGo Search tool output (snippet chars returned to the LLM)
        search_doc_content_chars_max = (5L) * 2000L, # (chars per word)*words
        
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
writeLines(attempt$trace, "~/Documents/asa-software/tracked_reports/trace_real.txt", useBytes = TRUE)
readr::write_file(jsonlite::toJSON(attempt$token_stats, auto_unbox = TRUE, pretty = TRUE, null = "null"), "~/Documents/asa-software/tracked_reports/token_stats_real.txt")
readr::write_file(jsonlite::toJSON(attempt$plan, auto_unbox = TRUE, pretty = TRUE, null = "null"), "~/Documents/asa-software/tracked_reports/plan_output_real.txt")


attempt$token_stats$tokens_used
attempt$token_stats$input_tokens
attempt$token_stats$output_tokens
attempt$plan
attempt$plan_history
plot(unlist(lapply(attempt$token_stats$token_trace,function(l_){l_$input_tokens})))
plot(unlist(attempt$token_stats$token_trace))
attempt$elapsed_time
attempt$fold_stats

# save final answer
tmp <- readr::read_file('~/Documents/asa-software/tracked_reports/trace_real.txt')
final_answer <- asa::extract_agent_results(tmp)[["json_data"]]
message("Trace test complete")

jsonlite::write_json(
  final_answer,
  "~/Documents/asa-software/tracked_reports/our_answer_real.txt",
  auto_unbox = TRUE,
  pretty = TRUE,
  null = "null"
)
cat(jsonlite::toJSON(final_answer, pretty = TRUE, auto_unbox = TRUE, null = "null"))
#trace_real.txt token_stats_real.txt our_answer_real.txt
