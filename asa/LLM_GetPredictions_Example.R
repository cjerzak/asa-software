{
# set WD 
options(error = NULL)
setwd("~/Documents/asa")

# define some prelimaries 
INITIALIZED_CUSTOM_ENV_TAG <- FALSE
CustomLLMBackend <- "openai"

# ---------------------------
# Minimal Reproducible Interface AI Search Agent (No SQL)
# ---------------------------
# What this does:
# - Takes (person_name, country_, year_)
# - Looks up PARTIES_OF_COUNTRY
# - Builds the "e:" context block and a short instruction
# - Either: (a) uses a mock model illustration only or
#           (b) calls custom AI search agent protocol (assumes OPENAI_API_KEY is set)
# - Parses model JSON and returns a simple list of values

# Dependencies:
#   conda environment called CustomLLMSearch (see README.md)
#   install.packages("jsonlite")      # required
#   install.packages("httr")   

suppressWarnings(suppressMessages({
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Please install jsonlite: install.packages('jsonlite')", call. = FALSE)
  }
}))

# ---------------------------
# 1) Tiny, illustrative country -> parties map
# ---------------------------
country_party_map <- list(
  "United States of America" = c("Democratic Party", "Republican Party", "Libertarian Party", "Green Party"),
  "India"                    = c("Bharatiya Janata Party", "Indian National Congress", "Aam Aadmi Party"),
  "Nigeria"                  = c("All Progressives Congress", "People's Democratic Party", "Labour Party"),
  "Brazil"                   = c("Workers' Party (PT)", "Liberal Party (PL)", "Brazilian Social Democracy Party (PSDB)"),
  "South Africa"             = c("African National Congress (ANC)", "Democratic Alliance (DA)", "Economic Freedom Fighters (EFF)")
)

# ---------------------------
# 2) Light text cleaner
# ---------------------------
CleanText <- function(x) {
  x <- as.character(x)
  x <- gsub("\\s+", " ", x)    # collapse runs of whitespace
  x <- trimws(x)               # strip leading/trailing
  x
}

build_prompt <- function(person_name, country_, year_, options_of_country) {
  e_block <- paste0(
    "e: ", CleanText(person_name),
    "\n", "- Country: ", country_,
    "\n", "- Approximate year: ", year_,
    "\n", "- Potential Parties in this Country (PARTIES_OF_COUNTRY): ",
    paste(CleanText(options_of_country), collapse = ", ")
  )
  
  instructions <- paste0(
    "Task: Infer the most likely political party (pol_party) for the person above.\n",
    "Pick EXACTLY ONE value from PARTIES_OF_COUNTRY.\n",
    "Return STRICT JSON with the following fields only:\n",
    "{\n",
    '  "pol_party": <string from PARTIES_OF_COUNTRY>,\n',
    '  "pol_party_relaxed": <string (can repeat pol_party)>,\n',
    '  "justification": <string>,\n',
    '  "confidence": <number between 0 and 1>\n',
    "}\n",
    "Do not include markdown. Do not add commentary outside the JSON."
  )
  
  paste(e_block, "", instructions, sep = "\n\n")
}

# ---------------------------
# 4) Deterministic mock model (default)
#     - Minimal, reproducible behavior without any API keys
# ---------------------------
call_test_agent <- function(prompt, options_of_country) {
  candidate <- options_of_country[1]
  lower_prompt <- tolower(prompt)
  for (p in options_of_country) {
    token <- tolower(strsplit(CleanText(p), "\\s+")[[1]][1])
    if (nzchar(token) && grepl(paste0("\\b", token), lower_prompt, perl = TRUE)) {
      candidate <- p; break
    }
  }
  jsonlite::toJSON(
    list(
      pol_party            = candidate,
      pol_party_relaxed    = candidate,
      justification        = "Mock model: selected a plausible party from the provided options.",
      confidence           = 0.30
    ),
    auto_unbox = TRUE, pretty = TRUE
  )
}

# ---------------------------
# 5) Call Agent
#    Set env var: Sys.setenv(OPENAI_API_KEY = "sk-...")
# ---------------------------
call_real_agent <- function(prompt, model = "gpt5-nano") {
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("To use model='openai', please install httr: install.packages('httr')", call. = FALSE)
  }
  api_key <- Sys.getenv("OPENAI_API_KEY", unset = "")
  if (identical(api_key, "")) {
    stop("OPENAI_API_KEY not set. Use Sys.setenv(OPENAI_API_KEY='...') or use model='mock'.", call. = FALSE)
  }
  
  # Minimal, stable call; adjust 'model' if you prefer a different one.
  # ---- Custom ASA invocation (no direct HTTP call here) ----
  # Variables consumed by the ASA scripts (seen via local=TRUE scoping)
  thePrompt <- prompt
  CustomLLMBackend <- "openai"                 # or "grok"/"exo" if you prefer
  modelName <- model
  
  # 1) Helpers (safe if unused) + 2) initialize ASA + 3) run ASA
  source("./run_asa/Internal/LLM_Helpers.R",              local = TRUE)  # helper fns
  source("./run_asa/Internal/LLM_CustomLLM_Invokation.R", local = TRUE)  # run (uses thePrompt)
  
  # Collect agent result placed by the ASA script
  content_txt <- tryCatch({
    if (exists("response", inherits = FALSE) &&
        is.list(response) &&
        isTRUE(response$status_code == 200L)) {
      response$message
    } else if (exists("theResponseText", inherits = FALSE) &&
               is.character(theResponseText) && nzchar(theResponseText)) {
      theResponseText
    } else {
      NA_character_
    }
  }, error = function(e) NA_character_)
  
 
  if (is.na(content_txt)) {
    stop("OpenAI response did not include choices[[1]]$message$content.", call. = FALSE)
  }
  content_txt
}

# ---------------------------
# 6) Parse JSON (with or without code fences)
# ---------------------------
extract_json <- function(x) {
  # If the model wrapped JSON in ```json ... ```, peel that off; otherwise treat as raw JSON
  y <- sub("(?s).*```json\\s*", "", x, perl = TRUE)
  y <- sub("(?s)\\s*```.*$", "", y, perl = TRUE)
  z <- if (nzchar(trimws(y))) y else x
  # Parse JSON or return an empty list on failure
  tryCatch(jsonlite::fromJSON(z), error = function(e) list())
}

# ---------------------------
# 7) Public entry point
# ---------------------------
predict_pol_party <- function(person_name,
                              country_,
                              year_,
                              model) {
  # Lookup options for this country
  options_of_country <- country_party_map[[country_]]
  if (is.null(options_of_country)) {
    stop(sprintf("No party options available for country: '%s'. Please add it to country_party_map.", country_),
         call. = FALSE)
  }
  
  # Compose the prompt (the collaborator can read this to see the interface)
  thePrompt <- build_prompt(person_name, country_, year_, options_of_country)
  
  # Get a response
  raw_output <- if (model == "mock") {
    call_test_agent(thePrompt, options_of_country)
  } else {
    call_real_agent( thePrompt, model = model)
  }
  
  # Parse model JSON
  parsed <- extract_json(raw_output)
  
  # Normalize fields
  out <- list(
    pol_party                 = parsed[["pol_party"]],
    pol_party_relaxed         = parsed[["pol_party_relaxed"]],
    justification             = parsed[["justification"]],
    confidence                = parsed[["confidence"]],
    prompt_sent_to_model      = thePrompt,
    raw_output_from_model     = raw_output
  )
  out
}

# ---------------------------
# 8) Pretty-printer for convenience
# ---------------------------
print.pol_party_prediction <- function(x, ...) {
  cat("\n--- Prediction ---\n")
  cat("pol_party           : ", if (is.null(x$pol_party)) "NA" else x$pol_party, "\n", sep = "")
  cat("pol_party_relaxed   : ", if (is.null(x$pol_party_relaxed)) "NA" else x$pol_party_relaxed, "\n", sep = "")
  cat("confidence          : ", if (is.null(x$confidence)) "NA" else x$confidence, "\n", sep = "")
  cat("justification       : ", if (is.null(x$justification)) "NA" else x$justification, "\n", sep = "")
  invisible(x)
}

# ---------------------------
# 9) Tiny demo (safe to leave in; does nothing unless you run it)
# ---------------------------
asa_run <- function(person_name, 
                     country_, 
                     year_,
                     model) {
  res <- predict_pol_party(person_name, country_, year_, 
                           model = model)
  
  cat("\n--- Prompt sent to model:---\n")
  cat(res$prompt_sent_to_model, "\n")
  return(res)
}

# initialize agent (do once)
source("./run_asa/Internal/LLM_CustomLLM_Invokation.R", local = TRUE)  # init (sets tag TRUE)

# run agent 
results <- 
  asa_run(person_name = "Shashi Tharoor",
        country_  = "India",
        year_  = 2014,
        model = "mock"  #  NO AI search (illustrative only)
        #model = "gpt-5-nano" 
        #  AI search agent run (requires OpenAI API key saved in .RProfile file as OPENAI_API_KEY)
  )
results$pol_party
# In production model, we run the search agent using a SQL database
# for read/write backbone. 
# See Dropbox link for full codebase.
}
