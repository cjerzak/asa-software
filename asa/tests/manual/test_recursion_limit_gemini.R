# Manual test: Recursion limit with Gemini Flash 3
# Verifies: (a) valid predictions AND (b) finalize node triggered
#
# Usage:
#   Rscript asa/tests/manual/test_recursion_limit_gemini.R
#
# Requires: GOOGLE_API_KEY or GEMINI_API_KEY environment variable

library(asa)

cat("\n========================================\n")
cat("Recursion Limit Test with Gemini Flash 3\n")
cat("========================================\n\n")

# Prefer the project conda environment when present.
try(
  suppressWarnings(reticulate::use_condaenv("asa_env", required = FALSE)),
  silent = TRUE
)

# Check API key
api_key <- Sys.getenv("GOOGLE_API_KEY", unset = "")
if (!nzchar(api_key)) {
  api_key <- Sys.getenv("GEMINI_API_KEY", unset = "")
}
if (!nzchar(api_key)) {
  stop("Gemini API key not found. Set GOOGLE_API_KEY or GEMINI_API_KEY.")
}
cat("✓ API key found\n")

# Load Python modules
python_path <- system.file("python", package = "asa")
if (!nzchar(python_path)) {
  stop("Could not find asa Python path")
}
cat("✓ Python path:", python_path, "\n")

if (!reticulate::py_module_available("bs4")) {
  stop("Missing Python module 'bs4' in the active reticulate environment. Use asa_env or install beautifulsoup4.")
}

custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = python_path)
chat_models <- reticulate::import("langchain_google_genai")
cat("✓ Python modules loaded\n")

# Get model name
gemini_model <- asa:::ASA_DEFAULT_GEMINI_MODEL
cat("✓ Using model:", gemini_model, "\n")

# Create Gemini Flash 3 LLM
llm <- chat_models$ChatGoogleGenerativeAI(
  model = gemini_model,
  temperature = 0,
  api_key = api_key
)
cat("✓ LLM initialized\n")

# Create fake search tool (deterministic)
reticulate::py_run_string('
from langchain_core.tools import Tool
import json

def _fake_search(query: str) -> str:
    """Fixed payload so the agent has something it could use if the tool ran."""
    return json.dumps([
        {"name": "Ada Lovelace", "birth_year": 1815, "field": "mathematics", "key_contribution": None},
        {"name": "Alan Turing", "birth_year": 1912, "field": "computer science", "key_contribution": None},
        {"name": "Grace Hopper", "birth_year": 1906, "field": "computer science", "key_contribution": None}
    ])

fake_search_tool = Tool(
    name="Search",
    description="Deterministic Search tool for recursion-limit tests.",
    func=_fake_search,
)
')
cat("✓ Fake search tool created\n\n")

# Create agent
agent <- custom_ddg$create_standard_agent(
  model = llm,
  tools = list(reticulate::py$fake_search_tool),
  checkpointer = NULL,
  debug = TRUE
)
cat("✓ Agent created\n")

# Shared prompt helper (keeps manual + testthat prompts in sync)
if (!exists("asa_test_recursion_limit_prompt", mode = "function")) {
  helper_candidates <- c(
    file.path(getwd(), "tests", "testthat", "helper-asa-env.R"),
    file.path(getwd(), "asa", "tests", "testthat", "helper-asa-env.R"),
    file.path(dirname(getwd()), "asa", "tests", "testthat", "helper-asa-env.R")
  )
  helper_path <- helper_candidates[file.exists(helper_candidates)][1]
  if (length(helper_path) == 1 && nzchar(helper_path)) {
    source(helper_path, local = TRUE)
  }
}

# Test prompt
prompt <- if (exists("asa_test_recursion_limit_prompt", mode = "function")) {
  asa_test_recursion_limit_prompt()
} else {
  paste0(
    "You are doing a multi-step research task.\n",
    "1) You MUST call the Search tool first with the query: \"asa recursion limit test data\".\n",
    "   Do NOT output the final JSON until after you have tool output.\n",
    "2) After you get tool output, produce ONLY valid JSON with this exact schema:\n",
    "{\n",
    "  \"status\": \"complete\"|\"partial\",\n",
    "  \"items\": [\n",
    "    {\"name\": string, \"birth_year\": integer, \"field\": string, \"key_contribution\": string|null}\n",
    "  ],\n",
    "  \"missing\": [string],\n",
    "  \"notes\": string\n",
    "}\n",
    "Missing-data rules:\n",
    "- If you could not run Search or did not get tool output, set status=\"partial\".\n",
    "- Use null for unknown key_contribution.\n",
    "- List any unknown fields in missing.\n",
    "- Do NOT speculate.\n",
    "Known seed data (you may use this even without Search):\n",
    "- Ada Lovelace (birth_year=1815, field=\"mathematics\")\n",
    "- Alan Turing (birth_year=1912, field=\"computer science\")\n",
    "- Grace Hopper (birth_year=1906, field=\"computer science\")\n",
    "Your items MUST include these three people at minimum.\n"
  )
}

# Run tests with different recursion limits
for (rec_limit in c(3L, 4L, 5L)) {
  cat("\n========================================\n")
  cat(sprintf("Testing with recursion_limit = %d\n", rec_limit))
  cat("========================================\n\n")

  final_state <- tryCatch({
    agent$invoke(
      list(messages = list(list(role = "user", content = prompt))),
      config = list(recursion_limit = rec_limit)
    )
  }, error = function(e) {
    cat("ERROR:", conditionMessage(e), "\n")
    NULL
  })

  if (is.null(final_state)) {
    cat("⚠ Agent invocation failed\n")
    next
  }

  # Check stop_reason
  stop_reason <- final_state$stop_reason
  cat("stop_reason:", if (is.null(stop_reason)) "NULL" else stop_reason, "\n")
  cat("Finalize node triggered:", !is.null(stop_reason) && stop_reason == "recursion_limit", "\n")

  # Extract response text
  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  cat("\nResponse text (first 800 chars):\n")
  cat("---\n")
  cat(substr(response_text, 1, 800), "\n")
  if (nchar(response_text) > 800) cat("... [truncated]\n")
  cat("---\n")

  # Parse JSON
  cat("\nJSON validation:\n")
  parsed <- tryCatch({
    jsonlite::fromJSON(response_text)
  }, error = function(e) {
    cat("✗ JSON parse failed:", conditionMessage(e), "\n")
    NULL
  })

  if (!is.null(parsed)) {
    cat("✓ JSON parsed successfully\n")
    cat("  - status:", parsed$status, "\n")
    cat("  - items count:", if (is.data.frame(parsed$items)) nrow(parsed$items) else length(parsed$items), "\n")
    cat("  - notes:", substr(as.character(parsed$notes), 1, 100), "\n")

    # Check required schema fields
    required_fields <- c("status", "items", "missing", "notes")
    present <- required_fields %in% names(parsed)
    if (all(present)) {
      cat("✓ All required schema fields present\n")
    } else {
      cat("✗ Missing schema fields:", required_fields[!present], "\n")
    }

    # Check items contain expected names
    if (is.data.frame(parsed$items) && "name" %in% names(parsed$items)) {
      expected_names <- c("Ada Lovelace", "Alan Turing", "Grace Hopper")
      found <- expected_names[expected_names %in% parsed$items$name]
      cat("  Found expected names:", paste(found, collapse = ", "), "\n")
    }
  }
}

cat("\n========================================\n")
cat("Test complete\n")
cat("========================================\n")
