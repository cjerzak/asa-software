# Manual test: Recursion limit with Gemini Flash 3
# Verifies: schema-valid best-effort JSON, Search tool activity, and recursion_limit finalization.
#
# Usage:
#   Rscript asa/tests/manual/test_recursion_limit_gemini.R
#
# Requires: GOOGLE_API_KEY or GEMINI_API_KEY environment variable

library(asa)

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) y else x
}

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

fixture_items <- if (exists("asa_test_recursion_limit_fixture_items", mode = "function")) {
  asa_test_recursion_limit_fixture_items()
} else {
  data.frame(
    name = c("Ada Lovelace", "Alan Turing", "Grace Hopper"),
    birth_year = c(1815L, 1912L, 1906L),
    field = c("mathematics", "computer science", "computer science"),
    stringsAsFactors = FALSE
  )
}

has_search_tool_activity <- if (exists("asa_test_has_search_tool_activity", mode = "function")) {
  asa_test_has_search_tool_activity
} else {
  function(messages) {
    if (is.null(messages) || length(messages) == 0L) {
      return(FALSE)
    }
    any(vapply(messages, function(msg) {
      tool_calls <- tryCatch(msg$tool_calls, error = function(e) NULL)
      if (is.null(tool_calls) || length(tool_calls) == 0L) {
        return(FALSE)
      }
      any(vapply(tool_calls, function(call) {
        identical(as.character(call$name %||% "")[[1]], "Search")
      }, logical(1)))
    }, logical(1)))
  }
}

coerce_items_df <- function(x) {
  if (is.data.frame(x)) {
    return(x)
  }
  if (is.list(x) && length(x) > 0L) {
    return(tryCatch({
      if (!is.null(names(x)) && all(c("name", "birth_year", "field") %in% names(x))) {
        as.data.frame(x, stringsAsFactors = FALSE)
      } else if (all(vapply(x, is.list, logical(1)))) {
        do.call(
          rbind,
          lapply(x, function(entry) as.data.frame(entry, stringsAsFactors = FALSE))
        )
      } else {
        NULL
      }
    }, error = function(e) NULL))
  }
  NULL
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
    "- If you include any items, only use facts supported by the Search output.\n"
  )
}

# Run tests with different recursion limits
failures <- character(0)
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
    failures <- c(failures, sprintf("recursion_limit=%d: invocation failed", rec_limit))
    next
  }

  # Check stop_reason
  stop_reason <- final_state$stop_reason
  run_failures <- character(0)
  cat("stop_reason:", if (is.null(stop_reason)) "NULL" else stop_reason, "\n")
  cat("Finalize node triggered:", !is.null(stop_reason) && stop_reason == "recursion_limit", "\n")
  if (!identical(stop_reason, "recursion_limit")) {
    run_failures <- c(run_failures, sprintf("Expected stop_reason recursion_limit, got %s", stop_reason %||% "NULL"))
  }
  if (!isTRUE(has_search_tool_activity(final_state$messages))) {
    run_failures <- c(run_failures, "No Search tool call recorded in message history")
  }

  # Extract response text
  response_text <- asa:::.extract_response_text(final_state, backend = "gemini")
  cat("\nResponse text (first 800 chars):\n")
  cat("---\n")
  cat(substr(response_text, 1, 800), "\n")
  if (nchar(response_text) > 800) cat("... [truncated]\n")
  cat("---\n")

  # Parse JSON
  cat("\nJSON validation:\n")
  parse_error <- NULL
  parsed <- tryCatch({
    jsonlite::fromJSON(response_text)
  }, error = function(e) {
    parse_error <<- conditionMessage(e)
    cat("✗ JSON parse failed:", parse_error, "\n")
    NULL
  })

  if (!is.null(parse_error)) {
    run_failures <- c(run_failures, sprintf("JSON parse failed: %s", parse_error))
  }

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
      run_failures <- c(run_failures, sprintf(
        "Missing schema fields: %s",
        paste(required_fields[!present], collapse = ", ")
      ))
    }

    items_df <- coerce_items_df(parsed$items)
    if (!is.null(items_df) && nrow(items_df) >= 1L) {
      matched_idx <- match(as.character(items_df$name), fixture_items$name)
      if (any(is.na(matched_idx))) {
        unexpected_names <- unique(as.character(items_df$name[is.na(matched_idx)]))
        cat("  Unexpected names:", paste(unexpected_names, collapse = ", "), "\n")
        run_failures <- c(run_failures, sprintf(
          "Returned item names not present in Search fixture: %s",
          paste(unexpected_names, collapse = ", ")
        ))
      } else {
        birth_year_matches <- as.integer(items_df$birth_year) == fixture_items$birth_year[matched_idx]
        field_matches <- as.character(items_df$field) == fixture_items$field[matched_idx]
        if (!all(birth_year_matches) || !all(field_matches)) {
          run_failures <- c(run_failures, "Returned item attributes did not match Search fixture values")
        }
        cat("  Returned fixture-backed names:", paste(as.character(items_df$name), collapse = ", "), "\n")
      }
    }
  }

  if (length(run_failures) > 0L) {
    cat("\nAssertion failures:\n")
    for (msg in run_failures) {
      cat(" - ", msg, "\n", sep = "")
    }
    failures <- c(failures, sprintf("recursion_limit=%d: %s", rec_limit, paste(run_failures, collapse = "; ")))
  } else {
    cat("✓ Assertions passed for this recursion_limit\n")
  }
}

cat("\n========================================\n")
cat("Test complete\n")
cat("========================================\n")

if (length(failures) > 0L) {
  stop(
    paste(
      "Recursion limit manual test failed:",
      paste(failures, collapse = " | ")
    ),
    call. = FALSE
  )
}
