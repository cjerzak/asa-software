{
# Script: LilAgentTest.R
# Minimal test script for the agent pipeline
# Tests the Custom LLM agent with a search-intensive query
setwd("~/Dropbox/APIs/AgentFunctions/")

system("brew services start tor")

# Configuration
CustomLLMBackend <- "openai"
#modelName <- "gpt-4.1-nano"
modelName <- "gpt-5-mini-2025-08-07"

# Test prompt requiring extensive web searches
#thePrompt <- "Who was the PhD supervisor of Ethan Jerzak? Please search thoroughly and provide details about the supervisor."
thePrompt <- "Perform a deep multi-step trace of the academic genealogy of philosopher Ethan Jerzak. 
1. First, identify who his PhD dissertation chair was at UC Berkeley. 
2. Then, find out who *that* supervisor's PhD advisor was. 
3. Continue this chain backward for at least four generations total (Ethan -> Supervisor -> Grand-Supervisor -> Great-Grand-Supervisor). 
For each generation, provide the Name, PhD Institution, and Year of defense. verify the details across multiple sources if possible."

# Output directory for test results
test_output_dir <- "./TestOutputs"
if (!dir.exists(test_output_dir)) {
  dir.create(test_output_dir, recursive = TRUE)
}

# Initialize flag (FALSE to force initialization)
INITIALIZED_CUSTOM_ENV_TAG <- FALSE

cat("=== LilAgentTest: Starting Agent Pipeline Test ===\n")
cat(sprintf("Backend: %s\n", CustomLLMBackend))
cat(sprintf("Model: %s\n", modelName))
cat(sprintf("Prompt: %s\n", thePrompt))
cat("==================================================\n\n")

# Source the custom LLM invocation script (handles initialization and agent creation)
cat("Initializing agent environment...\n")
source(theSource <- "~/Dropbox/APIs/AgentFunctions/LLM_CustomLLM_Invokation.R")
#source(theSource <- "~/Dropbox/APIs/AgentFunctions/LLM_CustomLLM_Invokation_experimental.R")

cat("Initializing agent environment...\n")
source(theSource)

# Store results
test_results <- list(
  timestamp = Sys.time(),
  backend = CustomLLMBackend,
  model = modelName,
  prompt = thePrompt,
  raw_response = if (exists("raw_response")) raw_response else NULL,
  final_answer = if (exists("theResponseText")) theResponseText else NULL,
  text_blob = if (exists("text_blob")) text_blob else NULL,
  status = if (exists("response")) response$status_code else NA
)

# Display results
cat("\n=== TEST RESULTS ===\n")
cat(sprintf("Status: %s\n", test_results$status))
cat(sprintf("Timestamp: %s\n", test_results$timestamp))
cat("\n--- Final Answer ---\n")
cat(test_results$final_answer %||% "No answer received")
cat("\n\n")

# Save full trace to file for review
trace_file <- file.path(test_output_dir, sprintf("agent_trace_%s.txt",
                                                   format(Sys.time(), "%Y%m%d_%H%M%S")))
cat(sprintf("Saving full agent trace to: %s\n", trace_file))

# Build comprehensive trace output
trace_output <- paste0(
  "=== AGENT PIPELINE TEST TRACE ===\n",
  sprintf("Timestamp: %s\n", test_results$timestamp),
  sprintf("Backend: %s\n", test_results$backend),
  sprintf("Model: %s\n", test_results$model),
  sprintf("Prompt: %s\n", test_results$prompt),
  sprintf("Status: %s\n\n", test_results$status),
  "=== FULL AGENT TRACE ===\n",
  test_results$text_blob %||% "No trace available",
  "\n\n=== FINAL ANSWER ===\n",
  test_results$final_answer %||% "No answer received",
  "\n"
)

# Write trace to file
writeLines(trace_output, trace_file)

# Also save as RDS for programmatic access
rds_file <- file.path(test_output_dir, sprintf("agent_results_%s.rds",
                                                 format(Sys.time(), "%Y%m%d_%H%M%S")))
saveRDS(test_results, rds_file)
cat(sprintf("Saving results object to: %s\n", rds_file))

# Print summary of agent messages if available
if (!is.null(test_results$raw_response) && !inherits(test_results$raw_response, "try-error")) {
  cat("\n=== AGENT MESSAGE TRACE ===\n")
  messages <- test_results$raw_response$messages
  if (!is.null(messages)) {
    for (i in seq_along(messages)) {
      msg <- messages[[i]]
      msg_type <- class(msg)[1]
      cat(sprintf("\n[Message %d] Type: %s\n", i, msg_type))

      # Try to extract content
      content <- tryCatch({
        if (!is.null(msg$content)) {
          if (is.character(msg$content)) {
            msg$content
          } else if (is.list(msg$content)) {
            paste(sapply(msg$content, function(x) {
              if (is.list(x) && !is.null(x$text)) x$text else as.character(x)
            }), collapse = "\n")
          } else {
            as.character(msg$content)
          }
        } else {
          "(no content)"
        }
      }, error = function(e) sprintf("(error extracting content: %s)", e$message))

      # Truncate long content for display
      if (nchar(content) > 500) {
        content <- paste0(substr(content, 1, 500), "... [truncated]")
      }
      cat(content, "\n")

      # Show tool calls if present
      tool_calls <- tryCatch(msg$tool_calls, error = function(e) NULL)
      if (!is.null(tool_calls) && length(tool_calls) > 0) {
        cat("  Tool calls:\n")
        for (tc in tool_calls) {
          cat(sprintf("    - %s\n", tc$name %||% "unknown tool"))
        }
      }
    }
  }
}

cat("\n=== TEST COMPLETE ===\n")
cat(sprintf("Trace file: %s\n", trace_file))
cat(sprintf("Results RDS: %s\n", rds_file))
}
