#!/usr/bin/env Rscript
# ==============================================================================
# run_single_task.R - Execute a Single ASA Agent Task
# ==============================================================================
# This script is called by run_batch.sh for each parallel task.
# It reads a task from the tasks file and runs it through the asa agent.
#
# Usage:
#   Rscript run_single_task.R <task_id>
#
# Environment Variables:
#   ASA_TASKS_FILE    - Path to tasks CSV (default: ./tasks.csv)
#   ASA_OUTPUT_DIR    - Directory for results (default: ./results)
#   ASA_AGENT_BACKEND - Agent runtime backend (default: opencode; use agent for built-in LangGraph)
#   ASA_BACKEND       - LLM backend (default: openai; use azure-openai for Azure)
#   ASA_MODEL         - Model name/deployment name (default: gpt-4.1-mini)
#   AZURE_OPENAI_ENDPOINT / AZURE_OPENAI_API_KEY / AZURE_OPENAI_DEPLOYMENT
#                     - Required for ASA_BACKEND=azure-openai
#   ASA_AZURE_RPM / ASA_AZURE_TPM / ASA_AZURE_MAX_CONCURRENT_REQUESTS
#                     - Optional shared Azure deployment limiter settings
#   HTTP_PROXY        - Proxy URL (set by run_batch.sh)
#
# Tasks CSV Format:
#   id,prompt,output_format,template_vars
#   1,"What is the capital of France?",text,
#   2,"Find info about {{person}}",json,"{""person"":""Marie Curie""}"
#
# ==============================================================================

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript run_single_task.R <task_id>", call. = FALSE)
}

task_id <- as.integer(args[1])

# Configuration from environment
tasks_file <- Sys.getenv("ASA_TASKS_FILE", "tasks.csv")
output_dir <- Sys.getenv("ASA_OUTPUT_DIR", "results")
agent_backend <- Sys.getenv("ASA_AGENT_BACKEND", "opencode")
backend <- Sys.getenv("ASA_BACKEND", "openai")
model <- Sys.getenv("ASA_MODEL", "gpt-4.1-mini")
conda_env <- Sys.getenv("ASA_CONDA_ENV", "asa_env")

# Create output directory
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# Load the asa package
suppressPackageStartupMessages({
  library(asa)
})

# Read task from CSV
if (!file.exists(tasks_file)) {
  stop(sprintf("Tasks file not found: %s", tasks_file), call. = FALSE)
}

tasks <- read.csv(tasks_file, stringsAsFactors = FALSE)

# Find the task by ID
if (!"id" %in% names(tasks)) {
  # If no id column, use row number
  tasks$id <- seq_len(nrow(tasks))
}

task_row <- tasks[tasks$id == task_id, ]
if (nrow(task_row) == 0) {
  stop(sprintf("Task ID %d not found in %s", task_id, tasks_file), call. = FALSE)
}

# Extract task parameters
prompt <- task_row$prompt[1]
output_format <- if ("output_format" %in% names(task_row)) {
  task_row$output_format[1]
} else {
  "text"
}

# Handle template variables if present
if ("template_vars" %in% names(task_row) && !is.na(task_row$template_vars[1]) &&
    nzchar(task_row$template_vars[1])) {
  template_vars <- jsonlite::fromJSON(task_row$template_vars[1])
  prompt <- do.call(build_prompt, c(list(template = prompt), template_vars))
}

# Get proxy from environment (set by run_batch.sh)
proxy <- Sys.getenv("HTTP_PROXY", "socks5h://127.0.0.1:9050")
if (proxy == "") proxy <- NULL

# Initialize agent (uses shared conda environment)
agent <- tryCatch({
  initialize_agent(
    agent_backend = agent_backend,
    backend = backend,
    model = model,
    conda_env = conda_env,
    proxy = proxy,
    verbose = FALSE
  )
}, error = function(e) {
  # If agent init fails, try without proxy
  message(sprintf("[Task %d] Agent init with proxy failed, retrying without proxy...", task_id))
  initialize_agent(
    agent_backend = agent_backend,
    backend = backend,
    model = model,
    conda_env = conda_env,
    proxy = NULL,
    verbose = FALSE
  )
})

# Run the task
result <- tryCatch({
  run_task(
    prompt = prompt,
    output_format = output_format,
    agent = agent
  )
}, error = function(e) {
  # Return error result
  asa_result(
    prompt = prompt,
    message = NA_character_,
    parsed = NULL,
    raw_output = sprintf("Error: %s", e$message),
    elapsed_time = NA_real_,
    status = "error"
  )
})

# Prepare output
output <- list(
  id = task_id,
  prompt = prompt,
  message = result$message,
  status = result$status,
  elapsed_time = result$elapsed_time,
  timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
)

# Add parsed fields if present
if (!is.null(result$parsed)) {
  output$parsed <- result$parsed
}

# Save result as JSON
output_file <- file.path(output_dir, sprintf("task_%06d.json", task_id))
jsonlite::write_json(output, output_file, auto_unbox = TRUE, pretty = TRUE)

# Print summary to stdout (captured by parallel)
cat(sprintf("[%s] Task %d: %s (%.2f min)\n",
            format(Sys.time(), "%H:%M:%S"),
            task_id,
            result$status,
            result$elapsed_time %||% 0))
