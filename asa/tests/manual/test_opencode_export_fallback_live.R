# Manual smoke for the OpenCode JSONL export fallback.
#
# Usage:
#   conda run -n asa_env Rscript --vanilla asa/tests/manual/test_opencode_export_fallback_live.R
#
# Equivalent here-doc smoke:
#   Rscript --vanilla - <<'RS'
#   Sys.setenv(ASA_AGENT_BACKEND = "opencode")
#   Sys.setenv(ASA_LLM_BACKEND = "gemini")
#   Sys.setenv(ASA_MODEL_NAME = "gemini-3-flash-preview")
#   readRenviron("~/.Renviron")
#
#   suppressPackageStartupMessages(library(asa))
#   `%||%` <- function(x, y) {
#     if (is.null(x) || length(x) == 0L || all(is.na(x))) y else x
#   }
#
#   agent <- initialize_agent(
#     agent_backend = "opencode",
#     backend = "gemini",
#     model = "gemini-3-flash-preview",
#     conda_env = "asa_env",
#     proxy = NA,
#     use_browser = FALSE,
#     timeout = 180,
#     verbose = TRUE
#   )
#
#   res <- run_task(
#     prompt = 'Return exactly this JSON object and do not call tools: {"ok": true, "confidence": 1}',
#     output_format = "raw",
#     expected_schema = list(ok = "boolean", confidence = "number"),
#     use_plan_mode = TRUE,
#     agent = agent
#   )
#
#   cat("status:", res$status, "\n")
#   cat("message:", res$message, "\n")
#   cat("trace has export fallback:", grepl("opencode_export_fallback", res$trace_json %||% ""), "\n")
#
#   stopifnot(identical(res$status, "success"))
#   stopifnot(grepl('"ok"', res$message))
#   RS

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) y else x
}

Sys.setenv(ASA_AGENT_BACKEND = "opencode")
Sys.setenv(ASA_LLM_BACKEND = "gemini")
Sys.setenv(ASA_MODEL_NAME = "gemini-3-flash-preview")
readRenviron("~/.Renviron")

suppressPackageStartupMessages({
  if (requireNamespace("devtools", quietly = TRUE) && dir.exists("asa")) {
    devtools::load_all("asa", quiet = TRUE)
  } else {
    library(asa)
  }
})

agent <- initialize_agent(
  agent_backend = "opencode",
  backend = "gemini",
  model = "gemini-3-flash-preview",
  conda_env = "asa_env",
  proxy = NA,
  use_browser = FALSE,
  timeout = 180,
  verbose = TRUE
)

res <- run_task(
  prompt = 'Return exactly this JSON object and do not call tools: {"ok": true, "confidence": 1}',
  output_format = "raw",
  expected_schema = list(ok = "boolean", confidence = "number"),
  use_plan_mode = TRUE,
  agent = agent
)

cat("status:", res$status, "\n")
cat("message:", res$message, "\n")
cat("trace has export fallback:", grepl("opencode_export_fallback", res$trace_json %||% ""), "\n")

stopifnot(identical(res$status, "success"))
stopifnot(grepl('"ok"', res$message))
