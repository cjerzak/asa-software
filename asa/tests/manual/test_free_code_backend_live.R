# Manual verification for the free-code backend bridge.
#
# Usage:
#   conda run -n asa_env Rscript asa/tests/manual/test_free_code_backend_live.R
#
# This script verifies:
# - live provider routing through the free-code backend,
# - structured output round-tripping,
# - API traffic staying direct even when a bad proxy is configured,
# - MCP web_search/web_fetch traffic flowing through the proxied stdio server.

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) y else x
}

suppressPackageStartupMessages({
  if (requireNamespace("devtools", quietly = TRUE) && dir.exists("asa")) {
    devtools::load_all("asa", quiet = TRUE)
  } else {
    library(asa)
  }
  library(reticulate)
})

try(
  suppressWarnings(reticulate::use_condaenv("asa_env", required = FALSE)),
  silent = TRUE
)

default_repo <- "/Users/cjerzak/Documents/free-code"
if (!nzchar(Sys.getenv("ASA_FREE_CODE_REPO", unset = "")) && dir.exists(default_repo)) {
  Sys.setenv(ASA_FREE_CODE_REPO = default_repo)
}

if (!nzchar(Sys.getenv("ASA_FREE_CODE_REPO", unset = ""))) {
  stop("Set ASA_FREE_CODE_REPO to the local free-code checkout.")
}

if (!nzchar(Sys.which("bun"))) {
  stop("bun is required for the repo-entrypoint free-code backend checks.")
}

good_proxy <- Sys.getenv("ASA_TOR_TEST_PROXY", unset = "socks5h://127.0.0.1:9150")
bad_proxy <- Sys.getenv("ASA_FREE_CODE_BAD_PROXY", unset = "socks5h://127.0.0.1:9050")

cat("\n========================================\n")
cat("free-code Backend Live Verification\n")
cat("========================================\n\n")
cat("Repo:", Sys.getenv("ASA_FREE_CODE_REPO"), "\n")
cat("Good proxy:", good_proxy, "\n")
cat("Bad proxy:", bad_proxy, "\n\n")

provider_configs <- list(
  list(backend = "openai", model = "gpt-4.1-mini", env = "OPENAI_API_KEY"),
  list(backend = "groq", model = "llama-3.3-70b-versatile", env = "GROQ_API_KEY"),
  list(backend = "xai", model = "grok-3-mini", env = "XAI_API_KEY"),
  list(backend = "anthropic", model = asa:::ASA_DEFAULT_ANTHROPIC_MODEL, env = "ANTHROPIC_API_KEY"),
  list(backend = "openrouter", model = "openai/gpt-4.1-mini", env = "OPENROUTER_API_KEY")
)

failures <- character(0)

record_failure <- function(label, message) {
  failures <<- c(failures, paste(label, "-", message))
}

run_text_smoke <- function(cfg, proxy = NULL) {
  label <- paste0(cfg$backend, "/", cfg$model)
  key <- Sys.getenv(cfg$env, unset = "")
  if (!nzchar(key)) {
    cat("SKIP ", label, " (missing ", cfg$env, ")\n", sep = "")
    return(invisible(NULL))
  }

  agent <- asa::initialize_agent(
    agent_backend = "free-code",
    backend = cfg$backend,
    model = cfg$model,
    proxy = proxy,
    timeout = 30
  )
  on.exit(asa::reset_agent(), add = TRUE)

  resp <- asa::run_task(
    "Reply with exactly OK and nothing else.",
    agent = agent,
    output_format = "text"
  )

  message <- trimws(resp$message %||% "")
  cat(label, " -> status=", resp$status_code, ", message=", message, "\n", sep = "")
  if (!identical(resp$status_code, asa:::ASA_STATUS_SUCCESS) || !identical(message, "OK")) {
    record_failure(label, paste("expected status 200 / OK, got", resp$status_code, shQuote(message)))
  }
}

run_structured_smoke <- function(cfg) {
  label <- paste0(cfg$backend, "/", cfg$model, " structured")
  key <- Sys.getenv(cfg$env, unset = "")
  if (!nzchar(key)) {
    cat("SKIP ", label, " (missing ", cfg$env, ")\n", sep = "")
    return(invisible(NULL))
  }

  agent <- asa::initialize_agent(
    agent_backend = "free-code",
    backend = cfg$backend,
    model = cfg$model,
    timeout = 30
  )
  on.exit(asa::reset_agent(), add = TRUE)

  resp <- asa::run_task(
    paste(
      "Return valid JSON with exactly these fields:",
      "answer = \"OK\" and status = \"complete\".",
      "Do not include any other keys."
    ),
    agent = agent,
    output_format = c("answer", "status")
  )

  answer <- resp$parsed$answer %||% NA_character_
  status <- resp$parsed$status %||% NA_character_
  cat(label, " -> status=", resp$status_code, ", answer=", answer, ", parsed_status=", status, "\n", sep = "")
  if (!identical(resp$status_code, asa:::ASA_STATUS_SUCCESS) ||
      !identical(answer, "OK") ||
      !identical(status, "complete")) {
    record_failure(label, "structured output did not round-trip cleanly")
  }
}

run_tool_probe <- function(label, proxy, allow_read_webpages, prompt, expect_message) {
  log_file <- tempfile("asa-free-code-mcp-", fileext = ".log")
  old_log <- Sys.getenv("ASA_FREE_CODE_MCP_LOG_FILE", unset = NA_character_)
  on.exit({
    if (is.na(old_log)) {
      Sys.unsetenv("ASA_FREE_CODE_MCP_LOG_FILE")
    } else {
      Sys.setenv(ASA_FREE_CODE_MCP_LOG_FILE = old_log)
    }
    asa::reset_agent()
  }, add = TRUE)

  Sys.setenv(ASA_FREE_CODE_MCP_LOG_FILE = log_file)
  agent <- asa::initialize_agent(
    agent_backend = "free-code",
    backend = "openai",
    model = "gpt-4.1-mini",
    proxy = proxy,
    timeout = 30,
    search = asa::search_options(
      max_results = 5L,
      allow_read_webpages = allow_read_webpages
    )
  )

  resp <- asa::run_task(
    prompt,
    agent = agent,
    output_format = "text",
    allow_read_webpages = allow_read_webpages
  )
  log_text <- if (file.exists(log_file)) paste(readLines(log_file, warn = FALSE), collapse = "\n") else ""
  message <- trimws(resp$message %||% "")
  cat(label, " -> status=", resp$status_code, ", message=", message, "\n", sep = "")
  cat("  MCP log: ", if (nzchar(log_text)) log_text else "<empty>", "\n", sep = "")

  if (!identical(resp$status_code, asa:::ASA_STATUS_SUCCESS)) {
    record_failure(label, paste("expected status 200, got", resp$status_code))
  }
  if (!identical(message, expect_message)) {
    record_failure(label, paste("expected message", shQuote(expect_message), "got", shQuote(message)))
  }
  if (!nzchar(log_text)) {
    record_failure(label, "expected MCP log activity but log file was empty")
  }
}

cat("Provider smokes\n")
for (cfg in provider_configs) {
  run_text_smoke(cfg)
}
cat("\nStructured-output smokes\n")
for (cfg in provider_configs[c(1, 2, 4)]) {
  run_structured_smoke(cfg)
}

cat("\nAPI/proxy split smoke\n")
run_text_smoke(
  list(backend = "openai", model = "gpt-4.1-mini", env = "OPENAI_API_KEY"),
  proxy = bad_proxy
)

cat("\nMCP proxy routing\n")
run_tool_probe(
  label = "web_search positive",
  proxy = good_proxy,
  allow_read_webpages = FALSE,
  prompt = paste(
    "You must call mcp__asa_search__web_search exactly once with the query \"site:openai.com GPT-4.1\".",
    "If the tool call fails or returns no usable results, reply with exactly TOOL_FAILED.",
    "Otherwise reply with the first result URL only."
  ),
  expect_message = "https://openai.com/index/gpt-4-1"
)
run_tool_probe(
  label = "web_fetch positive",
  proxy = good_proxy,
  allow_read_webpages = TRUE,
  prompt = paste(
    "You must call mcp__asa_search__web_fetch on https://example.com with query \"main heading\".",
    "If the tool call fails, reply with exactly TOOL_FAILED.",
    "Otherwise reply with the heading text only."
  ),
  expect_message = "Example Domain"
)
run_tool_probe(
  label = "web_fetch negative",
  proxy = bad_proxy,
  allow_read_webpages = TRUE,
  prompt = paste(
    "You must call mcp__asa_search__web_fetch on https://example.com with query \"main heading\".",
    "If the tool call fails, reply with exactly TOOL_FAILED.",
    "Otherwise reply with the heading text only."
  ),
  expect_message = "TOOL_FAILED"
)

if (length(failures) > 0L) {
  cat("\nFailures:\n")
  for (item in failures) {
    cat(" - ", item, "\n", sep = "")
  }
  stop("free-code backend manual verification failed.")
}

cat("\nAll free-code backend checks passed.\n")
