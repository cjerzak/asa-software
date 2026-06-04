# Manual Azure OpenAI smoke tests.
#
# Required environment:
#   AZURE_OPENAI_API_KEY
#   AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_API_BASE
#   AZURE_OPENAI_DEPLOYMENT
#
# Run from the repository root in the asa_env conda environment:
#   R -q -f asa/tests/manual/test_azure_openai_live.R

library(asa)

missing_env <- function(name) !nzchar(Sys.getenv(name, unset = ""))

if (missing_env("AZURE_OPENAI_API_KEY") ||
    (missing_env("AZURE_OPENAI_ENDPOINT") && missing_env("AZURE_OPENAI_API_BASE")) ||
    missing_env("AZURE_OPENAI_DEPLOYMENT")) {
  message("Skipping Azure OpenAI manual smoke test; required Azure env vars are not set.")
  quit(save = "no", status = 0)
}

withr::local_envvar(c(ASA_REQUIRE_TOR_PROXY = "false"))

agent <- asa::initialize_agent(
  backend = "azure-openai",
  model = Sys.getenv("AZURE_OPENAI_DEPLOYMENT"),
  proxy = NULL,
  use_browser = FALSE,
  verbose = TRUE
)

single <- asa::run_task(
  "Reply with exactly AZURE_OK",
  agent = agent,
  output_format = "text"
)
print(single$message)

cfg <- asa::asa_config(
  backend = "azure-openai",
  model = Sys.getenv("AZURE_OPENAI_DEPLOYMENT"),
  workers = 4,
  rate_limit = 0.01,
  proxy = NULL,
  use_browser = FALSE
)

parallel <- asa::run_task_batch(
  rep("Reply with exactly AZURE_OK", 4),
  config = cfg,
  parallel = TRUE,
  workers = 4,
  progress = TRUE
)
print(vapply(parallel, function(x) {
  value <- x$message
  if (is.null(value) || length(value) == 0L || is.na(value[[1]])) {
    return("")
  }
  as.character(value[[1]])
}, character(1)))
