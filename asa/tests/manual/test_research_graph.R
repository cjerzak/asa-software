# test_research_graph.R
# Manual test script for research graph components
#
# Run with: Rscript tests/manual/test_research_graph.R

library(asa)
library(reticulate)

cat("=== Research Graph Component Test ===\n\n")

# Check for API key
if (Sys.getenv("OPENAI_API_KEY") == "") {
  stop("OPENAI_API_KEY environment variable not set")
}

# Activate conda environment
cat("1. Activating conda environment...\n")
use_condaenv("asa_env", required = TRUE)

# Import modules
cat("2. Importing Python modules...\n")
python_path <- system.file("python", package = "asa")
research_graph <- import_from_path("workflows.research_graph_workflow", path = python_path)
wikidata_tool <- import_from_path("tools.entity_wikidata_tool", path = python_path)

# Test ResearchConfig
cat("\n3. Testing ResearchConfig...\n")
config <- research_graph$ResearchConfig(
  max_workers = 2L,
  max_rounds = 3L,
  budget_queries = 10L,
  budget_tokens = 50000L,
  budget_time_sec = 60L,
  target_items = 5L,
  use_wikidata = TRUE
)
cat(sprintf("   max_workers: %d\n", config$max_workers))
cat(sprintf("   target_items: %d\n", config$target_items))

# Test utility functions
cat("\n4. Testing utility functions...\n")

# Hash result
schema <- list(name = "character", state = "character")
fields1 <- list(name = "John Smith", state = "California")
fields2 <- list(name = "john smith", state = "california")
fields3 <- list(name = "Jane Doe", state = "Texas")

hash1 <- research_graph$`_hash_result`(fields1, schema)
hash2 <- research_graph$`_hash_result`(fields2, schema)
hash3 <- research_graph$`_hash_result`(fields3, schema)

cat(sprintf("   Hash('John Smith', 'California'): %s\n", hash1))
cat(sprintf("   Hash('john smith', 'california'): %s\n", hash2))
cat(sprintf("   Hash('Jane Doe', 'Texas'): %s\n", hash3))
cat(sprintf("   Hash1 == Hash2: %s (should be TRUE for case-insensitive)\n", hash1 == hash2))

# Fuzzy match
cat("\n5. Testing fuzzy name matching...\n")
test_pairs <- list(
  c("John Smith", "John Smith"),
  c("John Smith", "john smith"),
  c("John Smith", "J. Smith"),
  c("John Smith", "Jane Doe"),
  c("Senator John Smith", "John Smith"),
  c("", "John Smith")
)

for (pair in test_pairs) {
  score <- research_graph$`_fuzzy_match_name`(pair[1], pair[2])
  cat(sprintf("   '%s' vs '%s': %.2f\n", pair[1], pair[2], score))
}

# Test novelty calculation
cat("\n6. Testing novelty calculation...\n")
new_results <- list(
  list(fields = list(name = "Alice", state = "NY")),
  list(fields = list(name = "Bob", state = "CA")),
  list(fields = list(name = "Alice", state = "NY"))  # Duplicate
)
existing_hashes <- list()

novelty <- research_graph$`_calculate_novelty`(new_results, existing_hashes, schema)
cat(sprintf("   Novelty rate (3 results, 1 dup, 0 existing): %.2f\n", novelty))

# Initialize agent for further tests
cat("\n7. Initializing agent...\n")
agent <- initialize_agent(
  backend = "openai",
  model = "gpt-4.1-mini",
  verbose = FALSE
)
cat("   Agent initialized\n")

# Create Wikidata tool
cat("\n8. Creating Wikidata tool...\n")
wd_tool <- wikidata_tool$create_wikidata_tool()
cat(sprintf("   Tool: %s\n", wd_tool$name))

# Access package environment for LLM and tools
asa_env <- asa:::asa_env

# Create research graph
cat("\n9. Creating research graph...\n")
graph <- research_graph$create_research_graph(
  llm = asa_env$llm,
  tools = asa_env$tools,
  config = config,
  checkpointer = asa_env$MemorySaver(),
  wikidata_tool = wd_tool
)
cat("   Graph created successfully\n")

# Test a simple query with the graph
cat("\n10. Running simple test query...\n")
cat("    Query: 'Find 3 US states'\n")

result <- research_graph$run_research(
  graph = graph,
  query = "Find 3 US states with their capitals",
  schema = list(name = "character", capital = "character"),
  config_dict = list(
    max_workers = 1L,
    max_rounds = 2L,
    budget_queries = 5L,
    use_wikidata = TRUE
  )
)

cat(sprintf("\n   Status: %s\n", result$status))
cat(sprintf("   Stop reason: %s\n", result$stop_reason %||% "N/A"))
cat(sprintf("   Results found: %d\n", length(result$results)))

if (length(result$results) > 0) {
  cat("\n   Results:\n")
  for (i in seq_len(min(5, length(result$results)))) {
    r <- result$results[[i]]
    fields <- r$fields %||% list()
    name <- fields$name %||% "?"
    capital <- fields$capital %||% "?"
    cat(sprintf("   %d. %s - %s\n", i, name, capital))
  }
}

if (length(result$errors) > 0) {
  cat("\n   Errors:\n")
  for (err in result$errors) {
    cat(sprintf("   - %s: %s\n", err$stage, err$error))
  }
}

cat("\n=== Research Graph Test Complete ===\n")

# Null coalescing
`%||%` <- function(x, y) if (is.null(x)) y else x
