# test_wikidata_standalone.R
# Manual test script for Wikidata SPARQL tool
#
# Run with: Rscript tests/manual/test_wikidata_standalone.R

library(asa)
library(reticulate)

cat("=== Wikidata Tool Standalone Test ===\n\n")

# Activate conda environment
cat("1. Activating conda environment...\n")
use_condaenv("asa_env", required = TRUE)

# Import wikidata_tool module
cat("2. Importing wikidata_tool module...\n")
# Use development path (inst/python) or installed path
python_path <- system.file("python", package = "asa")
if (python_path == "" || !file.exists(file.path(python_path, "tools/entity_wikidata_tool.py"))) {
  # Try development path
  python_path <- file.path(getwd(), "inst/python")
  if (!file.exists(file.path(python_path, "tools/entity_wikidata_tool.py"))) {
    python_path <- file.path(dirname(getwd()), "asa/inst/python")
  }
}
cat(sprintf("   Python path: %s\n", python_path))
wikidata <- import_from_path("tools.entity_wikidata_tool", path = python_path)

# Explicitly load optional template catalog for this manual test.
template_path <- file.path(python_path, "shared/resources/wikidata_templates.json")
if (file.exists(template_path)) {
  wikidata$configure_entity_templates(template_path = template_path)
  cat(sprintf("   Template path: %s\n", template_path))
} else {
  stop("shared/resources/wikidata_templates.json not found; pass template path explicitly for manual Wikidata tests")
}

# Test 1: List known entity types
cat("\n3. Known entity types:\n")
entity_types <- wikidata$get_known_entity_types()
for (et in entity_types) {
  template <- wikidata$get_entity_template(et)
  cat(sprintf("   - %s: %s\n", et, template$description))
}

# Test 2: Query US Senators
cat("\n4. Querying Wikidata for US Senators...\n")
start_time <- Sys.time()
senators <- wikidata$wikidata_query_senators()
elapsed <- difftime(Sys.time(), start_time, units = "secs")

cat(sprintf("   Found %d senators in %.1f seconds\n", length(senators), elapsed))

# Show first 10
cat("\n5. First 10 senators:\n")
for (i in seq_len(min(10, length(senators)))) {
  s <- senators[[i]]
  name <- s$name %||% "Unknown"
  state <- s$state %||% s$stateLabel %||% "?"
  party <- s$party %||% s$partyLabel %||% "?"
  cat(sprintf("   %2d. %s (%s) - %s\n", i, name, state, party))
}

# Test 3: Query Countries
cat("\n6. Querying Wikidata for countries...\n")
start_time <- Sys.time()
countries <- wikidata$wikidata_query_countries()
elapsed <- difftime(Sys.time(), start_time, units = "secs")

cat(sprintf("   Found %d countries in %.1f seconds\n", length(countries), elapsed))

# Show first 5
cat("\n7. First 5 countries:\n")
for (i in seq_len(min(5, length(countries)))) {
  c <- countries[[i]]
  name <- c$name %||% "Unknown"
  capital <- c$capital %||% c$capitalLabel %||% "?"
  cat(sprintf("   %2d. %s - Capital: %s\n", i, name, capital))
}

# Test 4: Entity type inference
cat("\n8. Testing entity type inference:\n")
test_queries <- c(
  "all US senators",
  "Fortune 500 companies",
  "countries in the world",
  "US states and capitals",
  "random query that won't match"
)

for (query in test_queries) {
  inferred <- wikidata$infer_entity_type(query)
  result <- if (is.null(inferred)) "None" else inferred
  cat(sprintf("   '%s' -> %s\n", query, result))
}

# Test 5: Create LangChain tool
cat("\n9. Creating WikidataSearchTool...\n")
tool <- wikidata$create_wikidata_tool()
cat(sprintf("   Tool name: %s\n", tool$name))
cat(sprintf("   Description (first 100 chars): %s...\n",
            substr(tool$description, 1, 100)))

# Test 6: Run tool with entity type prefix
cat("\n10. Running tool with 'entity_type:us_states'...\n")
result <- tool$`_run`("entity_type:us_states")
cat("   Result (first 500 chars):\n")
cat(substr(result, 1, 500), "\n")

cat("\n=== Wikidata Tool Test Complete ===\n")

# Null coalescing for display
`%||%` <- function(x, y) if (is.null(x)) y else x
