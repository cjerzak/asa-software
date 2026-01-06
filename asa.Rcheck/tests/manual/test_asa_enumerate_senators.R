# test_asa_enumerate_senators.R
# Integration test: Find all US Senators using asa_enumerate()
#
# Run with: Rscript tests/manual/test_asa_enumerate_senators.R

library(asa)

cat("==========================================================\n")
cat("   ASA Enumeration Integration Test: US Senators\n")
cat("==========================================================\n\n")

# Check for API key
if (Sys.getenv("OPENAI_API_KEY") == "") {
  stop("OPENAI_API_KEY environment variable not set")
}

# Test 1: Basic enumeration with auto schema
cat("TEST 1: Basic enumeration with Wikidata (quick test)\n")
cat("--------------------------------------------------\n")

result1 <- asa_enumerate(

  query = "Find all current United States senators",
  schema = c(name = "character", state = "character", party = "character"),
  max_workers = 2,
  max_rounds = 3,
  budget = list(queries = 20, tokens = 100000, time_sec = 120),
  stop_policy = list(
    target_items = 100,
    plateau_rounds = 2,
    novelty_min = 0.05
  ),
  sources = list(web = FALSE, wikipedia = FALSE, wikidata = TRUE),
  progress = TRUE,
  include_provenance = TRUE,
  checkpoint = TRUE,
  verbose = TRUE
)

cat("\n--- Results ---\n")
print(result1)

cat("\n--- Summary ---\n")
summary(result1)

# Save to CSV
csv_file <- tempfile(pattern = "senators_", fileext = ".csv")
write.csv(result1$data, csv_file, row.names = FALSE)
cat(sprintf("\nCSV saved to: %s\n", csv_file))

# Test 2: Full search with all sources
cat("\n\nTEST 2: Full search with all sources\n")
cat("------------------------------------\n")

result2 <- asa_enumerate(
  query = "Find all 100 current US senators with their state, political party, and term end date",
  schema = c(
    name = "character",
    state = "character",
    party = "character",
    term_end = "character"
  ),
  max_workers = 3,
  max_rounds = 5,
  budget = list(queries = 30, tokens = 150000, time_sec = 180),
  stop_policy = list(
    target_items = 100,
    plateau_rounds = 2
  ),
  sources = list(web = TRUE, wikipedia = TRUE, wikidata = TRUE),
  progress = TRUE,
  include_provenance = TRUE,
  verbose = TRUE
)

cat("\n--- Results ---\n")
print(result2)

# Show data preview
cat("\n--- Data Preview (first 20 rows) ---\n")
print(head(result2$data, 20))

# Show provenance if available
if (!is.null(result2$provenance)) {
  cat("\n--- Provenance (first 10 rows) ---\n")
  print(head(result2$provenance, 10))
}

# Party breakdown
if ("party" %in% names(result2$data)) {
  cat("\n--- Party Breakdown ---\n")
  print(table(result2$data$party, useNA = "ifany"))
}

# State coverage
if ("state" %in% names(result2$data)) {
  cat("\n--- State Coverage ---\n")
  n_states <- length(unique(result2$data$state))
  cat(sprintf("Unique states found: %d / 50\n", n_states))

  # Missing states (if any)
  all_states <- c("Alabama", "Alaska", "Arizona", "Arkansas", "California",
                  "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
                  "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
                  "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
                  "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
                  "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
                  "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
                  "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
                  "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
                  "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")

  found_states <- unique(result2$data$state)
  missing <- setdiff(all_states, found_states)
  if (length(missing) > 0) {
    cat(sprintf("Missing states (%d): %s\n", length(missing),
                paste(missing, collapse = ", ")))
  }
}

cat("\n==========================================================\n")
cat("   Test Complete\n")
cat("==========================================================\n")
