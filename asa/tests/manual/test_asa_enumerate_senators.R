# test_asa_enumerate_senators.R
# Integration test: Find all US Senators using asa_enumerate()
#
# Run with: Rscript tests/manual/test_asa_enumerate_senators.R

library(asa)

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || all(is.na(x))) y else x

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

normalize_text <- function(x) {
  x <- trimws(as.character(x))
  x[is.na(x)] <- ""
  x
}

validate_enumeration_result <- function(result,
                                        scenario,
                                        required_cols,
                                        min_rows,
                                        min_unique_names,
                                        min_unique_states,
                                        require_provenance = TRUE,
                                        min_nonempty_fields = list()) {
  errors <- character(0)

  if (!inherits(result, "asa_enumerate_result")) {
    errors <- c(errors, "Result is not an asa_enumerate_result object")
  }

  status <- as.character(result$status %||% NA_character_)[1]
  if (!(status %in% c("complete", "partial"))) {
    errors <- c(errors, sprintf("Unexpected status: %s", status %||% "NA"))
  }

  if (!is.data.frame(result$data)) {
    errors <- c(errors, "Result data is not a data.frame")
  } else {
    missing_cols <- setdiff(required_cols, names(result$data))
    if (length(missing_cols) > 0L) {
      errors <- c(errors, sprintf("Missing required columns: %s", paste(missing_cols, collapse = ", ")))
    }

    row_count <- nrow(result$data)
    if (row_count < min_rows) {
      errors <- c(errors, sprintf("Expected at least %d rows, got %d", min_rows, row_count))
    }

    name_values <- if ("name" %in% names(result$data)) normalize_text(result$data$name) else character(0)
    unique_names <- unique(name_values[nzchar(name_values)])
    if (length(unique_names) < min_unique_names) {
      errors <- c(errors, sprintf(
        "Expected at least %d unique senator names, got %d",
        min_unique_names,
        length(unique_names)
      ))
    }

    if ("state" %in% names(result$data)) {
      state_values <- unique(normalize_text(result$data$state))
      state_values <- state_values[nzchar(state_values)]
      if (length(state_values) < min_unique_states) {
        errors <- c(errors, sprintf(
          "Expected at least %d unique states, got %d",
          min_unique_states,
          length(state_values)
        ))
      }
      missing_states <- setdiff(all_states, state_values)
      if (length(missing_states) > (50L - min_unique_states)) {
        errors <- c(errors, sprintf(
          "State coverage too low: missing %d states",
          length(missing_states)
        ))
      }
    } else {
      errors <- c(errors, "Missing state column needed for coverage checks")
    }

    for (field in names(min_nonempty_fields)) {
      threshold <- as.integer(min_nonempty_fields[[field]])
      if (!field %in% names(result$data)) {
        errors <- c(errors, sprintf("Missing field for completeness check: %s", field))
        next
      }
      nonempty <- sum(nzchar(normalize_text(result$data[[field]])))
      if (nonempty < threshold) {
        errors <- c(errors, sprintf(
          "Expected at least %d non-empty values for %s, got %d",
          threshold,
          field,
          nonempty
        ))
      }
    }
  }

  if (isTRUE(require_provenance)) {
    if (is.null(result$provenance) || !is.data.frame(result$provenance) || nrow(result$provenance) == 0L) {
      errors <- c(errors, "Expected non-empty provenance output")
    } else if (!"source_url" %in% names(result$provenance)) {
      errors <- c(errors, "Provenance is missing source_url")
    } else {
      nonempty_sources <- sum(nzchar(normalize_text(result$provenance$source_url)))
      if (nonempty_sources == 0L) {
        errors <- c(errors, "Provenance source_url values are empty")
      }
    }
  }

  if (length(errors) > 0L) {
    cat(sprintf("\n[FAIL] %s\n", scenario))
    for (msg in errors) {
      cat(" - ", msg, "\n", sep = "")
    }
    stop(sprintf("%s smoke checks failed", scenario), call. = FALSE)
  }

  cat(sprintf("\n[PASS] %s smoke checks\n", scenario))
}

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

validate_enumeration_result(
  result = result1,
  scenario = "Wikidata-only senator enumeration",
  required_cols = c("name", "state", "party"),
  min_rows = 80L,
  min_unique_names = 80L,
  min_unique_states = 45L,
  require_provenance = TRUE
)

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
  found_states <- unique(result2$data$state)
  missing <- setdiff(all_states, found_states)
  if (length(missing) > 0) {
    cat(sprintf("Missing states (%d): %s\n", length(missing),
                paste(missing, collapse = ", ")))
  }
}

validate_enumeration_result(
  result = result2,
  scenario = "Full-source senator enumeration",
  required_cols = c("name", "state", "party", "term_end"),
  min_rows = 90L,
  min_unique_names = 90L,
  min_unique_states = 45L,
  require_provenance = TRUE,
  min_nonempty_fields = list(term_end = 50L)
)

cat("\n==========================================================\n")
cat("   Test Complete\n")
cat("==========================================================\n")
