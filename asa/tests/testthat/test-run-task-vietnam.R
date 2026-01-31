# Integration test: run_task JSON output with Gemini backend

test_that("run_task returns Vietnam first-level divisions in JSON (Gemini)", {
  skip_on_cran()

  skip_api_tests <- tolower(Sys.getenv("ASA_CI_SKIP_API_TESTS")) %in% c("true", "1", "yes")
  skip_if(skip_api_tests, "ASA_CI_SKIP_API_TESTS is set")

  skip_if(
    !reticulate::py_module_available("langchain_google_genai"),
    "Missing Python module langchain_google_genai"
  )
  skip_if(
    !any(nzchar(Sys.getenv(c("GOOGLE_API_KEY", "GEMINI_API_KEY")))),
    "Missing GOOGLE_API_KEY or GEMINI_API_KEY"
  )

  agent <- initialize_agent(
    backend = "gemini",
    model = "gemini-3-flash-preview",
    verbose = FALSE
  )

  prompt <- paste(
    "List all the first-level administrative divisions of Vietnam.",
    "Include all 58 provinces and 5 centrally governed municipalities (total 63).",
    "Use search and Wikipedia if needed.",
    "Return only valid JSON with a single top-level object and fields:",
    "country (string), divisions (flat array of strings), count (integer).",
    "Example:",
    "{\"country\":\"Vietnam\",\"divisions\":[\"An Giang\", \"Ba Ria-Vung Tau\"],\"count\":63}"
  )

  result <- run_task(
    prompt = prompt,
    output_format = "json",
    expected_fields = c("country", "divisions", "count"),
    agent = agent,
    allow_read_webpages = TRUE,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")
  parsed <- result$parsed
  expect_false(is.null(parsed), info = "Parsed JSON is NULL")
  if (!is.null(result$parsing_status) && !isTRUE(result$parsing_status$valid)) {
    message(
      "Parsing status: ",
      result$parsing_status$reason,
      " (missing: ",
      paste(result$parsing_status$missing, collapse = ", "),
      ")"
    )
  }

  divisions <- character(0)
  declared_count <- NULL

  division_keys <- c(
    "divisions", "first_level_divisions", "administrative_divisions",
    "province_level", "provinces", "municipalities", "cities",
    "centrally_governed_municipalities", "central_municipalities"
  )
  item_keys <- c("division", "name", "province", "municipality", "city", "unit")

  extract_divisions <- function(x) {
    if (is.null(x)) return(character(0))
    if (is.character(x)) return(x)

    if (is.data.frame(x)) {
      hit <- intersect(names(x), item_keys)
      if (length(hit) > 0) return(x[[hit[1]]])
      char_cols <- names(x)[vapply(x, is.character, logical(1))]
      if (length(char_cols) > 0) return(x[[char_cols[1]]])
      return(character(0))
    }

    if (is.list(x)) {
      # Direct division fields
      hit <- intersect(names(x), division_keys)
      if (length(hit) > 0) {
        return(unlist(x[hit], use.names = FALSE))
      }

      # Common wrappers
      for (wrapper in c("results", "items", "data")) {
        if (wrapper %in% names(x)) {
          return(extract_divisions(x[[wrapper]]))
        }
      }

      # List of objects
      if (all(vapply(x, is.list, logical(1)))) {
        out <- vapply(x, function(item) {
          for (key in item_keys) {
            if (!is.null(item[[key]])) return(as.character(item[[key]]))
          }
          NA_character_
        }, character(1))
        return(out)
      }

      # List of character vectors
      if (all(vapply(x, is.character, logical(1)))) {
        return(unlist(x, use.names = FALSE))
      }
    }

    character(0)
  }

  divisions <- extract_divisions(parsed)

  if (is.list(divisions) && !is.character(divisions)) {
    if (all(vapply(divisions, is.list, logical(1)))) {
      divisions <- vapply(divisions, function(item) {
        val <- NULL
        for (key in item_keys) {
          if (!is.null(item[[key]])) {
            val <- item[[key]]
            break
          }
        }
        if (is.null(val)) NA_character_ else as.character(val)
      }, character(1))
    } else {
      divisions <- unlist(divisions, use.names = FALSE)
    }
  }

  divisions <- as.character(divisions)
  divisions <- trimws(divisions)
  divisions <- divisions[nzchar(divisions)]

  if (length(divisions) == 0) {
    parsed_keys <- if (is.list(parsed)) paste(names(parsed), collapse = ", ") else "<non-list>"
    msg <- result$message
    if (is.null(msg)) msg <- ""
    message("Parsed keys: ", parsed_keys)
    message("Raw response preview: ", substr(msg, 1, 400))
  }
  expect_true(length(divisions) > 0, info = "No divisions parsed from JSON output")

  if (!is.null(declared_count) && length(declared_count) == 1 && !is.na(declared_count)) {
    declared_count <- suppressWarnings(as.integer(declared_count))
    if (!is.na(declared_count)) {
      message("Declared count in JSON: ", declared_count)
    }
  }

  sample_size <- min(5L, length(divisions))
  sample_divisions <- divisions[seq_len(sample_size)]
  message("Sample divisions: ", paste(sample_divisions, collapse = ", "))

  count <- length(divisions)
  unique_count <- length(unique(divisions))
  message(sprintf("Division count returned: %d (unique: %d)", count, unique_count))

  expect_equal(count, 63, info = sprintf("Expected 63 divisions, got %d", count))
  expect_equal(unique_count, 63, info = sprintf("Expected 63 unique divisions, got %d", unique_count))
})
