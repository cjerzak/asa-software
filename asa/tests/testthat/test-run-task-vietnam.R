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
    "Use search and Wikipedia if needed.",
    "Return JSON only with fields:",
    "country (string), divisions (array of strings), count (integer)."
  )

  result <- run_task(
    prompt = prompt,
    output_format = "json",
    agent = agent,
    allow_read_webpages = TRUE,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")
  parsed <- result$parsed
  expect_false(is.null(parsed), info = "Parsed JSON is NULL")

  divisions <- NULL
  declared_count <- NULL

  if (is.list(parsed) && !is.data.frame(parsed) && "divisions" %in% names(parsed)) {
    divisions <- parsed$divisions
    if ("count" %in% names(parsed)) declared_count <- parsed$count
  } else if (is.data.frame(parsed)) {
    if ("division" %in% names(parsed)) {
      divisions <- parsed$division
    } else if ("name" %in% names(parsed)) {
      divisions <- parsed$name
    }
  } else if (is.character(parsed)) {
    divisions <- parsed
  }

  if (is.list(divisions) && !is.character(divisions)) {
    if (all(vapply(divisions, is.list, logical(1)))) {
      divisions <- vapply(divisions, function(item) {
        val <- NULL
        if (!is.null(item$division)) val <- item$division
        if (is.null(val) && !is.null(item$name)) val <- item$name
        if (is.null(val)) NA_character_ else as.character(val)
      }, character(1))
    } else {
      divisions <- unlist(divisions, use.names = FALSE)
    }
  }

  divisions <- as.character(divisions)
  divisions <- trimws(divisions)
  divisions <- divisions[nzchar(divisions)]

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
