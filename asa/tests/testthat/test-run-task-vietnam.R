# Integration test: run_task JSON output with Gemini backend

test_that("run_task returns current Vietnam first-level divisions in JSON (Gemini)", {
  asa_test_skip_api_tests()

  # Activate asa_env before checking for Python modules
  tryCatch(
    reticulate::use_condaenv("asa_env", required = TRUE),
    error = function(e) skip("asa_env conda environment not available")
  )

  skip_if(
    !reticulate::py_module_available("langchain_google_genai"),
    "Missing Python module langchain_google_genai"
  )
  asa_test_require_gemini_key()

  agent <- initialize_agent(
    backend = "gemini",
    model = "gemini-3-flash-preview",
    verbose = FALSE
  )

  expected_divisions <- c(
    "An Giang", "Bac Ninh", "Ca Mau", "Can Tho", "Cao Bang", "Da Nang",
    "Dak Lak", "Dien Bien", "Dong Nai", "Dong Thap", "Gia Lai", "Ha Tinh",
    "Hai Phong", "Hanoi", "Ho Chi Minh City", "Hue", "Hung Yen", "Khanh Hoa",
    "Lai Chau", "Lam Dong", "Lang Son", "Lao Cai", "Nghe An", "Ninh Binh",
    "Phu Tho", "Quang Ngai", "Quang Ninh", "Quang Tri", "Son La",
    "Tay Ninh", "Thai Nguyen", "Thanh Hoa", "Tuyen Quang", "Vinh Long"
  )

  normalize_division_name <- function(x) {
    x <- iconv(as.character(x), from = "", to = "ASCII//TRANSLIT")
    x[is.na(x)] <- ""
    x <- tolower(trimws(x))
    x <- gsub("&", " and ", x, fixed = TRUE)
    x <- gsub("[^a-z0-9]+", "", x)
    x <- gsub("province$", "", x)
    x <- gsub("municipality$", "", x)

    alias_map <- c(
      hanoicity = "hanoi",
      haiphongcity = "haiphong",
      huecity = "hue",
      danangcity = "danang",
      canthocity = "cantho",
      hochiminh = "hochiminhcity",
      tphochiminh = "hochiminhcity",
      hcmc = "hochiminhcity"
    )
    hit <- x %in% names(alias_map)
    x[hit] <- unname(alias_map[x[hit]])
    x
  }

  expected_lookup <- setNames(
    expected_divisions,
    normalize_division_name(expected_divisions)
  )

  prompt <- paste(
    "List the current first-level administrative divisions of Vietnam as of March 9, 2026.",
    "Vietnam implemented a provincial administrative reform on July 1, 2025.",
    "Return the 28 provinces and 6 centrally governed cities (total 34), not the pre-reform 63-division structure.",
    "Use standard English names and official government or Wikipedia sources if needed.",
    "Return only valid JSON with a single top-level object and fields:",
    "country (string), divisions (flat array of strings), count (integer).",
    "Example:",
    "{\"country\":\"Vietnam\",\"divisions\":[\"An Giang\", \"Hanoi\", \"Ho Chi Minh City\"],\"count\":34}"
  )

  result <- run_task(
    prompt = prompt,
    output_format = "json",
    expected_fields = c("country", "divisions", "count"),
    expected_schema = list(
      country = "string",
      divisions = list("string"),
      count = "integer"
    ),
    agent = agent,
    allow_read_webpages = TRUE,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_result")
  parsed <- result$parsed
  if (is.null(parsed)) {
    msg <- result$message
    if (is.null(msg)) msg <- ""
    parsed_reason <- if (!is.null(result$parsing_status$reason)) {
      as.character(result$parsing_status$reason)[1]
    } else {
      NA_character_
    }
    completion_reason <- if (!is.null(result$completion_gate$reason)) {
      as.character(result$completion_gate$reason)[1]
    } else {
      NA_character_
    }
    message("Parsing failed. Reason: ", parsed_reason)
    message("Completion gate reason: ", completion_reason)
    message("Raw response preview: ", substr(msg, 1, 400))
  }
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
  expect_true(
    isTRUE(result$parsing_status$valid),
    info = "Expected valid JSON output for current Vietnam divisions"
  )

  completion_reason <- if (!is.null(result$completion_gate$reason)) {
    as.character(result$completion_gate$reason)[1]
  } else {
    NA_character_
  }
  expect_false(
    identical(completion_reason, "no_schema"),
    info = "Expected explicit expected_schema to avoid no_schema completion gate"
  )

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

  # Extract declared count from JSON if available
  count_keys <- c("count", "total", "total_count", "division_count")
  for (ck in count_keys) {
    if (!is.null(parsed[[ck]])) {
      declared_count <- parsed[[ck]]
      break
    }
  }

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

  country_name <- if (!is.null(parsed$country)) as.character(parsed$country)[1] else NA_character_
  expect_equal(
    normalize_division_name(country_name),
    "vietnam",
    info = sprintf("Expected country to be Vietnam, got %s", country_name)
  )

  sample_size <- min(5L, length(divisions))
  sample_divisions <- divisions[seq_len(sample_size)]
  message("Sample divisions: ", paste(sample_divisions, collapse = ", "))

  normalized_divisions <- normalize_division_name(divisions)
  normalized_divisions <- normalized_divisions[nzchar(normalized_divisions)]
  normalized_unique <- unique(normalized_divisions)
  count <- length(divisions)
  unique_count <- length(normalized_unique)
  missing_ids <- setdiff(names(expected_lookup), normalized_unique)
  unexpected_ids <- setdiff(normalized_unique, names(expected_lookup))

  if (length(missing_ids) > 0) {
    message("Missing divisions: ", paste(unname(expected_lookup[missing_ids]), collapse = ", "))
  }
  if (length(unexpected_ids) > 0) {
    message("Unexpected divisions: ", paste(sort(unique(divisions[normalized_divisions %in% unexpected_ids])), collapse = ", "))
  }

  message(sprintf("Division count returned: %d (normalized unique: %d)", count, unique_count))

  expect_equal(count, 34, info = sprintf("Expected 34 divisions, got %d", count))
  expect_equal(unique_count, 34, info = sprintf("Expected 34 unique divisions, got %d", unique_count))
  if (!is.null(declared_count) && !is.na(declared_count)) {
    expect_equal(declared_count, 34L, info = sprintf("Expected declared count 34, got %d", declared_count))
  }
  expect_setequal(
    normalized_unique,
    names(expected_lookup),
    info = "Returned divisions did not match Vietnam's current 34 first-level units"
  )
})
