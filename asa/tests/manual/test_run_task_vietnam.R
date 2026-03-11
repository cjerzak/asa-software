#!/usr/bin/env Rscript

# Manual live test: Vietnam post-2025 first-level divisions with Gemini backend.
#
# Run with:
#   Rscript asa/tests/manual/test_run_task_vietnam.R
#
# Prerequisites:
# - conda environment "asa_env"
# - GOOGLE_API_KEY or GEMINI_API_KEY
# - Optional: ASA_TEST_GEMINI_MODEL or ASA_GEMINI_MODEL to override model

loaded_dev <- FALSE
dev_paths <- c(
  ".",
  "..",
  "asa",
  file.path(getwd(), "asa"),
  file.path(dirname(getwd()), "asa")
)
for (p in dev_paths) {
  if (file.exists(file.path(p, "DESCRIPTION"))) {
    tryCatch({
      devtools::load_all(p)
      cat(sprintf("Loaded development version from: %s\n", normalizePath(p)))
      loaded_dev <- TRUE
      break
    }, error = function(e) NULL)
  }
}
if (!loaded_dev) {
  library(asa)
  cat("Loaded installed version of asa\n")
}

library(reticulate)

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || anyNA(x)) y else x

cat("=== Vietnam Live Gemini Test ===\n\n")

tryCatch({
  reticulate::use_condaenv("asa_env", required = TRUE)
  cat("Conda environment: asa_env\n")
}, error = function(e) {
  stop("asa_env conda environment not available: ", conditionMessage(e))
})

if (!reticulate::py_module_available("langchain_google_genai")) {
  stop("Missing Python module 'langchain_google_genai' in asa_env.")
}

api_key <- Sys.getenv("GOOGLE_API_KEY", unset = "")
if (!nzchar(api_key)) {
  api_key <- Sys.getenv("GEMINI_API_KEY", unset = "")
}
if (!nzchar(api_key)) {
  stop("Gemini API key not found. Set GOOGLE_API_KEY or GEMINI_API_KEY.")
}

gemini_model <- Sys.getenv("ASA_TEST_GEMINI_MODEL", unset = "")
if (!nzchar(gemini_model)) {
  gemini_model <- Sys.getenv("ASA_GEMINI_MODEL", unset = "")
}
if (!nzchar(gemini_model)) {
  gemini_model <- asa:::ASA_DEFAULT_GEMINI_MODEL
}
cat(sprintf("Using Gemini model: %s\n\n", gemini_model))

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
  "Use standard English names.",
  "Return only valid JSON with a single top-level object and fields:",
  "country (string), divisions (flat array of strings), count (integer).",
  "Do not include explanatory text or markdown."
)

print_debug <- function(name, value) {
  cat(sprintf("\n%s:\n", name))
  utils::str(value, max.level = 3)
}

extract_divisions <- function(x) {
  division_keys <- c(
    "divisions", "first_level_divisions", "administrative_divisions",
    "province_level", "provinces", "municipalities", "cities",
    "centrally_governed_municipalities", "central_municipalities"
  )
  item_keys <- c("division", "name", "province", "municipality", "city", "unit")

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
    hit <- intersect(names(x), division_keys)
    if (length(hit) > 0) {
      return(unlist(x[hit], use.names = FALSE))
    }
    for (wrapper in c("results", "items", "data")) {
      if (wrapper %in% names(x)) {
        return(extract_divisions(x[[wrapper]]))
      }
    }
    if (all(vapply(x, is.list, logical(1)))) {
      out <- vapply(x, function(item) {
        for (key in item_keys) {
          if (!is.null(item[[key]])) return(as.character(item[[key]]))
        }
        NA_character_
      }, character(1))
      return(out)
    }
    if (all(vapply(x, is.character, logical(1)))) {
      return(unlist(x, use.names = FALSE))
    }
  }

  character(0)
}

agent <- initialize_agent(
  backend = "gemini",
  model = gemini_model,
  verbose = FALSE
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

parsed <- result$parsed
divisions <- extract_divisions(parsed)
divisions <- trimws(as.character(divisions))
divisions <- divisions[nzchar(divisions)]
normalized_divisions <- normalize_division_name(divisions)
normalized_divisions <- normalized_divisions[nzchar(normalized_divisions)]
normalized_unique <- unique(normalized_divisions)
country_name <- if (!is.null(parsed$country)) as.character(parsed$country)[1] else NA_character_
declared_count <- if (!is.null(parsed$count) && length(parsed$count) == 1) suppressWarnings(as.integer(parsed$count)) else NA_integer_

missing_ids <- setdiff(names(expected_lookup), normalized_unique)
unexpected_ids <- setdiff(normalized_unique, names(expected_lookup))

cat("Sample divisions: ", paste(utils::head(divisions, 5L), collapse = ", "), "\n", sep = "")
cat(sprintf("Division count returned: %d (normalized unique: %d)\n", length(divisions), length(normalized_unique)))

errors <- character(0)
if (is.null(parsed)) {
  errors <- c(errors, "Parsed JSON is NULL")
}
if (!isTRUE(result$parsing_status$valid)) {
  errors <- c(errors, sprintf(
    "Parsing status invalid: %s (%s)",
    result$parsing_status$reason %||% "unknown_reason",
    paste(result$parsing_status$missing %||% character(0), collapse = ", ")
  ))
}
if (!identical(normalize_division_name(country_name), "vietnam")) {
  errors <- c(errors, sprintf("Expected country Vietnam, got %s", country_name %||% "NA"))
}
if (!identical(length(divisions), 34L)) {
  errors <- c(errors, sprintf("Expected 34 divisions, got %d", length(divisions)))
}
if (!identical(length(normalized_unique), 34L)) {
  errors <- c(errors, sprintf("Expected 34 unique divisions, got %d", length(normalized_unique)))
}
if (!is.na(declared_count) && !identical(declared_count, 34L)) {
  errors <- c(errors, sprintf("Expected declared count 34, got %d", declared_count))
}
if (!setequal(normalized_unique, names(expected_lookup))) {
  errors <- c(errors, "Returned divisions did not match Vietnam's current 34 first-level units")
}

if (length(errors) > 0) {
  cat("\nFAILURES:\n")
  for (msg in errors) {
    cat(" - ", msg, "\n", sep = "")
  }
  if (length(missing_ids) > 0) {
    cat("Missing divisions: ", paste(unname(expected_lookup[missing_ids]), collapse = ", "), "\n", sep = "")
  }
  if (length(unexpected_ids) > 0) {
    cat("Unexpected divisions: ", paste(sort(unique(divisions[normalized_divisions %in% unexpected_ids])), collapse = ", "), "\n", sep = "")
  }
  print_debug("parsing_status", result$parsing_status)
  print_debug("completion_gate", result$completion_gate)
  print_debug("field_status", result$execution$field_status)
  print_debug("json_repair", result$json_repair)
  print_debug("final_payload", result$final_payload)
  stop("Vietnam live test failed.", call. = FALSE)
}

cat("\nPASS: Vietnam live Gemini test succeeded.\n")
