#' Parse JSON from Text Response
#'
#' Attempts to parse JSON from a text string. First tries direct parsing,
#' then scans for embedded JSON objects/arrays using brace counting.
#'
#' @param response_text Character string potentially containing JSON
#'
#' @return Parsed R object, or NULL if no valid JSON found
#'
#' @keywords internal
.parse_json_response <- function(response_text) {
  if (is.na(response_text) || response_text == "") {
    return(NULL)
  }

  # Try to parse full response first
  parsed <- .try_or(jsonlite::fromJSON(response_text))
  if (!is.null(parsed)) {
    return(parsed)
  }

  starts <- gregexpr("[\\{\\[]", response_text)[[1]]
  if (starts[1] == -1) {
    return(NULL)
  }

  for (start in starts) {
    json_str <- .extract_json_object(response_text, start = start)
    if (is.null(json_str)) {
      next
    }
    parsed <- .try_or(jsonlite::fromJSON(json_str))
    if (!is.null(parsed)) {
      return(parsed)
    }
  }

  NULL
}

#' Extract JSON Object from Text
#'
#' Uses brace/bracket counting to extract a complete JSON object or array
#' from a text string starting at a given position.
#'
#' @param text Text containing JSON
#' @param start Optional 1-based start index for extraction
#'
#' @return JSON string, or NULL if no complete object found
#'
#' @keywords internal
.extract_json_object <- function(text, start = NULL) {
  # Try to find JSON object or array in text
  if (is.null(start)) {
    start_obj <- regexpr("\\{", text)
    start_arr <- regexpr("\\[", text)

    starts <- c(start_obj, start_arr)
    starts <- starts[starts > 0]
    if (length(starts) == 0) return(NULL)

    start <- min(starts)
  }
  if (start <= 0) {
    return(NULL)
  }

  # Count braces/brackets to find matching close
  depth <- 0
  in_string <- FALSE
  escape_next <- FALSE

  chars <- strsplit(text, "")[[1]]
  end <- -1

  for (i in start:length(chars)) {
    char <- chars[i]

    if (escape_next) {
      escape_next <- FALSE
      next
    }

    if (char == "\\") {
      escape_next <- TRUE
      next
    }

    if (char == '"' && !escape_next) {
      in_string <- !in_string
      next
    }

    if (!in_string) {
      if (char == "{" || char == "[") depth <- depth + 1
      if (char == "}" || char == "]") {
        depth <- depth - 1
        if (depth == 0) {
          end <- i
          break
        }
      }
    }
  }

  if (end > start) {
    json_str <- paste(chars[start:end], collapse = "")
    return(json_str)
  }

  NULL
}

