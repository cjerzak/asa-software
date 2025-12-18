#' Extract Structured Data from Agent Traces
#'
#' Parses raw agent output to extract search snippets, Wikipedia content,
#' URLs, and JSON data. This is the main function for post-processing
#' agent traces.
#'
#' @param raw_output Raw output string from agent invocation (the trace field
#'   from an asa_response object)
#'
#' @return A list with components:
#' \itemize{
#'   \item search_snippets: Character vector of search result content
#'   \item search_urls: Character vector of URLs from search results
#'   \item wikipedia_snippets: Character vector of Wikipedia content
#'   \item json_data: Extracted JSON data as a list (if present)
#' }
#'
#' @examples
#' \dontrun{
#' response <- run_agent("Who is the president of France?", agent)
#' extracted <- extract_agent_results(response$trace)
#' print(extracted$search_snippets)
#' }
#'
#' @export
extract_agent_results <- function(raw_output) {
  if (is.null(raw_output) || raw_output == "") {
    return(list(
      search_snippets = character(0),
      search_urls = character(0),
      wikipedia_snippets = character(0),
      json_data = NULL
    ))
  }

  # Extract components
  json_data <- .extract_json_from_trace(raw_output)

  list(
    search_snippets = extract_search_snippets(raw_output),
    search_urls = extract_urls(raw_output),
    wikipedia_snippets = extract_wikipedia_content(raw_output),
    json_data = json_data
  )
}

#' Extract Search Snippets by Source Number
#'
#' Extracts content from Search tool messages in the agent trace.
#'
#' @param text Raw agent trace text
#'
#' @return Character vector of search snippets, ordered by source number
#'
#' @examples
#' \dontrun{
#' snippets <- extract_search_snippets(response$trace)
#' }
#'
#' @export
extract_search_snippets <- function(text) {
  source_contents <- list()

  # Pattern for ToolMessage with Search
  tool_pattern <- paste0(
    "ToolMessage\\(content=(?:'([^']*(?:\\\\'[^']*)*)'|",
    "\"([^\"]*(?:\\\\\"[^\"]*)*)\"), ",
    "name=(?:'([sS]earch)'|\"([sS]earch)\").*?\\)"
  )

  tool_matches <- gregexpr(tool_pattern, text, perl = TRUE)[[1]]

  if (tool_matches[1] != -1) {
    match_texts <- regmatches(text, list(tool_matches))[[1]]

    for (match_text in match_texts) {
      content_match <- regmatches(match_text, regexec(tool_pattern, match_text, perl = TRUE))[[1]]

      # Content in position 2 or 3
      content <- ifelse(nchar(content_match[2]) > 0, content_match[2], content_match[3])

      if (!is.na(content) && nchar(content) > 0) {
        # Clean escape sequences
        content <- gsub("\\\\n", " ", content)
        content <- gsub("\\\\'", "'", content)
        content <- gsub('\\\\"', '"', content)
        content <- gsub("\\\\\\\\", "\\\\", content)

        # Extract sources with dotall
        source_pattern <- "(?s)__START_OF_SOURCE (\\d+)__ <CONTENT>(.*?)</CONTENT>.*?__END_OF_SOURCE.*?__"
        source_matches <- gregexpr(source_pattern, content, perl = TRUE)[[1]]

        if (source_matches[1] != -1) {
          source_match_texts <- regmatches(content, list(source_matches))[[1]]

          for (src_match in source_match_texts) {
            num_match <- regmatches(src_match, regexec("(?s)__START_OF_SOURCE (\\d+)__.*", src_match, perl = TRUE))[[1]]
            source_num <- as.integer(num_match[2])

            cont_match <- regmatches(src_match, regexec("(?s)<CONTENT>(.*?)</CONTENT>", src_match, perl = TRUE))[[1]]
            src_content <- cont_match[2]

            cleaned_content <- trimws(gsub("\\s+", " ", src_content))

            if (nchar(cleaned_content) > 0) {
              key <- as.character(source_num)
              if (is.null(source_contents[[key]])) {
                source_contents[[key]] <- c()
              }
              source_contents[[key]] <- c(source_contents[[key]], cleaned_content)
            }
          }
        }
      }
    }
  }

  # Create output vector ordered by source number
  if (length(source_contents) == 0) {
    return(character(0))
  }

  max_source <- max(as.integer(names(source_contents)))
  result <- character(max_source)

  for (i in seq_len(max_source)) {
    source_key <- as.character(i)
    if (source_key %in% names(source_contents)) {
      result[i] <- paste(source_contents[[source_key]], collapse = " ")
    } else {
      result[i] <- ""
    }
  }

  result
}

#' Extract Wikipedia Content
#'
#' Extracts content from Wikipedia tool messages in the agent trace.
#'
#' @param text Raw agent trace text
#'
#' @return Character vector of Wikipedia snippets
#'
#' @examples
#' \dontrun{
#' wiki <- extract_wikipedia_content(response$trace)
#' }
#'
#' @export
extract_wikipedia_content <- function(text) {
  snippets <- character(0)

  # Pattern for ToolMessage with Wikipedia
  tool_pattern <- paste0(
    "ToolMessage\\(content=(?:'([^']*(?:\\\\'[^']*)*)'|",
    "\"([^\"]*(?:\\\\\"[^\"]*)*)\"), ",
    "name=(?:'([wW]ikipedia)'|\"([wW]ikipedia)\").*?\\)"
  )

  tool_matches <- gregexpr(tool_pattern, text, perl = TRUE)[[1]]

  if (tool_matches[1] != -1) {
    match_texts <- regmatches(text, list(tool_matches))[[1]]

    for (match_text in match_texts) {
      content_match <- regmatches(match_text, regexec(tool_pattern, match_text, perl = TRUE))[[1]]

      content <- ifelse(nchar(content_match[2]) > 0, content_match[2], content_match[3])

      if (!is.na(content) && nchar(content) > 0) {
        # Clean escape sequences
        content <- gsub("\\\\n", " ", content)
        content <- gsub("\\\\'", "'", content)
        content <- gsub('\\\\"', '"', content)
        content <- gsub("\\\\\\\\", "\\\\", content)

        # Only add valid Wikipedia results
        if ((grepl("Page:", content) || grepl("Summary:", content)) &&
            !grepl("No good Wikipedia Search Result", content, ignore.case = TRUE)) {
          cleaned <- trimws(gsub("\\s+", " ", content))
          if (nchar(cleaned) > 0) {
            snippets <- c(snippets, cleaned)
          }
        }
      }
    }
  }

  snippets
}

#' Extract URLs by Source Number
#'
#' Extracts URLs from Search tool messages in the agent trace.
#'
#' @param text Raw agent trace text
#'
#' @return Character vector of URLs, ordered by source number
#'
#' @examples
#' \dontrun{
#' urls <- extract_urls(response$trace)
#' }
#'
#' @export
extract_urls <- function(text) {
  source_urls <- list()

  # Pattern for ToolMessage with Search
  tool_pattern <- paste0(
    "ToolMessage\\(content=(?:'([^']*(?:\\\\'[^']*)*)'|",
    "\"([^\"]*(?:\\\\\"[^\"]*)*)\"), ",
    "name=(?:'([sS]earch)'|\"([sS]earch)\").*?\\)"
  )

  tool_matches <- gregexpr(tool_pattern, text, perl = TRUE)[[1]]

  if (tool_matches[1] != -1) {
    match_texts <- regmatches(text, list(tool_matches))[[1]]

    for (match_text in match_texts) {
      content_match <- regmatches(match_text, regexec(tool_pattern, match_text, perl = TRUE))[[1]]

      content <- ifelse(nchar(content_match[2]) > 0, content_match[2], content_match[3])

      if (!is.na(content) && nchar(content) > 0) {
        # Clean escape sequences
        content <- gsub("\\\\n", " ", content)
        content <- gsub("\\\\'", "'", content)
        content <- gsub('\\\\"', '"', content)
        content <- gsub("\\\\\\\\", "\\\\", content)

        # Extract sources with URLs
        source_pattern <- "(?s)__START_OF_SOURCE (\\d+)__.*?<URL>(.*?)</URL>.*?__END_OF_SOURCE.*?__"
        source_matches <- gregexpr(source_pattern, content, perl = TRUE)[[1]]

        if (source_matches[1] != -1) {
          source_match_texts <- regmatches(content, list(source_matches))[[1]]

          for (src_match in source_match_texts) {
            num_match <- regmatches(src_match, regexec("(?s)__START_OF_SOURCE (\\d+)__.*", src_match, perl = TRUE))[[1]]
            source_num <- as.integer(num_match[2])

            url_match <- regmatches(src_match, regexec("(?s)<URL>(.*?)</URL>", src_match, perl = TRUE))[[1]]
            url <- trimws(url_match[2])

            if (nchar(url) > 0) {
              key <- as.character(source_num)
              if (is.null(source_urls[[key]])) {
                source_urls[[key]] <- c()
              }
              source_urls[[key]] <- unique(c(source_urls[[key]], url))
            }
          }
        }
      }
    }
  }

  # Create output vector
  if (length(source_urls) == 0) {
    return(character(0))
  }

  max_source <- max(as.integer(names(source_urls)))
  result <- character(max_source)

  for (i in seq_len(max_source)) {
    source_key <- as.character(i)
    if (source_key %in% names(source_urls)) {
      result[i] <- paste(source_urls[[source_key]], collapse = " ")
    } else {
      result[i] <- ""
    }
  }

  result
}

#' Extract JSON from Agent Traces
#'
#' Internal function to extract JSON data from raw agent traces.
#'
#' @param text Raw trace text
#' @return Parsed JSON data as a list, or NULL if no JSON found
#' @keywords internal
.extract_json_from_trace <- function(text) {
  tryCatch({
    # Extract JSON from AIMessage
    json_patterns <- c(
      "AIMessage\\(content='(\\{[^']*(?:\\\\'[^']*)*\\})'",
      "AIMessage\\(content=\"(\\{[^\"]*(?:\\\\\"[^\"]*)*\\})\"",
      "AIMessage\\(content=['\"]([\\s\\S]*?)['\"], additional_kwargs",
      "(\\{[^{}]*\\})"
    )

    for (pattern in json_patterns) {
      matches <- regmatches(text, gregexpr(pattern, text, perl = TRUE))[[1]]

      if (length(matches) > 0) {
        # Try matches from last to first (most recent response)
        for (i in rev(seq_along(matches))) {
          json_str <- gsub(pattern, "\\1", matches[i], perl = TRUE)

          # Clean escape sequences
          json_str <- gsub("\\\\n", "\n", json_str)
          json_str <- gsub('\\\\"', '"', json_str)
          json_str <- gsub("\\\\'", "'", json_str)
          json_str <- gsub("\\\\\\\\", "\\\\", json_str)

          # Try to parse as JSON
          tryCatch({
            data <- jsonlite::fromJSON(json_str)
            if (is.list(data) && length(data) > 0) {
              return(data)
            }
          }, error = function(e) {
            # Continue to next match
          })
        }
      }
    }

    NULL
  }, error = function(e) {
    warning(paste("Error extracting JSON:", e$message), call. = FALSE)
    NULL
  })
}

#' Process Multiple Agent Outputs
#'
#' Processes a data frame of raw agent outputs, extracting structured data.
#'
#' @param df Data frame with a 'raw_output' column containing agent traces
#' @param parallel Use parallel processing
#' @param workers Number of workers
#'
#' @return The input data frame with additional extracted columns:
#'   search_count, wiki_count, and any JSON fields found
#'
#' @export
process_outputs <- function(df, parallel = FALSE, workers = 10L) {
  if (!"raw_output" %in% names(df)) {
    stop("Data frame must have a 'raw_output' column", call. = FALSE)
  }

  # Process function
  process_one <- function(raw_out) {
    result <- extract_agent_results(raw_out)
    list(
      search_count = length(result$search_snippets),
      wiki_count = length(result$wikipedia_snippets),
      json_data = result$json_data
    )
  }

  # Process all rows
  if (parallel && requireNamespace("future.apply", quietly = TRUE)) {
    future.apply::plan(future.apply::multisession(workers = workers))
    on.exit(future.apply::plan(future.apply::sequential), add = TRUE)

    results <- future.apply::future_lapply(df$raw_output, process_one)
  } else {
    results <- lapply(df$raw_output, process_one)
  }

  # Add basic columns
  df$search_count <- vapply(results, function(r) r$search_count, integer(1))
  df$wiki_count <- vapply(results, function(r) r$wiki_count, integer(1))

  # Collect JSON field names from all results
  all_json_fields <- unique(unlist(lapply(results, function(r) {
    if (!is.null(r$json_data) && is.list(r$json_data)) {
      names(r$json_data)
    } else {
      character(0)
    }
  })))

  # Add JSON fields as columns
  for (field in all_json_fields) {
    df[[field]] <- vapply(results, function(r) {
      if (!is.null(r$json_data) && field %in% names(r$json_data)) {
        val <- r$json_data[[field]]
        if (length(val) == 1) as.character(val) else NA_character_
      } else {
        NA_character_
      }
    }, character(1))
  }

  df
}
