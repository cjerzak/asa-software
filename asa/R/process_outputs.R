#' Extract Structured Data from Agent Traces
#'
#' Parses raw agent output to extract search snippets, Wikipedia content,
#' URLs, JSON data, and search tier information. This is the main function
#' for post-processing agent traces.
#'
#' @param raw_output Raw output string from agent invocation (the trace field
#'   from an asa_response object), or a structured JSON trace (asa_trace_v1)
#'   from \code{asa_result$trace_json}
#'
#' @return A list with components:
#' \itemize{
#'   \item search_snippets: Character vector of search result content
#'   \item search_urls: Character vector of URLs from search results
#'   \item wikipedia_snippets: Character vector of Wikipedia content
#'   \item json_data: Extracted JSON data as a list (if present)
#'   \item search_tiers: Character vector of unique search tiers used
#'     (e.g., "primp", "selenium", "ddgs", "requests")
#' }
#'
#' @examples
#' \dontrun{
#' response <- run_agent("Who is the president of France?", agent)
#' extracted <- extract_agent_results(response$trace)
#' print(extracted$search_snippets)
#' print(extracted$search_tiers)  # Shows which search tier was used
#' }
#'
#' @export
extract_agent_results <- function(raw_output) {
  if (is.null(raw_output) || raw_output == "") {
    return(list(
      search_snippets = character(0),
      search_urls = character(0),
      wikipedia_snippets = character(0),
      json_data = NULL,
      search_tiers = character(0)
    ))
  }

  # Extract components
  json_data <- .extract_json_from_trace(raw_output)

  list(
    search_snippets = extract_search_snippets(raw_output),
    search_urls = extract_urls(raw_output),
    wikipedia_snippets = extract_wikipedia_content(raw_output),
    json_data = json_data,
    search_tiers = extract_search_tiers(raw_output)
  )
}

#' Parse Structured Trace JSON (asa_trace_v1)
#' @keywords internal
.parse_trace_json_messages <- function(text) {
  if (!is.character(text) || length(text) != 1 || is.na(text) || !nzchar(text)) {
    return(NULL)
  }
  txt <- trimws(text)
  if (!startsWith(txt, "{") || !grepl("\"format\"\\s*:\\s*\"asa_trace_v1\"", txt)) {
    return(NULL)
  }
  parsed <- .try_or(jsonlite::fromJSON(txt, simplifyVector = FALSE))
  if (is.null(parsed) || !identical(parsed$format, "asa_trace_v1")) {
    return(NULL)
  }
  if (!is.list(parsed$messages)) {
    return(NULL)
  }
  parsed$messages
}

#' Extract tool message contents from either text trace or asa_trace_v1 JSON.
#' @keywords internal
.extract_tool_contents <- function(text, tool_name) {
  msgs <- .parse_trace_json_messages(text)
  if (is.null(msgs)) {
    return(NULL)
  }
  tool_name <- tolower(tool_name)
  contents <- character(0)
  for (m in msgs) {
    if (!is.list(m)) next
    m_type <- tolower(as.character(m$message_type %||% ""))
    m_name <- tolower(as.character(m$name %||% ""))
    if (!(m_type %in% c("toolmessage", "tool"))) next
    if (m_name != tool_name) next
    contents <- c(contents, as.character(m$content %||% ""))
  }
  contents
}

#' Decode legacy ToolMessage content (Python repr-like escapes)
#' @keywords internal
.decode_legacy_toolmessage_content <- function(x) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    return("")
  }

  decoded <- .decode_trace_escaped_string(x)
  if (is.character(decoded) && length(decoded) == 1L && !is.null(decoded)) {
    return(decoded)
  }

  # Backward-compatible fallback: manual unescaping for common sequences.
  out <- x
  out <- gsub("\\\\n", " ", out)
  out <- gsub("\\\\'", "'", out)
  out <- gsub('\\\\"', '"', out)
  out <- gsub("\\\\\\\\", "\\\\", out)
  out
}

#' Extract tool contents from legacy (non-JSON) ToolMessage(...) traces
#' @keywords internal
.extract_tool_contents_legacy <- function(text, tool_name) {
  if (!is.character(text) || length(text) != 1 || is.na(text) || !nzchar(text)) {
    return(character(0))
  }

  tool_name <- as.character(tool_name)
  tool_name_esc <- gsub("([\\\\.^$|()\\[\\]{}*+?])", "\\\\\\1", tool_name, perl = TRUE)

  tool_pattern <- paste0(
    "ToolMessage\\(content=(?:'([^']*(?:\\\\'[^']*)*)'|",
    "\"([^\"]*(?:\\\\\"[^\"]*)*)\"), ",
    "name=(?:'(?i:", tool_name_esc, ")'|\"(?i:", tool_name_esc, ")\").*?\\)"
  )

  tool_matches <- gregexpr(tool_pattern, text, perl = TRUE)[[1]]
  if (tool_matches[1] == -1) {
    return(character(0))
  }

  match_texts <- regmatches(text, list(tool_matches))[[1]]
  contents <- character(0)

  for (match_text in match_texts) {
    content_match <- regmatches(match_text, regexec(tool_pattern, match_text, perl = TRUE))[[1]]

    # Content is in group 2 or 3 (single-quoted vs double-quoted).
    content <- ifelse(nchar(content_match[2]) > 0, content_match[2], content_match[3])
    if (is.na(content) || !nzchar(content)) next

    contents <- c(contents, .decode_legacy_toolmessage_content(content))
  }

  contents
}

#' Extract tool contents from either asa_trace_v1 JSON or legacy ToolMessage traces.
#' @keywords internal
.extract_tool_contents_any <- function(text, tool_name) {
  tool_contents <- .extract_tool_contents(text, tool_name)
  if (!is.null(tool_contents)) {
    return(tool_contents)
  }
  .extract_tool_contents_legacy(text, tool_name)
}

#' Select sources from an ordered vector (1-indexed)
#' @keywords internal
.select_sources <- function(result, source = NULL) {
  if (is.null(source)) {
    return(result)
  }

  source <- as.integer(source)
  source <- source[!is.na(source) & source > 0]
  if (length(source) == 0) {
    return(character(0))
  }

  result[source]
}

#' Extract per-source search fields (content or URL) from Search tool traces
#' @keywords internal
.extract_search_sources <- function(text, field = c("content", "url"), source = NULL) {
  field <- match.arg(field)

  tool_contents <- .extract_tool_contents_any(text, "search")
  if (length(tool_contents) == 0) {
    return(character(0))
  }

  source_values <- list()

  source_pattern <- if (field == "content") {
    "(?s)__START_OF_SOURCE (\\d+)__.*?<CONTENT>(.*?)</CONTENT>.*?__END_OF_SOURCE.*?__"
  } else {
    "(?s)__START_OF_SOURCE (\\d+)__.*?<URL>(.*?)</URL>.*?__END_OF_SOURCE.*?__"
  }

  for (content in tool_contents) {
    if (is.na(content) || !nzchar(content)) next

    source_matches <- gregexpr(source_pattern, content, perl = TRUE)[[1]]
    if (source_matches[1] == -1) next

    source_match_texts <- regmatches(content, list(source_matches))[[1]]
    for (src_match in source_match_texts) {
      m <- regmatches(src_match, regexec(source_pattern, src_match, perl = TRUE))[[1]]
      if (length(m) < 3) next

      source_num <- suppressWarnings(as.integer(m[2]))
      if (is.na(source_num) || source_num <= 0) next

      value <- m[3]
      if (field == "content") {
        value <- trimws(gsub("\\s+", " ", value))
      } else {
        value <- trimws(value)
      }

      if (!nzchar(value)) next

      key <- as.character(source_num)
      if (is.null(source_values[[key]])) {
        source_values[[key]] <- character(0)
      }

      if (field == "url") {
        source_values[[key]] <- unique(c(source_values[[key]], value))
      } else {
        source_values[[key]] <- c(source_values[[key]], value)
      }
    }
  }

  if (length(source_values) == 0) {
    return(character(0))
  }

  max_source <- max(as.integer(names(source_values)))
  result <- character(max_source)
  for (i in seq_len(max_source)) {
    source_key <- as.character(i)
    if (source_key %in% names(source_values)) {
      result[i] <- paste(source_values[[source_key]], collapse = " ")
    } else {
      result[i] <- ""
    }
  }

  .select_sources(result, source)
}

#' Extract Search Snippets by Source Number
#'
#' Extracts content from Search tool messages in the agent trace.
#'
#' @param text Raw agent trace text, or a structured JSON trace (asa_trace_v1)
#' @param source Optional integer vector of source numbers to return (1-indexed).
#'   If NULL (default), returns a character vector for all sources encountered
#'   (with empty strings for missing indices).
#'
#' @return Character vector of search snippets, ordered by source number
#'
#' @examples
#' \dontrun{
#' snippets <- extract_search_snippets(response$trace)
#' }
#'
#' @export
extract_search_snippets <- function(text, source = NULL) {
  .extract_search_sources(text, field = "content", source = source)
}

#' Extract Wikipedia Content
#'
#' Extracts content from Wikipedia tool messages in the agent trace.
#'
#' @param text Raw agent trace text, or a structured JSON trace (asa_trace_v1)
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

  tool_contents <- .extract_tool_contents_any(text, "wikipedia")
  for (content in tool_contents) {
    if (is.na(content) || !nzchar(content)) next
    if ((grepl("Page:", content) || grepl("Summary:", content)) &&
        !grepl("No good Wikipedia Search Result", content, ignore.case = TRUE)) {
      cleaned <- trimws(gsub("\\s+", " ", content))
      if (nchar(cleaned) > 0) {
        snippets <- c(snippets, cleaned)
      }
    }
  }

  snippets
}

#' Extract URLs by Source Number
#'
#' Extracts URLs from Search tool messages in the agent trace.
#'
#' @param text Raw agent trace text, or a structured JSON trace (asa_trace_v1)
#' @param source Optional integer vector of source numbers to return (1-indexed).
#'   If NULL (default), returns a character vector for all sources encountered
#'   (with empty strings for missing indices).
#'
#' @return Character vector of URLs, ordered by source number
#'
#' @examples
#' \dontrun{
#' urls <- extract_urls(response$trace)
#' }
#'
#' @export
extract_urls <- function(text, source = NULL) {
  .extract_search_sources(text, field = "url", source = source)
}

#' Extract Search Tier Information
#'
#' Extracts which search tier was used from the agent trace.
#' The search module uses a multi-tier fallback system:
#' \itemize{
#'   \item primp: Fast HTTP client with browser impersonation (Tier 0)
#'   \item selenium: Headless browser for JS-rendered content (Tier 1)
#'   \item ddgs: Standard DDGS Python library (Tier 2)
#'   \item requests: Raw POST to DuckDuckGo HTML endpoint (Tier 3)
#' }
#'
#' @param text Raw agent trace text, or a structured JSON trace (asa_trace_v1)
#'
#' @return Character vector of unique tier names encountered
#'   (e.g., "primp", "selenium", "ddgs", "requests")
#'
#' @examples
#' \dontrun{
#' tiers <- extract_search_tiers(response$trace)
#' print(tiers)  # e.g., "primp"
#' }
#'
#' @export
extract_search_tiers <- function(text) {
  if (is.null(text) || text == "") {
    return(character(0))
  }

  tool_contents <- .extract_tool_contents(text, "search")
  if (!is.null(tool_contents) && length(tool_contents) > 0) {
    tier_pattern <- "['\"]_tier['\"]:\\s*['\"]?(primp|selenium|ddgs|requests)['\"]?"
    tiers <- character(0)
    for (content in tool_contents) {
      matches <- gregexpr(tier_pattern, content, perl = TRUE)[[1]]
      if (matches[1] == -1) next
      match_texts <- regmatches(content, list(matches))[[1]]
      tiers <- c(tiers, gsub(tier_pattern, "\\1", match_texts, perl = TRUE))
    }
    tiers <- unique(tiers)
    return(tiers)
  }

  # Match '_tier': 'primp' patterns in Python dict repr
  # Handle both single and double quotes
  tier_pattern <- "['\"]_tier['\"]:\\s*['\"]?(primp|selenium|ddgs|requests)['\"]?"
  matches <- gregexpr(tier_pattern, text, perl = TRUE)[[1]]

  if (matches[1] == -1) {
    return(character(0))
  }

  match_texts <- regmatches(text, list(matches))[[1]]
  tiers <- gsub(tier_pattern, "\\1", match_texts, perl = TRUE)
  unique(tiers)
}

#' Make a string safe to wrap as a JSON string literal
#'
#' Ensures every double quote is escaped (i.e., preceded by an odd number of
#' backslashes), so `jsonlite::fromJSON(paste0('"', x, '"'))` can be used to
#' decode one layer of trace-escaped content.
#'
#' @param x Character scalar
#' @keywords internal
.make_json_string_wrapper_safe <- function(x) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    return("")
  }

  chars <- strsplit(x, "", fixed = TRUE)[[1]]
  out <- character(0)

  for (ch in chars) {
    if (ch == '"') {
      # Count consecutive backslashes immediately before this quote in the output
      # built so far. If the count is even, the quote would terminate a JSON
      # string literal; insert one more backslash to escape it.
      bs_count <- 0L
      j <- length(out)
      while (j >= 1L && out[j] == "\\") {
        bs_count <- bs_count + 1L
        j <- j - 1L
      }
      if (bs_count %% 2L == 0L) {
        out <- c(out, "\\")
      }
      out <- c(out, ch)
    } else {
      out <- c(out, ch)
    }
  }

  paste0(out, collapse = "")
}

#' Decode one layer of trace-escaped content into a string
#'
#' Many raw traces contain AI message content escaped as a Python-esque string
#' literal (e.g. `\"`, `\\n`). This function attempts to decode one layer of
#' escaping using JSON string decoding, while normalizing `\\'` (a Python escape
#' that is not valid JSON) back to `'`.
#'
#' @param x Character scalar
#' @return Character scalar, or NULL
#' @keywords internal
.decode_trace_escaped_string <- function(x) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    return(NULL)
  }

  # JSON doesn't support `\'`, but Python repr does (when single-quoting). If it
  # exists here, it's not valid JSON text yet; normalize before decoding.
  x <- gsub("\\'", "'", x, fixed = TRUE)

  safe <- .make_json_string_wrapper_safe(x)
  wrapped <- paste0('"', safe, '"')
  decoded <- .try_or(jsonlite::fromJSON(wrapped))

  if (is.character(decoded) && length(decoded) == 1L) {
    decoded
  } else {
    NULL
  }
}

#' Try to parse a JSON candidate extracted from a raw trace
#'
#' @param x Character scalar candidate (may be escaped)
#' @param max_decode_layers Maximum number of decode layers to attempt
#' @return Parsed JSON (list/data.frame), or NULL
#' @keywords internal
.parse_trace_json_candidate <- function(x, max_decode_layers = 3L) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    return(NULL)
  }

  current <- x

  unescape_quotes_only <- function(txt) {
    # Convert `\"` -> `"` without decoding other escapes (e.g. preserve `\\`).
    gsub("\\\"", "\"", txt, fixed = TRUE)
  }

  parse_one <- function(txt) {
    # Prefer the shared robust extractor (handles JSON embedded in text).
    parsed <- .try_or(.parse_json_response(txt))
    if (is.list(parsed) && length(parsed) > 0) {
      return(parsed)
    }
    NULL
  }

  parsed <- parse_one(current)
  if (!is.null(parsed)) {
    return(parsed)
  }

  max_decode_layers <- as.integer(max_decode_layers)
  if (is.na(max_decode_layers) || max_decode_layers < 1L) {
    return(NULL)
  }

  for (i in seq_len(max_decode_layers)) {
    decoded <- .decode_trace_escaped_string(current)
    if (is.null(decoded) || identical(decoded, current)) {
      break
    }
    current <- decoded

    parsed <- parse_one(current)
    if (!is.null(parsed)) {
      return(parsed)
    }

    parsed <- parse_one(unescape_quotes_only(current))
    if (!is.null(parsed)) {
      return(parsed)
    }
  }

  # Last resort: if decoding didn't help, try unescaping quotes only once on the
  # original string (useful when backslashes are already JSON-level).
  parsed <- parse_one(unescape_quotes_only(x))
  if (!is.null(parsed)) {
    return(parsed)
  }

  NULL
}

#' Count the ratio of Unknown/empty fields in a parsed JSON list
#' @param parsed A named list from JSON extraction
#' @return Numeric ratio between 0 and 1
#' @keywords internal
.count_unknown_ratio <- function(parsed) {
  if (!is.list(parsed) || length(parsed) == 0) return(1.0)
  total <- 0L
  unknown <- 0L
  for (nm in names(parsed)) {
    # Skip source/meta fields for this count
    if (endsWith(nm, "_source") || nm %in% c("confidence", "justification")) next
    total <- total + 1L
    val <- parsed[[nm]]
    if (is.null(val) || (is.character(val) && (tolower(val) == "unknown" || val == ""))) {
      unknown <- unknown + 1L
    }
  }
  if (total == 0L) return(1.0)
  unknown / total
}

#' Mine tool message text for biographical field values
#'
#' Scans all ToolMessage contents in a trace for biographical data
#' (birth year, prior occupation, class background) using regex patterns.
#' Used as a fallback when the final AIMessage returns mostly Unknown fields.
#'
#' @param text Raw trace text
#' @return Named list of extracted field values, or NULL if nothing found
#' @keywords internal
.mine_trace_for_fields <- function(text) {
  if (!is.character(text) || length(text) != 1 || is.na(text) || !nzchar(text)) {
    return(NULL)
  }

  # Extract all tool message contents
  tool_contents <- character(0)

  # Try structured trace (asa_trace_v1)
  msgs <- .parse_trace_json_messages(text)
  if (!is.null(msgs)) {
    for (m in msgs) {
      if (!is.list(m)) next
      m_type <- tolower(as.character(m$message_type %||% ""))
      if (m_type %in% c("toolmessage", "tool")) {
        content <- as.character(m$content %||% "")
        if (nzchar(content)) tool_contents <- c(tool_contents, content)
      }
    }
  }

  # Also try legacy ToolMessage format
  if (length(tool_contents) == 0) {
    # Match ToolMessage(content='...' patterns
    legacy_pattern <- "ToolMessage\\(content='([^']*(?:\\\\'[^']*)*)', name="
    matches <- regmatches(text, gregexpr(legacy_pattern, text, perl = TRUE))[[1]]
    for (match_text in matches) {
      content <- gsub(legacy_pattern, "\\1", match_text, perl = TRUE)
      if (nzchar(content)) tool_contents <- c(tool_contents, content)
    }
  }

  # Also extract AI message contents (they may contain synthesized info)
  ai_contents <- character(0)
  ai_pattern <- "AIMessage\\(content='([^']*(?:\\\\'[^']*)*)', additional_kwargs"
  ai_matches <- regmatches(text, gregexpr(ai_pattern, text, perl = TRUE))[[1]]
  for (match_text in ai_matches) {
    content <- gsub(ai_pattern, "\\1", match_text, perl = TRUE)
    if (nzchar(content) && nchar(content) > 20) ai_contents <- c(ai_contents, content)
  }

  if (length(tool_contents) == 0 && length(ai_contents) == 0) return(NULL)

  combined_text <- paste(c(tool_contents, ai_contents), collapse = "\n\n")

  # Extract URLs for source fields
  url_pattern <- "https?://[^\\s\"'<>\\]\\)]{10,200}"
  all_urls <- unique(regmatches(combined_text, gregexpr(url_pattern, combined_text, perl = TRUE))[[1]])
  best_url <- if (length(all_urls) > 0) all_urls[1] else NULL
  # Prefer government/official URLs
  for (u in all_urls) {
    if (grepl("gob\\.bo|vicepresidencia|congreso|parlamento|asamblea", u, ignore.case = TRUE)) {
      best_url <- u
      break
    }
  }

  result <- list()

  # --- Extract target individual name from HumanMessage prompt ---
  # We need this to avoid attributing other people's birth dates to the target
  target_name <- NULL
  name_m <- regmatches(text, regexec(
    "(?:TARGET INDIVIDUAL|Name):\\s*[-]?\\s*([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)+)",
    text, perl = TRUE))[[1]]
  if (length(name_m) >= 2) {
    target_name <- tolower(name_m[2])
  }

  # --- Birth year ---
  # Only extract birth years that appear in context mentioning the target individual.
  # This avoids wrongly attributing other people's birth dates (e.g., suplentes).
  # We search each tool message individually and check if target name appears nearby.
  all_individual_texts <- c(tool_contents, ai_contents)
  for (txt_chunk in all_individual_texts) {
    # Skip chunks that don't mention our target
    if (!is.null(target_name)) {
      name_parts <- strsplit(target_name, "\\s+")[[1]]
      # Require at least surname match
      surname_match <- any(sapply(name_parts, function(np) {
        grepl(np, txt_chunk, ignore.case = TRUE)
      }))
      if (!surname_match) next
    }

    # Check if this chunk has a birth date pattern in direct association with the name
    birth_patterns <- c(
      "Fecha de nacimiento:\\s*(?:\\n\\s*)?(\\d{4})",
      "(?:naci[o\u00f3]|born|nacimiento)[^\\n]{0,30}?(\\d{4})",
      "(?:nacida? en|born in|born on)[^.]{0,50}?(\\d{4})"
    )
    for (pat in birth_patterns) {
      m <- regmatches(txt_chunk, regexec(pat, txt_chunk, perl = TRUE, ignore.case = TRUE))[[1]]
      if (length(m) >= 2) {
        year <- suppressWarnings(as.integer(m[2]))
        if (!is.na(year) && year >= 1900 && year <= 2010) {
          # Extra check: make sure this birth date isn't for a "Suplente" or other person
          # by checking the context around the date
          date_pos <- regexpr(pat, txt_chunk, perl = TRUE, ignore.case = TRUE)
          if (date_pos > 0) {
            context_before <- substr(txt_chunk, max(1, date_pos - 200), date_pos)
            # If "Suplente de:" appears before the date and target name appears after,
            # this date belongs to the suplente, not the target
            if (grepl("Suplente", context_before, ignore.case = TRUE) &&
                !grepl(paste(name_parts, collapse = ".*"), context_before, ignore.case = TRUE)) {
              next
            }
          }
          result$birth_year <- year
          result$birth_year_source <- best_url
          break
        }
      }
    }
    if (!is.null(result$birth_year)) break
  }

  # --- Prior occupation ---
  # Search tool messages that mention the target person for occupation info.
  # This avoids extracting occupation/activities for suplentes or other people.
  occ_leader_patterns <- c(
    "(?:Presidenta?|Presidente?)\\s+(?:de\\s+)?(?:la?\\s+)?(?:Organizaci[o\u00f3]n|Sub[- ]?[Cc]entral|Central|Federaci[o\u00f3]n)[^.;\\n]{3,100}",
    "(?:dirigente|l[i\u00ed]der(?:esa)?|capitana?)\\s+(?:de\\s+)?(?:la?\\s+)?[^.;\\n]{3,100}",
    "representante\\s+ind[i\u00ed]gena[^.;\\n]{0,80}",
    "ind[i\u00ed]gena\\s+que\\s+proviene\\s+(?:del?|de\\s+la?)\\s+[^.;]{3,60}"
  )
  occ_generic_patterns <- c(
    "(?:ocupaci[o\u00f3]n|profesi[o\u00f3]n|cargo|occupation)[:\\s]+([^.;\\n]{5,120})",
    "Actividades realizadas\\s*(?:\\n\\s*)?\\.?\\s*([^\\n]{5,200})"
  )

  # First pass: search in chunks that directly mention target name
  for (txt_chunk in all_individual_texts) {
    # Check if chunk mentions target
    chunk_mentions_target <- FALSE
    if (!is.null(target_name)) {
      name_parts_occ <- strsplit(target_name, "\\s+")[[1]]
      chunk_mentions_target <- any(sapply(name_parts_occ, function(np) {
        grepl(np, txt_chunk, ignore.case = TRUE)
      }))
    }
    if (!chunk_mentions_target) next

    # But skip if this chunk's data is about someone else (suplente profiles)
    # If chunk says "Suplente de: TARGET" then the activities above belong to the suplente
    if (grepl("Suplente de:", txt_chunk, ignore.case = TRUE)) {
      # This is the suplente's page; the activities listed are for the suplente
      next
    }

    # Extract URL from this specific chunk for more accurate source attribution
    chunk_urls <- regmatches(txt_chunk, gregexpr(url_pattern, txt_chunk, perl = TRUE))[[1]]
    chunk_url <- if (length(chunk_urls) > 0) chunk_urls[1] else best_url
    # Prefer government URLs within the chunk
    for (cu in chunk_urls) {
      if (grepl("gob\\.bo|vicepresidencia|congreso|parlamento", cu, ignore.case = TRUE)) {
        chunk_url <- cu
        break
      }
    }

    # Try leader patterns first
    for (pat in occ_leader_patterns) {
      m <- regmatches(txt_chunk, regexec(pat, txt_chunk, perl = TRUE, ignore.case = TRUE))[[1]]
      if (length(m) >= 1 && nchar(m[1]) > 5) {
        occ_val <- trimws(m[1])
        occ_val <- gsub("[,.]$", "", occ_val)
        # Truncate at clause boundary to keep it concise
        if (nchar(occ_val) > 60) {
          # Cut at conjunction boundaries (y + verb, pero, aunque)
          # Avoid cutting at "que" since it's common in relative clauses
          short <- sub("\\s+(?:y\\s+(?:desecha|niega|rechaz|dijo|dice|lucha|fue)|pero\\s+|aunque\\s+|donde\\s+).*$",
                       "", occ_val, perl = TRUE, ignore.case = TRUE)
          if (nchar(short) >= 15 && nchar(short) < nchar(occ_val)) {
            occ_val <- short
          } else {
            # Fallback: cut at " y " only if result is long enough
            short2 <- sub("\\s+y\\s+.*$", "", occ_val, perl = TRUE)
            if (nchar(short2) >= 15 && nchar(short2) < nchar(occ_val)) {
              occ_val <- short2
            }
          }
        }
        if (nchar(occ_val) > 3 && nchar(occ_val) < 200) {
          result$prior_occupation <- occ_val
          result$prior_occupation_source <- chunk_url
          break
        }
      }
    }
    if (!is.null(result$prior_occupation)) break

    # Try generic patterns
    for (pat in occ_generic_patterns) {
      m <- regmatches(txt_chunk, regexec(pat, txt_chunk, perl = TRUE, ignore.case = TRUE))[[1]]
      if (length(m) >= 2 && nchar(m[2]) > 3) {
        occ_val <- trimws(m[2])
        occ_val <- gsub("[,.]$", "", occ_val)
        if (nchar(occ_val) > 3 && nchar(occ_val) < 150) {
          result$prior_occupation <- occ_val
          result$prior_occupation_source <- best_url
          break
        }
      }
    }
    if (!is.null(result$prior_occupation)) break
  }

  # --- Class background (derive from occupation) ---
  if (!is.null(result$prior_occupation)) {
    occ_lower <- tolower(result$prior_occupation)
    working_class_keywords <- c("dirigente", "lider", "l\u00edder", "campesina", "campesino",
                                 "agricult", "trabajador", "obrer", "indigenous", "leader",
                                 "farmer", "ind\u00edgena", "indigena", "capitana", "capitan",
                                 "sub central", "subcentral", "organizaci", "federaci")
    middle_class_keywords <- c("abogad", "ingenier", "profesor", "maestr", "doctor",
                                "lawyer", "teacher", "engineer", "licenciad", "docente",
                                "civil servant", "funcionario")
    upper_class_keywords <- c("empresari", "ejecutiv", "hacendad", "terrateniente",
                               "executive", "business owner", "aristocra")

    if (any(sapply(working_class_keywords, function(w) grepl(w, occ_lower, fixed = TRUE)))) {
      result$class_background <- "Working class"
    } else if (any(sapply(middle_class_keywords, function(w) grepl(w, occ_lower, fixed = TRUE)))) {
      result$class_background <- "Middle class/professional"
    } else if (any(sapply(upper_class_keywords, function(w) grepl(w, occ_lower, fixed = TRUE)))) {
      result$class_background <- "Upper/elite"
    }
  }

  if (length(result) == 0) return(NULL)
  result
}

#' Extract JSON from Agent Traces
#'
#' Extracts JSON data from raw agent traces. First tries the standard approach
#' (last AIMessage JSON), then if the result has >60% Unknown fields, enhances
#' it by mining ToolMessage contents for biographical data.
#'
#' @param text Raw trace text
#' @return Parsed JSON data as a list, or NULL if no JSON found
#' @keywords internal
.extract_json_from_trace <- function(text) {
  # Try the standard extraction first
  json_data <- .extract_json_from_trace_inner(text)

  # If mostly Unknown, enhance with trace mining
  if (!is.null(json_data)) {
    unknown_ratio <- .count_unknown_ratio(json_data)
    if (unknown_ratio > 0.6) {
      mined <- .mine_trace_for_fields(text)
      if (!is.null(mined) && is.list(mined)) {
        for (field in names(mined)) {
          current_val <- json_data[[field]]
          if (is.null(current_val) ||
              (is.character(current_val) && tolower(current_val) %in% c("unknown", ""))) {
            json_data[[field]] <- mined[[field]]
          }
        }
      }
    }
  }

  json_data
}

#' Inner extraction logic for JSON from Agent Traces
#'
#' Internal function to extract JSON data from raw agent traces.
#' Called by .extract_json_from_trace which wraps it with trace mining fallback.
#'
#' @param text Raw trace text
#' @return Parsed JSON data as a list, or NULL if no JSON found
#' @keywords internal
.extract_json_from_trace_inner <- function(text) {
  # Structured trace JSON path (asa_trace_v1)
  msgs <- .parse_trace_json_messages(text)
  if (!is.null(msgs)) {
    ai_contents <- character(0)
    for (m in msgs) {
      if (!is.list(m)) next
      m_type <- tolower(as.character(m$message_type %||% ""))
      if (!(m_type %in% c("aimessage", "ai"))) next
      content <- as.character(m$content %||% "")
      if (nzchar(content)) {
        ai_contents <- c(ai_contents, content)
      }
    }
    if (length(ai_contents) > 0) {
      # Prefer the most recent AI message
      for (i in rev(seq_along(ai_contents))) {
        parsed <- .try_or(.parse_json_response(ai_contents[i]))
        if (is.list(parsed) && length(parsed) > 0) {
          return(parsed)
        }
      }
    }
  }

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

          data <- .parse_trace_json_candidate(json_str, max_decode_layers = 3L)
          if (is.list(data) && length(data) > 0) {
            return(data)
          }
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
  # Validate inputs
  .validate_process_outputs(
    df = df,
    parallel = parallel,
    workers = workers
  )

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
  if (parallel &&
      requireNamespace("future", quietly = TRUE) &&
      requireNamespace("future.apply", quietly = TRUE)) {
    future::plan(future::multisession(workers = workers))
    on.exit(future::plan(future::sequential), add = TRUE)

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
