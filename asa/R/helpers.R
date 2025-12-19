#' Null-Coalescing Operator
#'
#' Returns the left-hand side if it is not NULL, otherwise returns the
#' right-hand side.
#'
#' @param x Left-hand side value
#' @param y Right-hand side value (default)
#'
#' @return x if not NULL, otherwise y
#'
#' @keywords internal
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Clean Text for JSON Output
#'
#' Escapes special characters in text for safe inclusion in JSON strings.
#'
#' @param x Character string to escape
#'
#' @return Escaped string
#'
#' @keywords internal
json_escape <- function(x) {
  if (is.null(x) || is.na(x)) return("")

  x <- gsub("\\\\", "\\\\\\\\", x)
  x <- gsub('"', '\\\\"', x)
  x <- gsub("\n", "\\\\n", x)
  x <- gsub("\r", "\\\\r", x)
  x <- gsub("\t", "\\\\t", x)

  x
}

#' Decode HTML Entities
#'
#' Converts HTML entities to their character equivalents.
#'
#' @param x Character string with HTML entities
#'
#' @return Decoded string
#'
#' @keywords internal
decode_html <- function(x) {
  if (is.null(x) || is.na(x)) return(x)

  # Common HTML entities
  entities <- c(
    "&amp;" = "&",
    "&lt;" = "<",
    "&gt;" = ">",
    "&quot;" = '"',
    "&#39;" = "'",
    "&apos;" = "'",
    "&nbsp;" = " ",
    "&#x27;" = "'",
    "&#x2F;" = "/"
  )

  for (entity in names(entities)) {
    x <- gsub(entity, entities[entity], x, fixed = TRUE)
  }

  x
}

#' Clean Whitespace
#'
#' Normalizes whitespace in a string by collapsing multiple spaces and
#' trimming leading/trailing whitespace.
#'
#' @param x Character string
#'
#' @return Cleaned string
#'
#' @keywords internal
clean_whitespace <- function(x) {
  if (is.null(x) || is.na(x)) return(x)

  x <- gsub("\\s+", " ", x)
  trimws(x)
}

#' Truncate String
#'
#' Truncates a string to a maximum length, adding ellipsis if truncated.
#'
#' @param x Character string
#' @param max_length Maximum length
#' @param ellipsis String to append when truncated
#'
#' @return Truncated string
#'
#' @keywords internal
truncate_string <- function(x, max_length = 100, ellipsis = "...") {
  if (is.null(x) || is.na(x)) return(x)

  if (nchar(x) > max_length) {
    paste0(substr(x, 1, max_length - nchar(ellipsis)), ellipsis)
  } else {
    x
  }
}

#' Safe JSON Parse
#'
#' Attempts to parse JSON, returning NULL on failure.
#'
#' @param x JSON string
#'
#' @return Parsed R object or NULL
#'
#' @keywords internal
safe_json_parse <- function(x) {
  if (is.null(x) || is.na(x) || x == "") return(NULL)

  tryCatch(
    jsonlite::fromJSON(x),
    error = function(e) NULL
  )
}

#' Format Time Duration
#'
#' Formats a numeric duration (in minutes) as a human-readable string.
#'
#' @param minutes Numeric duration in minutes
#'
#' @return Formatted string
#'
#' @keywords internal
format_duration <- function(minutes) {
  if (is.null(minutes) || is.na(minutes)) return("N/A")

  if (minutes < 1) {
    sprintf("%.1f seconds", minutes * 60)
  } else if (minutes < 60) {
    sprintf("%.1f minutes", minutes)
  } else {
    hours <- floor(minutes / 60)
    mins <- minutes %% 60
    sprintf("%d hours %.1f minutes", hours, mins)
  }
}

#' Check if Tor is Running
#'
#' Checks if Tor is running and accessible on the default port.
#'
#' @param port Port number (default: 9050)
#'
#' @return Logical indicating if Tor appears to be running
#'
#' @examples
#' \dontrun{
#' if (!is_tor_running()) {
#'   message("Start Tor with: brew services start tor")
#' }
#' }
#'
#' @export
is_tor_running <- function(port = 9050L) {
  tryCatch({
    con <- socketConnection(host = "127.0.0.1", port = port, timeout = 2)
    close(con)
    TRUE
  }, error = function(e) {
    FALSE
  })
}

#' Get External IP via Tor
#'
#' Retrieves the external IP address as seen through Tor proxy.
#'
#' @param proxy Tor proxy URL
#'
#' @return IP address string or NA on failure
#'
#' @examples
#' \dontrun{
#' ip <- get_tor_ip()
#' message("Current Tor IP: ", ip)
#' }
#'
#' @export
get_tor_ip <- function(proxy = "socks5h://127.0.0.1:9050") {
  tryCatch({
    cmd <- sprintf("curl -s --proxy %s https://api.ipify.org?format=json", proxy)
    result <- system(cmd, intern = TRUE)
    parsed <- jsonlite::fromJSON(result)
    parsed$ip
  }, error = function(e) {
    NA_character_
  })
}

#' Rotate Tor Circuit
#'
#' Requests a new Tor circuit by restarting the Tor service.
#'
#' @param method Method to restart: "brew" (macOS), "systemctl" (Linux), or "signal"
#' @param wait Seconds to wait for new circuit (default: 12)
#'
#' @return Invisibly returns NULL
#'
#' @examples
#' \dontrun{
#' rotate_tor_circuit()
#' }
#'
#' @export
rotate_tor_circuit <- function(method = c("brew", "systemctl", "signal"),
                               wait = 12L) {
  method <- match.arg(method)

  cmd <- switch(method,
    "brew" = "brew services restart tor",
    "systemctl" = "sudo systemctl restart tor",
    "signal" = "kill -HUP $(pgrep -x tor)"
  )

  system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
  Sys.sleep(wait)

  invisible(NULL)
}

#' Print Utility
#'
#' Wrapper around cat for consistent output formatting.
#'
#' @param ... Arguments passed to cat
#'
#' @keywords internal
print2 <- function(...) {
  cat(..., "\n", sep = "")
}


#' Configure Python Search Logging Level
#'
#' Sets the logging level for the Python search module. This controls
#' how much diagnostic output is produced during web searches.
#'
#' @param level Log level: "DEBUG", "INFO", "WARNING" (default), "ERROR", or "CRITICAL"
#' @param conda_env Name of the conda environment (default: "asa_env")
#'
#' @return Invisibly returns the current logging level
#'
#' @details
#' Log levels from most to least verbose:
#' \itemize{
#'   \item DEBUG: Detailed diagnostic information for debugging
#'   \item INFO: General operational information
#'   \item WARNING: Indicates something unexpected but not an error (default)
#'   \item ERROR: Serious problems that prevented an operation
#'   \item CRITICAL: Very serious errors
#' }
#'
#' @examples
#' \dontrun{
#' # Enable verbose debugging output
#' configure_search_logging("DEBUG")
#'
#' # Run a search (will show detailed logs)
#' result <- run_task("What is the population of Tokyo?", agent = agent)
#'
#' # Disable verbose output
#' configure_search_logging("WARNING")
#' }
#'
#' @export
configure_search_logging <- function(level = "WARNING",
                                     conda_env = "asa_env") {
  # Validate level
  valid_levels <- c("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
  level <- toupper(level)
  if (!level %in% valid_levels) {
    stop("Invalid log level. Must be one of: ",
         paste(valid_levels, collapse = ", "),
         call. = FALSE)
  }

  # Use specified conda environment
  reticulate::use_condaenv(conda_env, required = TRUE)

  # Import the custom_ddg_production module and configure logging
  tryCatch({
    ddg_module <- reticulate::import("custom_ddg_production")
    ddg_module$configure_logging(level)
    message("Search logging level set to: ", level)
  }, error = function(e) {
    warning("Could not configure Python logging: ", e$message, call. = FALSE)
  })

  invisible(level)
}


#' Configure Python Search Parameters
#'
#' Sets global configuration values for the Python search module.
#' These values control timeouts, retry behavior, and result limits.
#'
#' @param max_results Maximum number of search results to return (default: 10)
#' @param timeout HTTP request timeout in seconds (default: 15)
#' @param max_retries Maximum retry attempts on failure (default: 3)
#' @param retry_delay Initial delay between retries in seconds (default: 2)
#' @param backoff_multiplier Multiplier for exponential backoff (default: 1.5)
#' @param captcha_backoff_base Base multiplier for CAPTCHA backoff (default: 3)
#' @param page_load_wait Wait time after page load in seconds (default: 2)
#' @param conda_env Name of the conda environment (default: "asa_env")
#'
#' @return Invisibly returns a list with the current configuration
#'
#' @examples
#' \dontrun{
#' # Increase timeout for slow connections
#' configure_search(timeout = 30, max_retries = 5)
#'
#' # Get more results
#' configure_search(max_results = 20)
#' }
#'
#' @export
configure_search <- function(max_results = NULL,
                             timeout = NULL,
                             max_retries = NULL,
                             retry_delay = NULL,
                             backoff_multiplier = NULL,
                             captcha_backoff_base = NULL,
                             page_load_wait = NULL,
                             conda_env = "asa_env") {
  # Use specified conda environment
  reticulate::use_condaenv(conda_env, required = TRUE)

  # Import the custom_ddg_production module and configure
  tryCatch({
    ddg_module <- reticulate::import("custom_ddg_production")
    config <- ddg_module$configure_search(
      max_results = max_results,
      timeout = timeout,
      max_retries = max_retries,
      retry_delay = retry_delay,
      backoff_multiplier = backoff_multiplier,
      captcha_backoff_base = captcha_backoff_base,
      page_load_wait = page_load_wait
    )

    # Build message showing which values were set
    set_values <- c()
    if (!is.null(max_results)) set_values <- c(set_values, paste0("max_results=", max_results))
    if (!is.null(timeout)) set_values <- c(set_values, paste0("timeout=", timeout))
    if (!is.null(max_retries)) set_values <- c(set_values, paste0("max_retries=", max_retries))
    if (!is.null(retry_delay)) set_values <- c(set_values, paste0("retry_delay=", retry_delay))

    if (length(set_values) > 0) {
      message("Search configuration updated: ", paste(set_values, collapse = ", "))
    }

    invisible(config)
  }, error = function(e) {
    warning("Could not configure search parameters: ", e$message, call. = FALSE)
    invisible(NULL)
  })
}
