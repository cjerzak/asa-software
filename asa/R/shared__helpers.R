# NOTE: Foundational helpers live in dedicated files:
# - `asa/R/utils_try.R`: `%||%`, `.try_or`, `.try_or_warn`
# - `asa/R/py_bridge.R`: `.py_get`
# - `asa/R/json_parse.R`: `.parse_json_response`, `.extract_json_object`
# - `asa/R/proxy.R`: `.resolve_proxy`, `.format_proxy`
# - `asa/R/recursion_limits.R`: `.normalize_recursion_limit`, `.default_recursion_limit`,
#   `.resolve_effective_recursion_limit`

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

  # Strip markdown code fences (```json ... ``` or ``` ... ```)
  txt <- trimws(x)
  if (startsWith(txt, "```")) {
    parts <- strsplit(txt, "```", fixed = TRUE)[[1]]
    if (length(parts) >= 2) {
      inner <- parts[2]
      inner <- sub("^json\\s*", "", inner)
      inner <- sub("^\\n", "", inner)
      txt <- trimws(inner)
    }
  }

  .try_or(jsonlite::fromJSON(txt))
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
#' @param proxy Tor proxy URL (e.g., "socks5h://127.0.0.1:9050" for default,
#'   or "socks5h://127.0.0.1:9055" for instance on port 9055)
#' @param timeout Timeout in seconds (default: 30). Useful for parallel
#'   workloads where some Tor exits may be slow.
#'
#' @return IP address string or NA on failure
#'
#' @examples
#' \dontrun{
#' # Default Tor instance
#' ip <- get_tor_ip()
#' message("Current Tor IP: ", ip)
#'
#' # Check specific Tor instance (e.g., for parallel jobs)
#' ip <- get_tor_ip(proxy = "socks5h://127.0.0.1:9055")
#' }
#'
#' @export
get_tor_ip <- function(proxy = "socks5h://127.0.0.1:9050", timeout = 30L) {
  tryCatch({
    result <- .run_command(
      "curl",
      args = c("-s", "--proxy", proxy, "https://api.ipify.org?format=json"),
      timeout = timeout
    )
    if (result$status != 0) {
      # LOW FIX: Log stderr context for debugging Tor connection failures
      stderr_msg <- trimws(result$stderr)
      if (nzchar(stderr_msg)) {
        message("[asa] get_tor_ip failed (exit ", result$status, "): ", stderr_msg)
      }
      return(NA_character_)
    }
    parsed <- jsonlite::fromJSON(result$stdout)
    parsed$ip
  }, error = function(e) {
    # LOW FIX: Log error message for debugging
    message("[asa] get_tor_ip error: ", conditionMessage(e))
    NA_character_
  })
}

#' Rotate Tor Circuit (R-side, daemon restart)
#'
#' Requests a new Tor circuit by restarting the Tor service or sending SIGHUP.
#'
#' @param method Method to restart: "brew" (macOS), "systemctl" (Linux), or "signal"
#' @param wait Seconds to wait for new circuit (default: 12)
#' @param pid Optional PID of specific Tor process (only used with method="signal").
#'   If NULL (default), finds the Tor process via pgrep.
#'
#' @return Invisibly returns TRUE on success, FALSE on failure
#'
#' @details
#' MEDIUM FIX: This function restarts the entire Tor daemon, which kills ALL
#' circuits and affects parallel execution. For production use, prefer the
#' Python-side control port rotation which sends SIGNAL NEWNYM to get a new
#' circuit without restarting the daemon.
#'
#' For parallel Tor setups with multiple instances, consider using Tor's built-in
#' circuit rotation via \code{MaxCircuitDirtiness} and \code{NewCircuitPeriod}
#' config options instead of this function.
#'
#' @note The "brew" and "systemctl" methods restart the entire Tor daemon and
#' should only be used as a last resort for recovery. The "signal" method is
#' preferred but still affects all circuits on the process.
#'
#' @examples
#' \dontrun{
#' # Preferred: Use Python-side control port rotation (via run_task/asa_enumerate)
#' # This R function is for manual recovery only
#'
#' # Send SIGHUP to Tor process (least disruptive)
#' rotate_tor_circuit(method = "signal")
#'
#' # macOS with Homebrew (restarts daemon - use sparingly)
#' rotate_tor_circuit(method = "brew")
#'
#' # Linux with systemd (restarts daemon - use sparingly)
#' rotate_tor_circuit(method = "systemctl")
#' }
#'
#' @export
rotate_tor_circuit <- function(method = c("signal", "brew", "systemctl"),
                               wait = 12L,
                               pid = NULL) {
  method <- match.arg(method)

  # MEDIUM FIX: Warn about daemon restart methods

  if (method %in% c("brew", "systemctl")) {
    warning(
      "rotate_tor_circuit with method='", method, "' restarts the entire Tor daemon, ",
      "which kills ALL circuits and affects parallel execution. ",
      "Consider using method='signal' or the Python-side control port rotation instead.",
      call. = FALSE
    )
  }

  success <- tryCatch({
    if (method == "brew") {
      .run_command("brew", c("services", "restart", "tor"))
    } else if (method == "systemctl") {
      .run_command("sudo", c("systemctl", "restart", "tor"))
    } else if (method == "signal") {
      # If no PID provided, find the Tor process
      if (is.null(pid)) {
        pgrep_result <- .run_command("pgrep", c("-x", "tor"))
        if (pgrep_result$status != 0 || nchar(trimws(pgrep_result$stdout)) == 0) {
          warning("Could not find Tor process", call. = FALSE)
          return(invisible(FALSE))
        }
        pid <- trimws(pgrep_result$stdout)
        # Handle multiple PIDs (take first line)
        pid <- strsplit(pid, "\n")[[1]][1]
      }
      .run_command("kill", c("-HUP", as.character(pid)))
    }
    TRUE
  }, error = function(e) {
    warning("Failed to rotate Tor circuit: ", e$message, call. = FALSE)
    FALSE
  })

  if (success) {
    Sys.sleep(wait)
  }

  invisible(success)
}

#' Configure Tor Exit Registry
#'
#' Sets up the shared Tor exit health registry used by the Python search stack
#' to avoid reusing tainted or overused exit nodes.
#'
#' @param registry_path Path to the SQLite registry file (default: user cache).
#' @param enable Enable the registry (set FALSE to disable tracking).
#' @param bad_ttl Seconds to keep a bad/tainted exit before reuse.
#' @param good_ttl Seconds to treat an exit as good before refreshing.
#' @param overuse_threshold Maximum recent uses before a good exit is treated as overloaded.
#' @param overuse_decay Window (seconds) for counting recent uses before decay.
#' @param max_rotation_attempts Maximum rotations to find a clean exit.
#' @param ip_cache_ttl Seconds to cache exit IP lookups.
#' @param conda_env Conda environment name for the Python module. Defaults to
#'   the package option \code{asa.default_conda_env} (or \code{"asa_env"} if unset).
#'
#' @return Invisibly returns a list of the configured values (or NULL on error).
#'
#' @export
configure_tor_registry <- function(registry_path = NULL,
                                   enable = ASA_TOR_REGISTRY_ENABLED,
                                   bad_ttl = ASA_TOR_BAD_TTL,
                                   good_ttl = ASA_TOR_GOOD_TTL,
                                   overuse_threshold = ASA_TOR_OVERUSE_THRESHOLD,
                                   overuse_decay = ASA_TOR_OVERUSE_DECAY,
                                   max_rotation_attempts = ASA_TOR_MAX_ROTATION_ATTEMPTS,
                                   ip_cache_ttl = ASA_TOR_IP_CACHE_TTL,
                                   conda_env = NULL) {

  conda_env <- conda_env %||% .get_default_conda_env()

  .validate_configure_tor_registry(
    registry_path = registry_path,
    enable = enable,
    bad_ttl = bad_ttl,
    good_ttl = good_ttl,
    overuse_threshold = overuse_threshold,
    overuse_decay = overuse_decay,
    max_rotation_attempts = max_rotation_attempts,
    ip_cache_ttl = ip_cache_ttl,
    conda_env = conda_env
  )

  normalized <- .normalize_tor_registry_options(
    registry_path = registry_path,
    enable = enable,
    bad_ttl = bad_ttl,
    good_ttl = good_ttl,
    overuse_threshold = overuse_threshold,
    overuse_decay = overuse_decay,
    max_rotation_attempts = max_rotation_attempts,
    ip_cache_ttl = ip_cache_ttl,
    create_parent_for_custom = TRUE
  )
  registry_path <- normalized$registry_path

  # Keep env vars in sync for child processes
  Sys.setenv(TOR_EXIT_DB = registry_path, ASA_TOR_EXIT_DB = registry_path)

  reticulate::use_condaenv(conda_env, required = TRUE)

  tryCatch({
    ddg_module <- .import_backend_api(required = TRUE)
    cfg <- ddg_module$configure_tor_registry(
      registry_path = registry_path,
      enable = normalized$enable,
      bad_ttl = normalized$bad_ttl,
      good_ttl = normalized$good_ttl,
      overuse_threshold = normalized$overuse_threshold,
      overuse_decay = normalized$overuse_decay,
      max_rotation_attempts = normalized$max_rotation_attempts,
      ip_cache_ttl = normalized$ip_cache_ttl
    )
    invisible(cfg)
  }, error = function(e) {
    warning("Could not configure Tor registry: ", e$message, call. = FALSE)
    invisible(NULL)
  })
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
#' @param conda_env Name of the conda environment. Defaults to the package
#'   option \code{asa.default_conda_env} (or \code{"asa_env"} if unset).
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
                                     conda_env = NULL) {
  conda_env <- conda_env %||% .get_default_conda_env()
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

  # Import the backend API module and configure logging.
  tryCatch({
    ddg_module <- .import_backend_api(required = TRUE)
    ddg_module$configure_logging(level)
    message("Search logging level set to: ", level)
  }, error = function(e) {
    warning("Could not configure Python logging: ", e$message, call. = FALSE)
  })

  invisible(level)
}


#' Configure Temporal Filtering for Search
#'
#' Sets or clears temporal filtering on the DuckDuckGo search tool.
#' This affects all subsequent searches until changed or cleared.
#'
#' @param time_filter DuckDuckGo time filter: "d" (day), "w" (week),
#'   "m" (month), "y" (year), or NULL/NA/"none" to clear
#'
#' @return Invisibly returns the previous time filter setting
#'
#' @details
#' This function modifies the search tool's time parameter, which is
#' passed to DuckDuckGo as the \code{df} parameter. The filter restricts
#' results to content indexed within the specified time period.
#'
#' Note: This only affects DuckDuckGo searches. For Wikidata queries
#' with temporal filtering, use \code{asa_enumerate()} with its
#' \code{temporal} parameter.
#'
#' @section Time Filter Values:
#' \itemize{
#'   \item "d": Past 24 hours (day)
#'   \item "w": Past 7 days (week)
#'   \item "m": Past 30 days (month)
#'   \item "y": Past 365 days (year)
#'   \item NULL, NA, or "none": No time restriction (default)
#' }
#'
#' @examples
#' \dontrun{
#' # Restrict to past year
#' configure_temporal("y")
#' result <- run_task("Find recent AI breakthroughs", agent = agent)
#'
#' # Clear temporal filter
#' configure_temporal(NULL)
#'
#' # Past week only
#' configure_temporal("w")
#' }
#'
#' @seealso \code{\link{run_task}}, \code{\link{asa_enumerate}}
#'
#' @export
configure_temporal <- function(time_filter = NULL) {
  # Check if agent is initialized
  if (!.is_initialized()) {
    stop("Agent not initialized. Call initialize_agent() first.", call. = FALSE)
  }

  # Validate time_filter
  if (!is.null(time_filter) && !is.na(time_filter) && time_filter != "none") {
    valid_filters <- c("d", "w", "m", "y")
    if (!time_filter %in% valid_filters) {
      stop("`time_filter` must be one of: 'd', 'w', 'm', 'y', or NULL/NA/'none'",
           call. = FALSE)
    }
  }

  # Normalize NULL/NA/"none" to "none" (DuckDuckGo's no-filter value)
  if (is.null(time_filter) || is.na(time_filter) || time_filter == "none") {
    time_filter <- "none"
  }

  # Get the search tool (second in the tools list)
  search_tool <- asa_env$tools[[2]]

  # Store previous value for return
  prev_filter <- .try_or(search_tool$api_wrapper$time, "none")

  # Update the time filter on the API wrapper
  tryCatch({
    search_tool$api_wrapper$time <- time_filter
    if (time_filter != "none") {
      message("DuckDuckGo time filter set to: ", time_filter,
              " (", switch(time_filter,
                           "d" = "past day",
                           "w" = "past week",
                           "m" = "past month",
                           "y" = "past year"), ")")
    } else {
      message("DuckDuckGo time filter cleared")
    }
  }, error = function(e) {
    warning("Could not set DuckDuckGo time filter: ", e$message, call. = FALSE)
  })

  invisible(prev_filter)
}

#' Resolve Temporal Inputs
#'
#' Internal helper that merges temporal defaults from asa_config and coerces
#' asa_temporal objects to plain lists.
#'
#' @param temporal NULL, list, or asa_temporal
#' @param config Optional asa_config supplying default temporal
#' @return Temporal list or NULL
#' @keywords internal
.resolve_temporal_input <- function(temporal, config = NULL) {
  if (is.null(temporal) &&
      !is.null(config) &&
      inherits(config, "asa_config") &&
      !is.null(config$temporal)) {
    temporal <- config$temporal
  }

  if (inherits(temporal, "asa_temporal")) {
    temporal <- as.list(temporal)
  }

  temporal
}

#' Resolve temporal + webpage reader settings for a single call
#'
#' Internal helper that keeps run_task() and asa_enumerate() in sync.
#'
#' @param temporal Temporal settings (NULL/list/asa_temporal)
#' @param config Optional asa_config
#' @param config_search Optional config$search (asa_search or list)
#' @param allow_read_webpages Passed through to .resolve_webpage_reader_settings()
#' @param webpage_relevance_mode Passed through to .resolve_webpage_reader_settings()
#' @param webpage_heuristic_profile Passed through to .resolve_webpage_reader_settings()
#' @param webpage_embedding_provider Passed through to .resolve_webpage_reader_settings()
#' @param webpage_embedding_model Passed through to .resolve_webpage_reader_settings()
#' @return List with temporal + resolved webpage reader settings
#' @keywords internal
.resolve_temporal_and_webpage_reader <- function(temporal = NULL,
                                                config = NULL,
                                                config_search = NULL,
                                                allow_read_webpages = NULL,
                                                webpage_relevance_mode = NULL,
                                                webpage_heuristic_profile = NULL,
                                                webpage_embedding_provider = NULL,
                                                webpage_embedding_model = NULL,
                                                webpage_timeout = NULL,
                                                webpage_max_bytes = NULL,
                                                webpage_max_chars = NULL,
                                                webpage_max_chunks = NULL,
                                                webpage_chunk_chars = NULL,
                                                webpage_embedding_api_base = NULL,
                                                webpage_prefilter_k = NULL,
                                                webpage_use_mmr = NULL,
                                                webpage_mmr_lambda = NULL,
                                                webpage_cache_enabled = NULL,
                                                webpage_cache_max_entries = NULL,
                                                webpage_cache_max_text_chars = NULL,
                                                webpage_blocked_cache_ttl_sec = NULL,
                                                webpage_blocked_cache_max_entries = NULL,
                                                webpage_blocked_probe_bytes = NULL,
                                                webpage_blocked_detect_on_200 = NULL,
                                                webpage_blocked_body_scan_bytes = NULL,
                                                webpage_pdf_enabled = NULL,
                                                webpage_pdf_timeout = NULL,
                                                webpage_pdf_max_bytes = NULL,
                                                webpage_pdf_max_pages = NULL,
                                                webpage_pdf_max_text_chars = NULL,
                                                webpage_user_agent = NULL) {
  temporal <- .resolve_temporal_input(temporal, config)
  webpage_settings <- .resolve_webpage_reader_settings(
    config_search,
    allow_read_webpages,
    webpage_relevance_mode,
    webpage_heuristic_profile,
    webpage_embedding_provider,
    webpage_embedding_model,
    webpage_timeout,
    webpage_max_bytes,
    webpage_max_chars,
    webpage_max_chunks,
    webpage_chunk_chars,
    webpage_embedding_api_base,
    webpage_prefilter_k,
    webpage_use_mmr,
    webpage_mmr_lambda,
    webpage_cache_enabled,
    webpage_cache_max_entries,
    webpage_cache_max_text_chars,
    webpage_blocked_cache_ttl_sec,
    webpage_blocked_cache_max_entries,
    webpage_blocked_probe_bytes,
    webpage_blocked_detect_on_200,
    webpage_blocked_body_scan_bytes,
    webpage_pdf_enabled,
    webpage_pdf_timeout,
    webpage_pdf_max_bytes,
    webpage_pdf_max_pages,
    webpage_pdf_max_text_chars,
    webpage_user_agent
  )

  list(
    temporal = temporal,
    allow_read_webpages = webpage_settings$allow_read_webpages,
    relevance_mode = webpage_settings$relevance_mode,
    heuristic_profile = webpage_settings$heuristic_profile,
    embedding_provider = webpage_settings$embedding_provider,
    embedding_model = webpage_settings$embedding_model,
    timeout = webpage_settings$timeout,
    max_bytes = webpage_settings$max_bytes,
    max_chars = webpage_settings$max_chars,
    max_chunks = webpage_settings$max_chunks,
    chunk_chars = webpage_settings$chunk_chars,
    embedding_api_base = webpage_settings$embedding_api_base,
    prefilter_k = webpage_settings$prefilter_k,
    use_mmr = webpage_settings$use_mmr,
    mmr_lambda = webpage_settings$mmr_lambda,
    cache_enabled = webpage_settings$cache_enabled,
    cache_max_entries = webpage_settings$cache_max_entries,
    cache_max_text_chars = webpage_settings$cache_max_text_chars,
    blocked_cache_ttl_sec = webpage_settings$blocked_cache_ttl_sec,
    blocked_cache_max_entries = webpage_settings$blocked_cache_max_entries,
    blocked_probe_bytes = webpage_settings$blocked_probe_bytes,
    blocked_detect_on_200 = webpage_settings$blocked_detect_on_200,
    blocked_body_scan_bytes = webpage_settings$blocked_body_scan_bytes,
    pdf_enabled = webpage_settings$pdf_enabled,
    pdf_timeout = webpage_settings$pdf_timeout,
    pdf_max_bytes = webpage_settings$pdf_max_bytes,
    pdf_max_pages = webpage_settings$pdf_max_pages,
    pdf_max_text_chars = webpage_settings$pdf_max_text_chars,
    user_agent = webpage_settings$user_agent
  )
}


#' Apply Temporal Filtering for a Single Operation
#'
#' Internal helper that applies temporal filtering, runs a function,
#' and restores the original setting. Used by run_task() and run_task_batch().
#'
#' @param temporal Named list with temporal options (time_filter, after, before)
#' @param fn Function to run with temporal filtering applied
#' @param agent Optional asa_agent whose tools should receive the time filter
#' @return Result of fn()
#' @keywords internal
.with_temporal <- function(temporal, fn, agent = NULL) {
  if (is.null(temporal)) {
    return(fn())
  }

  tools <- NULL
  if (!is.null(agent) && inherits(agent, "asa_agent") && !is.null(agent$tools)) {
    tools <- agent$tools
  } else if (.is_initialized()) {
    tools <- asa_env$tools
  }

  # Store original setting
  original_filter <- .try_or({
    if (!is.null(tools) && length(tools) >= 2) {
      tools[[2]]$api_wrapper$time
    } else if (.is_initialized()) {
      asa_env$tools[[2]]$api_wrapper$time
    } else {
      "none"
    }
  }, "none")

  set_filter <- function(value, warn = TRUE) {
    if (!is.null(tools) && length(tools) >= 2) {
      tryCatch(
        tools[[2]]$api_wrapper$time <- value,
        error = function(e) {
          if (warn) {
            warning("Could not set DuckDuckGo time filter: ", e$message, call. = FALSE)
          }
          NULL
        }
      )
      return(invisible(NULL))
    }
    if (.is_initialized()) {
      tryCatch(
        configure_temporal(value),
        error = function(e) {
          if (warn) {
            warning("Could not set DuckDuckGo time filter: ", e$message, call. = FALSE)
          }
          NULL
        }
      )
    }
    invisible(NULL)
  }

  # Apply temporal filter if specified
  if (!is.null(temporal$time_filter)) {
    set_filter(temporal$time_filter, warn = TRUE)
  }

  # Run the function
  result <- tryCatch(
    fn(),
    finally = {
      # Restore original setting
      if (!is.null(temporal$time_filter)) {
        tryCatch(
          set_filter(original_filter, warn = FALSE),
          error = function(e) NULL
        )
      }
    }
  )

  result
}


#' Run Code with a Temporary Configuration Snapshot
#'
#' Internal helper that snapshots a runtime config, executes code, then
#' best-effort restores the original config on exit.
#'
#' @param snapshot_fn Function returning previous config snapshot.
#' @param restore_fn Function restoring previous config.
#' @param fn Function to execute while temporary config is active.
#' @param should_restore Optional predicate that decides whether restore should run.
#' @param on_exit_fn Optional cleanup function executed on exit.
#' @return Result of \code{fn()}.
#' @keywords internal

#' Compare Runtime Configuration Values
#'
#' Internal helper that compares two config fragments while ignoring
#' attribute differences (e.g., integer vs numeric scalars from Python).
#'
#' @param lhs Left-hand value/list
#' @param rhs Right-hand value/list
#' @return TRUE when equivalent, FALSE otherwise
#' @keywords internal


#' Apply Search Configuration for a Single Operation
#'
#' Internal helper that applies search settings, runs a function,
#' and restores the original configuration afterward.
#'
#' @param search asa_search object or list of search settings
#' @param conda_env Conda env used by search tools
#' @param fn Function to run with search config applied
#' @return Result of fn()
#' @keywords internal
#' Resolve a single option from config
#'
#' Returns \code{value} unless it is \code{NULL} and \code{config[[key]]}
#' exists, in which case the config value is returned.
#'
#' @param value Explicit value (returned if non-NULL).
#' @param config Configuration list to fall back to.
#' @param key Key to look up in \code{config}.
#' @return The resolved value.
#' @keywords internal
#' Resolve Webpage Reader Settings
#'
#' Internal helper that merges per-call webpage reader options with defaults
#' from config$search.
#'
#' @param config_search asa_search object or list of search settings
#' @param allow_read_webpages TRUE/FALSE/NULL
#' @param webpage_relevance_mode "auto", "lexical", "embeddings", or NULL
#' @param webpage_heuristic_profile "generic" or NULL
#' @param webpage_embedding_provider "auto", "openai", "sentence_transformers", or NULL
#' @param webpage_embedding_model Embedding model identifier or NULL
#' @param webpage_timeout OpenWebpage timeout in seconds, or NULL to use the
#'   Python default.
#' @param webpage_max_bytes OpenWebpage download cap in bytes, or NULL to use
#'   the Python default.
#' @param webpage_max_chars OpenWebpage output cap in characters, or NULL to use
#'   the Python default.
#' @param webpage_max_chunks Max relevant excerpts to return, or NULL to use the
#'   Python default.
#' @param webpage_chunk_chars Approximate chunk size (characters), or NULL to
#'   use the Python default.
#' @param webpage_embedding_api_base OpenAI-compatible base URL for embeddings,
#'   or NULL to use the Python default.
#' @param webpage_prefilter_k Optional lexical prefilter size before embedding
#'   selection.
#' @param webpage_use_mmr Whether to apply maximal marginal relevance (MMR) for
#'   diverse excerpts.
#' @param webpage_mmr_lambda MMR tradeoff between relevance (1.0) and diversity (0.0).
#' @param webpage_cache_enabled Enable per-run caching, or NULL to use the Python
#'   default.
#' @param webpage_cache_max_entries Max cached entries per run, or NULL.
#' @param webpage_cache_max_text_chars Max extracted text chars to cache per page,
#'   or NULL.
#' @param webpage_blocked_cache_ttl_sec TTL in seconds for blocked URL cache.
#' @param webpage_blocked_cache_max_entries Max blocked cache entries.
#' @param webpage_blocked_probe_bytes Max bytes inspected for blocked-marker detection.
#' @param webpage_blocked_detect_on_200 Detect anti-bot interstitial pages served
#'   with HTTP 200 and classify them as blocked.
#' @param webpage_blocked_body_scan_bytes Max bytes scanned for blocked markers on
#'   HTTP 200 pages.
#' @param webpage_pdf_enabled Enable PDF extraction via pdftotext.
#' @param webpage_pdf_timeout Timeout in seconds for pdftotext.
#' @param webpage_pdf_max_bytes Max bytes to download per PDF.
#' @param webpage_pdf_max_pages Max pages to extract from PDF.
#' @param webpage_pdf_max_text_chars Max PDF text chars retained before ranking.
#' @param webpage_user_agent User-Agent string for webpage fetches, or NULL.
#' @return List with resolved settings
#' @keywords internal
.resolve_webpage_reader_settings <- function(config_search = NULL,
                                             allow_read_webpages = NULL,
                                             webpage_relevance_mode = NULL,
                                             webpage_heuristic_profile = NULL,
                                             webpage_embedding_provider = NULL,
                                             webpage_embedding_model = NULL,
                                             webpage_timeout = NULL,
                                             webpage_max_bytes = NULL,
                                             webpage_max_chars = NULL,
                                             webpage_max_chunks = NULL,
                                             webpage_chunk_chars = NULL,
                                             webpage_embedding_api_base = NULL,
                                             webpage_prefilter_k = NULL,
                                             webpage_use_mmr = NULL,
                                             webpage_mmr_lambda = NULL,
                                             webpage_cache_enabled = NULL,
                                             webpage_cache_max_entries = NULL,
                                             webpage_cache_max_text_chars = NULL,
                                             webpage_blocked_cache_ttl_sec = NULL,
                                             webpage_blocked_cache_max_entries = NULL,
                                             webpage_blocked_probe_bytes = NULL,
                                             webpage_blocked_detect_on_200 = NULL,
                                             webpage_blocked_body_scan_bytes = NULL,
                                             webpage_pdf_enabled = NULL,
                                             webpage_pdf_timeout = NULL,
                                             webpage_pdf_max_bytes = NULL,
                                             webpage_pdf_max_pages = NULL,
                                             webpage_pdf_max_text_chars = NULL,
                                             webpage_user_agent = NULL) {
  # Map from result-list key -> (explicit value, config_search key)
  fields <- list(
    allow_read_webpages  = allow_read_webpages,
    relevance_mode       = webpage_relevance_mode,
    heuristic_profile    = webpage_heuristic_profile,
    embedding_provider   = webpage_embedding_provider,
    embedding_model      = webpage_embedding_model,
    timeout              = webpage_timeout,
    max_bytes            = webpage_max_bytes,
    max_chars            = webpage_max_chars,
    max_chunks           = webpage_max_chunks,
    chunk_chars          = webpage_chunk_chars,
    embedding_api_base   = webpage_embedding_api_base,
    prefilter_k          = webpage_prefilter_k,
    use_mmr              = webpage_use_mmr,
    mmr_lambda           = webpage_mmr_lambda,
    cache_enabled        = webpage_cache_enabled,
    cache_max_entries    = webpage_cache_max_entries,
    cache_max_text_chars = webpage_cache_max_text_chars,
    blocked_cache_ttl_sec = webpage_blocked_cache_ttl_sec,
    blocked_cache_max_entries = webpage_blocked_cache_max_entries,
    blocked_probe_bytes = webpage_blocked_probe_bytes,
    blocked_detect_on_200 = webpage_blocked_detect_on_200,
    blocked_body_scan_bytes = webpage_blocked_body_scan_bytes,
    pdf_enabled = webpage_pdf_enabled,
    pdf_timeout = webpage_pdf_timeout,
    pdf_max_bytes = webpage_pdf_max_bytes,
    pdf_max_pages = webpage_pdf_max_pages,
    pdf_max_text_chars = webpage_pdf_max_text_chars,
    user_agent           = webpage_user_agent
  )

  # config_search keys use the "webpage_" prefix except allow_read_webpages
  config_keys <- c(
    allow_read_webpages  = "allow_read_webpages",
    relevance_mode       = "webpage_relevance_mode",
    heuristic_profile    = "webpage_heuristic_profile",
    embedding_provider   = "webpage_embedding_provider",
    embedding_model      = "webpage_embedding_model",
    timeout              = "webpage_timeout",
    max_bytes            = "webpage_max_bytes",
    max_chars            = "webpage_max_chars",
    max_chunks           = "webpage_max_chunks",
    chunk_chars          = "webpage_chunk_chars",
    embedding_api_base   = "webpage_embedding_api_base",
    prefilter_k          = "webpage_prefilter_k",
    use_mmr              = "webpage_use_mmr",
    mmr_lambda           = "webpage_mmr_lambda",
    cache_enabled        = "webpage_cache_enabled",
    cache_max_entries    = "webpage_cache_max_entries",
    cache_max_text_chars = "webpage_cache_max_text_chars",
    blocked_cache_ttl_sec = "webpage_blocked_cache_ttl_sec",
    blocked_cache_max_entries = "webpage_blocked_cache_max_entries",
    blocked_probe_bytes = "webpage_blocked_probe_bytes",
    blocked_detect_on_200 = "webpage_blocked_detect_on_200",
    blocked_body_scan_bytes = "webpage_blocked_body_scan_bytes",
    pdf_enabled = "webpage_pdf_enabled",
    pdf_timeout = "webpage_pdf_timeout",
    pdf_max_bytes = "webpage_pdf_max_bytes",
    pdf_max_pages = "webpage_pdf_max_pages",
    pdf_max_text_chars = "webpage_pdf_max_text_chars",
    user_agent           = "webpage_user_agent"
  )

  result <- lapply(names(fields), function(nm) {
    .resolve_option(fields[[nm]], config_search, config_keys[[nm]])
  })
  names(result) <- names(fields)
  if (!is.null(result$heuristic_profile)) {
    profile <- tolower(as.character(result$heuristic_profile)[1])
    profile <- profile[!is.na(profile) & nzchar(profile)]
    profile <- if (length(profile) > 0L) profile[[1]] else ""
    if (!identical(profile, "generic")) {
      stop(
        "`webpage_heuristic_profile` must be \"generic\". Legacy heuristic profiles are no longer supported.",
        call. = FALSE
      )
    }
    result$heuristic_profile <- profile
  }
  result
}


#' Apply Webpage Reader Configuration for a Single Operation
#'
#' Internal helper that enables/disables the Python webpage reader tool
#' for the duration of a single operation, then restores the previous setting.
#'
#' @param allow_read_webpages TRUE/FALSE to enable/disable webpage reading
#' @param relevance_mode Relevance selection for opened webpages:
#'   "auto", "lexical", or "embeddings". "auto" uses embeddings when available,
#'   otherwise falls back to lexical overlap.
#' @param heuristic_profile Heuristic profile for hyperlink annotation:
#'   "generic" (task-agnostic default).
#' @param embedding_provider Embedding provider for relevance ("auto",
#'   "openai", or "sentence_transformers").
#' @param embedding_model Embedding model identifier for relevance.
#' @param timeout Timeout (seconds) for webpage fetches/embeddings.
#' @param max_bytes Download cap in bytes.
#' @param max_chars Output cap in characters.
#' @param max_chunks Max relevant excerpts to return.
#' @param chunk_chars Approximate chunk size (characters).
#' @param embedding_api_base OpenAI-compatible base URL for embeddings.
#' @param prefilter_k Optional lexical prefilter size before embedding.
#' @param use_mmr Whether to apply maximal marginal relevance for diverse excerpts.
#' @param mmr_lambda MMR tradeoff between relevance (1.0) and diversity (0.0).
#' @param cache_enabled Enable per-run caching.
#' @param cache_max_entries Max cached entries per run.
#' @param cache_max_text_chars Max extracted text chars to cache per page.
#' @param blocked_cache_ttl_sec TTL in seconds for blocked URL cache entries.
#' @param blocked_cache_max_entries Max blocked URL cache entries.
#' @param blocked_probe_bytes Max bytes read for blocked detection on HTTP errors.
#' @param blocked_detect_on_200 Detect anti-bot interstitials served with HTTP 200.
#' @param blocked_body_scan_bytes Max bytes scanned for blocked markers on HTTP 200 pages.
#' @param pdf_enabled Enable PDF extraction via pdftotext.
#' @param pdf_timeout Timeout in seconds for pdftotext.
#' @param pdf_max_bytes Max PDF bytes downloaded before extraction.
#' @param pdf_max_pages Max pages extracted per PDF.
#' @param pdf_max_text_chars Max text chars retained from extracted PDF content.
#' @param user_agent User-Agent string for webpage fetches.
#' @param conda_env Conda env used by Python tools
#' @param fn Function to run with webpage reader config applied
#' @return Result of fn()
#' @keywords internal
.with_webpage_reader_config <- function(allow_read_webpages,
                                       relevance_mode = NULL,
                                       heuristic_profile = NULL,
                                       embedding_provider = NULL,
                                       embedding_model = NULL,
                                       timeout = NULL,
                                       max_bytes = NULL,
                                       max_chars = NULL,
                                       max_chunks = NULL,
                                       chunk_chars = NULL,
                                       embedding_api_base = NULL,
                                       prefilter_k = NULL,
                                       use_mmr = NULL,
                                       mmr_lambda = NULL,
                                       cache_enabled = NULL,
                                       cache_max_entries = NULL,
                                       cache_max_text_chars = NULL,
                                       blocked_cache_ttl_sec = NULL,
                                       blocked_cache_max_entries = NULL,
                                       blocked_probe_bytes = NULL,
                                       blocked_detect_on_200 = NULL,
                                       blocked_body_scan_bytes = NULL,
                                       pdf_enabled = NULL,
                                       pdf_timeout = NULL,
                                       pdf_max_bytes = NULL,
                                       pdf_max_pages = NULL,
                                       pdf_max_text_chars = NULL,
                                       user_agent = NULL,
                                       conda_env = NULL,
                                       fn) {
  conda_env <- conda_env %||% .get_default_conda_env()
  allow_arg <- if (is.null(allow_read_webpages)) NULL else isTRUE(allow_read_webpages)
  requested <- .compact_runtime_snapshot_args(list(
    allow_read_webpages = allow_arg,
    relevance_mode = relevance_mode,
    heuristic_profile = heuristic_profile,
    embedding_provider = embedding_provider,
    embedding_model = embedding_model,
    timeout = timeout,
    max_bytes = max_bytes,
    max_chars = max_chars,
    max_chunks = max_chunks,
    chunk_chars = chunk_chars,
    embedding_api_base = embedding_api_base,
    prefilter_k = prefilter_k,
    use_mmr = use_mmr,
    mmr_lambda = mmr_lambda,
    cache_enabled = cache_enabled,
    cache_max_entries = cache_max_entries,
    cache_max_text_chars = cache_max_text_chars,
    blocked_cache_ttl_sec = blocked_cache_ttl_sec,
    blocked_cache_max_entries = blocked_cache_max_entries,
    blocked_probe_bytes = blocked_probe_bytes,
    blocked_detect_on_200 = blocked_detect_on_200,
    blocked_body_scan_bytes = blocked_body_scan_bytes,
    pdf_enabled = pdf_enabled,
    pdf_timeout = pdf_timeout,
    pdf_max_bytes = pdf_max_bytes,
    pdf_max_pages = pdf_max_pages,
    pdf_max_text_chars = pdf_max_text_chars,
    user_agent = user_agent
  ))
  if (length(requested) == 0L) {
    return(fn())
  }

  # Ensure conda env is active (module lives in that env).
  reticulate::use_condaenv(conda_env, required = TRUE)

  # Ensure module is available. initialize_agent() imports this already, but
  # keep this resilient for call paths that bypass initialization order.
  if (is.null(asa_env$webpage_tool)) {
    .import_python_module(
      "tools.webpage_reader_tool",
      env_name = "webpage_tool",
      required = FALSE
    )
  }
  if (is.null(asa_env$webpage_tool)) {
    warning("Webpage reader module unavailable; continuing without webpage reading.", call. = FALSE)
    return(fn())
  }

  clear_cache <- function() {
    tryCatch(
      asa_env$webpage_tool$clear_webpage_reader_cache(),
      error = function(e) NULL
    )
  }

  # Clear cache at the start of this operation (per-run caching semantics).
  if (isTRUE(allow_read_webpages)) {
    clear_cache()
  }

  previous_cfg <- tryCatch(
    asa_env$webpage_tool$configure_webpage_reader(),
    error = function(e) NULL
  )
  previous <- .serialize_runtime_config_snapshot(previous_cfg)

  apply_snapshot <- function(snapshot) {
    args <- .compact_runtime_snapshot_args(snapshot)
    do.call(asa_env$webpage_tool$configure_webpage_reader, args)
  }

  needs_update <- TRUE
  if (!is.null(previous)) {
    current_requested <- previous[names(requested)]
    needs_update <- !.config_values_equal(current_requested, requested)
  }

  result <- tryCatch(
    {
      if (!needs_update) {
        fn()
      } else {
        .with_config_snapshot(
          snapshot_fn = function() {
            previous
          },
          restore_fn = function(previous) {
            apply_snapshot(previous)
          },
          should_restore = function(previous) {
            !is.null(previous) && length(previous) > 0L
          },
          fn = function() {
            tryCatch(
              apply_snapshot(requested),
              error = function(e) NULL
            )
            fn()
          }
        )
      }
    },
    finally = {
      # Clear any per-run cache after the operation.
      if (isTRUE(allow_read_webpages)) {
        clear_cache()
      }
    }
  )

  result
}


#' Configure Python Search Parameters
#'
#' Sets global configuration values for the Python search module.
#' These values control timeouts, retry behavior, and result limits.
#'
#' @param max_results Maximum number of search results to return (default: 10)
#' @param timeout HTTP request timeout in seconds (default: 30)
#' @param max_retries Maximum retry attempts on failure (default: 3)
#' @param retry_delay Initial delay between retries in seconds (default: 2)
#' @param backoff_multiplier Multiplier for exponential backoff (default: 1.5)
#' @param captcha_backoff_base Base multiplier for CAPTCHA backoff (default: 3)
#' @param page_load_wait Wait time after page load in seconds (default: 2)
#' @param inter_search_delay Delay between consecutive searches in seconds (default: 1.5)
#' @param conda_env Name of the conda environment. Defaults to the package
#'   option \code{asa.default_conda_env} (or \code{"asa_env"} if unset).
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
#'
#' # Add delay between searches to avoid rate limiting
#' configure_search(inter_search_delay = 2.0)
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
                             inter_search_delay = NULL,
                             conda_env = NULL) {
  conda_env <- conda_env %||% .get_default_conda_env()
  # Validate inputs
  .validate_configure_search(
    max_results = max_results,
    timeout = timeout,
    max_retries = max_retries,
    retry_delay = retry_delay,
    backoff_multiplier = backoff_multiplier,
    captcha_backoff_base = captcha_backoff_base,
    page_load_wait = page_load_wait,
    inter_search_delay = inter_search_delay,
    conda_env = conda_env
  )

  # Use specified conda environment
  reticulate::use_condaenv(conda_env, required = TRUE)

  # Import the backend API module and configure.
  tryCatch({
    ddg_module <- .import_backend_api(required = TRUE)
    config <- ddg_module$configure_search(
      max_results = max_results,
      timeout = timeout,
      max_retries = max_retries,
      retry_delay = retry_delay,
      backoff_multiplier = backoff_multiplier,
      captcha_backoff_base = captcha_backoff_base,
      page_load_wait = page_load_wait,
      inter_search_delay = inter_search_delay
    )

    # Build message showing which values were set
    set_values <- c()
    if (!is.null(max_results)) set_values <- c(set_values, paste0("max_results=", max_results))
    if (!is.null(timeout)) set_values <- c(set_values, paste0("timeout=", timeout))
    if (!is.null(max_retries)) set_values <- c(set_values, paste0("max_retries=", max_retries))
    if (!is.null(retry_delay)) set_values <- c(set_values, paste0("retry_delay=", retry_delay))
    if (!is.null(inter_search_delay)) set_values <- c(set_values, paste0("inter_search_delay=", inter_search_delay))

    if (length(set_values) > 0) {
      message("Search configuration updated: ", paste(set_values, collapse = ", "))
    }

    invisible(config)
  }, error = function(e) {
    warning("Could not configure search parameters: ", e$message, call. = FALSE)
    invisible(NULL)
  })
}
