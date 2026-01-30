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
#' @noRd
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

#' Run External Command (processx if available, system2 fallback)
#' @keywords internal
.run_command <- function(command,
                         args = character(),
                         timeout = NULL,
                         stdin = NULL,
                         echo = FALSE) {
  has_processx <- tryCatch(requireNamespace("processx", quietly = TRUE), error = function(e) FALSE)

  if (has_processx) {
    return(processx::run(
      command = command,
      args = args,
      timeout = timeout,
      stdin = stdin %||% "",
      stdout = if (isTRUE(echo)) "" else "|",
      stderr = if (isTRUE(echo)) "" else "|",
      echo = isTRUE(echo),
      error_on_status = FALSE
    ))
  }

  # Fallback: base system2() with temp files for stdout/stderr.
  stdout_file <- tempfile()
  stderr_file <- tempfile()
  on.exit({
    unlink(stdout_file)
    unlink(stderr_file)
  }, add = TRUE)

  run_one <- function() {
    system2(
      command,
      args = args,
      stdout = if (isTRUE(echo)) "" else stdout_file,
      stderr = if (isTRUE(echo)) "" else stderr_file,
      stdin = stdin %||% "",
      wait = TRUE
    )
  }

  status <- tryCatch({
    if (!is.null(timeout)) {
      setTimeLimit(elapsed = timeout, transient = TRUE)
      on.exit(setTimeLimit(elapsed = Inf, transient = FALSE), add = TRUE)
    }
    run_one()
  }, error = function(e) {
    124L
  })

  stdout <- if (!isTRUE(echo) && file.exists(stdout_file)) {
    paste(readLines(stdout_file, warn = FALSE), collapse = "\n")
  } else {
    ""
  }

  stderr <- if (!isTRUE(echo) && file.exists(stderr_file)) {
    paste(readLines(stderr_file, warn = FALSE), collapse = "\n")
  } else {
    ""
  }

  list(status = as.integer(status), stdout = stdout, stderr = stderr)
}

#' Import Python Module into asa_env
#'
#' Generic helper for importing Python modules from inst/python.
#' Handles caching, path resolution, and error handling.
#'
#' @param module_name Name of the Python module (without .py)
#' @param env_name Name in asa_env (defaults to module_name)
#' @param required If TRUE, error on failure; if FALSE, return NULL
#'
#' @return The imported Python module (invisibly), or NULL on failure if not required
#'
#' @keywords internal
.import_python_module <- function(module_name, env_name = module_name, required = TRUE) {
  # Return cached module if available
  if (!is.null(asa_env[[env_name]])) {
    return(invisible(asa_env[[env_name]]))
  }

  # Get Python path
  python_path <- .get_python_path()
  if (python_path == "" || !dir.exists(python_path)) {
    if (required) {
      stop(sprintf("Python path not found for module '%s'. Package may not be installed correctly.",
                   module_name), call. = FALSE)
    }
    return(NULL)
  }

  # Attempt import
  asa_env[[env_name]] <- tryCatch(
    reticulate::import_from_path(module_name, path = python_path),
    error = function(e) {
      if (required) {
        stop(sprintf("Could not import Python module '%s': %s", module_name, e$message),
             call. = FALSE)
      }
      NULL
    }
  )

  invisible(asa_env[[env_name]])
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
#' @param conda_env Conda environment name for the Python module.
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
                                   conda_env = "asa_env") {

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

  if (is.null(registry_path) || registry_path == "") {
    base_dir <- tools::R_user_dir("asa", which = "cache")
    dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
    registry_path <- file.path(base_dir, "tor_exit_registry.sqlite")
  } else {
    dir.create(dirname(registry_path), recursive = TRUE, showWarnings = FALSE)
  }

  # Keep env vars in sync for child processes
  Sys.setenv(TOR_EXIT_DB = registry_path, ASA_TOR_EXIT_DB = registry_path)

  reticulate::use_condaenv(conda_env, required = TRUE)

  tryCatch({
    ddg_module <- reticulate::import("custom_ddg_production")
    cfg <- ddg_module$configure_tor_registry(
      registry_path = registry_path,
      enable = enable,
      bad_ttl = as.numeric(bad_ttl),
      good_ttl = as.numeric(good_ttl),
      overuse_threshold = as.integer(overuse_threshold),
      overuse_decay = as.numeric(overuse_decay),
      max_rotation_attempts = as.integer(max_rotation_attempts),
      ip_cache_ttl = as.numeric(ip_cache_ttl)
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
  prev_filter <- tryCatch(
    search_tool$api_wrapper$time,
    error = function(e) "none"
  )

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
  original_filter <- tryCatch({
    if (!is.null(tools) && length(tools) >= 2) {
      tools[[2]]$api_wrapper$time
    } else if (.is_initialized()) {
      asa_env$tools[[2]]$api_wrapper$time
    } else {
      "none"
    }
  }, error = function(e) "none")

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
.with_search_config <- function(search, conda_env = "asa_env", fn) {
  if (is.null(search)) {
    return(fn())
  }

  if (!inherits(search, "asa_search")) {
    if (is.list(search)) {
      search <- tryCatch(do.call(search_options, search), error = function(e) NULL)
    } else {
      warning("search config must be an asa_search object or list; ignoring", call. = FALSE)
      return(fn())
    }
  }

  if (is.null(search)) {
    return(fn())
  }

  previous <- tryCatch({
    cfg <- configure_search(conda_env = conda_env)
    if (is.null(cfg)) {
      return(NULL)
    }
    list(
      max_results = cfg$max_results,
      timeout = cfg$timeout,
      max_retries = cfg$max_retries,
      retry_delay = cfg$retry_delay,
      backoff_multiplier = cfg$backoff_multiplier,
      captcha_backoff_base = cfg$captcha_backoff_base,
      page_load_wait = cfg$page_load_wait,
      inter_search_delay = cfg$inter_search_delay
    )
  }, error = function(e) NULL)

  on.exit({
    if (!is.null(previous)) {
      tryCatch(
        configure_search(
          max_results = previous$max_results,
          timeout = previous$timeout,
          max_retries = previous$max_retries,
          retry_delay = previous$retry_delay,
          backoff_multiplier = previous$backoff_multiplier,
          captcha_backoff_base = previous$captcha_backoff_base,
          page_load_wait = previous$page_load_wait,
          inter_search_delay = previous$inter_search_delay,
          conda_env = conda_env
        ),
        error = function(e) NULL
      )
    }
  }, add = TRUE)

  tryCatch(
    configure_search(
      max_results = search$max_results,
      timeout = search$timeout,
      max_retries = search$max_retries,
      retry_delay = search$retry_delay,
      backoff_multiplier = search$backoff_multiplier,
      inter_search_delay = search$inter_search_delay,
      conda_env = conda_env
    ),
    error = function(e) NULL
  )

  fn()
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
#' @param inter_search_delay Delay between consecutive searches in seconds (default: 1.5)
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
                             conda_env = "asa_env") {
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
