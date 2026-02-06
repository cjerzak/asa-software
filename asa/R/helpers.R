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

#' Try Expression with Fallback
#'
#' Evaluates an expression, returning a default value on error.
#' Replaces the common \code{tryCatch(expr, error = function(e) NULL)} pattern.
#'
#' @param expr Expression to evaluate
#' @param default Value to return on error (default: NULL)
#'
#' @return Result of \code{expr}, or \code{default} on error
#'
#' @keywords internal
.try_or <- function(expr, default = NULL) {
  tryCatch(expr, error = function(e) default)
}

#' Safe Python Field Extraction
#'
#' Safely extracts a field from a Python/reticulate object with optional
#' type coercion. Handles NULL, missing fields, and conversion errors.
#'
#' @param obj Object to extract from (typically a reticulate Python object)
#' @param field Field name to extract
#' @param default Value to return if extraction fails
#' @param as_type Optional coercion function (e.g., \code{as.integer})
#'
#' @return Extracted value, or \code{default} on failure
#'
#' @keywords internal
.py_get <- function(obj, field, default = NULL, as_type = NULL) {
  val <- .try_or(obj[[field]], default = default)
  if (is.null(val)) return(default)
  if (!is.null(as_type)) .try_or(as_type(val), default = default) else val
}

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

#' Resolve Proxy Configuration
#'
#' Interprets ASA proxy inputs consistently across functions:
#' - `NA`: auto-detect from environment variables (ASA_PROXY, HTTP_PROXY, HTTPS_PROXY)
#' - `NULL`: disable proxying
#' - string: explicit proxy URL
#'
#' @param proxy Proxy specification (NA, NULL, or character scalar)
#' @param env_vars Character vector of env vars to consult in auto mode
#' @return A list with `proxy` (character scalar or NULL), `mode`, and `source`
#' @keywords internal
.resolve_proxy <- function(proxy,
                           env_vars = c("ASA_PROXY", "HTTP_PROXY", "HTTPS_PROXY")) {
  # Explicit disable
  if (is.null(proxy)) {
    return(list(proxy = NULL, mode = "off", source = NULL))
  }

  # Auto mode (NA)
  if (isTRUE(is.na(proxy))) {
    for (var in env_vars) {
      value <- Sys.getenv(var, unset = "")
      if (nzchar(value)) {
        return(list(proxy = value, mode = "auto", source = var))
      }
    }
    return(list(proxy = NULL, mode = "auto", source = NULL))
  }

  # Explicit string
  if (!is.character(proxy) || length(proxy) != 1) {
    stop("`proxy` must be NA (auto), NULL (disable), or a single proxy URL string.",
         call. = FALSE)
  }
  if (!nzchar(proxy)) {
    return(list(proxy = NULL, mode = "off", source = NULL))
  }
  list(proxy = proxy, mode = "manual", source = NULL)
}

#' Format Proxy for Printing
#' @keywords internal
.format_proxy <- function(proxy, mode = NULL, source = NULL) {
  if (!is.null(mode)) {
    mode <- as.character(mode)
    if (identical(mode, "auto")) {
      if (is.null(proxy) || (is.character(proxy) && length(proxy) == 1 && !nzchar(proxy))) {
        return("Auto (none found)")
      }
      if (!is.null(source) && nzchar(source)) {
        return(paste0("Auto (", source, "): ", proxy))
      }
      return(paste0("Auto: ", proxy))
    }
    if (identical(mode, "off")) {
      return("None")
    }
    # manual or unknown mode falls back to showing proxy
  }

  if (is.null(proxy)) return("None")
  if (isTRUE(is.na(proxy))) return("Auto")
  as.character(proxy)
}

#' Normalize Optional Recursion Limit
#' @param recursion_limit Optional recursion limit value
#' @return Integer scalar or NULL
#' @keywords internal
.normalize_recursion_limit <- function(recursion_limit) {
  if (is.null(recursion_limit) || length(recursion_limit) == 0) {
    return(NULL)
  }
  value <- suppressWarnings(as.integer(recursion_limit)[1])
  if (is.na(value)) {
    return(NULL)
  }
  value
}

#' Default Recursion Limit for Agent Mode
#' @param use_memory_folding Whether memory folding is enabled
#' @return Integer recursion limit
#' @keywords internal
.default_recursion_limit <- function(use_memory_folding = TRUE) {
  if (isTRUE(use_memory_folding)) {
    as.integer(ASA_RECURSION_LIMIT_FOLDING)
  } else {
    as.integer(ASA_RECURSION_LIMIT_STANDARD)
  }
}

#' Resolve Effective Recursion Limit
#' @param recursion_limit Per-call override
#' @param config Agent config list
#' @param use_memory_folding Whether memory folding is enabled
#' @return Integer recursion limit used for invocation
#' @keywords internal
.resolve_effective_recursion_limit <- function(recursion_limit = NULL,
                                               config = NULL,
                                               use_memory_folding = TRUE) {
  if (!is.null(recursion_limit)) {
    .validate_recursion_limit(recursion_limit, "recursion_limit")
    return(as.integer(recursion_limit))
  }

  config_limit <- NULL
  if (!is.null(config)) {
    config_limit <- .normalize_recursion_limit(config$recursion_limit %||% NULL)
  }
  if (!is.null(config_limit)) {
    .validate_recursion_limit(config_limit, "agent$config$recursion_limit")
    return(config_limit)
  }

  .default_recursion_limit(use_memory_folding = use_memory_folding)
}

#' Check Whether an Agent Matches an asa_config
#' @keywords internal
.agent_matches_config <- function(agent, config) {
  if (is.null(agent) || is.null(config)) return(FALSE)
  if (!inherits(agent, "asa_agent")) return(FALSE)
  if (!inherits(config, "asa_config")) return(FALSE)

  desired_proxy <- .try_or({
    info <- .resolve_proxy(config$proxy)
    proxy_value <- info$proxy
    if (identical(info$mode, "auto") && !is.null(proxy_value)) {
      proxy_value <- .try_or({
        .validate_proxy_url(proxy_value, "proxy")
        proxy_value
      })
    }
    proxy_value
  }, config$proxy)

  agent_folding <- agent$config$use_memory_folding %||% agent$config$memory_folding

  agent_use_browser <- agent$config$use_browser %||% ASA_DEFAULT_USE_BROWSER
  config_use_browser <- config$use_browser %||% ASA_DEFAULT_USE_BROWSER

  same_backend <- identical(agent$backend, config$backend)
  same_model <- identical(agent$model, config$model)
  same_conda <- identical(agent$config$conda_env, config$conda_env)
  same_proxy <- identical(agent$config$proxy, desired_proxy)
  same_browser <- identical(agent_use_browser, config_use_browser)
  same_folding <- identical(agent_folding, config$memory_folding)
  same_threshold <- identical(agent$config$memory_threshold, config$memory_threshold)
  same_keep <- identical(agent$config$memory_keep_recent, config$memory_keep_recent)
  same_rate <- identical(agent$config$rate_limit, config$rate_limit)
  same_timeout <- identical(agent$config$timeout, config$timeout)
  same_recursion_limit <- identical(
    .normalize_recursion_limit(agent$config$recursion_limit %||% NULL),
    .normalize_recursion_limit(config$recursion_limit %||% NULL)
  )
  same_tor <- identical(agent$config$tor, config$tor)

  # Some search settings affect tool construction (Wikipedia/Search snippet caps).
  # Treat changes as requiring a new agent instance.
  agent_search <- agent$config$search %||% NULL
  config_search <- config$search %||% NULL

  agent_wiki_top_k <- ASA_DEFAULT_WIKI_TOP_K
  agent_wiki_chars <- ASA_DEFAULT_WIKI_CHARS
  agent_search_chars <- 500L
  if (!is.null(agent_search) && (inherits(agent_search, "asa_search") || is.list(agent_search))) {
    agent_wiki_top_k <- agent_search$wiki_top_k_results %||% agent_wiki_top_k
    agent_wiki_chars <- agent_search$wiki_doc_content_chars_max %||% agent_wiki_chars
    agent_search_chars <- agent_search$search_doc_content_chars_max %||% agent_search_chars
  }

  config_wiki_top_k <- ASA_DEFAULT_WIKI_TOP_K
  config_wiki_chars <- ASA_DEFAULT_WIKI_CHARS
  config_search_chars <- 500L
  if (!is.null(config_search) && (inherits(config_search, "asa_search") || is.list(config_search))) {
    config_wiki_top_k <- config_search$wiki_top_k_results %||% config_wiki_top_k
    config_wiki_chars <- config_search$wiki_doc_content_chars_max %||% config_wiki_chars
    config_search_chars <- config_search$search_doc_content_chars_max %||% config_search_chars
  }

  same_search_tools <- identical(as.integer(agent_wiki_top_k), as.integer(config_wiki_top_k)) &&
    identical(as.integer(agent_wiki_chars), as.integer(config_wiki_chars)) &&
    identical(as.integer(agent_search_chars), as.integer(config_search_chars))

  isTRUE(same_backend && same_model && same_conda && same_proxy && same_browser &&
           same_folding && same_threshold && same_keep &&
           same_rate && same_timeout && same_recursion_limit &&
           same_tor && same_search_tools)
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

#' Run External Command (processx if available, system2 fallback)
#' @keywords internal
.run_command <- function(command,
                         args = character(),
                         timeout = NULL,
                         stdin = NULL,
                         echo = FALSE) {
  has_processx <- .try_or(requireNamespace("processx", quietly = TRUE), FALSE)

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

#' Normalize Tor registry settings
#'
#' Shared helper used by tor_options() and configure_tor_registry() to keep
#' defaults and type coercions consistent.
#'
#' @param registry_path Registry path (NULL/"" for default)
#' @param enable Enable/disable registry tracking
#' @param bad_ttl Seconds to keep a bad/tainted exit before reuse
#' @param good_ttl Seconds to treat an exit as good before refreshing
#' @param overuse_threshold Maximum recent uses before treating an exit as overloaded
#' @param overuse_decay Window (seconds) for counting recent uses
#' @param max_rotation_attempts Maximum rotations to find a clean exit
#' @param ip_cache_ttl Seconds to cache exit IP lookups
#' @param create_parent_for_custom If TRUE, create parent dir for custom paths
#' @return List with normalized fields (including resolved registry_path)
#' @keywords internal
.normalize_tor_registry_options <- function(registry_path = NULL,
                                           enable = ASA_TOR_REGISTRY_ENABLED,
                                           bad_ttl = ASA_TOR_BAD_TTL,
                                           good_ttl = ASA_TOR_GOOD_TTL,
                                           overuse_threshold = ASA_TOR_OVERUSE_THRESHOLD,
                                           overuse_decay = ASA_TOR_OVERUSE_DECAY,
                                           max_rotation_attempts = ASA_TOR_MAX_ROTATION_ATTEMPTS,
                                           ip_cache_ttl = ASA_TOR_IP_CACHE_TTL,
                                           create_parent_for_custom = FALSE) {
  if (is.null(registry_path) || registry_path == "") {
    base_dir <- tools::R_user_dir("asa", which = "cache")
    dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
    registry_path <- file.path(base_dir, "tor_exit_registry.sqlite")
  } else if (isTRUE(create_parent_for_custom)) {
    dir.create(dirname(registry_path), recursive = TRUE, showWarnings = FALSE)
  }

  list(
    registry_path = registry_path,
    enable = isTRUE(enable),
    bad_ttl = as.numeric(bad_ttl),
    good_ttl = as.numeric(good_ttl),
    overuse_threshold = as.integer(overuse_threshold),
    overuse_decay = as.numeric(overuse_decay),
    max_rotation_attempts = as.integer(max_rotation_attempts),
    ip_cache_ttl = as.numeric(ip_cache_ttl)
  )
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
    ddg_module <- reticulate::import("custom_ddg_production")
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
#' @param webpage_embedding_provider Passed through to .resolve_webpage_reader_settings()
#' @param webpage_embedding_model Passed through to .resolve_webpage_reader_settings()
#' @return List with temporal + resolved webpage reader settings
#' @keywords internal
.resolve_temporal_and_webpage_reader <- function(temporal = NULL,
                                                config = NULL,
                                                config_search = NULL,
                                                allow_read_webpages = NULL,
                                                webpage_relevance_mode = NULL,
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
                                                webpage_user_agent = NULL) {
  temporal <- .resolve_temporal_input(temporal, config)
  webpage_settings <- .resolve_webpage_reader_settings(
    config_search,
    allow_read_webpages,
    webpage_relevance_mode,
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
    webpage_user_agent
  )

  list(
    temporal = temporal,
    allow_read_webpages = webpage_settings$allow_read_webpages,
    relevance_mode = webpage_settings$relevance_mode,
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
    user_agent = webpage_settings$user_agent
  )
}

.resolve_agent_config_value <- function(config = NULL, agent = NULL, key) {
  value <- NULL

  if (!is.null(config) && inherits(config, "asa_config")) {
    value <- config[[key]] %||% NULL
  }

  if (is.null(value)) {
    if (!is.null(agent) && inherits(agent, "asa_agent")) {
      value <- agent$config[[key]] %||% NULL
    } else if (is.null(agent) && .is_initialized()) {
      value <- .try_or(get_agent()$config[[key]] %||% NULL)
    }
  }

  value
}

.resolve_runtime_settings <- function(config = NULL,
                                     agent = NULL,
                                     temporal = NULL,
                                     allow_read_webpages = NULL,
                                     webpage_relevance_mode = NULL,
                                     webpage_embedding_provider = NULL,
                                     webpage_embedding_model = NULL) {
  config_search <- .resolve_agent_config_value(config = config, agent = agent, key = "search")
  config_conda_env <- .resolve_agent_config_value(config = config, agent = agent, key = "conda_env")

  resolved <- .resolve_temporal_and_webpage_reader(
    temporal = temporal,
    config = config,
    config_search = config_search,
    allow_read_webpages = allow_read_webpages,
    webpage_relevance_mode = webpage_relevance_mode,
    webpage_embedding_provider = webpage_embedding_provider,
    webpage_embedding_model = webpage_embedding_model
  )

  c(
    list(
      config_search = config_search,
      config_conda_env = config_conda_env
    ),
    resolved
  )
}

.resolve_runtime_inputs <- function(config = NULL,
                                   agent = NULL,
                                   temporal = NULL,
                                   allow_read_webpages = NULL,
                                   webpage_relevance_mode = NULL,
                                   webpage_embedding_provider = NULL,
                                   webpage_embedding_model = NULL) {
  runtime <- .resolve_runtime_settings(
    config = config,
    agent = agent,
    temporal = temporal,
    allow_read_webpages = allow_read_webpages,
    webpage_relevance_mode = webpage_relevance_mode,
    webpage_embedding_provider = webpage_embedding_provider,
    webpage_embedding_model = webpage_embedding_model
  )

  list(
    runtime = runtime,
    temporal = runtime$temporal,
    allow_rw = runtime$allow_read_webpages
  )
}

.with_runtime_wrappers <- function(runtime, conda_env = NULL, agent = NULL, fn) {
  conda_env <- conda_env %||% runtime$config_conda_env %||% .get_default_conda_env()

  .with_search_config(runtime$config_search, conda_env = conda_env, function() {
    .with_webpage_reader_config(
      runtime$allow_read_webpages,
      relevance_mode = runtime$relevance_mode,
      embedding_provider = runtime$embedding_provider,
      embedding_model = runtime$embedding_model,
      timeout = runtime$timeout,
      max_bytes = runtime$max_bytes,
      max_chars = runtime$max_chars,
      max_chunks = runtime$max_chunks,
      chunk_chars = runtime$chunk_chars,
      embedding_api_base = runtime$embedding_api_base,
      prefilter_k = runtime$prefilter_k,
      use_mmr = runtime$use_mmr,
      mmr_lambda = runtime$mmr_lambda,
      cache_enabled = runtime$cache_enabled,
      cache_max_entries = runtime$cache_max_entries,
      cache_max_text_chars = runtime$cache_max_text_chars,
      user_agent = runtime$user_agent,
      conda_env = conda_env,
      fn = function() {
        .with_temporal(runtime$temporal, fn, agent = agent)
      }
    )
  })
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
.with_search_config <- function(search, conda_env = NULL, fn) {
  conda_env <- conda_env %||% .get_default_conda_env()
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
.resolve_option <- function(value, config, key) {
  if (is.null(value) && is.list(config) && !is.null(config[[key]])) config[[key]] else value
}

#' Resolve Webpage Reader Settings
#'
#' Internal helper that merges per-call webpage reader options with defaults
#' from config$search.
#'
#' @param config_search asa_search object or list of search settings
#' @param allow_read_webpages TRUE/FALSE/NULL
#' @param webpage_relevance_mode "auto", "lexical", "embeddings", or NULL
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
#' @param webpage_user_agent User-Agent string for webpage fetches, or NULL.
#' @return List with resolved settings
#' @keywords internal
.resolve_webpage_reader_settings <- function(config_search = NULL,
                                             allow_read_webpages = NULL,
                                             webpage_relevance_mode = NULL,
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
                                             webpage_user_agent = NULL) {
  # Map from result-list key -> (explicit value, config_search key)
  fields <- list(
    allow_read_webpages  = allow_read_webpages,
    relevance_mode       = webpage_relevance_mode,
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
    user_agent           = webpage_user_agent
  )

  # config_search keys use the "webpage_" prefix except allow_read_webpages
  config_keys <- c(
    allow_read_webpages  = "allow_read_webpages",
    relevance_mode       = "webpage_relevance_mode",
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
    user_agent           = "webpage_user_agent"
  )

  result <- lapply(names(fields), function(nm) {
    .resolve_option(fields[[nm]], config_search, config_keys[[nm]])
  })
  names(result) <- names(fields)
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
#' @param user_agent User-Agent string for webpage fetches.
#' @param conda_env Conda env used by Python tools
#' @param fn Function to run with webpage reader config applied
#' @return Result of fn()
#' @keywords internal
.with_webpage_reader_config <- function(allow_read_webpages,
                                       relevance_mode = NULL,
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
                                       user_agent = NULL,
                                       conda_env = NULL,
                                       fn) {
  conda_env <- conda_env %||% .get_default_conda_env()
  # If nothing was specified, don't touch Python config.
  if (is.null(allow_read_webpages) &&
      is.null(relevance_mode) &&
      is.null(embedding_provider) &&
      is.null(embedding_model) &&
      is.null(timeout) &&
      is.null(max_bytes) &&
      is.null(max_chars) &&
      is.null(max_chunks) &&
      is.null(chunk_chars) &&
      is.null(embedding_api_base) &&
      is.null(prefilter_k) &&
      is.null(use_mmr) &&
      is.null(mmr_lambda) &&
      is.null(cache_enabled) &&
      is.null(cache_max_entries) &&
      is.null(cache_max_text_chars) &&
      is.null(user_agent)) {
    return(fn())
  }

  # Ensure conda env is active (module lives in that env).
  reticulate::use_condaenv(conda_env, required = TRUE)

  # Ensure module is available. initialize_agent() imports this already, but
  # keep this resilient for call paths that bypass initialization order.
  if (is.null(asa_env$webpage_tool)) {
    .import_python_module("webpage_tool", env_name = "webpage_tool", required = FALSE)
  }
  if (is.null(asa_env$webpage_tool)) {
    warning("Webpage reader module unavailable; continuing without webpage reading.", call. = FALSE)
    return(fn())
  }

  previous <- tryCatch({
    cfg <- asa_env$webpage_tool$configure_webpage_reader()
    list(
      allow_read_webpages = cfg$allow_read_webpages,
      relevance_mode = cfg$relevance_mode,
      embedding_provider = cfg$embedding_provider,
      embedding_model = cfg$embedding_model,
      timeout = cfg$timeout,
      max_bytes = cfg$max_bytes,
      max_chars = cfg$max_chars,
      max_chunks = cfg$max_chunks,
      chunk_chars = cfg$chunk_chars,
      embedding_api_base = cfg$embedding_api_base,
      prefilter_k = cfg$prefilter_k,
      use_mmr = cfg$use_mmr,
      mmr_lambda = cfg$mmr_lambda,
      cache_enabled = cfg$cache_enabled,
      cache_max_entries = cfg$cache_max_entries,
      cache_max_text_chars = cfg$cache_max_text_chars,
      user_agent = cfg$user_agent
    )
  }, error = function(e) NULL)

  on.exit({
    if (!is.null(previous) && !is.null(previous$allow_read_webpages)) {
      tryCatch(
        asa_env$webpage_tool$configure_webpage_reader(
          allow_read_webpages = previous$allow_read_webpages,
          relevance_mode = previous$relevance_mode,
          embedding_provider = previous$embedding_provider,
          embedding_model = previous$embedding_model,
          timeout = previous$timeout,
          max_bytes = previous$max_bytes,
          max_chars = previous$max_chars,
          max_chunks = previous$max_chunks,
          chunk_chars = previous$chunk_chars,
          embedding_api_base = previous$embedding_api_base,
          prefilter_k = previous$prefilter_k,
          use_mmr = previous$use_mmr,
          mmr_lambda = previous$mmr_lambda,
          cache_enabled = previous$cache_enabled,
          cache_max_entries = previous$cache_max_entries,
          cache_max_text_chars = previous$cache_max_text_chars,
          user_agent = previous$user_agent
        ),
        error = function(e) NULL
      )
    }

    # Clear any per-run cache after the operation.
    if (isTRUE(allow_read_webpages)) {
      tryCatch(
        asa_env$webpage_tool$clear_webpage_reader_cache(),
        error = function(e) NULL
      )
    }
  }, add = TRUE)

  # Clear cache at the start of this operation (per-run caching semantics).
  if (isTRUE(allow_read_webpages)) {
    tryCatch(
      asa_env$webpage_tool$clear_webpage_reader_cache(),
      error = function(e) NULL
    )
  }

  allow_arg <- if (is.null(allow_read_webpages)) NULL else isTRUE(allow_read_webpages)

  tryCatch(
    asa_env$webpage_tool$configure_webpage_reader(
      allow_read_webpages = allow_arg,
      relevance_mode = relevance_mode,
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
      user_agent = user_agent
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
