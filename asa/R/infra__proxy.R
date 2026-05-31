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

#' Is Required Tor Proxy Enforcement Enabled
#' @keywords internal
.require_tor_proxy_enabled <- function() {
  value <- tolower(trimws(Sys.getenv("ASA_REQUIRE_TOR_PROXY", unset = "true")))
  !value %in% c("0", "false", "f", "no", "n", "off")
}

#' Require a Local Tor SOCKS Proxy
#' @keywords internal
.require_tor_proxy <- function(proxy_info) {
  proxy <- trimws(as.character(proxy_info$proxy %||% ""))
  source <- as.character(proxy_info$source %||% "")
  mode <- as.character(proxy_info$mode %||% "")

  if (!nzchar(proxy)) {
    stop(
      paste0(
        "ASA_PROXY is required for ASA agent web/search pipelines. ",
        "Set ASA_PROXY to a local Tor SOCKS proxy such as ",
        "socks5h://127.0.0.1:9050, or set ASA_REQUIRE_TOR_PROXY=false ",
        "only for intentional direct-connection development/tests."
      ),
      call. = FALSE
    )
  }

  if (identical(mode, "auto") && !identical(source, "ASA_PROXY")) {
    stop(
      paste0(
        "ASA_PROXY is required for ASA agent web/search pipelines. ",
        "Found ", source, " instead, but HTTP_PROXY/HTTPS_PROXY are not accepted ",
        "for the production Tor guard. Set ASA_PROXY=socks5h://127.0.0.1:<port>."
      ),
      call. = FALSE
    )
  }

  match <- regexec("^socks5h://(127\\.0\\.0\\.1|localhost):([0-9]{1,5})$", proxy)
  parts <- regmatches(proxy, match)[[1]]
  if (length(parts) != 3L) {
    stop(
      sprintf(
        paste0(
          "Invalid proxy '%s'. ASA agent web/search pipelines require a local ",
          "Tor SOCKS proxy in the form socks5h://127.0.0.1:<port> or ",
          "socks5h://localhost:<port>."
        ),
        proxy
      ),
      call. = FALSE
    )
  }

  port <- suppressWarnings(as.integer(parts[[3L]]))
  if (is.na(port) || port < 1L || port > 65535L) {
    stop(
      sprintf(
        "Invalid proxy '%s'. Proxy port must be between 1 and 65535.",
        proxy
      ),
      call. = FALSE
    )
  }

  proxy
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
