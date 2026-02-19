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

