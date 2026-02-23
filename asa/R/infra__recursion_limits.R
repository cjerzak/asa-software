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
    config_names <- names(config) %||% character(0)
    if ("recusion_limit" %in% config_names && !("recursion_limit" %in% config_names)) {
      .stop_validation(
        "config$recursion_limit",
        "use the correctly spelled key name",
        actual = "recusion_limit",
        fix = "Rename `config$recusion_limit` to `config$recursion_limit`"
      )
    }
    config_limit <- .normalize_recursion_limit(config$recursion_limit %||% NULL)
  }
  if (!is.null(config_limit)) {
    .validate_recursion_limit(config_limit, "agent$config$recursion_limit")
    return(config_limit)
  }

  .default_recursion_limit(use_memory_folding = use_memory_folding)
}

