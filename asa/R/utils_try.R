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

#' Try Expression with Fallback and Warning
#'
#' Like \code{.try_or()} but emits a \code{warning()} with the error message
#' and a context label before returning the default.
#'
#' @param expr Expression to evaluate
#' @param default Value to return on error (default: NULL)
#' @param context Label included in the warning message (default: "")
#'
#' @return Result of \code{expr}, or \code{default} on error (with warning)
#'
#' @keywords internal
.try_or_warn <- function(expr, default = NULL, context = "") {
  tryCatch(expr, error = function(e) {
    label <- if (nzchar(context)) paste0("[", context, "] ") else ""
    warning(label, "Falling back to default: ", conditionMessage(e), call. = FALSE)
    default
  })
}

