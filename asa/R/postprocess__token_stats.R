#' Cast a Value to a Single Integer
#'
#' Internal helper that converts to a scalar integer, returning \code{default}
#' when conversion fails or yields a missing value.
#'
#' @param value Value to cast.
#' @param default Fallback scalar integer when casting fails.
#' @return Scalar integer.
#' @keywords internal
.as_scalar_int <- function(value, default = NA_integer_) {
  cast <- .try_or(as.integer(value), integer(0))
  if (length(cast) == 0 || is.na(cast[[1]])) {
    return(default)
  }
  cast[[1]]
}

#' Build Token Stats Diagnostic Structure
#'
#' Creates a standardized token usage breakdown from agent response fields.
#'
#' @param tokens_used Total tokens (integer)
#' @param input_tokens Input/prompt tokens (integer)
#' @param output_tokens Output/completion tokens (integer)
#' @param token_trace List of per-node trace entries
#'
#' @return A list with tokens_used, input_tokens, output_tokens, fold_tokens,
#'   and token_trace
#'
#' @keywords internal
.build_token_stats <- function(tokens_used, input_tokens, output_tokens,
                               token_trace) {
  fold_tokens <- 0L
  for (entry in token_trace) {
    if (is.list(entry) && identical(entry$node, "summarize")) {
      fold_tokens <- fold_tokens + .as_scalar_int(entry$total_tokens)
    }
  }
  # Ensure fold_tokens is not NA
  if (is.na(fold_tokens)) fold_tokens <- 0L
  list(
    tokens_used   = .as_scalar_int(tokens_used),
    input_tokens  = .as_scalar_int(input_tokens),
    output_tokens = .as_scalar_int(output_tokens),
    fold_tokens   = fold_tokens,
    token_trace   = token_trace
  )
}

