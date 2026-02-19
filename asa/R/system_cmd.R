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

