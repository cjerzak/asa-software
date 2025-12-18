#' asa: AI Search Agent for Large-Scale Research Automation
#'
#' @description
#' The asa package provides an LLM-powered research agent for performing
#' AI search tasks at large scales using web search capabilities.
#'
#' The agent uses a ReAct (Reasoning + Acting) pattern implemented via
#' LangGraph, with tools for searching DuckDuckGo and Wikipedia. It supports
#' multiple LLM backends (OpenAI, Groq, xAI) and implements DeepAgent-style
#' memory folding for managing long conversations.
#'
#' @section Main Functions:
#' \itemize{
#'   \item \code{\link{build_backend}}: Set up the Python conda environment
#'   \item \code{\link{initialize_agent}}: Initialize the search agent
#'   \item \code{\link{run_agent}}: Run the agent with a custom prompt
#'   \item \code{\link{run_task}}: Run a structured task with the agent
#'   \item \code{\link{run_task_batch}}: Run multiple tasks in batch
#' }
#'
#' @section Configuration:
#' The package requires a Python environment with LangChain and related
#' packages. Use \code{\link{build_backend}} to create this environment
#' automatically.
#'
#' For anonymous searching, the package can use Tor as a SOCKS5 proxy.
#' Install Tor via \code{brew install tor} (macOS) and start it with
#' \code{brew services start tor}.
#'
#' @docType package
#' @name asa-package
#' @aliases asa
#' @keywords internal
"_PACKAGE"

# Package environment for storing Python objects and initialized state
# This avoids polluting the global environment and allows proper cleanup
asa_env <- new.env(parent = emptyenv())

#' Check if ASA Agent is Initialized
#'
#' @return Logical indicating if the agent has been initialized
#' @keywords internal
.is_initialized <- function() {

  exists("initialized", envir = asa_env) && isTRUE(asa_env$initialized)
}

#' Get Package Python Module Path
#'
#' Returns the path to the Python modules shipped with the package.
#'
#' @return Character string with the path to inst/python
#' @keywords internal
.get_python_path <- function() {
  system.file("python", package = "asa")
}

#' Get External Data Path
#'
#' Returns the path to the package's external data directory.
#'
#' @param filename Optional filename within extdata directory
#' @return Character string with the path
#' @keywords internal
.get_extdata_path <- function(filename = NULL) {
  if (is.null(filename)) {
    system.file("extdata", package = "asa")
  } else {
    system.file("extdata", filename, package = "asa")
  }
}

#' Close HTTP Clients
#'
#' Safely closes any open httpx clients to prevent resource leaks.
#' This is called automatically by reset_agent() and when reinitializing.
#'
#' @return Invisibly returns NULL
#' @keywords internal
.close_http_clients <- function() {
  # Close sync client if it exists

if (exists("http_clients", envir = asa_env) && !is.null(asa_env$http_clients)) {
    tryCatch({
      if (!is.null(asa_env$http_clients$sync)) {
        asa_env$http_clients$sync$close()
      }
    }, error = function(e) {
      # Silently ignore errors during cleanup
    })

    tryCatch({
      if (!is.null(asa_env$http_clients$async)) {
        # For async client, we need to use aclose() in an async context
        # but since we're in R, we'll try the sync close method if available
        # or just let Python's garbage collector handle it
        if (reticulate::py_has_attr(asa_env$http_clients$async, "aclose")) {
          # Try to close using asyncio
          tryCatch({
            asyncio <- reticulate::import("asyncio")
            loop <- tryCatch(asyncio$get_event_loop(), error = function(e) asyncio$new_event_loop())
            if (!loop$is_running()) {
              loop$run_until_complete(asa_env$http_clients$async$aclose())
            }
          }, error = function(e) {
            # If async close fails, the client will be garbage collected
          })
        }
      }
    }, error = function(e) {
      # Silently ignore errors during cleanup
    })

    asa_env$http_clients <- NULL
  }

  invisible(NULL)
}

#' @keywords internal
.onUnload <- function(libpath) {
  # Clean up HTTP clients when package is unloaded
  .close_http_clients()
}
