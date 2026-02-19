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

#' Import ASA Backend API Module
#'
#' Imports and caches the stable backend API module used by runtime/config
#' entrypoints. This intentionally targets the new package module path
#' instead of the legacy top-level compatibility import.
#'
#' @param required If TRUE, error on failure; if FALSE, return NULL
#'
#' @return Imported Python module (invisibly), or NULL when not required
#' @keywords internal
.import_backend_api <- function(required = TRUE) {
  .import_python_module(
    module_name = "asa_backend.agent_api",
    env_name = "backend_api",
    required = required
  )
}
