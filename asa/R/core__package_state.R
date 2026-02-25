#' asa: AI Search Agent for Large-Scale Research Automation
#'
#' @description
#' The asa package provides an LLM-powered research agent for performing
#' AI search tasks at large scales using web search capabilities.
#'
#' The agent uses a ReAct (Reasoning + Acting) pattern implemented via
#' LangGraph, with tools for searching DuckDuckGo and Wikipedia. It supports
#' multiple LLM backends (OpenAI, Groq, xAI, Gemini, Anthropic, Bedrock,
#' OpenRouter, Exo) and implements DeepAgent-style
#' memory folding for managing long conversations.
#'
#' @section Main Functions:
#' \itemize{
#'   \item \code{\link{build_backend}}: Set up the Python conda environment
#'   \item \code{\link{initialize_agent}}: Initialize the search agent
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
#' @importFrom utils capture.output tail
#' @keywords internal
"_PACKAGE"

# Package environment for storing Python objects and initialized state
# This avoids polluting the global environment and allows proper cleanup
asa_env <- new.env(parent = emptyenv())

# Initialize counter for tracking re-initializations (resource leak prevention)
asa_env$init_count <- 0L

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

#' Resolve Runtime Control API Module
#'
#' @param required If TRUE, throw an error when backend API is unavailable.
#' @return Python backend API module or NULL.
#' @keywords internal
.runtime_control_api <- function(required = FALSE) {
  backend_api <- asa_env$backend_api %||% NULL

  if (is.null(backend_api) && exists(".import_backend_api", mode = "function")) {
    backend_api <- tryCatch(
      .import_backend_api(required = FALSE),
      error = function(e) NULL
    )
  }

  if (required && is.null(backend_api)) {
    stop("Backend API not available; runtime controls require an initialized Python backend.", call. = FALSE)
  }

  backend_api
}

#' Call Python Runtime Control Method
#'
#' @param method Runtime method name exported by asa_backend.api.agent_api.
#' @param ... Arguments forwarded to the runtime method.
#' @param default Default return value when backend/runtime method is unavailable.
#' @param required If TRUE, throw when method is unavailable or raises.
#' @param quiet If FALSE, emit debug diagnostics on runtime call failure.
#' @return Runtime call result converted to R, or default on soft failure.
#' @keywords internal
.runtime_control_call <- function(method, ..., default = NULL, required = FALSE, quiet = TRUE) {
  backend_api <- .runtime_control_api(required = required)
  if (is.null(backend_api) || !requireNamespace("reticulate", quietly = TRUE)) {
    return(default)
  }

  has_method <- tryCatch(
    isTRUE(reticulate::py_has_attr(backend_api, method)),
    error = function(e) FALSE
  )
  if (!has_method) {
    if (required) {
      stop("Backend runtime method missing: ", method, call. = FALSE)
    }
    return(default)
  }

  method_fn <- tryCatch(
    reticulate::py_get_attr(backend_api, method),
    error = function(e) NULL
  )
  if (is.null(method_fn) || !is.function(method_fn)) {
    if (required) {
      stop("Backend runtime method is not callable: ", method, call. = FALSE)
    }
    return(default)
  }

  out <- tryCatch(
    method_fn(...),
    error = function(e) {
      if (!isTRUE(quiet) && isTRUE(getOption("asa.debug"))) {
        message("[asa] runtime control call failed (", method, "): ", conditionMessage(e))
      }
      if (required) {
        stop("Backend runtime call failed (", method, "): ", conditionMessage(e), call. = FALSE)
      }
      return(default)
    }
  )

  if (is.null(out)) {
    return(default)
  }

  tryCatch(
    reticulate::py_to_r(out),
    error = function(e) out
  )
}

#' Initialize Python Runtime Controls
#'
#' @param config Optional named list of runtime-control configuration values.
#' @return Invisibly returns runtime control snapshot (or empty list).
#' @keywords internal
.runtime_control_init <- function(config = NULL) {
  out <- .runtime_control_call(
    "runtime_control_init",
    config = config,
    default = list(),
    quiet = FALSE
  )
  invisible(out)
}

#' Reset Python Runtime Controls
#'
#' @return Invisibly returns runtime control snapshot (or empty list).
#' @keywords internal
.runtime_control_reset <- function() {
  out <- .runtime_control_call(
    "runtime_control_reset",
    default = list(),
    quiet = FALSE
  )
  invisible(out)
}

#' Register HTTP Clients in Python Runtime
#'
#' @param direct Direct httpx client for model/provider requests.
#' @param proxied Proxied httpx client for search requests.
#' @return Invisibly returns NULL.
#' @keywords internal
.runtime_register_http_clients <- function(direct = NULL, proxied = NULL) {
  .runtime_control_call(
    "runtime_clients_register",
    direct = direct,
    proxied = proxied,
    default = NULL,
    quiet = FALSE
  )
  invisible(NULL)
}

#' Close HTTP Clients
#'
#' Safely closes the synchronous httpx client to prevent resource leaks.
#' This is called automatically by reset_agent() and when reinitializing.
#'
#' Note: We no longer create or manage async clients from R (R-CRIT-001 fix).
#' LangChain manages its own async client lifecycle internally.
#'
#' @return Invisibly returns NULL
#' @keywords internal
.close_http_clients <- function() {
  # Primary path: Python runtime owns HTTP client lifecycle.
  .runtime_control_call("runtime_clients_close", default = NULL, quiet = FALSE)

  # Legacy fallback in case clients were created before runtime delegation.
  if (exists("http_clients", envir = asa_env) && !is.null(asa_env$http_clients)) {
    if (!is.null(asa_env$http_clients$direct)) {
      tryCatch({
        asa_env$http_clients$direct$close()
      }, error = function(e) {
        if (isTRUE(getOption("asa.debug"))) {
          message("[asa] HTTP direct client cleanup error: ", conditionMessage(e))
        }
      })
    }

    if (!is.null(asa_env$http_clients$proxied)) {
      tryCatch({
        asa_env$http_clients$proxied$close()
      }, error = function(e) {
        if (isTRUE(getOption("asa.debug"))) {
          message("[asa] HTTP proxied client cleanup error: ", conditionMessage(e))
        }
      })
    }
  }

  asa_env$http_clients <- NULL
  invisible(NULL)
}

# ============================================================================
# HUMANIZED TIMING - The Nervous Pulse of a Tired Hand
# ============================================================================
# The problem with clean randomness: it's too clean. Uniform distributions
# smell like bleach. Real humans have texture - hesitation, fatigue,
# distraction, the micro-stutter of uncertainty.

# Session state for fatigue modeling
asa_env$session_start <- NULL
asa_env$request_count <- 0L

#' Generate a Delay That Feels Human
#'
#' Not uniform jitter. This models the messy, inefficient pause between
#' intention and action - the entropy of a tired hand:
#' \itemize{
#'   \item Log-normal base: most actions quick, occasional long pauses (thinking)
#'   \item Micro-stutters: tiny random additions (the tremor of uncertainty)
#'   \item Fatigue curve: delays drift longer as session ages
#'   \item Occasional spikes: the pause of a mind changing
#' }
#'
#' @param base_delay The nominal delay in seconds
#' @param enabled Whether humanized timing is enabled (default from constants)
#' @return A delay that breathes like a human
#' @keywords internal
.humanize_delay <- function(base_delay, enabled = NULL) {
  enabled <- enabled %||% ASA_HUMANIZE_TIMING

  if (!enabled || base_delay <= 0) {
    return(base_delay)
  }

  # Initialize session tracking on first call
  if (is.null(asa_env$session_start)) {
    asa_env$session_start <- Sys.time()
  }
  asa_env$request_count <- asa_env$request_count + 1L

  # 1. Log-normal base: right-skewed, mostly quick but occasional long pauses
  # sigma = 0.4: moderate spread
  log_normal_factor <- stats::rlnorm(1, meanlog = 0, sdlog = 0.4)
  log_normal_factor <- max(0.5, min(3.0, log_normal_factor))  # Clamp to 0.5x-3x

  # 2. Micro-stutter: tiny random addition (50-200ms) - the tremor
  micro_stutter <- stats::runif(1, 0.05, 0.2)

  # 3. Fatigue curve: delays drift longer over session
  session_minutes <- as.numeric(difftime(Sys.time(), asa_env$session_start, units = "mins"))
  fatigue_factor <- 1.0 + (session_minutes * 0.01) + (asa_env$request_count * 0.001)
  fatigue_factor <- min(fatigue_factor, 1.5)  # Cap at 50% increase

  # 4. Occasional thinking pause: 5% chance of a longer hesitation
  thinking_pause <- 0
  if (stats::runif(1) < 0.05) {
    thinking_pause <- stats::runif(1, 0.5, 2.0)
  }

  # 5. The hesitation before commit: slight pause before action
  pre_commit_hesitation <- stats::runif(1, 0.02, 0.08)

  # Combine: base * log_normal * fatigue + stutter + thinking + hesitation
  delay <- (base_delay * log_normal_factor * fatigue_factor +
            micro_stutter + thinking_pause + pre_commit_hesitation)

  max(0.1, delay)
}

# ============================================================================
# PROACTIVE RATE LIMITING (Token Bucket Implementation)
# ============================================================================

#' Initialize Rate Limiter
#'
#' Initializes the token bucket rate limiter in asa_env. Called automatically
#' on first use if not already initialized.
#'
#' @param rate Requests per second (tokens refill rate)
#' @param bucket_size Maximum tokens in bucket
#' @return Invisibly returns NULL
#' @keywords internal
.rate_limiter_init <- function(rate = NULL, bucket_size = NULL) {
  resolved_rate <- if (is.null(rate)) ASA_DEFAULT_RATE_LIMIT else rate
  resolved_bucket <- if (is.null(bucket_size)) ASA_RATE_LIMIT_BUCKET_SIZE else bucket_size
  .runtime_control_init(config = list(
    rate_limit = as.numeric(resolved_rate),
    rate_limit_bucket_size = as.numeric(resolved_bucket)
  ))
  invisible(NULL)
}

#' Acquire a Rate Limit Token (Proactive Rate Limiting)
#'
#' Acquires a token from the runtime rate limiter before making a request.
#' If no tokens are available, waits until one becomes available.
#'
#' @param verbose Print waiting message if TRUE
#' @return The wait time in seconds (0 if no wait was needed)
#' @keywords internal
.acquire_rate_limit_token <- function(verbose = FALSE) {
  if (!isTRUE(ASA_RATE_LIMIT_PROACTIVE)) {
    return(0)
  }

  wait_time <- .runtime_control_call(
    "runtime_rate_acquire",
    verbose = isTRUE(verbose),
    default = 0,
    quiet = FALSE
  )
  wait_time <- suppressWarnings(as.numeric(wait_time)[1])
  if (!is.finite(wait_time) || wait_time < 0) {
    wait_time <- 0
  }
  wait_time
}

#' Reset Rate Limiter
#'
#' Resets the runtime rate limiter to full capacity.
#'
#' @return Invisibly returns NULL
#' @keywords internal
.rate_limiter_reset <- function() {
  .runtime_control_call("runtime_rate_reset", default = NULL, quiet = FALSE)
  invisible(NULL)
}

# ============================================================================
# CIRCUIT BREAKER (Prevents Cascading Failures in Batch Runs)
# ============================================================================

#' Initialize Circuit Breaker
#'
#' Initializes the circuit breaker in asa_env. Called automatically
#' before batch operations if circuit_breaker=TRUE.
#'
#' @return Invisibly returns NULL
#' @keywords internal
.circuit_breaker_init <- function() {
  .runtime_control_call("runtime_circuit_init", default = list(), quiet = FALSE)
  invisible(NULL)
}

#' Record Result in Circuit Breaker
#'
#' Records a success or error in the circuit breaker's sliding window.
#' If error rate exceeds threshold, trips the breaker.
#'
#' @param status Either "success" or "error"
#' @param verbose Print message when breaker trips
#' @return Invisibly returns whether breaker is now tripped
#' @keywords internal
.circuit_breaker_record <- function(status, verbose = FALSE) {
  tripped <- .runtime_control_call(
    "runtime_circuit_record",
    status = status,
    verbose = isTRUE(verbose),
    default = FALSE,
    quiet = FALSE
  )
  invisible(isTRUE(tripped))
}

#' Check Circuit Breaker State
#'
#' Checks if the circuit breaker is tripped. If cooldown has passed,
#' automatically resets the breaker.
#'
#' @param verbose Print message when breaker resets
#' @return TRUE if requests can proceed, FALSE if breaker is tripped
#' @keywords internal
.circuit_breaker_check <- function(verbose = FALSE) {
  can_proceed <- .runtime_control_call(
    "runtime_circuit_check",
    verbose = isTRUE(verbose),
    default = TRUE,
    quiet = FALSE
  )
  isTRUE(can_proceed)
}

#' Get Circuit Breaker Status
#'
#' Returns the current state of the circuit breaker for monitoring.
#'
#' @return List with tripped, error_rate, recent_count, and trip_count
#' @keywords internal
.circuit_breaker_status <- function() {
  status <- .runtime_control_call(
    "runtime_circuit_status",
    default = list(),
    quiet = FALSE
  )
  if (!is.list(status)) {
    status <- list()
  }
  tripped <- isTRUE(status$tripped)
  error_rate <- suppressWarnings(as.numeric(status$error_rate)[1])
  if (!is.finite(error_rate) || error_rate < 0) {
    error_rate <- 0
  }
  recent_count <- suppressWarnings(as.integer(status$recent_count)[1])
  if (!is.finite(recent_count) || recent_count < 0) {
    recent_count <- 0L
  }
  trip_count <- suppressWarnings(as.integer(status$trip_count)[1])
  if (!is.finite(trip_count) || trip_count < 0) {
    trip_count <- 0L
  }
  list(
    tripped = tripped,
    error_rate = error_rate,
    recent_count = recent_count,
    trip_count = trip_count
  )
}


# ============================================================================
# ADAPTIVE RATE LIMITING
# ============================================================================
# Dynamically adjusts delays based on success/error patterns.
# - On CAPTCHA/block: Increase delay multiplier by 50% (cap at 5x)
# - On 10 consecutive successes: Decrease multiplier by 10% (floor at 0.5x)
# ============================================================================

#' Initialize Adaptive Rate Limiting State
#'
#' Sets up the adaptive rate limiting state in asa_env. Called during
#' agent initialization.
#'
#' @return Invisibly returns NULL
#' @keywords internal
.adaptive_rate_init <- function() {
  .runtime_control_call("runtime_adaptive_reset", default = list(), quiet = FALSE)
  invisible(NULL)
}

#' Record Result for Adaptive Rate Limiting
#'
#' Records a success or error result and adjusts the delay multiplier accordingly.
#' Tracks a sliding window of recent results to determine adaptation.
#'
#' @param status One of "success", "captcha", "blocked", or "error"
#' @param verbose If TRUE, prints adjustment messages
#' @return Invisibly returns the current multiplier
#' @keywords internal
.adaptive_rate_record <- function(status, verbose = FALSE) {
  if (!isTRUE(ASA_ADAPTIVE_RATE_ENABLED)) {
    return(invisible(1.0))
  }

  status_out <- .runtime_control_call(
    "runtime_adaptive_record",
    status = status,
    verbose = isTRUE(verbose),
    default = list(multiplier = 1.0),
    quiet = FALSE
  )
  if (!is.list(status_out)) {
    return(invisible(1.0))
  }

  multiplier <- suppressWarnings(as.numeric(status_out$multiplier)[1])
  if (!is.finite(multiplier) || multiplier <= 0) {
    multiplier <- 1.0
  }
  invisible(multiplier)
}

#' Get Current Adaptive Rate Multiplier
#'
#' Returns the current delay multiplier for use in rate limiting calculations.
#'
#' @return Numeric multiplier (1.0 = normal, >1 = slower, <1 = faster)
#' @keywords internal
.adaptive_rate_get_multiplier <- function() {
  if (!isTRUE(ASA_ADAPTIVE_RATE_ENABLED)) {
    return(1.0)
  }

  status <- .runtime_control_call(
    "runtime_adaptive_status",
    default = list(multiplier = 1.0),
    quiet = FALSE
  )
  if (!is.list(status)) {
    return(1.0)
  }
  multiplier <- suppressWarnings(as.numeric(status$multiplier)[1])
  if (!is.finite(multiplier) || multiplier <= 0) {
    return(1.0)
  }
  multiplier
}

#' Get Adaptive Rate Status
#'
#' Returns the current state of adaptive rate limiting for monitoring.
#'
#' @return List with multiplier, success_streak, recent_count, and enabled status
#' @keywords internal
.adaptive_rate_status <- function() {
  if (!isTRUE(ASA_ADAPTIVE_RATE_ENABLED)) {
    return(list(
      enabled = FALSE,
      multiplier = 1.0,
      success_streak = 0L,
      recent_count = 0L
    ))
  }

  status <- .runtime_control_call(
    "runtime_adaptive_status",
    default = list(),
    quiet = FALSE
  )
  if (!is.list(status)) {
    status <- list()
  }
  multiplier <- suppressWarnings(as.numeric(status$multiplier)[1])
  if (!is.finite(multiplier) || multiplier <= 0) {
    multiplier <- 1.0
  }
  success_streak <- suppressWarnings(as.integer(status$success_streak)[1])
  if (!is.finite(success_streak) || success_streak < 0) {
    success_streak <- 0L
  }
  recent_count <- suppressWarnings(as.integer(status$recent_count)[1])
  if (!is.finite(recent_count) || recent_count < 0) {
    recent_count <- 0L
  }
  list(
    enabled = isTRUE(status$enabled),
    multiplier = multiplier,
    success_streak = as.integer(success_streak),
    recent_count = as.integer(recent_count)
  )
}

#' Reset Adaptive Rate Limiting
#'
#' Resets the adaptive rate limiting state to defaults.
#'
#' @return Invisibly returns NULL
#' @keywords internal
.adaptive_rate_reset <- function() {
  .runtime_control_call("runtime_adaptive_reset", default = list(), quiet = FALSE)
  invisible(NULL)
}


#' Register Session Finalizer for HTTP Client Cleanup
#'
#' Registers a finalizer that will clean up HTTP clients when the R session
#' ends or the package environment is garbage collected. This provides an
#' additional safety net beyond .onUnload for resource leak prevention.
#'
#' @return Invisibly returns NULL
#' @keywords internal
.register_cleanup_finalizer <- function() {
  # Retained for compatibility; Python runtime now owns client lifecycle.
  asa_env$finalizer_registered <- TRUE
  invisible(NULL)
}

#' @keywords internal
.onUnload <- function(libpath) {
  # Clean up HTTP clients when package is unloaded
  .close_http_clients()
  .runtime_control_reset()
}
