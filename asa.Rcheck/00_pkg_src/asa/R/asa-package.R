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
  if (!exists("http_clients", envir = asa_env) || is.null(asa_env$http_clients)) {
    return(invisible(NULL))
  }

  # Close direct client (for LLM API calls) if it exists
  if (!is.null(asa_env$http_clients$direct)) {
    tryCatch({
      asa_env$http_clients$direct$close()
    }, error = function(e) {
      # Sync client cleanup rarely fails, but ignore if it does
    })
  }

  # Close proxied client (for search tools) if it exists
  if (!is.null(asa_env$http_clients$proxied)) {
    tryCatch({
      asa_env$http_clients$proxied$close()
    }, error = function(e) {
      # Sync client cleanup rarely fails, but ignore if it does
    })
  }

  # Note: async client is intentionally NULL - LangChain manages its own.
  # This fixes R-CRIT-001 where async cleanup from R was unreliable.

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
  rate <- rate %||% ASA_DEFAULT_RATE_LIMIT
  bucket_size <- bucket_size %||% ASA_RATE_LIMIT_BUCKET_SIZE

  asa_env$rate_limiter <- list(
    tokens = as.numeric(bucket_size),
    last_refill = Sys.time(),
    bucket_size = as.numeric(bucket_size),
    refill_rate = as.numeric(rate)
  )
  invisible(NULL)
}

#' Acquire a Rate Limit Token (Proactive Rate Limiting)
#'
#' Acquires a token from the rate limiter bucket before making a request.
#' If no tokens are available, waits until one becomes available.
#' This is called BEFORE making requests to prevent rate limit errors.
#' Wait times are humanized with random jitter when ASA_HUMANIZE_TIMING is TRUE.
#'
#' @param verbose Print waiting message if TRUE
#' @return The wait time in seconds (0 if no wait was needed)
#' @keywords internal
.acquire_rate_limit_token <- function(verbose = FALSE) {
  # Skip if proactive rate limiting is disabled
  if (!isTRUE(ASA_RATE_LIMIT_PROACTIVE)) {
    return(0)
  }

 # Initialize if not already done
  if (is.null(asa_env$rate_limiter)) {
    .rate_limiter_init()
  }

  rl <- asa_env$rate_limiter
  now <- Sys.time()

  # Calculate tokens to add based on elapsed time
  elapsed <- as.numeric(difftime(now, rl$last_refill, units = "secs"))
  tokens_to_add <- elapsed * rl$refill_rate
  rl$tokens <- min(rl$bucket_size, rl$tokens + tokens_to_add)
  rl$last_refill <- now

  wait_time <- 0

  # If no tokens available, calculate wait time
  if (rl$tokens < 1) {
    # How long until we have 1 token?
    base_wait <- (1 - rl$tokens) / rl$refill_rate
    # Apply adaptive rate multiplier (increases on CAPTCHA, decreases on success)
    adaptive_multiplier <- .adaptive_rate_get_multiplier()
    adjusted_wait <- base_wait * adaptive_multiplier
    # Apply humanized timing with random jitter
    wait_time <- .humanize_delay(adjusted_wait)
    if (verbose) {
      if (adaptive_multiplier != 1.0) {
        message(sprintf("  Rate limiter: waiting %.1fs (base=%.1fs, adaptive=%.2fx, humanized)",
                        wait_time, base_wait, adaptive_multiplier))
      } else {
        message(sprintf("  Rate limiter: waiting %.1fs for token (humanized from %.1fs)...",
                        wait_time, base_wait))
      }
    }
    Sys.sleep(wait_time)
    rl$tokens <- 1  # After waiting, we have exactly 1 token
  }

  # Consume one token
  rl$tokens <- rl$tokens - 1
  asa_env$rate_limiter <- rl

  return(wait_time)
}

#' Reset Rate Limiter
#'
#' Resets the rate limiter to full capacity. Useful after errors or
#' when starting a new batch of requests.
#'
#' @return Invisibly returns NULL
#' @keywords internal
.rate_limiter_reset <- function() {
  if (!is.null(asa_env$rate_limiter)) {
    asa_env$rate_limiter$tokens <- asa_env$rate_limiter$bucket_size
    asa_env$rate_limiter$last_refill <- Sys.time()
  }
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
  asa_env$circuit_breaker <- list(
    recent_results = character(0),
    tripped = FALSE,
    tripped_at = NULL,
    trip_count = 0L
  )
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
  if (is.null(asa_env$circuit_breaker)) {
    .circuit_breaker_init()
  }

  cb <- asa_env$circuit_breaker

  # Don't record if already tripped
  if (cb$tripped) {
    return(invisible(TRUE))
  }

  # Add result to window
  cb$recent_results <- c(cb$recent_results, status)

  # Keep only last N results
  if (length(cb$recent_results) > ASA_CIRCUIT_BREAKER_WINDOW) {
    cb$recent_results <- tail(cb$recent_results, ASA_CIRCUIT_BREAKER_WINDOW)
  }

  # Check error rate only if we have enough samples
  if (length(cb$recent_results) >= 5) {
    error_rate <- mean(cb$recent_results == "error")
    if (error_rate > ASA_CIRCUIT_BREAKER_THRESHOLD) {
      cb$tripped <- TRUE
      cb$tripped_at <- Sys.time()
      cb$trip_count <- cb$trip_count + 1L
      if (verbose) {
        warning(sprintf("Circuit breaker TRIPPED! Error rate: %.0f%% (threshold: %.0f%%)",
                        error_rate * 100, ASA_CIRCUIT_BREAKER_THRESHOLD * 100),
                call. = FALSE)
      }
    }
  }

  asa_env$circuit_breaker <- cb
  invisible(cb$tripped)
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
  if (is.null(asa_env$circuit_breaker)) {
    return(TRUE)
  }

  cb <- asa_env$circuit_breaker

  if (!cb$tripped) {
    return(TRUE)
  }

  # Check if cooldown has passed
  elapsed <- as.numeric(difftime(Sys.time(), cb$tripped_at, units = "secs"))
  if (elapsed >= ASA_CIRCUIT_BREAKER_COOLDOWN) {
    cb$tripped <- FALSE
    cb$recent_results <- character(0)
    asa_env$circuit_breaker <- cb
    if (verbose) {
      message(sprintf("Circuit breaker RESET after %.0fs cooldown", elapsed))
    }
    return(TRUE)
  }

  return(FALSE)
}

#' Get Circuit Breaker Status
#'
#' Returns the current state of the circuit breaker for monitoring.
#'
#' @return List with tripped, error_rate, recent_count, and trip_count
#' @keywords internal
.circuit_breaker_status <- function() {
  if (is.null(asa_env$circuit_breaker)) {
    return(list(
      tripped = FALSE,
      error_rate = 0,
      recent_count = 0,
      trip_count = 0L
    ))
  }

  cb <- asa_env$circuit_breaker
  error_rate <- if (length(cb$recent_results) > 0) {
    mean(cb$recent_results == "error")
  } else {
    0
  }

  list(
    tripped = cb$tripped,
    error_rate = error_rate,
    recent_count = length(cb$recent_results),
    trip_count = cb$trip_count
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
  asa_env$adaptive_rate <- list(
    recent_results = character(0),
    multiplier = 1.0,
    window_size = ASA_ADAPTIVE_RATE_WINDOW,
    success_streak = 0L
  )
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
  if (!ASA_ADAPTIVE_RATE_ENABLED) {
    return(invisible(1.0))
  }

  # Initialize if needed
  if (is.null(asa_env$adaptive_rate)) {
    .adaptive_rate_init()
  }

  ar <- asa_env$adaptive_rate
  ar$recent_results <- c(ar$recent_results, status)

  # Keep sliding window
  if (length(ar$recent_results) > ar$window_size) {
    ar$recent_results <- tail(ar$recent_results, ar$window_size)
  }

  # Update success streak
  if (status == "success") {
    ar$success_streak <- ar$success_streak + 1L
  } else {
    ar$success_streak <- 0L
  }

  # Adapt multiplier based on results
  old_multiplier <- ar$multiplier

  if (status %in% c("captcha", "blocked")) {
    # Increase delay on CAPTCHA/block
    ar$multiplier <- min(ASA_ADAPTIVE_RATE_MAX, ar$multiplier * ASA_ADAPTIVE_RATE_INCREASE)
    if (verbose && ar$multiplier != old_multiplier) {
      message(sprintf("Adaptive rate: INCREASED to %.2fx (was %.2fx) due to %s",
                      ar$multiplier, old_multiplier, status))
    }
  } else if (ar$success_streak >= 10L) {
    # Decrease delay after 10 consecutive successes
    ar$multiplier <- max(ASA_ADAPTIVE_RATE_MIN, ar$multiplier * ASA_ADAPTIVE_RATE_DECREASE)
    if (verbose && ar$multiplier != old_multiplier) {
      message(sprintf("Adaptive rate: DECREASED to %.2fx (was %.2fx) after %d successes",
                      ar$multiplier, old_multiplier, ar$success_streak))
    }
    # Reset streak to avoid continuous decreases
    ar$success_streak <- 0L
  }

  asa_env$adaptive_rate <- ar
  invisible(ar$multiplier)
}

#' Get Current Adaptive Rate Multiplier
#'
#' Returns the current delay multiplier for use in rate limiting calculations.
#'
#' @return Numeric multiplier (1.0 = normal, >1 = slower, <1 = faster)
#' @keywords internal
.adaptive_rate_get_multiplier <- function() {
  if (!ASA_ADAPTIVE_RATE_ENABLED || is.null(asa_env$adaptive_rate)) {
    return(1.0)
  }
  asa_env$adaptive_rate$multiplier %||% 1.0
}

#' Get Adaptive Rate Status
#'
#' Returns the current state of adaptive rate limiting for monitoring.
#'
#' @return List with multiplier, success_streak, recent_count, and enabled status
#' @keywords internal
.adaptive_rate_status <- function() {
  if (!ASA_ADAPTIVE_RATE_ENABLED || is.null(asa_env$adaptive_rate)) {
    return(list(
      enabled = ASA_ADAPTIVE_RATE_ENABLED,
      multiplier = 1.0,
      success_streak = 0L,
      recent_count = 0L
    ))
  }

  ar <- asa_env$adaptive_rate
  list(
    enabled = TRUE,
    multiplier = ar$multiplier,
    success_streak = ar$success_streak,
    recent_count = length(ar$recent_results)
  )
}

#' Reset Adaptive Rate Limiting
#'
#' Resets the adaptive rate limiting state to defaults.
#'
#' @return Invisibly returns NULL
#' @keywords internal
.adaptive_rate_reset <- function() {
  .adaptive_rate_init()
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
  # Only register once (on first initialization)
  if (isTRUE(asa_env$finalizer_registered)) {
    return(invisible(NULL))
  }

  # Register finalizer on asa_env - runs when env is garbage collected or session ends
  reg.finalizer(asa_env, function(e) {
    tryCatch({
      # Close HTTP clients if they exist
      if (exists("http_clients", envir = e) && !is.null(e$http_clients)) {
        if (!is.null(e$http_clients$direct)) {
          tryCatch(e$http_clients$direct$close(), error = function(err) NULL)
        }
        if (!is.null(e$http_clients$proxied)) {
          tryCatch(e$http_clients$proxied$close(), error = function(err) NULL)
        }
        e$http_clients <- NULL
      }
    }, error = function(err) {
      # Silently ignore cleanup errors during finalization
      NULL
    })
  }, onexit = TRUE)

  asa_env$finalizer_registered <- TRUE
  invisible(NULL)
}

#' @keywords internal
.onUnload <- function(libpath) {
  # Clean up HTTP clients when package is unloaded
  .close_http_clients()
}
