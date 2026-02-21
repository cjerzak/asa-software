# ============================================================================
# CONFIGURATION S3 CLASSES
# ============================================================================

#' Create ASA Configuration Object
#'
#' Creates a configuration object that encapsulates all settings for ASA tasks.
#' This provides a unified way to configure backend, model, search, temporal,
#' and resource settings in a single object.
#'
#' @param backend LLM backend: "openai", "groq", "xai", "gemini", "exo", "openrouter"
#' @param model Model identifier (e.g., "gpt-4.1-mini")
#' @param conda_env Conda environment name. Defaults to the package option
#'   \code{asa.default_conda_env} (or \code{"asa_env"} if unset).
#' @param proxy Proxy URL (e.g., Tor SOCKS5). Use \code{NA} (default) to
#'   auto-detect from environment variables (\code{ASA_PROXY}, \code{HTTP_PROXY},
#'   \code{HTTPS_PROXY}); use \code{NULL} to disable proxying.
#' @param use_browser Enable Selenium browser tier for DuckDuckGo search.
#' @param workers Number of parallel workers for batch operations
#' @param timeout Request timeout in seconds
#' @param rate_limit Requests per second
#' @param memory_folding Enable DeepAgent-style memory folding
#' @param memory_threshold Messages before folding triggers
#' @param memory_keep_recent Exchanges to preserve after folding. An exchange is
#'   a user turn plus the assistant response, including any tool calls and tool outputs.
#' @param use_observational_memory Enable observational memory pipeline.
#' @param om_observation_token_budget Token budget for stored observation messages.
#' @param om_reflection_token_budget Token budget for reflection summaries.
#' @param om_buffer_tokens Token budget for buffered context before reflection.
#' @param om_buffer_activation Fraction of buffer usage that triggers reflection.
#' @param om_block_after Fraction of context usage where buffering blocks generation.
#' @param om_async_prebuffer Whether to pre-buffer observations asynchronously.
#' @param om_cross_thread_memory Whether observational memory is shared across threads.
#' @param recursion_limit Optional default maximum number of agent steps.
#'   When NULL, mode-specific defaults are used at runtime unless overridden in
#'   \code{\link{run_task}}/\code{\link{run_task_batch}}.
#' @param temporal Temporal filtering options (use \code{temporal_options()})
#' @param search Search configuration (use \code{search_options()})
#' @param tor Tor registry options (use \code{tor_options()})
#'
#' @return An object of class \code{asa_config}
#'
#' @details
#' The configuration object can be passed to \code{run_task()},
#' \code{run_task_batch()}, \code{asa_enumerate()}, and other functions
#' to provide consistent settings across operations.
#'
#' @examples
#' \dontrun{
#' # Create configuration
#' config <- asa_config(
#'   backend = "openai",
#'   model = "gpt-4.1-mini",
#'   workers = 4,
#'   temporal = temporal_options(time_filter = "y")
#' )
#'
#' # Use with run_task
#' result <- run_task(prompt, config = config)
#' }
#'
#' @seealso \code{\link{temporal_options}}, \code{\link{search_options}}
#'
#' @export
asa_config <- function(backend = NULL,
                       model = NULL,
                       conda_env = NULL,
                       proxy = NA,
                       use_browser = NULL,
                       workers = NULL,
                       timeout = NULL,
                       rate_limit = NULL,
                       memory_folding = NULL,
                       memory_threshold = NULL,
                       memory_keep_recent = NULL,
                       use_observational_memory = NULL,
                       om_observation_token_budget = NULL,
                       om_reflection_token_budget = NULL,
                       om_buffer_tokens = NULL,
                       om_buffer_activation = NULL,
                       om_block_after = NULL,
                       om_async_prebuffer = NULL,
                       om_cross_thread_memory = NULL,
                       temporal = NULL,
                       search = NULL,
                       tor = NULL,
                       recursion_limit = NULL) {

  # Use defaults from constants.R if not specified
  backend <- backend %||% .get_default_backend()
  model <- model %||% .get_default_model_for_backend(backend)
  conda_env <- conda_env %||% .get_default_conda_env()
  workers <- workers %||% .get_default_workers()
  timeout <- timeout %||% ASA_DEFAULT_TIMEOUT
  rate_limit <- rate_limit %||% ASA_DEFAULT_RATE_LIMIT
  use_browser <- use_browser %||% ASA_DEFAULT_USE_BROWSER
  memory_folding <- memory_folding %||% ASA_DEFAULT_MEMORY_FOLDING
  memory_threshold <- memory_threshold %||% ASA_DEFAULT_MEMORY_THRESHOLD
  memory_keep_recent <- memory_keep_recent %||% ASA_DEFAULT_MEMORY_KEEP_RECENT
  use_observational_memory <- use_observational_memory %||% ASA_DEFAULT_USE_OBSERVATIONAL_MEMORY
  om_observation_token_budget <- om_observation_token_budget %||% ASA_DEFAULT_OM_OBSERVATION_TOKENS
  om_reflection_token_budget <- om_reflection_token_budget %||% ASA_DEFAULT_OM_REFLECTION_TOKENS
  om_buffer_tokens <- om_buffer_tokens %||% ASA_DEFAULT_OM_BUFFER_TOKENS
  om_buffer_activation <- om_buffer_activation %||% ASA_DEFAULT_OM_BUFFER_ACTIVATION
  om_block_after <- om_block_after %||% ASA_DEFAULT_OM_BLOCK_AFTER
  om_async_prebuffer <- om_async_prebuffer %||% ASA_DEFAULT_OM_ASYNC_PREBUFFER
  om_cross_thread_memory <- om_cross_thread_memory %||% ASA_DEFAULT_OM_CROSS_THREAD_MEMORY
  tor <- tor %||% tor_options()

  # Validate backend
  if (!backend %in% ASA_SUPPORTED_BACKENDS) {
    stop(sprintf("`backend` must be one of: %s",
                 paste(ASA_SUPPORTED_BACKENDS, collapse = ", ")),
         call. = FALSE)
  }

  # Validate proxy (NA = auto, NULL = disabled)
  .validate_proxy_url(proxy, "proxy")
  .validate_logical(use_browser, "use_browser")
  .validate_recursion_limit(recursion_limit, "recursion_limit")
  .validate_logical(use_observational_memory, "use_observational_memory")
  .validate_positive(om_observation_token_budget, "om_observation_token_budget", integer_only = TRUE)
  .validate_positive(om_reflection_token_budget, "om_reflection_token_budget", integer_only = TRUE)
  .validate_positive(om_buffer_tokens, "om_buffer_tokens", integer_only = TRUE)
  .validate_range(om_buffer_activation, "om_buffer_activation", min = 0.05, max = 0.99)
  .validate_range(om_block_after, "om_block_after", min = 0.10, max = 1.00)
  .validate_logical(om_async_prebuffer, "om_async_prebuffer")
  .validate_logical(om_cross_thread_memory, "om_cross_thread_memory")

  om_observation_token_budget <- as.integer(om_observation_token_budget)
  om_reflection_token_budget <- as.integer(max(om_reflection_token_budget, om_observation_token_budget))
  om_buffer_tokens <- as.integer(om_buffer_tokens)
  om_buffer_activation <- as.numeric(om_buffer_activation)
  om_block_after <- as.numeric(max(om_block_after, om_buffer_activation))

  # Validate temporal if provided
  if (!is.null(temporal) && !inherits(temporal, "asa_temporal")) {
    if (is.list(temporal)) {
      # Convert plain list to asa_temporal
      temporal <- do.call(temporal_options, temporal)
    } else {
      stop("`temporal` must be created with temporal_options() or be a list",
           call. = FALSE)
    }
  }

  # Validate search if provided
  if (!is.null(search) && !inherits(search, "asa_search")) {
    if (is.list(search)) {
      search <- do.call(search_options, search)
    } else {
      stop("`search` must be created with search_options() or be a list",
           call. = FALSE)
    }
  }

  # Validate tor if provided
  if (!inherits(tor, "asa_tor")) {
    if (is.list(tor)) {
      tor <- do.call(tor_options, tor)
    } else {
      stop("`tor` must be created with tor_options() or be a list",
           call. = FALSE)
    }
  }

  structure(
    list(
      backend = backend,
      model = model,
      conda_env = conda_env,
      proxy = proxy,
      use_browser = use_browser,
      workers = as.integer(workers),
      timeout = as.integer(timeout),
      rate_limit = rate_limit,
      memory_folding = memory_folding,
      memory_threshold = as.integer(memory_threshold),
      memory_keep_recent = as.integer(memory_keep_recent),
      use_observational_memory = use_observational_memory,
      om_observation_token_budget = om_observation_token_budget,
      om_reflection_token_budget = om_reflection_token_budget,
      om_buffer_tokens = om_buffer_tokens,
      om_buffer_activation = om_buffer_activation,
      om_block_after = om_block_after,
      om_async_prebuffer = om_async_prebuffer,
      om_cross_thread_memory = om_cross_thread_memory,
      recursion_limit = .normalize_recursion_limit(recursion_limit),
      temporal = temporal,
      search = search,
      tor = tor
    ),
    class = "asa_config"
  )
}

#' Print Method for asa_config Objects
#'
#' @param x An asa_config object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_config
#' @export
print.asa_config <- function(x, ...) {
  cat("ASA Configuration\n")
  cat("=================\n")
  cat("Backend:         ", x$backend, "\n", sep = "")
  cat("Model:           ", x$model, "\n", sep = "")
  cat("Conda Env:       ", x$conda_env, "\n", sep = "")
  cat("Proxy:           ", .format_proxy(x$proxy), "\n", sep = "")
  cat("Use Browser:     ", if (isTRUE(x$use_browser)) "Enabled" else "Disabled", "\n", sep = "")
  cat("Workers:         ", x$workers, "\n", sep = "")
  cat("Timeout:         ", x$timeout, "s\n", sep = "")
  cat("Rate Limit:      ", x$rate_limit, " req/s\n", sep = "")
  rec_limit <- .normalize_recursion_limit(x$recursion_limit %||% NULL)
  cat("Recursion Limit: ", if (is.null(rec_limit)) "Auto" else rec_limit, "\n", sep = "")
  cat("Memory Folding:  ", if (x$memory_folding) "Enabled" else "Disabled", "\n", sep = "")
  if (x$memory_folding) {
    cat("  Threshold:     ", x$memory_threshold, " messages\n", sep = "")
    cat("  Keep Recent:   ", x$memory_keep_recent, " exchanges\n", sep = "")
  }
  cat("Obs. Memory:     ", if (isTRUE(x$use_observational_memory)) "Enabled" else "Disabled", "\n", sep = "")
  if (isTRUE(x$use_observational_memory)) {
    cat("  Obs Tokens:    ", x$om_observation_token_budget %||% NA_integer_, "\n", sep = "")
    cat("  Refl Tokens:   ", x$om_reflection_token_budget %||% NA_integer_, "\n", sep = "")
    cat("  Buffer Tokens: ", x$om_buffer_tokens %||% NA_integer_, "\n", sep = "")
    cat("  Buffer Start:  ", x$om_buffer_activation %||% NA_real_, "\n", sep = "")
    cat("  Block After:   ", x$om_block_after %||% NA_real_, "\n", sep = "")
    cat("  Async Buffer:  ", if (isTRUE(x$om_async_prebuffer)) "Enabled" else "Disabled", "\n", sep = "")
    cat("  Cross Thread:  ", if (isTRUE(x$om_cross_thread_memory)) "Enabled" else "Disabled", "\n", sep = "")
  }
  if (!is.null(x$temporal)) {
    cat("\nTemporal Filtering:\n")
    print(x$temporal)
  }
  if (!is.null(x$tor)) {
    cat("\nTor Registry:\n")
    print(x$tor)
  }
  invisible(x)
}


#' Create Temporal Filtering Options
#'
#' Creates a temporal filtering configuration for constraining search results
#' by date. Supports DuckDuckGo time filters, date ranges, and strict
#' verification modes.
#'
#' @param time_filter DuckDuckGo time filter: "d" (day), "w" (week),
#'   "m" (month), "y" (year), or NULL for no filter
#' @param after ISO 8601 date string (e.g., "2020-01-01") - results after this date
#' @param before ISO 8601 date string (e.g., "2024-01-01") - results before this date
#' @param strictness Verification level: "best_effort" (default) or "strict"
#' @param use_wayback Use Wayback Machine for strict pre-date guarantees
#'
#' @return An object of class \code{asa_temporal}
#'
#' @details
#' Temporal filtering can operate at different levels:
#' \itemize{
#'   \item \strong{time_filter}: DuckDuckGo native filter (fast, approximate)
#'   \item \strong{after/before}: Date hints appended to prompts
#'   \item \strong{strict}: Post-hoc verification of result dates
#'   \item \strong{use_wayback}: Uses Internet Archive for guaranteed historical data
#' }
#'
#' @examples
#' \dontrun{
#' # Past year only
#' temporal <- temporal_options(time_filter = "y")
#'
#' # Specific date range
#' temporal <- temporal_options(
#'   after = "2020-01-01",
#'   before = "2024-01-01"
#' )
#'
#' # Strict historical verification
#' temporal <- temporal_options(
#'   before = "2015-01-01",
#'   strictness = "strict",
#'   use_wayback = TRUE
#' )
#' }
#'
#' @seealso \code{\link{asa_config}}, \code{\link{run_task}}
#'
#' @export
temporal_options <- function(time_filter = NULL,
                             after = NULL,
                             before = NULL,
                             strictness = "best_effort",
                             use_wayback = FALSE) {

  # Validate time_filter
  if (!is.null(time_filter)) {
    if (!time_filter %in% ASA_TIME_FILTERS) {
      stop(sprintf("`time_filter` must be one of: %s",
                   paste0('"', ASA_TIME_FILTERS, '"', collapse = ", ")),
           call. = FALSE)
    }
  }

  # Validate and parse dates
  if (!is.null(after)) {
    after_date <- tryCatch(
      as.Date(after),
      error = function(e) {
        stop("`after` must be a valid ISO 8601 date (YYYY-MM-DD)", call. = FALSE)
      }
    )
  }

  if (!is.null(before)) {
    before_date <- tryCatch(
      as.Date(before),
      error = function(e) {
        stop("`before` must be a valid ISO 8601 date (YYYY-MM-DD)", call. = FALSE)
      }
    )
  }

  # Validate date ordering
  if (!is.null(after) && !is.null(before)) {
    if (as.Date(after) >= as.Date(before)) {
      stop("`after` must be earlier than `before`", call. = FALSE)
    }
  }

  # Validate strictness
  if (!strictness %in% ASA_TEMPORAL_STRICTNESS) {
    stop(sprintf("`strictness` must be one of: %s",
                 paste0('"', ASA_TEMPORAL_STRICTNESS, '"', collapse = ", ")),
         call. = FALSE)
  }

  structure(
    list(
      time_filter = time_filter,
      after = after,
      before = before,
      strictness = strictness,
      use_wayback = use_wayback
    ),
    class = "asa_temporal"
  )
}

#' Print Method for asa_temporal Objects
#'
#' @param x An asa_temporal object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_temporal
#' @export
print.asa_temporal <- function(x, ...) {
  parts <- c()
  if (!is.null(x$time_filter)) {
    filter_desc <- switch(x$time_filter,
                          "d" = "past day",
                          "w" = "past week",
                          "m" = "past month",
                          "y" = "past year")
    parts <- c(parts, sprintf("Time Filter: %s (%s)", x$time_filter, filter_desc))
  }
  if (!is.null(x$after)) {
    parts <- c(parts, sprintf("After: %s", x$after))
  }
  if (!is.null(x$before)) {
    parts <- c(parts, sprintf("Before: %s", x$before))
  }
  parts <- c(parts, sprintf("Strictness: %s", x$strictness))
  if (x$use_wayback) {
    parts <- c(parts, "Wayback: Enabled")
  }
  cat("  ", paste(parts, collapse = ", "), "\n", sep = "")
  invisible(x)
}


#' Create Search Options
#'
#' Creates search configuration for controlling DuckDuckGo search behavior,
#' including rate limiting, retry policies, and result limits. These options
#' are used by the 4-tier search fallback system.
#'
#' @param max_results Maximum number of search results to return per query.
#'   Higher values provide more context but increase latency. Default: 10.
#' @param timeout Timeout in seconds for individual search requests.
#'   Applies to each tier attempt separately. Default: 30.
#' @param max_retries Maximum number of retry attempts when a search tier fails.
#'   After exhausting retries, the system falls back to the next tier. Default: 3.
#' @param retry_delay Initial delay in seconds before the first retry.
#'   Subsequent retries use exponential backoff. Default: 2.
#' @param backoff_multiplier Multiplier for exponential backoff between retries.
#'   E.g., with retry_delay=2 and multiplier=1.5, delays are 2s, 3s, 4.5s. Default: 1.5.
#' @param inter_search_delay Minimum delay in seconds between consecutive searches.
#'   Helps avoid rate limiting from search providers. Default: 1.5.
#' @param wiki_top_k_results Number of Wikipedia search results to fetch per
#'   query (default: 5). Higher values may provide more coverage at the cost of
#'   latency/noise.
#' @param wiki_doc_content_chars_max Maximum number of characters to include
#'   from each Wikipedia page/summary in the Wikipedia tool output (default: 1000).
#' @param search_doc_content_chars_max Maximum number of characters to include
#'   from DuckDuckGo Search tool outputs (default: 500). This caps the text
#'   returned to the LLM per Search tool invocation.
#' @param finalize_when_all_unresolved_exhausted Optional override for
#'   orchestration finalizer behavior. When TRUE, finalize once all unresolved
#'   fields have exhausted their search-attempt budget.
#' @param field_attempt_budget_mode Optional field-attempt budget mode passed to
#'   orchestration field_resolver. One of \code{"strict_cap"} or
#'   \code{"soft_cap"}.
#' @param allow_read_webpages If TRUE, allows the agent to open and read full
#'   webpages (HTML/text) via the OpenWebpage tool. Disabled by default.
#' @param webpage_relevance_mode Relevance selection for opened webpages.
#'   One of: "auto", "lexical", "embeddings".
#' @param webpage_heuristic_profile Heuristic profile used when annotating links
#'   from opened webpages. One of: "generic" (task-agnostic default) or
#'   "legacy" (compatibility mode with older domain-biased cues).
#' @param webpage_embedding_provider Embedding provider for relevance. One of:
#'   "auto", "openai", "sentence_transformers".
#' @param webpage_embedding_model Embedding model identifier for relevance.
#' @param webpage_timeout Timeout in seconds for OpenWebpage fetches and (when
#'   used) relevance embeddings.
#' @param webpage_max_bytes Maximum bytes to download per page (hard cap).
#' @param webpage_max_chars Maximum characters returned from an opened page
#'   (hard cap on tool output).
#' @param webpage_max_chunks Maximum number of relevant excerpts to include from
#'   a page.
#' @param webpage_chunk_chars Approximate size (in characters) of each excerpt.
#' @param webpage_embedding_api_base Optional OpenAI-compatible base URL for
#'   embeddings. If NULL, uses \code{OPENAI_API_BASE} or
#'   \code{https://api.openai.com/v1}.
#' @param webpage_prefilter_k Optional lexical prefilter size before embedding
#'   relevance selection.
#' @param webpage_use_mmr Whether to apply maximal marginal relevance (MMR) for
#'   more diverse excerpts.
#' @param webpage_mmr_lambda MMR tradeoff between relevance (1.0) and diversity (0.0).
#' @param webpage_cache_enabled Enable per-run caching of opened pages.
#' @param webpage_cache_max_entries Max cached page entries per run.
#' @param webpage_cache_max_text_chars Max characters of extracted page text to
#'   store per cached page (separate from \code{webpage_max_chars}, which caps
#'   tool output).
#' @param webpage_blocked_cache_ttl_sec TTL in seconds for cached blocked URL
#'   responses (HTTP 403/429 and anti-bot pages).
#' @param webpage_blocked_cache_max_entries Max blocked-URL cache entries per run.
#' @param webpage_blocked_probe_bytes Max bytes to inspect when classifying a
#'   response as blocked.
#' @param webpage_blocked_detect_on_200 If TRUE, classify anti-bot interstitials
#'   served with HTTP 200 as blocked responses.
#' @param webpage_blocked_body_scan_bytes Max response bytes scanned on HTTP 200
#'   pages for anti-bot marker detection.
#' @param webpage_pdf_enabled If TRUE, OpenWebpage attempts PDF extraction via
#'   \code{pdftotext}.
#' @param webpage_pdf_timeout Timeout in seconds for \code{pdftotext}.
#' @param webpage_pdf_max_bytes Max PDF bytes to download before extraction.
#' @param webpage_pdf_max_pages Max pages passed to \code{pdftotext}.
#' @param webpage_pdf_max_text_chars Max extracted PDF characters kept before
#'   relevance selection/output truncation.
#' @param webpage_user_agent User-Agent string used for webpage fetches.
#' @param wikidata_template_path Optional path to a Wikidata template JSON file.
#'   When provided, Wikidata known-entity catalogs are loaded from this file.
#'   If NULL (default), no task-specific template catalog is auto-loaded.
#'
#' @return An object of class \code{asa_search}
#'
#' @details
#' The search system uses a 4-tier fallback architecture:
#' \enumerate{
#'   \item \strong{PRIMP}: HTTP/2 with browser TLS fingerprint
#'   \item \strong{Selenium}: Headless browser for JS-rendered content
#'   \item \strong{DDGS}: Standard ddgs Python library
#'   \item \strong{Requests}: Raw POST to DuckDuckGo HTML endpoint
#' }
#'
#' The retry/backoff settings apply within each tier. If all retries
#' are exhausted, the system automatically falls back to the next tier.
#'
#' @examples
#' \dontrun{
#' # Default settings
#' search <- search_options()
#'
#' # More aggressive settings for faster searches
#' search <- search_options(
#'   max_results = 5,
#'   timeout = 10,
#'   max_retries = 2
#' )
#'
#' # Conservative settings for rate-limited environments
#' search <- search_options(
#'   inter_search_delay = 2.0,
#'   max_retries = 5,
#'   backoff_multiplier = 2.0
#' )
#'
#' # Use with asa_config
#' config <- asa_config(
#'   backend = "openai",
#'   search = search_options(max_results = 15)
#' )
#' }
#'
#' @seealso \code{\link{asa_config}}, \code{\link{configure_search}}
#'
#' @export
search_options <- function(max_results = NULL,
                           timeout = NULL,
                           max_retries = NULL,
                           retry_delay = NULL,
                           backoff_multiplier = NULL,
                           inter_search_delay = NULL,
                           humanize_timing = NULL,
                           jitter_factor = NULL,
                           allow_direct_fallback = NULL,
                           stability_profile = NULL,
                           auto_openwebpage_policy = NULL,
                           langgraph_node_retries = NULL,
                           langgraph_cache_enabled = NULL,
                           finalize_when_all_unresolved_exhausted = NULL,
                           field_attempt_budget_mode = NULL,
                           allow_read_webpages = NULL,
                           webpage_relevance_mode = NULL,
                           webpage_heuristic_profile = NULL,
                           webpage_embedding_provider = NULL,
                           webpage_embedding_model = NULL,
                           webpage_timeout = NULL,
                           webpage_max_bytes = NULL,
                           webpage_max_chars = NULL,
                           webpage_max_chunks = NULL,
                           webpage_chunk_chars = NULL,
                           webpage_embedding_api_base = NULL,
                           webpage_prefilter_k = NULL,
                           webpage_use_mmr = NULL,
                           webpage_mmr_lambda = NULL,
                           webpage_cache_enabled = NULL,
                           webpage_cache_max_entries = NULL,
                           webpage_cache_max_text_chars = NULL,
                           webpage_blocked_cache_ttl_sec = NULL,
                           webpage_blocked_cache_max_entries = NULL,
                           webpage_blocked_probe_bytes = NULL,
                           webpage_blocked_detect_on_200 = NULL,
                           webpage_blocked_body_scan_bytes = NULL,
                           webpage_pdf_enabled = NULL,
                           webpage_pdf_timeout = NULL,
                           webpage_pdf_max_bytes = NULL,
                           webpage_pdf_max_pages = NULL,
                           webpage_pdf_max_text_chars = NULL,
                           webpage_user_agent = NULL,
                           wikidata_template_path = NULL,
                           wiki_top_k_results = NULL,
                           wiki_doc_content_chars_max = NULL,
                           search_doc_content_chars_max = NULL) {
  profile <- tolower(as.character(stability_profile %||% "stealth_first"))
  if (!profile %in% c("deterministic", "balanced", "stealth_first")) {
    profile <- "stealth_first"
  }
  auto_policy <- tolower(as.character(auto_openwebpage_policy %||% ""))
  if (!auto_policy %in% c("off", "conservative", "aggressive")) {
    auto_policy <- switch(
      profile,
      deterministic = "off",
      balanced = "conservative",
      stealth_first = "conservative"
    )
  }
  field_attempt_mode <- field_attempt_budget_mode
  if (!is.null(field_attempt_mode)) {
    field_attempt_mode <- tolower(as.character(field_attempt_mode)[[1]])
    if (!field_attempt_mode %in% c("strict_cap", "soft_cap")) {
      field_attempt_mode <- NULL
    }
  }

  structure(
    list(
      max_results = max_results %||% ASA_DEFAULT_MAX_RESULTS,
      timeout = timeout %||% ASA_DEFAULT_SEARCH_TIMEOUT,
      max_retries = max_retries %||% ASA_DEFAULT_MAX_RETRIES,
      retry_delay = retry_delay %||% 2.0,
      backoff_multiplier = backoff_multiplier %||% 1.5,
      inter_search_delay = inter_search_delay %||% ASA_DEFAULT_INTER_SEARCH_DELAY,
      humanize_timing = humanize_timing %||% ASA_HUMANIZE_TIMING,
      jitter_factor = jitter_factor %||% ASA_JITTER_FACTOR,
      allow_direct_fallback = allow_direct_fallback %||% FALSE,
      stability_profile = profile,
      auto_openwebpage_policy = auto_policy,
      langgraph_node_retries = langgraph_node_retries %||% TRUE,
      langgraph_cache_enabled = langgraph_cache_enabled %||% FALSE,
      finalize_when_all_unresolved_exhausted = if (is.null(finalize_when_all_unresolved_exhausted)) NULL else isTRUE(finalize_when_all_unresolved_exhausted),
      field_attempt_budget_mode = field_attempt_mode,
      wiki_top_k_results = wiki_top_k_results %||% ASA_DEFAULT_WIKI_TOP_K,
      wiki_doc_content_chars_max = wiki_doc_content_chars_max %||% ASA_DEFAULT_WIKI_CHARS,
      search_doc_content_chars_max = search_doc_content_chars_max %||% 500L,
      allow_read_webpages = allow_read_webpages %||% FALSE,
      webpage_relevance_mode = webpage_relevance_mode %||% "auto",
      webpage_heuristic_profile = webpage_heuristic_profile %||% "generic",
      webpage_embedding_provider = webpage_embedding_provider %||% "auto",
      webpage_embedding_model = webpage_embedding_model %||% "text-embedding-3-small",
      # OpenWebpage tool tuning (optional; defaults to Python-side config)
      webpage_timeout = webpage_timeout,
      webpage_max_bytes = webpage_max_bytes,
      webpage_max_chars = webpage_max_chars,
      webpage_max_chunks = webpage_max_chunks,
      webpage_chunk_chars = webpage_chunk_chars,
      webpage_embedding_api_base = webpage_embedding_api_base,
      webpage_prefilter_k = webpage_prefilter_k,
      webpage_use_mmr = webpage_use_mmr,
      webpage_mmr_lambda = webpage_mmr_lambda,
      webpage_cache_enabled = webpage_cache_enabled,
      webpage_cache_max_entries = webpage_cache_max_entries,
      webpage_cache_max_text_chars = webpage_cache_max_text_chars,
      webpage_blocked_cache_ttl_sec = webpage_blocked_cache_ttl_sec,
      webpage_blocked_cache_max_entries = webpage_blocked_cache_max_entries,
      webpage_blocked_probe_bytes = webpage_blocked_probe_bytes,
      webpage_blocked_detect_on_200 = webpage_blocked_detect_on_200,
      webpage_blocked_body_scan_bytes = webpage_blocked_body_scan_bytes,
      webpage_pdf_enabled = webpage_pdf_enabled,
      webpage_pdf_timeout = webpage_pdf_timeout,
      webpage_pdf_max_bytes = webpage_pdf_max_bytes,
      webpage_pdf_max_pages = webpage_pdf_max_pages,
      webpage_pdf_max_text_chars = webpage_pdf_max_text_chars,
      webpage_user_agent = webpage_user_agent,
      wikidata_template_path = wikidata_template_path
    ),
    class = "asa_search"
  )
}

#' Print Method for asa_search Objects
#'
#' @param x An asa_search object
#' @param ... Additional arguments (ignored)
#'
#' @method print asa_search
#' @export
print.asa_search <- function(x, ...) {
  cat("Search Options: max_results=", x$max_results,
      ", timeout=", x$timeout, "s",
      ", retries=", x$max_retries,
      ", delay=", x$inter_search_delay, "s",
      ", profile=", x$stability_profile %||% "stealth_first",
      ", wiki_top_k_results=", x$wiki_top_k_results,
      ", wiki_doc_content_chars_max=", x$wiki_doc_content_chars_max,
      ", search_doc_content_chars_max=", x$search_doc_content_chars_max,
      ", allow_read_webpages=", x$allow_read_webpages, sep = "")
  if (!is.null(x$auto_openwebpage_policy) &&
      !identical(x$auto_openwebpage_policy, "conservative")) {
    cat(", auto_openwebpage_policy=", x$auto_openwebpage_policy, sep = "")
  }
  if (!is.null(x$finalize_when_all_unresolved_exhausted)) {
    cat(", finalize_when_all_unresolved_exhausted=", x$finalize_when_all_unresolved_exhausted, sep = "")
  }
  if (!is.null(x$field_attempt_budget_mode)) {
    cat(", field_attempt_budget_mode=", x$field_attempt_budget_mode, sep = "")
  }
  # Only show OpenWebpage size tuning when explicitly set.
  if (!is.null(x$webpage_max_chars)) {
    cat(", webpage_max_chars=", x$webpage_max_chars, sep = "")
  }
  if (!is.null(x$webpage_max_chunks)) {
    cat(", webpage_max_chunks=", x$webpage_max_chunks, sep = "")
  }
  if (!is.null(x$webpage_chunk_chars)) {
    cat(", webpage_chunk_chars=", x$webpage_chunk_chars, sep = "")
  }
  if (!is.null(x$webpage_heuristic_profile) &&
      !identical(x$webpage_heuristic_profile, "generic")) {
    cat(", webpage_heuristic_profile=", x$webpage_heuristic_profile, sep = "")
  }
  if (!is.null(x$wikidata_template_path) && nzchar(as.character(x$wikidata_template_path))) {
    cat(", wikidata_template_path=", x$wikidata_template_path, sep = "")
  }
  cat("\n")
  invisible(x)
}

#' Tor Options
#'
#' Configure shared Tor exit tracking for healthier circuit rotation.
#'
#' @param registry_path Path to the shared SQLite registry file (default: user cache).
#' @param dirty_tor_exists Enable the registry (tracks good/bad/overused exits).
#' @param bad_ttl Seconds to keep a bad/tainted exit before reuse (default: 3600).
#' @param good_ttl Seconds to treat an exit as good before refreshing (default: 1800).
#' @param overuse_threshold Max recent uses before a good exit is considered overloaded.
#' @param overuse_decay Window (seconds) for overuse counting before decaying.
#' @param max_rotation_attempts Max attempts to find a clean exit before giving up.
#' @param ip_cache_ttl Seconds to cache exit IP lookups.
#'
#' @return An object of class \code{asa_tor}
#'
#' @export
tor_options <- function(registry_path = NULL,
                        dirty_tor_exists = ASA_TOR_REGISTRY_ENABLED,
                        bad_ttl = ASA_TOR_BAD_TTL,
                        good_ttl = ASA_TOR_GOOD_TTL,
                        overuse_threshold = ASA_TOR_OVERUSE_THRESHOLD,
                        overuse_decay = ASA_TOR_OVERUSE_DECAY,
                        max_rotation_attempts = ASA_TOR_MAX_ROTATION_ATTEMPTS,
                        ip_cache_ttl = ASA_TOR_IP_CACHE_TTL) {

  normalized <- .normalize_tor_registry_options(
    registry_path = registry_path,
    enable = dirty_tor_exists,
    bad_ttl = bad_ttl,
    good_ttl = good_ttl,
    overuse_threshold = overuse_threshold,
    overuse_decay = overuse_decay,
    max_rotation_attempts = max_rotation_attempts,
    ip_cache_ttl = ip_cache_ttl,
    create_parent_for_custom = FALSE
  )

  structure(
    list(
      registry_path = normalized$registry_path,
      dirty_tor_exists = normalized$enable,
      bad_ttl = normalized$bad_ttl,
      good_ttl = normalized$good_ttl,
      overuse_threshold = normalized$overuse_threshold,
      overuse_decay = normalized$overuse_decay,
      max_rotation_attempts = normalized$max_rotation_attempts,
      ip_cache_ttl = normalized$ip_cache_ttl
    ),
    class = "asa_tor"
  )
}

#' Print Method for asa_tor Objects
#'
#' @param x An asa_tor object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_tor
#' @export
print.asa_tor <- function(x, ...) {
  cat("Tor Registry:    ", x$registry_path, "\n", sep = "")
  cat("Registry Enabled:", if (isTRUE(x$dirty_tor_exists)) "Yes" else "No", "\n", sep = " ")
  cat("Bad TTL:         ", x$bad_ttl, "s; Good TTL: ", x$good_ttl, "s\n", sep = "")
  cat("Overuse Thresh:  >", x$overuse_threshold, " uses in ", x$overuse_decay, "s\n", sep = "")
  invisible(x)
}


# ============================================================================
# AGENT S3 CLASSES
# ============================================================================

#' Constructor for asa_agent Objects
#'
#' Creates an S3 object representing an initialized ASA search agent.
#'
#' @param python_agent The underlying Python agent object
#' @param backend LLM backend name (e.g., "openai", "groq")
#' @param model Model identifier
#' @param config Agent configuration list
#' @param llm Optional LLM object used by LangGraph
#' @param tools Optional list of tools associated with the agent
#'
#' @return An object of class \code{asa_agent}
#'
#' @export
asa_agent <- function(python_agent, backend, model, config, llm = NULL, tools = NULL) {
  structure(
    list(
      python_agent = python_agent,
      backend = backend,
      model = model,
      config = config,
      llm = llm,
      tools = tools,
      created_at = Sys.time()
    ),
    class = "asa_agent"
  )
}

#' Print Method for asa_agent Objects
#'
#' @param x An asa_agent object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_agent
#' @export
print.asa_agent <- function(x, ...) {
  cat("ASA Search Agent\n")
  cat("================\n")
  cat("Backend:        ", x$backend, "\n", sep = "")
  cat("Model:          ", x$model, "\n", sep = "")

  use_folding <- isTRUE(x$config$use_memory_folding %||% x$config$memory_folding)
  cat("Memory Folding: ", if (use_folding) "Enabled" else "Disabled", "\n", sep = "")
  if (use_folding) {
    cat("  Threshold:    ", x$config$memory_threshold %||% "N/A", " messages\n", sep = "")
    cat("  Keep Recent:  ", x$config$memory_keep_recent %||% "N/A", " exchanges\n", sep = "")
  }
  rec_limit <- .normalize_recursion_limit(x$config$recursion_limit %||% NULL)
  cat("Recursion Limit:", if (is.null(rec_limit)) "Auto" else rec_limit, "\n", sep = " ")
  cat(
    "Proxy:          ",
    .format_proxy(x$config$proxy, mode = x$config$proxy_mode, source = x$config$proxy_source),
    "\n",
    sep = ""
  )
  cat("Created:        ", format(x$created_at, "%Y-%m-%d %H:%M:%S"), "\n", sep = "")

  invisible(x)
}

#' Summary Method for asa_agent Objects
#'
#' @param object An asa_agent object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_agent
#' @export
summary.asa_agent <- function(object, ...) {
  cat("ASA Agent Summary\n")
  cat("-----------------\n")
  cat("Configuration:\n")
  print(object$config)

  invisible(list(
    backend = object$backend,
    model = object$model,
    config = object$config
  ))
}

#' Constructor for asa_response Objects
#'
#' Creates an S3 object representing an agent response.
#'
#' @param message The final response text
#' @param status_code Status code (200 = success, 100 = error)
#' @param raw_response The full Python response object
#' @param trace Full text trace of agent execution
#' @param trace_json Structured JSON trace (when available)
#' @param elapsed_time Execution time in minutes
#' @param fold_stats Diagnostic metrics from memory folding (list).
#'   Includes \code{fold_count} (integer or error message string),
#'   \code{fold_messages_removed}, \code{fold_total_messages_removed},
#'   \code{fold_chars_input}, \code{fold_summary_chars} (effective per-fold
#'   summary growth chars), \code{fold_summary_total_chars},
#'   \code{fold_summary_delta_chars},
#'   \code{fold_trigger_reason}, \code{fold_safe_boundary_idx},
#'   \code{fold_compression_ratio}, \code{fold_parse_success}, and
#'   \code{fold_summarizer_latency_m}.
#' @param prompt The original prompt
#' @param thread_id Resolved LangGraph thread identifier used for this run.
#' @param stop_reason Terminal stop reason from agent state (when available).
#' @param budget_state Tool-call budget state snapshot from agent state.
#' @param field_status Per-field extraction status map from agent state.
#' @param diagnostics Runtime diagnostics counters and intervention notes.
#' @param json_repair JSON repair/fallback events emitted during execution.
#' @param final_payload Canonical terminal payload from agent state (when available).
#' @param terminal_valid Logical flag indicating whether a schema-valid terminal
#'   payload was emitted.
#' @param terminal_payload_hash Canonical hash of terminal payload from agent state.
#' @param payload_integrity Diagnostics about released payload provenance, repair,
#'   and truncation markers.
#' @param completion_gate Deterministic outcome-verification report from agent state.
#' @param tokens_used Total token count for this invocation (integer, or NA).
#' @param input_tokens Input (prompt) token count (integer, or NA).
#' @param output_tokens Output (completion) token count (integer, or NA).
#' @param token_trace Per-step token usage trace (list).
#'
#' @return An object of class \code{asa_response}
#'
#' @export
asa_response <- function(message, status_code, raw_response, trace,
                         elapsed_time, prompt,
                         trace_json = "", fold_stats = list(),
                         thread_id = NULL, stop_reason = NULL,
                         budget_state = list(), field_status = list(),
                         diagnostics = list(),
                         json_repair = list(),
                         final_payload = NULL,
                         terminal_valid = FALSE,
                         terminal_payload_hash = NULL,
                         payload_integrity = list(),
                         completion_gate = list(),
                         tokens_used = NA_integer_,
                         input_tokens = NA_integer_,
                         output_tokens = NA_integer_,
                         token_trace = list()) {
  structure(
    list(
      message = message,
      status_code = status_code,
      raw_response = raw_response,
      trace = trace,
      trace_json = trace_json,
      elapsed_time = elapsed_time,
      fold_stats = fold_stats,
      prompt = prompt,
      thread_id = thread_id,
      stop_reason = stop_reason,
      budget_state = budget_state,
      field_status = field_status,
      diagnostics = diagnostics,
      json_repair = json_repair,
      final_payload = final_payload,
      terminal_valid = terminal_valid,
      terminal_payload_hash = terminal_payload_hash,
      payload_integrity = payload_integrity,
      completion_gate = completion_gate,
      tokens_used = tokens_used,
      input_tokens = input_tokens,
      output_tokens = output_tokens,
      token_trace = token_trace,
      created_at = Sys.time()
    ),
    class = "asa_response"
  )
}

#' Print Method for asa_response Objects
#'
#' @param x An asa_response object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_response
#' @export
print.asa_response <- function(x, ...) {
  cat("ASA Agent Response\n")
  cat("==================\n")
  cat("Status:    ", if (x$status_code == 200) "Success" else "Error", " (", x$status_code, ")\n", sep = "")
  cat("Time:      ", format_duration(x$elapsed_time), "\n", sep = "")
  if (!is.na(x$tokens_used) && x$tokens_used > 0L) {
    cat("Tokens:    ", x$tokens_used, "\n", sep = "")
  }
  fc <- x$fold_stats$fold_count
  if (!is.null(fc) && !identical(fc, 0L)) {
    if (is.character(fc)) {
      cat("Fold error: ", fc, "\n", sep = "")
    } else {
      cat("Folds:     ", fc, "\n", sep = "")
      fs <- x$fold_stats
      if (length(fs) > 1) {
        cat("  Last fold:     ", fs$fold_messages_removed %||% 0L, " msgs removed, ",
            fs$fold_chars_input %||% 0L, " chars input\n", sep = "")
        cat("  Total removed: ", fs$fold_total_messages_removed %||% 0L, " msgs\n", sep = "")
        cat("  Summary growth:", fs$fold_summary_chars %||% 0L, " chars\n", sep = "")
        if (!is.null(fs$fold_summary_total_chars)) {
          cat("  Summary total: ", fs$fold_summary_total_chars, " chars\n", sep = "")
        }
        if (!is.null(fs$fold_compression_ratio)) {
          cat("  Compression:   ", format(round(as.numeric(fs$fold_compression_ratio), 3), nsmall = 3),
              "x (input/growth)\n", sep = "")
        }
      }
    }
  }
  cat("\nPrompt:\n")
  cat("  ", truncate_string(x$prompt, 80), "\n", sep = "")
  cat("\nResponse:\n")
  if (!is.na(x$message)) {
    # Word wrap the message
    msg_lines <- strwrap(x$message, width = 76)
    cat(paste("  ", msg_lines, collapse = "\n"), "\n", sep = "")
  } else {
    cat("  [No response]\n")
  }

  invisible(x)
}

#' Summary Method for asa_response Objects
#'
#' @param object An asa_response object
#' @param show_trace Include full trace in output
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_response
#' @export
summary.asa_response <- function(object, show_trace = FALSE, ...) {
  print(object)

  if (show_trace && !is.null(object$trace) && object$trace != "") {
    cat("\nFull Trace:\n")
    cat("----------\n")
    cat(object$trace, "\n")
  }

  invisible(list(
    status_code = object$status_code,
    elapsed_time = object$elapsed_time,
    message_length = nchar(object$message %||% ""),
    trace_length = nchar(object$trace %||% ""),
    fold_count = object$fold_stats$fold_count,
    fold_stats = object$fold_stats,
    thread_id = object$thread_id %||% NA_character_,
    stop_reason = object$stop_reason %||% NA_character_,
    budget_state = object$budget_state %||% list(),
    field_status = object$field_status %||% list()
  ))
}

#' Constructor for asa_result Objects
#'
#' Creates an S3 object representing the result of a research task.
#'
#' @param prompt The original prompt
#' @param message The agent's response text
#' @param parsed Parsed output (list or NULL)
#' @param raw_output Full agent trace
#' @param trace_json Structured JSON trace (when available)
#' @param elapsed_time Execution time in minutes
#' @param status Status ("success" or "error")
#' @param search_tier Which search tier was used ("primp", "selenium", "ddgs",
#'   "requests", or "unknown"). Useful for assessing result quality.
#' @param parsing_status List with JSON parsing validation info: valid (logical),
#'   reason ("ok", "parsing_failed", "not_object", "missing_fields", "null_values",
#'   "no_validation"), and missing (character vector of missing/invalid fields).
#' @param execution Optional operational metadata list. Common fields include
#'   \code{thread_id}, \code{stop_reason}, \code{status_code}, and tool budget
#'   counters extracted from agent state.
#'
#' @return An object of class \code{asa_result}
#'
#' @export
asa_result <- function(prompt, message, parsed, raw_output,
                       elapsed_time, status,
                       search_tier = "unknown", parsing_status = NULL,
                       trace_json = "", execution = NULL) {
  # Default parsing_status if not provided

  if (is.null(parsing_status)) {
    parsing_status <- list(valid = TRUE, reason = "no_validation", missing = character(0))
  }
  if (is.null(execution) || !is.list(execution)) {
    execution <- list()
  }

  structure(
    list(
      prompt = prompt,
      message = message,
      parsed = parsed,
      raw_output = raw_output,
      trace_json = trace_json,
      elapsed_time = elapsed_time,
      status = status,
      search_tier = search_tier,
      parsing_status = parsing_status,
      execution = execution,
      created_at = Sys.time()
    ),
    class = "asa_result"
  )
}

#' Print Method for asa_result Objects
#'
#' @param x An asa_result object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_result
#' @export
print.asa_result <- function(x, ...) {
  cat("ASA Task Result\n")
  cat("===============\n")
  cat("Status:  ", x$status, "\n", sep = "")
  cat("Time:    ", format_duration(x$elapsed_time), "\n", sep = "")
  if (!is.null(x$search_tier) && x$search_tier != "unknown") {
    cat("Search:  ", x$search_tier, "\n", sep = "")
  }
  stop_reason <- .try_or(as.character(x$execution$stop_reason), character(0))
  if (length(stop_reason) > 0 && nzchar(stop_reason[[1]])) {
    cat("Stop:    ", stop_reason[[1]], "\n", sep = "")
  }
  tool_used <- .as_scalar_int(x$execution$tool_calls_used)
  tool_lim <- .as_scalar_int(x$execution$tool_calls_limit)
  if (!is.na(tool_used) && !is.na(tool_lim)) {
    cat("Tools:   ", tool_used, "/", tool_lim, "\n", sep = "")
  }
  ts <- x$token_stats
  if (!is.null(ts) && !is.na(ts$tokens_used) && ts$tokens_used > 0L) {
    cat("Tokens:  ", ts$tokens_used, " (in=", ts$input_tokens,
        ", out=", ts$output_tokens, ")\n", sep = "")
    if (!is.na(ts$fold_tokens) && ts$fold_tokens > 0L) {
      cat("  Fold:  ", ts$fold_tokens, "\n", sep = "")
    }
  } else {
    tok <- .as_scalar_int(x$execution$tokens_used)
    if (!is.na(tok) && tok > 0L) {
      cat("Tokens:  ", tok, "\n", sep = "")
    }
  }
  cat("\n")
  cat("Prompt:\n")
  prompt_lines <- strwrap(x$prompt, width = 74)
  cat(paste("  ", prompt_lines, collapse = "\n"), "\n", sep = "")
  cat("\n")
  cat("Response:\n")
  if (!is.na(x$message) && x$message != "") {
    msg_lines <- strwrap(x$message, width = 74)
    cat(paste("  ", msg_lines, collapse = "\n"), "\n", sep = "")
  } else {
    cat("  [No response]\n")
  }

  if (!is.null(x$parsed)) {
    cat("\nParsed Output:\n")
    for (name in names(x$parsed)) {
      val <- x$parsed[[name]]
      if (length(val) == 1) {
        cat("  ", name, ": ", as.character(val), "\n", sep = "")
      } else {
        cat("  ", name, ": [", length(val), " items]\n", sep = "")
      }
    }
  }

  # Show parsing status if validation failed
  if (!is.null(x$parsing_status) && !isTRUE(x$parsing_status$valid)) {
    cat("\nParsing Status: FAILED\n")
    cat("  Reason: ", x$parsing_status$reason, "\n", sep = "")
    if (length(x$parsing_status$missing) > 0) {
      cat("  Missing/Invalid: ", paste(x$parsing_status$missing, collapse = ", "), "\n", sep = "")
    }
  }

  invisible(x)
}

#' Summary Method for asa_result Objects
#'
#' @param object An asa_result object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_result
#' @export
summary.asa_result <- function(object, ...) {
  cat("Task Result Summary\n")
  cat("-------------------\n")
  cat("Status: ", object$status, "\n", sep = "")
  cat("Time: ", format_duration(object$elapsed_time), "\n", sep = "")
  cat("Response length: ", nchar(object$message %||% ""), " chars\n", sep = "")
  tok <- .as_scalar_int(object$token_stats$tokens_used)
  if (is.na(tok)) {
    tok <- .as_scalar_int(object$execution$tokens_used)
  }
  if (!is.na(tok) && tok > 0L) {
    cat("Tokens: ", tok, "\n", sep = "")
  }
  stop_reason <- .try_or(as.character(object$execution$stop_reason), character(0))
  stop_reason_val <- if (length(stop_reason) > 0 && nzchar(stop_reason[[1]])) {
    stop_reason[[1]]
  } else {
    NA_character_
  }
  if (length(stop_reason) > 0 && nzchar(stop_reason[[1]])) {
    cat("Stop reason: ", stop_reason[[1]], "\n", sep = "")
  }
  if (!is.null(object$parsed)) {
    cat("Parsed fields: ", paste(names(object$parsed), collapse = ", "), "\n", sep = "")
  }

  invisible(list(
    status = object$status,
    elapsed_time = object$elapsed_time,
    message_length = nchar(object$message %||% ""),
    parsed_fields = names(object$parsed),
    stop_reason = stop_reason_val,
    thread_id = object$execution$thread_id %||% NA_character_,
    status_code = object$execution$status_code %||% NA_integer_,
    tool_calls_used = object$execution$tool_calls_used %||% NA_integer_,
    tool_calls_limit = object$execution$tool_calls_limit %||% NA_integer_,
    tool_calls_remaining = object$execution$tool_calls_remaining %||% NA_integer_,
    tokens_used = tok %||% NA_integer_
  ))
}

#' Convert asa_result to Data Frame
#'
#' @param x An asa_result object
#' @param ... Additional arguments (ignored)
#'
#' @return A single-row data frame
#'
#' @method as.data.frame asa_result
#' @export
as.data.frame.asa_result <- function(x, ...) {
  tokens_used <- .as_scalar_int(x$token_stats$tokens_used)
  if (is.na(tokens_used)) {
    tokens_used <- .as_scalar_int(x$execution$tokens_used)
  }
  df <- data.frame(
    prompt = x$prompt,
    message = x$message %||% NA_character_,
    status = x$status %||% NA_character_,
    elapsed_time = x$elapsed_time %||% NA_real_,
    stop_reason = x$execution$stop_reason %||% NA_character_,
    thread_id = x$execution$thread_id %||% NA_character_,
    status_code = x$execution$status_code %||% NA_integer_,
    tool_calls_used = x$execution$tool_calls_used %||% NA_integer_,
    tool_calls_limit = x$execution$tool_calls_limit %||% NA_integer_,
    tool_calls_remaining = x$execution$tool_calls_remaining %||% NA_integer_,
    fold_count = x$execution$fold_count %||% NA_integer_,
    tokens_used = tokens_used %||% NA_integer_,
    stringsAsFactors = FALSE
  )

  # Add parsed fields as columns
  if (!is.null(x$parsed) && is.list(x$parsed)) {
    for (name in names(x$parsed)) {
      val <- x$parsed[[name]]
      if (length(val) == 1) {
        df[[name]] <- as.character(val)
      }
    }
  }

  df
}


#' Constructor for asa_enumerate_result Objects
#'
#' Creates an S3 object representing the result of an enumeration task.
#'
#' @param data data.frame containing the enumeration results
#' @param status Result status: "complete", "partial", or "failed"
#' @param stop_reason Why the enumeration stopped (e.g., "target_reached", "novelty_plateau")
#' @param metrics List with execution metrics (rounds, queries_used, etc.)
#' @param provenance Optional data.frame with source information per row
#' @param plan The enumeration plan from the planner agent
#' @param checkpoint_file Path to saved checkpoint file
#' @param query The original enumeration query
#' @param schema The schema used for extraction
#'
#' @return An object of class \code{asa_enumerate_result}
#'
#' @export
asa_enumerate_result <- function(data, status, stop_reason, metrics,
                                 provenance = NULL, plan = NULL,
                                 checkpoint_file = NULL, query = NULL,
                                 schema = NULL) {
  structure(
    list(
      data = data,
      status = status,
      stop_reason = stop_reason,
      metrics = metrics,
      provenance = provenance,
      plan = plan,
      checkpoint_file = checkpoint_file,
      query = query,
      schema = schema,
      created_at = Sys.time()
    ),
    class = "asa_enumerate_result"
  )
}

#' Print Method for asa_enumerate_result Objects
#'
#' @param x An asa_enumerate_result object
#' @param n Number of data rows to preview (default: 6)
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_enumerate_result
#' @export
print.asa_enumerate_result <- function(x, n = 6, ...) {
  cat("ASA Enumeration Result\n")
  cat("===================\n")
  cat("Status:      ", x$status, "\n", sep = "")
  cat("Stop Reason: ", x$stop_reason %||% "N/A", "\n", sep = "")
  cat("Items Found: ", nrow(x$data), "\n", sep = "")

  # Metrics summary
  if (!is.null(x$metrics)) {
    cat("\nMetrics:\n")
    if (!is.null(x$metrics$round_number)) {
      cat("  Rounds:        ", x$metrics$round_number, "\n", sep = "")
    }
    if (!is.null(x$metrics$queries_used)) {
      cat("  Queries Used:  ", x$metrics$queries_used, "\n", sep = "")
    }
    if (!is.null(x$metrics$elapsed_total)) {
      cat("  Time Elapsed:  ", format_duration(x$metrics$elapsed_total), "\n", sep = "")
    }
    if (!is.null(x$metrics$novelty_rate)) {
      cat("  Novelty Rate:  ", sprintf("%.1f%%", x$metrics$novelty_rate * 100), "\n", sep = "")
    }
    if (!is.null(x$metrics$tokens_used) && x$metrics$tokens_used > 0) {
      cat("  Tokens Used:   ", x$metrics$tokens_used, "\n", sep = "")
      if (!is.null(x$metrics$input_tokens)) {
        cat("    Input:       ", x$metrics$input_tokens, "\n", sep = "")
        cat("    Output:      ", x$metrics$output_tokens, "\n", sep = "")
      }
      if (nrow(x$data) > 0) {
        cat("  Tokens/Result: ", round(x$metrics$tokens_used / nrow(x$data)), "\n", sep = "")
      }
    }
  }

  # Schema summary
  if (!is.null(x$schema)) {
    cat("\nSchema: ", paste(names(x$schema), collapse = ", "), "\n", sep = "")
  }

  # Data preview
  if (nrow(x$data) > 0) {
    cat("\nData Preview (first ", min(n, nrow(x$data)), " rows):\n", sep = "")
    print(utils::head(x$data, n))
  } else {
    cat("\n[No data]\n")
  }

  # Checkpoint info
  if (!is.null(x$checkpoint_file)) {
    cat("\nCheckpoint: ", x$checkpoint_file, "\n", sep = "")
  }

  invisible(x)
}

#' Summary Method for asa_enumerate_result Objects
#'
#' @param object An asa_enumerate_result object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_enumerate_result
#' @export
summary.asa_enumerate_result <- function(object, ...) {
  cat("Enumeration Result Summary\n")
  cat("-----------------------\n")
  cat("Query: ", truncate_string(object$query %||% "[unknown]", 60), "\n", sep = "")
  cat("Status: ", object$status, "\n", sep = "")
  cat("Stop Reason: ", object$stop_reason %||% "N/A", "\n", sep = "")
  cat("Total Items: ", nrow(object$data), "\n", sep = "")

  if (!is.null(object$schema)) {
    cat("Columns: ", paste(names(object$schema), collapse = ", "), "\n", sep = "")
  }

  if (!is.null(object$metrics)) {
    cat("\nExecution Metrics:\n")
    for (name in names(object$metrics)) {
      val <- object$metrics[[name]]
      if (is.numeric(val)) {
        if (grepl("time|elapsed", name, ignore.case = TRUE)) {
          cat("  ", name, ": ", format_duration(val), "\n", sep = "")
        } else if (grepl("rate", name, ignore.case = TRUE)) {
          cat("  ", name, ": ", sprintf("%.2f", val), "\n", sep = "")
        } else {
          cat("  ", name, ": ", val, "\n", sep = "")
        }
      }
    }
  }

  # Provenance summary
  if (!is.null(object$provenance) && nrow(object$provenance) > 0) {
    cat("\nProvenance:\n")
    sources <- unique(object$provenance$source_url)
    cat("  Unique sources: ", length(sources), "\n", sep = "")
    avg_conf <- mean(object$provenance$confidence, na.rm = TRUE)
    cat("  Avg confidence: ", sprintf("%.2f", avg_conf), "\n", sep = "")
  }

  invisible(list(
    status = object$status,
    stop_reason = object$stop_reason,
    n_items = nrow(object$data),
    n_columns = ncol(object$data),
    metrics = object$metrics,
    has_provenance = !is.null(object$provenance)
  ))
}

#' Convert asa_enumerate_result to Data Frame
#'
#' @param x An asa_enumerate_result object
#' @param ... Additional arguments (ignored)
#'
#' @return The data data.frame from the result
#'
#' @method as.data.frame asa_enumerate_result
#' @export
as.data.frame.asa_enumerate_result <- function(x, ...) {
  x$data
}

#' Write asa_enumerate_result to CSV
#'
#' @param x An asa_enumerate_result object
#' @param file Path to output CSV file
#' @param include_provenance Include provenance as additional columns
#' @param ... Additional arguments passed to write.csv
#'
#' @return Invisibly returns the file path
#'
#' @export
write_csv.asa_enumerate_result <- function(x, file, include_provenance = FALSE, ...) {
  data_to_write <- x$data

  if (include_provenance && !is.null(x$provenance)) {
    # Merge provenance columns
    prov <- x$provenance
    names(prov) <- paste0("_prov_", names(prov))
    if (nrow(prov) == nrow(data_to_write)) {
      data_to_write <- cbind(data_to_write, prov)
    }
  }

  utils::write.csv(data_to_write, file, row.names = FALSE, ...)
  message("Written to: ", file)
  invisible(file)
}

#' Constructor for asa_audit_result Objects
#'
#' Creates an S3 object representing the result of a data quality audit.
#'
#' @param data data.frame with original data plus audit columns (_audit_flag, _audit_notes)
#' @param audit_summary Character string with high-level findings
#' @param issues List of identified issues with severity and descriptions
#' @param recommendations Character vector of suggested remediation queries
#' @param completeness_score Numeric 0-1 score for data completeness
#' @param consistency_score Numeric 0-1 score for data consistency
#' @param backend_used Which backend performed the audit ("claude_code" or "langgraph")
#' @param elapsed_time Execution time in seconds
#' @param query The original query (if available)
#' @param checks Character vector of checks that were performed
#'
#' @return An object of class \code{asa_audit_result}
#'
#' @export
asa_audit_result <- function(data, audit_summary, issues, recommendations,
                             completeness_score, consistency_score,
                             backend_used, elapsed_time,
                             query = NULL, checks = NULL) {
  structure(
    list(
      data = data,
      audit_summary = audit_summary,
      issues = issues,
      recommendations = recommendations,
      completeness_score = completeness_score,
      consistency_score = consistency_score,
      backend_used = backend_used,
      elapsed_time = elapsed_time,
      query = query,
      checks = checks,
      created_at = Sys.time()
    ),
    class = "asa_audit_result"
  )
}

#' Print Method for asa_audit_result Objects
#'
#' @param x An asa_audit_result object
#' @param n Number of data rows to preview (default: 6)
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the object
#'
#' @method print asa_audit_result
#' @export
print.asa_audit_result <- function(x, n = 6, ...) {
  cat("ASA Audit Result\n")
  cat("================\n")

  # Scores
  cat("Completeness: ", sprintf("%.0f%%", x$completeness_score * 100), "\n", sep = "")
  cat("Consistency:  ", sprintf("%.0f%%", x$consistency_score * 100), "\n", sep = "")
  cat("Backend:      ", x$backend_used, "\n", sep = "")
  cat("Time:         ", format_duration(x$elapsed_time), "\n", sep = "")

  # Summary
  if (!is.null(x$audit_summary) && x$audit_summary != "") {
    cat("\nSummary:\n")
    summary_lines <- strwrap(x$audit_summary, width = 74)
    cat(paste("  ", summary_lines, collapse = "\n"), "\n", sep = "")
  }

  # Issues
  if (length(x$issues) > 0) {
    cat("\nIssues Found: ", length(x$issues), "\n", sep = "")
    for (i in seq_along(x$issues)) {
      issue <- x$issues[[i]]
      severity <- issue$severity %||% "info"
      desc <- issue$description %||% issue$message %||% "[no description]"
      cat("  [", toupper(severity), "] ", truncate_string(desc, 60), "\n", sep = "")
      if (i >= 5 && length(x$issues) > 5) {
        cat("  ... and ", length(x$issues) - 5, " more\n", sep = "")
        break
      }
    }
  } else {
    cat("\nNo issues found.\n")
  }

  # Recommendations
  if (length(x$recommendations) > 0) {
    cat("\nRecommendations:\n")
    for (i in seq_along(x$recommendations)) {
      cat("  - ", truncate_string(x$recommendations[[i]], 70), "\n", sep = "")
      if (i >= 3 && length(x$recommendations) > 3) {
        cat("  ... and ", length(x$recommendations) - 3, " more\n", sep = "")
        break
      }
    }
  }

  # Data preview with audit flags
  if (nrow(x$data) > 0) {
    # Count by flag
    if ("_audit_flag" %in% names(x$data)) {
      flag_counts <- table(x$data[["_audit_flag"]])
      cat("\nFlag Distribution:\n")
      for (flag in names(flag_counts)) {
        cat("  ", flag, ": ", flag_counts[[flag]], "\n", sep = "")
      }
    }

    cat("\nData Preview (first ", min(n, nrow(x$data)), " rows):\n", sep = "")
    # Show key columns including audit flags
    preview_cols <- names(x$data)
    if (length(preview_cols) > 6) {
      # Prioritize audit columns and first few data columns
      audit_cols <- grep("^_audit", preview_cols, value = TRUE)
      data_cols <- setdiff(preview_cols, audit_cols)[1:min(4, length(setdiff(preview_cols, audit_cols)))]
      preview_cols <- c(data_cols, audit_cols)
    }
    print(utils::head(x$data[, preview_cols, drop = FALSE], n))
  }

  invisible(x)
}

#' Summary Method for asa_audit_result Objects
#'
#' @param object An asa_audit_result object
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns a summary list
#'
#' @method summary asa_audit_result
#' @export
summary.asa_audit_result <- function(object, ...) {
  cat("Audit Result Summary\n")
  cat("--------------------\n")
  if (!is.null(object$query)) {
    cat("Query: ", truncate_string(object$query, 60), "\n", sep = "")
  }
  cat("Checks Performed: ", paste(object$checks %||% "all", collapse = ", "), "\n", sep = "")
  cat("Backend: ", object$backend_used, "\n", sep = "")
  cat("Time: ", format_duration(object$elapsed_time), "\n", sep = "")

  cat("\nScores:\n")
  cat("  Completeness: ", sprintf("%.1f%%", object$completeness_score * 100), "\n", sep = "")
  cat("  Consistency:  ", sprintf("%.1f%%", object$consistency_score * 100), "\n", sep = "")

  cat("\nData:\n")
  cat("  Total Rows: ", nrow(object$data), "\n", sep = "")

  if ("_audit_flag" %in% names(object$data)) {
    flag_counts <- table(object$data[["_audit_flag"]])
    cat("  Flags:\n")
    for (flag in names(flag_counts)) {
      pct <- flag_counts[[flag]] / nrow(object$data) * 100
      cat("    ", flag, ": ", flag_counts[[flag]], " (", sprintf("%.1f%%", pct), ")\n", sep = "")
    }
  }

  cat("\nIssues: ", length(object$issues), "\n", sep = "")
  if (length(object$issues) > 0) {
    severities <- sapply(object$issues, function(x) x$severity %||% "info")
    sev_table <- table(severities)
    for (sev in names(sev_table)) {
      cat("  ", sev, ": ", sev_table[[sev]], "\n", sep = "")
    }
  }

  cat("Recommendations: ", length(object$recommendations), "\n", sep = "")

  invisible(list(
    completeness_score = object$completeness_score,
    consistency_score = object$consistency_score,
    n_rows = nrow(object$data),
    n_issues = length(object$issues),
    n_recommendations = length(object$recommendations),
    backend = object$backend_used,
    elapsed_time = object$elapsed_time
  ))
}

#' Convert asa_audit_result to Data Frame
#'
#' @param x An asa_audit_result object
#' @param ... Additional arguments (ignored)
#'
#' @return The audited data.frame with audit columns
#'
#' @method as.data.frame asa_audit_result
#' @export
as.data.frame.asa_audit_result <- function(x, ...) {
  x$data
}
