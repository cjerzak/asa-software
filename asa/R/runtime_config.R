#' Check Whether an Agent Matches an asa_config
#' @keywords internal
.agent_matches_config <- function(agent, config) {
  if (is.null(agent) || is.null(config)) return(FALSE)
  if (!inherits(agent, "asa_agent")) return(FALSE)
  if (!inherits(config, "asa_config")) return(FALSE)

  desired_proxy <- .try_or({
    info <- .resolve_proxy(config$proxy)
    proxy_value <- info$proxy
    if (identical(info$mode, "auto") && !is.null(proxy_value)) {
      proxy_value <- .try_or({
        .validate_proxy_url(proxy_value, "proxy")
        proxy_value
      })
    }
    proxy_value
  }, config$proxy)

  agent_folding <- agent$config$use_memory_folding %||% agent$config$memory_folding

  agent_use_browser <- agent$config$use_browser %||% ASA_DEFAULT_USE_BROWSER
  config_use_browser <- config$use_browser %||% ASA_DEFAULT_USE_BROWSER

  same_backend <- identical(agent$backend, config$backend)
  same_model <- identical(agent$model, config$model)
  same_conda <- identical(agent$config$conda_env, config$conda_env)
  same_proxy <- identical(agent$config$proxy, desired_proxy)
  same_browser <- identical(agent_use_browser, config_use_browser)
  same_folding <- identical(agent_folding, config$memory_folding)
  same_threshold <- identical(agent$config$memory_threshold, config$memory_threshold)
  same_keep <- identical(agent$config$memory_keep_recent, config$memory_keep_recent)
  same_om_enabled <- identical(
    isTRUE(agent$config$use_observational_memory %||% FALSE),
    isTRUE(config$use_observational_memory %||% FALSE)
  )
  same_om_cross_thread <- identical(
    isTRUE(agent$config$om_cross_thread_memory %||% FALSE),
    isTRUE(config$om_cross_thread_memory %||% FALSE)
  )
  same_om_obs_budget <- identical(
    as.integer(agent$config$om_observation_token_budget %||% ASA_DEFAULT_OM_OBSERVATION_TOKENS),
    as.integer(config$om_observation_token_budget %||% ASA_DEFAULT_OM_OBSERVATION_TOKENS)
  )
  same_om_ref_budget <- identical(
    as.integer(agent$config$om_reflection_token_budget %||% ASA_DEFAULT_OM_REFLECTION_TOKENS),
    as.integer(config$om_reflection_token_budget %||% ASA_DEFAULT_OM_REFLECTION_TOKENS)
  )
  same_om_buffer <- identical(
    as.integer(agent$config$om_buffer_tokens %||% ASA_DEFAULT_OM_BUFFER_TOKENS),
    as.integer(config$om_buffer_tokens %||% ASA_DEFAULT_OM_BUFFER_TOKENS)
  )
  same_om_activation <- identical(
    as.numeric(agent$config$om_buffer_activation %||% ASA_DEFAULT_OM_BUFFER_ACTIVATION),
    as.numeric(config$om_buffer_activation %||% ASA_DEFAULT_OM_BUFFER_ACTIVATION)
  )
  same_om_block_after <- identical(
    as.numeric(agent$config$om_block_after %||% ASA_DEFAULT_OM_BLOCK_AFTER),
    as.numeric(config$om_block_after %||% ASA_DEFAULT_OM_BLOCK_AFTER)
  )
  same_om_async <- identical(
    isTRUE(agent$config$om_async_prebuffer %||% ASA_DEFAULT_OM_ASYNC_PREBUFFER),
    isTRUE(config$om_async_prebuffer %||% ASA_DEFAULT_OM_ASYNC_PREBUFFER)
  )
  same_rate <- identical(agent$config$rate_limit, config$rate_limit)
  same_timeout <- identical(agent$config$timeout, config$timeout)
  same_recursion_limit <- identical(
    .normalize_recursion_limit(agent$config$recursion_limit %||% NULL),
    .normalize_recursion_limit(config$recursion_limit %||% NULL)
  )
  same_tor <- identical(agent$config$tor, config$tor)

  # Some search settings affect tool construction (Wikipedia/Search snippet caps).
  # Treat changes as requiring a new agent instance.
  agent_search <- agent$config$search %||% NULL
  config_search <- config$search %||% NULL

  agent_wiki_top_k <- ASA_DEFAULT_WIKI_TOP_K
  agent_wiki_chars <- ASA_DEFAULT_WIKI_CHARS
  agent_search_chars <- 500L
  if (!is.null(agent_search) && (inherits(agent_search, "asa_search") || is.list(agent_search))) {
    agent_wiki_top_k <- agent_search$wiki_top_k_results %||% agent_wiki_top_k
    agent_wiki_chars <- agent_search$wiki_doc_content_chars_max %||% agent_wiki_chars
    agent_search_chars <- agent_search$search_doc_content_chars_max %||% agent_search_chars
  }

  config_wiki_top_k <- ASA_DEFAULT_WIKI_TOP_K
  config_wiki_chars <- ASA_DEFAULT_WIKI_CHARS
  config_search_chars <- 500L
  config_stability_profile <- "stealth_first"
  config_langgraph_node_retries <- FALSE
  config_langgraph_cache_enabled <- FALSE
  if (!is.null(config_search) && (inherits(config_search, "asa_search") || is.list(config_search))) {
    config_wiki_top_k <- config_search$wiki_top_k_results %||% config_wiki_top_k
    config_wiki_chars <- config_search$wiki_doc_content_chars_max %||% config_wiki_chars
    config_search_chars <- config_search$search_doc_content_chars_max %||% config_search_chars
    config_stability_profile <- tolower(as.character(config_search$stability_profile %||% config_stability_profile))
    config_langgraph_node_retries <- isTRUE(config_search$langgraph_node_retries %||% FALSE)
    config_langgraph_cache_enabled <- isTRUE(config_search$langgraph_cache_enabled %||% FALSE)
  }
  agent_stability_profile <- tolower(as.character(agent$config$stability_profile %||% "stealth_first"))
  agent_langgraph_node_retries <- isTRUE(agent$config$langgraph_node_retries %||% FALSE)
  agent_langgraph_cache_enabled <- isTRUE(agent$config$langgraph_cache_enabled %||% FALSE)

  same_search_tools <- identical(as.integer(agent_wiki_top_k), as.integer(config_wiki_top_k)) &&
    identical(as.integer(agent_wiki_chars), as.integer(config_wiki_chars)) &&
    identical(as.integer(agent_search_chars), as.integer(config_search_chars)) &&
    identical(agent_stability_profile, config_stability_profile) &&
    identical(agent_langgraph_node_retries, config_langgraph_node_retries) &&
    identical(agent_langgraph_cache_enabled, config_langgraph_cache_enabled)

  isTRUE(same_backend && same_model && same_conda && same_proxy && same_browser &&
           same_folding && same_threshold && same_keep &&
           same_om_enabled && same_om_cross_thread && same_om_obs_budget &&
           same_om_ref_budget && same_om_buffer && same_om_activation &&
           same_om_block_after && same_om_async &&
           same_rate && same_timeout && same_recursion_limit &&
           same_tor && same_search_tools)
}

.resolve_agent_config_value <- function(config = NULL, agent = NULL, key) {
  value <- NULL

  if (!is.null(config) && inherits(config, "asa_config")) {
    value <- config[[key]] %||% NULL
  }

  if (is.null(value)) {
    if (!is.null(agent) && inherits(agent, "asa_agent")) {
      value <- agent$config[[key]] %||% NULL
    } else if (is.null(agent) && .is_initialized()) {
      value <- .try_or(get_agent()$config[[key]] %||% NULL)
    }
  }

  value
}

.resolve_runtime_settings <- function(config = NULL,
                                     agent = NULL,
                                     temporal = NULL,
                                     allow_read_webpages = NULL,
                                     webpage_relevance_mode = NULL,
                                     webpage_heuristic_profile = NULL,
                                     webpage_embedding_provider = NULL,
                                     webpage_embedding_model = NULL) {
  config_search <- .resolve_agent_config_value(config = config, agent = agent, key = "search")
  config_conda_env <- .resolve_agent_config_value(config = config, agent = agent, key = "conda_env")

  resolved <- .resolve_temporal_and_webpage_reader(
    temporal = temporal,
    config = config,
    config_search = config_search,
    allow_read_webpages = allow_read_webpages,
    webpage_relevance_mode = webpage_relevance_mode,
    webpage_heuristic_profile = webpage_heuristic_profile,
    webpage_embedding_provider = webpage_embedding_provider,
    webpage_embedding_model = webpage_embedding_model
  )

  c(
    list(
      config_search = config_search,
      config_conda_env = config_conda_env
    ),
    resolved
  )
}

.resolve_runtime_inputs <- function(config = NULL,
                                   agent = NULL,
                                   temporal = NULL,
                                   allow_read_webpages = NULL,
                                   webpage_relevance_mode = NULL,
                                   webpage_heuristic_profile = NULL,
                                   webpage_embedding_provider = NULL,
                                   webpage_embedding_model = NULL) {
  runtime <- .resolve_runtime_settings(
    config = config,
    agent = agent,
    temporal = temporal,
    allow_read_webpages = allow_read_webpages,
    webpage_relevance_mode = webpage_relevance_mode,
    webpage_heuristic_profile = webpage_heuristic_profile,
    webpage_embedding_provider = webpage_embedding_provider,
    webpage_embedding_model = webpage_embedding_model
  )

  list(
    runtime = runtime,
    temporal = runtime$temporal,
    allow_rw = runtime$allow_read_webpages
  )
}

.with_runtime_wrappers <- function(runtime, conda_env = NULL, agent = NULL, fn) {
  conda_env <- conda_env %||% runtime$config_conda_env %||% .get_default_conda_env()

  .with_search_config(runtime$config_search, conda_env = conda_env, agent = agent, function() {
    .with_webpage_reader_config(
      runtime$allow_read_webpages,
      relevance_mode = runtime$relevance_mode,
      heuristic_profile = runtime$heuristic_profile,
      embedding_provider = runtime$embedding_provider,
      embedding_model = runtime$embedding_model,
      timeout = runtime$timeout,
      max_bytes = runtime$max_bytes,
      max_chars = runtime$max_chars,
      max_chunks = runtime$max_chunks,
      chunk_chars = runtime$chunk_chars,
      embedding_api_base = runtime$embedding_api_base,
      prefilter_k = runtime$prefilter_k,
      use_mmr = runtime$use_mmr,
      mmr_lambda = runtime$mmr_lambda,
      cache_enabled = runtime$cache_enabled,
      cache_max_entries = runtime$cache_max_entries,
      cache_max_text_chars = runtime$cache_max_text_chars,
      blocked_cache_ttl_sec = runtime$blocked_cache_ttl_sec,
      blocked_cache_max_entries = runtime$blocked_cache_max_entries,
      blocked_probe_bytes = runtime$blocked_probe_bytes,
      blocked_detect_on_200 = runtime$blocked_detect_on_200,
      blocked_body_scan_bytes = runtime$blocked_body_scan_bytes,
      pdf_enabled = runtime$pdf_enabled,
      pdf_timeout = runtime$pdf_timeout,
      pdf_max_bytes = runtime$pdf_max_bytes,
      pdf_max_pages = runtime$pdf_max_pages,
      pdf_max_text_chars = runtime$pdf_max_text_chars,
      user_agent = runtime$user_agent,
      conda_env = conda_env,
      fn = function() {
        .with_temporal(runtime$temporal, fn, agent = agent)
      }
    )
  })
}


#' Run Code with a Temporary Configuration Snapshot
#'
#' Internal helper that snapshots a runtime config, executes code, then
#' best-effort restores the original config on exit.
#'
#' @param snapshot_fn Function returning previous config snapshot.
#' @param restore_fn Function restoring previous config.
#' @param fn Function to execute while temporary config is active.
#' @param should_restore Optional predicate that decides whether restore should run.
#' @param on_exit_fn Optional cleanup function executed on exit.
#' @return Result of \code{fn()}.
#' @keywords internal
.with_config_snapshot <- function(snapshot_fn,
                                  restore_fn,
                                  fn,
                                  should_restore = NULL,
                                  on_exit_fn = NULL) {
  previous <- tryCatch(snapshot_fn(), error = function(e) NULL)

  on.exit({
    restore_ok <- tryCatch(
      if (is.function(should_restore)) {
        isTRUE(should_restore(previous))
      } else {
        !is.null(previous)
      },
      error = function(e) FALSE
    )

    if (restore_ok) {
      tryCatch(restore_fn(previous), error = function(e) NULL)
    }

    if (is.function(on_exit_fn)) {
      tryCatch(on_exit_fn(previous), error = function(e) NULL)
    }
  }, add = TRUE)

  fn()
}


#' Compare Runtime Configuration Values
#'
#' Internal helper that compares two config fragments while ignoring
#' attribute differences (e.g., integer vs numeric scalars from Python).
#'
#' @param lhs Left-hand value/list
#' @param rhs Right-hand value/list
#' @return TRUE when equivalent, FALSE otherwise
#' @keywords internal
.config_values_equal <- function(lhs, rhs) {
  if (is.null(lhs) && is.null(rhs)) {
    return(TRUE)
  }
  if (is.null(lhs) || is.null(rhs)) {
    return(FALSE)
  }
  isTRUE(all.equal(lhs, rhs, check.attributes = FALSE))
}


#' Apply Search Configuration for a Single Operation
#'
#' Internal helper that applies search settings, runs a function,
#' and restores the original configuration afterward.
#'
#' @param search asa_search object or list of search settings
#' @param conda_env Conda env used by search tools
#' @param agent Optional asa_agent used to apply per-wrapper search config
#' @param fn Function to run with search config applied
#' @return Result of fn()
#' @keywords internal
.with_search_config <- function(search, conda_env = NULL, agent = NULL, fn) {
  conda_env <- conda_env %||% .get_default_conda_env()
  if (is.null(search)) {
    return(fn())
  }

  if (!inherits(search, "asa_search")) {
    if (is.list(search)) {
      search <- tryCatch(do.call(search_options, search), error = function(e) NULL)
    } else {
      warning("search config must be an asa_search object or list; ignoring", call. = FALSE)
      return(fn())
    }
  }

  if (is.null(search)) {
    return(fn())
  }

  .resolve_search_api_wrapper <- function(agent = NULL) {
    tools <- NULL
    if (!is.null(agent) && inherits(agent, "asa_agent") && !is.null(agent$tools)) {
      tools <- agent$tools
    } else if (!is.null(asa_env$tools)) {
      tools <- asa_env$tools
    }
    if (is.null(tools) || length(tools) < 2) {
      return(NULL)
    }
    wrapper <- .try_or(tools[[2]]$api_wrapper, NULL)
    if (is.null(wrapper)) {
      return(NULL)
    }
    # Guard: wrapper should expose the patched search path.
    has_search_method <- .try_or(is.function(wrapper$`_search_text`), FALSE)
    if (!isTRUE(has_search_method)) {
      return(NULL)
    }
    wrapper
  }

  search_wrapper <- .resolve_search_api_wrapper(agent = agent)
  if (!is.null(search_wrapper)) {
    wrapper_result <- tryCatch({
      # Prefer wrapper-scoped SearchConfig to avoid global mutable-state bleed
      # across concurrent runs.
      reticulate::use_condaenv(conda_env, required = TRUE)
      ddg_module <- .import_backend_api(required = TRUE)
      previous_wrapper_cfg <- .try_or(search_wrapper$search_config, NULL)

      requested_wrapper_cfg <- ddg_module$SearchConfig(
        max_results = as.integer(search$max_results),
        timeout = as.numeric(search$timeout),
        max_retries = as.integer(search$max_retries),
        retry_delay = as.numeric(search$retry_delay),
        backoff_multiplier = as.numeric(search$backoff_multiplier),
        inter_search_delay = as.numeric(search$inter_search_delay),
        humanize_timing = isTRUE(search$humanize_timing %||% ASA_HUMANIZE_TIMING),
        jitter_factor = as.numeric(search$jitter_factor %||% ASA_JITTER_FACTOR),
        allow_direct_fallback = isTRUE(search$allow_direct_fallback %||% FALSE)
      )

      .with_config_snapshot(
        snapshot_fn = function() {
          previous_wrapper_cfg
        },
        restore_fn = function(previous) {
          search_wrapper$search_config <- previous
        },
        should_restore = function(previous) {
          TRUE
        },
        fn = function() {
          search_wrapper$search_config <- requested_wrapper_cfg
          fn()
        }
      )
    }, error = function(e) NULL)

    if (!is.null(wrapper_result)) {
      return(wrapper_result)
    }
  }

  # Fallback path for legacy callers without a live wrapper: use global backend
  # defaults (thread-safe in Python, but process-wide).
  previous_cfg <- tryCatch(
    configure_search(conda_env = conda_env),
    error = function(e) NULL
  )

  previous <- if (!is.null(previous_cfg)) {
    list(
      max_results = previous_cfg$max_results,
      timeout = previous_cfg$timeout,
      max_retries = previous_cfg$max_retries,
      retry_delay = previous_cfg$retry_delay,
      backoff_multiplier = previous_cfg$backoff_multiplier,
      captcha_backoff_base = previous_cfg$captcha_backoff_base,
      page_load_wait = previous_cfg$page_load_wait,
      inter_search_delay = previous_cfg$inter_search_delay
    )
  } else {
    NULL
  }

  requested <- list(
    max_results = search$max_results,
    timeout = search$timeout,
    max_retries = search$max_retries,
    retry_delay = search$retry_delay,
    backoff_multiplier = search$backoff_multiplier,
    inter_search_delay = search$inter_search_delay
  )

  if (!is.null(previous)) {
    current_requested <- previous[names(requested)]
    if (.config_values_equal(current_requested, requested)) {
      return(fn())
    }
  }

  .with_config_snapshot(
    snapshot_fn = function() {
      previous
    },
    restore_fn = function(previous) {
      configure_search(
        max_results = previous$max_results,
        timeout = previous$timeout,
        max_retries = previous$max_retries,
        retry_delay = previous$retry_delay,
        backoff_multiplier = previous$backoff_multiplier,
        captcha_backoff_base = previous$captcha_backoff_base,
        page_load_wait = previous$page_load_wait,
        inter_search_delay = previous$inter_search_delay,
        conda_env = conda_env
      )
    },
    should_restore = function(previous) {
      !is.null(previous)
    },
    fn = function() {
      tryCatch(
        configure_search(
          max_results = search$max_results,
          timeout = search$timeout,
          max_retries = search$max_retries,
          retry_delay = search$retry_delay,
          backoff_multiplier = search$backoff_multiplier,
          inter_search_delay = search$inter_search_delay,
          conda_env = conda_env
        ),
        error = function(e) NULL
      )
      fn()
    }
  )
}

#' Resolve a single option from config
#'
#' Returns \code{value} unless it is \code{NULL} and \code{config[[key]]}
#' exists, in which case the config value is returned.
#'
#' @param value Explicit value (returned if non-NULL).
#' @param config Configuration list to fall back to.
#' @param key Key to look up in \code{config}.
#' @return The resolved value.
#' @keywords internal
.resolve_option <- function(value, config, key) {
  if (is.null(value) && is.list(config) && !is.null(config[[key]])) config[[key]] else value
}
