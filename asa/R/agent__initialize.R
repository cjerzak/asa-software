#' Initialize the ASA Search Agent
#'
#' Initializes the Python environment and creates the LangGraph agent with
#' search tools (Wikipedia, DuckDuckGo). The agent can use multiple LLM
#' backends and supports DeepAgent-style memory folding.
#'
#' @param backend LLM backend to use. One of: "openai", "groq", "xai", "gemini", "exo", "openrouter", "anthropic", "bedrock"
#' @param model Model identifier (e.g., "gpt-4.1-mini", "llama-3.3-70b-versatile")
#' @param conda_env Name of the conda environment with Python dependencies
#' @param proxy Proxy URL for search tools.
#'   Use \code{NA} (default) to auto-detect from environment variables
#'   (\code{ASA_PROXY}, \code{HTTP_PROXY}, \code{HTTPS_PROXY}).
#'   Use \code{NULL} to disable proxying.
#'   Use \code{ASA_DEFAULT_PROXY} (or \code{"socks5h://127.0.0.1:9050"}) to route
#'   searches through Tor.
#' @param use_browser Enable Selenium browser tier for DuckDuckGo search
#'   (default: TRUE). Set to FALSE to avoid Chromedriver requirements.
#' @param search Optional search options (created with \code{\link{search_options}})
#'   used as defaults for \code{\link{run_task}}/\code{\link{asa_enumerate}} when
#'   no \code{asa_config} is provided. This is useful for setting OpenWebpage
#'   tuning knobs (e.g., \code{webpage_max_chars}, \code{webpage_max_chunks},
#'   \code{webpage_chunk_chars}) once when initializing an agent.
#' @param use_memory_folding Enable DeepAgent-style memory compression (default: TRUE)
#' @param memory_threshold Number of messages before folding triggers (default: 10;
#'   message-count backstop alongside \code{fold_char_budget})
#' @param memory_keep_recent Number of recent exchanges to preserve after folding
#'   (default: 4). An exchange is a user turn plus the assistant response,
#'   including any tool calls and tool outputs.
#' @param fold_char_budget Total character budget across all messages before
#'   memory folding triggers (default: 30000). Lower values fold more
#'   aggressively; higher values preserve more raw conversation.
#' @param use_observational_memory Enable observational memory pipeline.
#' @param om_observation_token_budget Token budget for stored observation messages.
#' @param om_reflection_token_budget Token budget for reflection summaries.
#' @param om_buffer_tokens Token budget for buffered context before reflection.
#' @param om_buffer_activation Fraction of buffer usage that triggers reflection.
#' @param om_block_after Fraction of context usage where buffering blocks generation.
#' @param om_async_prebuffer Whether to pre-buffer observations asynchronously.
#' @param om_cross_thread_memory Whether observational memory is shared across threads.
#' @param rate_limit Requests per second for rate limiting (default: 0.1)
#' @param timeout Request timeout in seconds (default: 120)
#' @param recursion_limit Optional default maximum number of agent steps to
#'   store in the initialized agent. When \code{NULL} (default), step limits
#'   fall back to mode-specific defaults at run time (memory folding: 100;
#'   standard agent: 20). Per-call \code{run_task(..., recursion_limit = ...)}
#'   overrides this value.
#' @param tor Tor registry options from \code{\link{tor_options}}. Disable shared
#'   tracking by setting \code{dirty_tor_exists = FALSE}.
#' @param verbose Print status messages (default: TRUE)
#'
#' @return An object of class \code{asa_agent} containing the initialized agent
#'   and configuration.
#'
#' @details
#' The agent is created with these tools:
#' \itemize{
#'   \item Wikipedia: For looking up encyclopedic information
#'   \item DuckDuckGo Search: For web searches with a 4-tier fallback system
#'     (PRIMP -> Selenium -> DDGS library -> raw requests)
#'   \item OpenWebpage: (Optional) Fetch and extract readable text from a public
#'     webpage URL. This capability is disabled by default and must be enabled
#'     per-call (e.g., via \code{run_task(..., allow_read_webpages = TRUE)}).
#' }
#'
#' Memory folding (enabled by default) compresses older messages into a summary
#' to manage context length in long conversations, following the DeepAgent paper.
#'
#' @section API Keys:
#' The following environment variables should be set based on your backend:
#' \itemize{
#'   \item OpenAI: \code{OPENAI_API_KEY}
#'   \item Groq: \code{GROQ_API_KEY}
#'   \item xAI: \code{XAI_API_KEY}
#'   \item Gemini: \code{GOOGLE_API_KEY} (or \code{GEMINI_API_KEY})
#'   \item Anthropic: \code{ANTHROPIC_API_KEY}
#'   \item Bedrock: \code{AWS_ACCESS_KEY_ID} + \code{AWS_SECRET_ACCESS_KEY}
#'     (optionally \code{AWS_DEFAULT_REGION}, defaults to "us-east-1")
#'   \item OpenRouter: \code{OPENROUTER_API_KEY}
#' }
#'
#' @section OpenRouter Models:
#' When using the \code{"openrouter"} backend, model names must be in
#' \code{provider/model-name} format. Examples:
#' \itemize{
#'   \item \code{"openai/gpt-4o"}
#'   \item \code{"anthropic/claude-3-sonnet"}
#'   \item \code{"google/gemma-2-9b-it:free"}
#'   \item \code{"meta-llama/llama-3-70b-instruct"}
#' }
#' See \url{https://openrouter.ai/models} for available models.
#'
#' @examples
#' \dontrun{
#' # Initialize with OpenAI
#' agent <- initialize_agent(
#'   backend = "openai",
#'   model = "gpt-4.1-mini"
#' )
#'
#' # Initialize with Groq and custom settings
#' agent <- initialize_agent(
#'   backend = "groq",
#'   model = "llama-3.3-70b-versatile",
#'   use_memory_folding = FALSE,
#'   proxy = NULL  # No Tor proxy
#' )
#'
#' # Initialize with OpenRouter (access to 100+ models)
#' agent <- initialize_agent(
#'   backend = "openrouter",
#'   model = "anthropic/claude-3-sonnet"  # Note: provider/model format
#' )
#' }
#'
#' @seealso \code{\link{run_task}}, \code{\link{run_task_batch}}
#'
#' @export
initialize_agent <- function(backend = NULL,
                             model = NULL,
                             conda_env = NULL,
                             proxy = NA,
                             use_browser = ASA_DEFAULT_USE_BROWSER,
                             search = NULL,
                             use_memory_folding = ASA_DEFAULT_MEMORY_FOLDING,
                             memory_threshold = ASA_DEFAULT_MEMORY_THRESHOLD,
                             memory_keep_recent = ASA_DEFAULT_MEMORY_KEEP_RECENT,
                             fold_char_budget = ASA_DEFAULT_FOLD_CHAR_BUDGET,
                             use_observational_memory = ASA_DEFAULT_USE_OBSERVATIONAL_MEMORY,
                             om_observation_token_budget = ASA_DEFAULT_OM_OBSERVATION_TOKENS,
                             om_reflection_token_budget = ASA_DEFAULT_OM_REFLECTION_TOKENS,
                             om_buffer_tokens = ASA_DEFAULT_OM_BUFFER_TOKENS,
                             om_buffer_activation = ASA_DEFAULT_OM_BUFFER_ACTIVATION,
                             om_block_after = ASA_DEFAULT_OM_BLOCK_AFTER,
                             om_async_prebuffer = ASA_DEFAULT_OM_ASYNC_PREBUFFER,
                             om_cross_thread_memory = ASA_DEFAULT_OM_CROSS_THREAD_MEMORY,
                             rate_limit = ASA_DEFAULT_RATE_LIMIT,
                             timeout = ASA_DEFAULT_TIMEOUT,
                             tor = tor_options(),
                             verbose = TRUE,
                             recursion_limit = NULL) {

  # Apply defaults (option-aware where available)
  backend <- backend %||% .get_default_backend()
  model <- model %||% .get_default_model_for_backend(backend)
  conda_env <- conda_env %||% .get_default_conda_env()

  # Validate backend
  backend <- match.arg(backend, ASA_SUPPORTED_BACKENDS)

  # Validate API key for the selected backend (fail-fast before expensive operations)
  .validate_api_key(backend)

  # Resolve proxy (NA = auto, NULL = disable)
  proxy_info <- .resolve_proxy(proxy)
  proxy <- proxy_info$proxy
  proxy_mode <- proxy_info$mode
  proxy_source <- proxy_info$source

  # If auto-detected proxy is malformed, warn and fall back to no proxy.
  if (identical(proxy_mode, "auto") && !is.null(proxy)) {
    proxy <- tryCatch({
      .validate_proxy_url(proxy, "proxy")
      proxy
    }, error = function(e) {
      warning(
        "Ignoring invalid proxy from environment",
        if (!is.null(proxy_source) && nzchar(proxy_source)) paste0(" (", proxy_source, ")") else "",
        ": ", conditionMessage(e),
        call. = FALSE
      )
      NULL
    })
  }

  # Normalize tor options early for validation
  if (!inherits(tor, "asa_tor")) {
    if (is.list(tor)) {
      tor <- do.call(tor_options, tor)
    } else {
      stop("`tor` must be created with tor_options() or be a list", call. = FALSE)
    }
  }

  # Normalize search options (optional). Used as default per-run search/webpage
  # settings when run_task()/asa_enumerate() are called without an asa_config.
  if (!is.null(search) && !inherits(search, "asa_search")) {
    if (is.list(search)) {
      search <- do.call(search_options, search)
    } else {
      stop("`search` must be created with search_options() or be a list", call. = FALSE)
    }
  }

  # Validate all inputs
  .validate_initialize_agent(
    backend = backend,
    model = model,
    conda_env = conda_env,
    proxy = proxy,
    use_browser = use_browser,
    use_memory_folding = use_memory_folding,
    memory_threshold = memory_threshold,
    memory_keep_recent = memory_keep_recent,
    use_observational_memory = use_observational_memory,
    om_observation_token_budget = om_observation_token_budget,
    om_reflection_token_budget = om_reflection_token_budget,
    om_buffer_tokens = om_buffer_tokens,
    om_buffer_activation = om_buffer_activation,
    om_block_after = om_block_after,
    om_async_prebuffer = om_async_prebuffer,
    om_cross_thread_memory = om_cross_thread_memory,
    rate_limit = rate_limit,
    timeout = timeout,
    recursion_limit = recursion_limit,
    verbose = verbose,
    tor = tor
  )

  recursion_limit <- .normalize_recursion_limit(recursion_limit)
  om_observation_token_budget <- as.integer(om_observation_token_budget)
  om_reflection_token_budget <- as.integer(max(om_reflection_token_budget, om_observation_token_budget))
  om_buffer_tokens <- as.integer(om_buffer_tokens)
  om_buffer_activation <- as.numeric(om_buffer_activation)
  om_block_after <- as.numeric(max(om_block_after, om_buffer_activation))

  om_config <- list(
    enabled = isTRUE(use_observational_memory),
    cross_thread_memory = isTRUE(om_cross_thread_memory),
    scope = if (isTRUE(om_cross_thread_memory)) "resource" else "thread",
    observation_message_tokens = om_observation_token_budget,
    reflection_observation_tokens = om_reflection_token_budget,
    buffer_tokens = om_buffer_tokens,
    buffer_activation = om_buffer_activation,
    block_after = om_block_after,
    async_prebuffer = isTRUE(om_async_prebuffer)
  )

  # Validate OpenRouter model format
  if (backend == "openrouter" && !grepl("/", model)) {
    warning("OpenRouter models should be in 'provider/model-name' format ",
            "(e.g., 'openai/gpt-4o', 'anthropic/claude-3-sonnet'). ",
            "Got: '", model, "'", call. = FALSE)
  }

  if (verbose) message("Initializing ASA agent...")

  # Track initialization count for resource leak detection
  asa_env$init_count <- (asa_env$init_count %||% 0L) + 1L

  # Warn and clean up on re-initialization (potential resource leak source)
  if (asa_env$init_count > 1L) {
    if (verbose) {
      message("  Note: Re-initializing agent (init #", asa_env$init_count, ")")
    }
    if (!is.null(asa_env$http_clients)) {
      if (verbose) message("  Cleaning up previous HTTP clients...")
    }
  }

  # Activate conda environment
  if (verbose) message("  Activating conda environment: ", conda_env)
  reticulate::use_condaenv(conda_env, required = TRUE)

  # Import Python packages
  if (verbose) message("  Importing Python packages...")
  .import_python_packages()

  # Python runtime now owns rate/circuit/adaptive state and cleanup lifecycle.
  runtime_humanize_timing <- search$humanize_timing %||% ASA_HUMANIZE_TIMING
  runtime_jitter_factor <- search$jitter_factor %||% ASA_JITTER_FACTOR
  .runtime_control_init(config = list(
    rate_limit = as.numeric(rate_limit),
    rate_limit_bucket_size = as.numeric(ASA_RATE_LIMIT_BUCKET_SIZE),
    rate_limit_proactive = isTRUE(ASA_RATE_LIMIT_PROACTIVE),
    humanize_timing = isTRUE(runtime_humanize_timing),
    jitter_factor = as.numeric(runtime_jitter_factor),
    adaptive_enabled = isTRUE(ASA_ADAPTIVE_RATE_ENABLED),
    adaptive_window = as.integer(ASA_ADAPTIVE_RATE_WINDOW),
    adaptive_increase = as.numeric(ASA_ADAPTIVE_RATE_INCREASE),
    adaptive_decrease = as.numeric(ASA_ADAPTIVE_RATE_DECREASE),
    adaptive_max = as.numeric(ASA_ADAPTIVE_RATE_MAX),
    adaptive_min = as.numeric(ASA_ADAPTIVE_RATE_MIN),
    circuit_enabled = isTRUE(ASA_CIRCUIT_BREAKER_ENABLED),
    circuit_threshold = as.numeric(ASA_CIRCUIT_BREAKER_THRESHOLD),
    circuit_window = as.integer(ASA_CIRCUIT_BREAKER_WINDOW),
    circuit_cooldown = as.numeric(ASA_CIRCUIT_BREAKER_COOLDOWN)
  ))

  # Configure shared Tor registry (exit health tracking)
  configure_tor_registry(
    registry_path = tor$registry_path,
    enable = tor$dirty_tor_exists,
    bad_ttl = tor$bad_ttl,
    good_ttl = tor$good_ttl,
    overuse_threshold = tor$overuse_threshold,
    overuse_decay = tor$overuse_decay,
    max_rotation_attempts = tor$max_rotation_attempts,
    ip_cache_ttl = tor$ip_cache_ttl,
    conda_env = conda_env
  )

  # Configure Tor control port for circuit rotation verification
  control_port_env <- Sys.getenv("TOR_CONTROL_PORT", unset = "9051")
  control_port <- suppressWarnings(as.integer(control_port_env))
  if (is.na(control_port)) control_port <- 9051L
  control_password <- Sys.getenv("TOR_CONTROL_PASSWORD", unset = "")
  if (nzchar(control_password)) {
    control_password <- control_password
  } else {
    control_password <- NULL
  }
  # Keep env in sync so worker processes see the chosen control port
  Sys.setenv(TOR_CONTROL_PORT = as.character(control_port))
  ddg_module <- asa_env$backend_api %||% .import_backend_api(required = TRUE)
  ddg_module$configure_tor(
    control_port = as.integer(control_port),
    control_password = control_password,
    min_rotation_interval = ASA_TOR_MIN_ROTATION_INTERVAL
  )

  # Set up proxy from environment if not specified
  # Close any existing HTTP clients before creating new ones (prevents resource leak)
  .close_http_clients()

  # Create HTTP clients for backends that need them (OpenAI-compatible APIs)
  # Dual-client approach: direct client for LLM API, proxied client for search tools
  clients <- NULL
  if (backend %in% c("openai", "openrouter")) {
    clients <- .create_http_clients(search_proxy = proxy, timeout = timeout)
    # Store clients in asa_env for cleanup on reset/unload
    asa_env$http_clients <- clients
  }
  .runtime_register_http_clients(
    direct = clients$direct %||% NULL,
    proxied = clients$proxied %||% NULL
  )

  # Create LLM instance
  if (verbose) message("  Creating LLM (", backend, "/", model, ")...")
  llm <- .create_llm(
    backend,
    model,
    clients,
    rate_limit,
    timeout = timeout,
    stability_profile = search$stability_profile %||% "stealth_first"
  )

  # Create search tools
  if (verbose) message("  Creating search tools...")
  tools <- .create_tools(proxy, use_browser = use_browser, search = search)

  # Create agent
  if (verbose) message("  Creating agent (memory_folding=", use_memory_folding, ")...")
  agent <- .create_agent(
    llm = llm,
    tools = tools,
    use_memory_folding = use_memory_folding,
    memory_threshold = memory_threshold,
    memory_keep_recent = memory_keep_recent,
    fold_char_budget = fold_char_budget,
    om_config = om_config,
    langgraph_node_retries = isTRUE(search$langgraph_node_retries %||% FALSE),
    langgraph_cache_enabled = isTRUE(search$langgraph_cache_enabled %||% FALSE)
  )

  # Store in package environment
  asa_env$initialized <- TRUE
  asa_env$agent <- agent
  asa_env$llm <- llm
  asa_env$tools <- tools
  asa_env$config <- list(
    backend = backend,
    model = model,
    conda_env = conda_env,
    proxy = proxy,
    proxy_mode = proxy_mode,
    proxy_source = proxy_source,
    use_browser = use_browser,
    rate_limit = rate_limit,
    timeout = timeout,
    recursion_limit = recursion_limit,
    use_memory_folding = use_memory_folding,
    memory_folding = use_memory_folding,
    memory_threshold = memory_threshold,
    memory_keep_recent = memory_keep_recent,
    use_observational_memory = om_config$enabled,
    om_cross_thread_memory = om_config$cross_thread_memory,
    om_observation_token_budget = om_config$observation_message_tokens,
    om_reflection_token_budget = om_config$reflection_observation_tokens,
    om_buffer_tokens = om_config$buffer_tokens,
    om_buffer_activation = om_config$buffer_activation,
    om_block_after = om_config$block_after,
    om_async_prebuffer = om_config$async_prebuffer,
    om_config = om_config,
    search = search,
    stability_profile = search$stability_profile %||% "stealth_first",
    auto_openwebpage_policy = search$auto_openwebpage_policy %||% "auto",
    langgraph_node_retries = isTRUE(search$langgraph_node_retries %||% FALSE),
    langgraph_cache_enabled = isTRUE(search$langgraph_cache_enabled %||% FALSE),
    tor = tor
  )

  if (verbose) message("  Done!")

  # Return S3 object
  asa_agent(
    python_agent = agent,
    backend = backend,
    model = model,
    config = asa_env$config,
    llm = llm,
    tools = tools
  )
}

#' Import Required Python Packages
#' @keywords internal
.import_python_packages <- function() {
  # Store imports in package environment
  asa_env$community_utils <- reticulate::import("langchain_community.utilities")
  asa_env$community_tools <- reticulate::import("langchain_community.tools")
  asa_env$langgraph <- reticulate::import("langgraph")
  asa_env$MemorySaver <- reticulate::import("langgraph.checkpoint.memory")$MemorySaver
  asa_env$RateLimit <- reticulate::import("langchain_core.rate_limiters")
  asa_env$httpx <- reticulate::import("httpx")
  asa_env$fake_headers <- reticulate::import("fake_headers", convert = FALSE)

  # Import backend API module from package
  python_path <- .get_python_path()
  if (python_path != "" && dir.exists(python_path)) {
    asa_env$backend_api <- reticulate::import_from_path(
      "asa_backend.api.agent_api",
      path = python_path
    )
    # Optional tool: webpage reader (disabled by default via Python config)
    asa_env$webpage_tool <- tryCatch(
      reticulate::import_from_path("tools.webpage_reader_tool", path = python_path),
      error = function(e) NULL
    )
  } else {
    stop("Python module not found. Package may not be installed correctly.",
         call. = FALSE)
  }

  invisible(NULL)
}

#' Create HTTP Clients for API Calls
#'
#' Creates two synchronous httpx clients: one direct (no proxy) for LLM API calls,
#' and one proxied (with Tor) for search tools. This dual-client approach ensures
#' OpenAI/OpenRouter API calls don't route through Tor (which causes failures),
#' while DuckDuckGo searches still use Tor to avoid IP blocks.
#'
#' Note: We intentionally do NOT create async clients. LangChain/OpenAI SDK
#' creates its own async client internally when needed (for async operations).
#' This avoids R-CRIT-001 where async client cleanup was unreliable from R
#' since aclose() requires an async context.
#'
#' @param search_proxy Proxy URL for search tools (e.g., Tor SOCKS5) or NULL
#' @param timeout Timeout in seconds
#' @return A list with 'direct' client (no proxy, for LLM) and 'proxied' client (for search)
#' @keywords internal
.create_http_clients <- function(search_proxy, timeout) {
  # Generate fake headers for browser-like fingerprint
  headers <- asa_env$fake_headers$Headers(headers = TRUE)$generate()

  # Direct client for LLM API calls (OpenAI, OpenRouter) - NO proxy
  # OpenAI blocks/fails through Tor exit nodes, so LLM calls must be direct
  direct_client <- asa_env$httpx$Client(
    proxy = NULL,
    timeout = as.integer(timeout),
    http2 = FALSE,
    headers = headers,
    follow_redirects = TRUE
  )

  # Proxied client for search tools (DuckDuckGo, Wikipedia) - WITH Tor proxy
  # Tor is needed to avoid IP blocks from DuckDuckGo

  proxied_client <- if (!is.null(search_proxy)) {
    asa_env$httpx$Client(
      proxy = search_proxy,
      timeout = as.integer(timeout),
      http2 = FALSE,
      headers = headers,
      follow_redirects = TRUE
    )
  } else {
    NULL
  }

  # Return both clients; async is NULL to let LangChain manage its own
  # This fixes R-CRIT-001: async client cleanup was unreliable from R
  list(direct = direct_client, proxied = proxied_client, async = NULL)
}

#' Create LLM Instance
#' @param backend Backend name
#' @param model Model identifier
#' @param clients HTTP clients (for OpenAI)
#' @param rate_limit Requests per second
#' @param timeout Request timeout in seconds
#' @keywords internal
.create_llm <- function(backend, model, clients, rate_limit,
                        timeout = ASA_DEFAULT_TIMEOUT,
                        stability_profile = "stealth_first") {
  stability_profile <- tolower(as.character(stability_profile %||% "stealth_first"))
  if (!stability_profile %in% c("deterministic", "balanced", "stealth_first")) {
    stability_profile <- "stealth_first"
  }

  .profile_temperature <- function(name, profile = stability_profile) {
    profile <- tolower(as.character(profile))
    if (profile == "deterministic") {
      return(switch(name,
                    openai = 0.0,
                    xai = 0.0,
                    openrouter = 0.0,
                    gemini = 0.1,
                    groq = 0.0,
                    anthropic = 0.0,
                    bedrock = 0.0,
                    exo = 0.01,
                    0.0))
    }
    if (profile == "balanced") {
      return(switch(name,
                    openai = 0.5,
                    xai = 0.5,
                    openrouter = 0.5,
                    gemini = 1.0,
                    groq = 0.01,
                    anthropic = ASA_DEFAULT_TEMPERATURES$anthropic,
                    bedrock = ASA_DEFAULT_TEMPERATURES$bedrock,
                    exo = 0.01,
                    0.5))
    }
    # stealth_first profile: keep some variation while reducing extraction drift.
    switch(name,
           openai = 0.2,
           xai = 0.2,
           openrouter = 0.2,
           gemini = 0.3,
           groq = 0.01,
           anthropic = 0.2,
           bedrock = 0.2,
           exo = 0.01,
           0.2)
  }

  # Create rate limiter
  rate_limiter <- asa_env$RateLimit$InMemoryRateLimiter(
    requests_per_second = rate_limit,
    check_every_n_seconds = 0.1,
    max_bucket_size = 1L
  )

  # OpenAI-compatible backends share ChatOpenAI with per-backend config.
  # Build config only for the requested backend to avoid eager side-effects
  # (e.g. .get_local_ip() for exo when not using exo).
  openai_compat_names <- c("openai", "xai", "exo", "openrouter")

  if (backend %in% openai_compat_names) {
    cfg <- switch(backend,
      openai = list(
        env = "OPENAI_API_KEY",
        base = Sys.getenv("OPENAI_API_BASE", unset = "https://api.openai.com/v1"),
        temperature = .profile_temperature("openai"),
        http_client = clients$direct,
        include_response_headers = FALSE,
        rate_limiter = rate_limiter
      ),
      xai = list(
        env = "XAI_API_KEY",
        base = "https://api.x.ai/v1",
        temperature = .profile_temperature("xai"),
        rate_limiter = rate_limiter
      ),
      exo = list(
        base = sprintf("http://%s:52415/v1", .get_local_ip()),
        temperature = .profile_temperature("exo")
      ),
      openrouter = list(
        env = "OPENROUTER_API_KEY",
        base = "https://openrouter.ai/api/v1",
        temperature = .profile_temperature("openrouter"),
        http_client = clients$direct,
        include_response_headers = FALSE,
        rate_limiter = rate_limiter,
        default_headers = list(
          "HTTP-Referer" = "https://github.com/cjerzak/asa-software",
          "X-Title" = "asa"
        )
      )
    )
    chat_models <- reticulate::import("langchain_openai")
    args <- list(
      model_name = model,
      openai_api_base = cfg$base,
      temperature = cfg$temperature,
      streaming = TRUE,
      stream_usage = TRUE
    )
    if (!is.null(cfg$env)) args$openai_api_key <- Sys.getenv(cfg$env)
    if (!is.null(cfg$http_client)) args$http_client <- cfg$http_client
    if (!is.null(cfg$include_response_headers)) args$include_response_headers <- cfg$include_response_headers
    if (!is.null(cfg$rate_limiter)) args$rate_limiter <- cfg$rate_limiter
    if (!is.null(cfg$default_headers)) args$default_headers <- cfg$default_headers
    llm <- do.call(chat_models$ChatOpenAI, args)
  } else if (backend == "groq") {
    chat_models <- reticulate::import("langchain_groq")
    llm <- chat_models$ChatGroq(
      groq_api_key = Sys.getenv("GROQ_API_KEY"),
      model = model,
      temperature = .profile_temperature("groq"),
      streaming = TRUE,
      rate_limiter = rate_limiter
    )
  } else if (backend == "gemini") {
    chat_models <- reticulate::import("langchain_google_genai")
    api_key <- Sys.getenv("GOOGLE_API_KEY", unset = "")
    if (!nzchar(api_key)) {
      api_key <- Sys.getenv("GEMINI_API_KEY", unset = "")
    }

    args <- list(
      model = model,
      temperature = .profile_temperature("gemini"),
      timeout = as.numeric(timeout),
      rate_limiter = rate_limiter
    )
    if (nzchar(api_key)) {
      args$api_key <- api_key
    }

    llm <- do.call(chat_models$ChatGoogleGenerativeAI, args)
  } else if (backend == "anthropic") {
    chat_models <- reticulate::import("langchain_anthropic")
    llm <- chat_models$ChatAnthropic(
      api_key = Sys.getenv("ANTHROPIC_API_KEY"),
      model = model,
      temperature = .profile_temperature("anthropic"),
      rate_limiter = rate_limiter
    )
  } else if (backend == "bedrock") {
    chat_models <- reticulate::import("langchain_aws")
    region <- Sys.getenv("AWS_DEFAULT_REGION", unset = "us-east-1")
    llm <- chat_models$ChatBedrockConverse(
      model_id = model,
      region_name = region,
      temperature = .profile_temperature("bedrock")
    )
  } else {
    stop("Unsupported backend: '", backend, "'. Supported backends: ",
         paste(ASA_SUPPORTED_BACKENDS, collapse = ", "), call. = FALSE)
  }

  llm
}

#' Create Search Tools
#' @param proxy Proxy URL or NULL
#' @param use_browser Enable Selenium browser tier for DuckDuckGo search
#' @keywords internal
.create_tools <- function(proxy, use_browser = ASA_DEFAULT_USE_BROWSER, search = NULL) {
  # Resolve tool output caps (configurable via search_options()).
  wiki_top_k_results <- ASA_DEFAULT_WIKI_TOP_K
  wiki_doc_content_chars_max <- ASA_DEFAULT_WIKI_CHARS
  search_doc_content_chars_max <- 500L
  search_max_results <- ASA_DEFAULT_MAX_RESULTS
  search_humanize_timing <- ASA_HUMANIZE_TIMING
  search_jitter_factor <- ASA_JITTER_FACTOR
  search_allow_direct_fallback <- FALSE

  if (!is.null(search) && inherits(search, "asa_search")) {
    wiki_top_k_results <- search$wiki_top_k_results %||% wiki_top_k_results
    wiki_doc_content_chars_max <- search$wiki_doc_content_chars_max %||% wiki_doc_content_chars_max
    search_doc_content_chars_max <- search$search_doc_content_chars_max %||% search_doc_content_chars_max
    search_max_results <- search$max_results %||% search_max_results
    search_humanize_timing <- search$humanize_timing %||% search_humanize_timing
    search_jitter_factor <- search$jitter_factor %||% search_jitter_factor
    search_allow_direct_fallback <- search$allow_direct_fallback %||% search_allow_direct_fallback
  }

  wiki_top_k_results <- as.integer(wiki_top_k_results)
  wiki_doc_content_chars_max <- as.integer(wiki_doc_content_chars_max)
  search_doc_content_chars_max <- as.integer(search_doc_content_chars_max)
  search_max_results <- as.integer(search_max_results)
  search_cfg <- asa_env$backend_api$SearchConfig(
    max_results = as.integer(search_max_results),
    timeout = as.numeric(search$timeout %||% ASA_DEFAULT_SEARCH_TIMEOUT),
    max_retries = as.integer(search$max_retries %||% ASA_DEFAULT_MAX_RETRIES),
    retry_delay = as.numeric(search$retry_delay %||% 2.0),
    backoff_multiplier = as.numeric(search$backoff_multiplier %||% 1.5),
    inter_search_delay = as.numeric(search$inter_search_delay %||% ASA_DEFAULT_INTER_SEARCH_DELAY),
    humanize_timing = isTRUE(search_humanize_timing),
    jitter_factor = as.numeric(search_jitter_factor),
    allow_direct_fallback = isTRUE(search_allow_direct_fallback)
  )

  # Configure baseline OpenWebpage timeouts (Tor-friendly). Per-run overrides can
  # still be applied via run_task(..., webpage_timeout=...).
  if (!is.null(asa_env$webpage_tool)) {
    timeout_init <- NULL
    pdf_timeout_init <- NULL
    if (!is.null(search) && inherits(search, "asa_search")) {
      timeout_init <- search$webpage_timeout %||% NULL
      pdf_timeout_init <- search$webpage_pdf_timeout %||% NULL
    }
    if (is.null(timeout_init)) {
      timeout_init <- if (!is.null(proxy)) 45 else 20
    }
    if (is.null(pdf_timeout_init)) {
      pdf_timeout_init <- timeout_init
    }
    tryCatch(
      asa_env$webpage_tool$configure_webpage_reader(
        timeout = as.numeric(timeout_init),
        pdf_timeout = as.numeric(pdf_timeout_init)
      ),
      error = function(e) NULL
    )
  }

  # Wikipedia tool
  wiki <- asa_env$community_tools$WikipediaQueryRun(
    api_wrapper = asa_env$community_utils$WikipediaAPIWrapper(
      top_k_results = wiki_top_k_results,
      doc_content_chars_max = wiki_doc_content_chars_max
    ),
    verbose = FALSE,
    timeout = 90L,
    proxy = proxy,
    max_concurrency = 1L
  )

  # DuckDuckGo search with custom patched wrapper
  search <- asa_env$backend_api$PatchedDuckDuckGoSearchRun(
    name = "Search",
    description = "DuckDuckGo web search",
    api_wrapper = asa_env$backend_api$PatchedDuckDuckGoSearchAPIWrapper(
      proxy = proxy,
      use_browser = isTRUE(use_browser),
      max_results = search_max_results,
      safesearch = "moderate",
      time = "none",
      search_config = search_cfg
    ),
    doc_content_chars_max = search_doc_content_chars_max,
    timeout = 90L,
    verbose = FALSE,
    max_concurrency = 1L
  )

  # Optional: open/read full webpages (tool is gated by allow_read_webpages)
  webpage <- NULL
  if (!is.null(asa_env$webpage_tool)) {
    webpage <- tryCatch(
      asa_env$webpage_tool$create_webpage_reader_tool(proxy = proxy),
      error = function(e) NULL
    )
  }

  tools <- list(wiki, search)
  if (!is.null(webpage)) {
    tools <- c(tools, list(webpage))
  }

  tools
}

#' Create the LangGraph Agent
#' @param llm LLM instance
#' @param tools List of tools
#' @param use_memory_folding Whether to use memory folding
#' @param memory_threshold Messages before folding
#' @param memory_keep_recent Exchanges to keep
#' @param fold_char_budget Char budget before fold triggers
#' @keywords internal
.create_agent <- function(llm, tools, use_memory_folding,
                          memory_threshold, memory_keep_recent,
                          fold_char_budget = ASA_DEFAULT_FOLD_CHAR_BUDGET,
                          min_fold_batch = ASA_DEFAULT_MIN_FOLD_BATCH,
                          min_fold_chars = ASA_DEFAULT_MIN_FOLD_CHARS,
                          min_fold_tokens_est = ASA_DEFAULT_MIN_FOLD_TOKENS_EST,
                          summarizer_max_output_tokens = ASA_DEFAULT_MEMORY_SUMMARIZER_MAX_OUTPUT_TOKENS,
                          om_config = NULL,
                          langgraph_node_retries = FALSE,
                          langgraph_cache_enabled = FALSE) {
  node_retry_policy <- if (isTRUE(langgraph_node_retries)) {
    list(
      max_attempts = 2L,
      initial_interval = 0.5,
      backoff_factor = 2.0,
      jitter = TRUE
    )
  } else {
    NULL
  }
  if (use_memory_folding) {
    # Use custom memory folding agent with unified API
    agent <- asa_env$backend_api$create_memory_folding_agent(
      model = llm,
      tools = tools,
      checkpointer = asa_env$MemorySaver(),
      message_threshold = as.integer(memory_threshold),
      keep_recent = as.integer(memory_keep_recent),
      fold_char_budget = as.integer(fold_char_budget),
      min_fold_batch = as.integer(min_fold_batch),
      min_fold_chars = as.integer(min_fold_chars),
      min_fold_tokens_est = as.integer(min_fold_tokens_est),
      summarizer_max_output_tokens = as.integer(summarizer_max_output_tokens),
      debug = FALSE,
      om_config = om_config %||% list(enabled = FALSE, scope = "thread", cross_thread_memory = FALSE),
      node_retry_policy = node_retry_policy,
      cache_enabled = isTRUE(langgraph_cache_enabled)
    )
  } else {
    # Use standard ReAct agent with RemainingSteps guard
    agent <- asa_env$backend_api$create_standard_agent(
      model = llm,
      tools = tools,
      checkpointer = asa_env$MemorySaver(),
      debug = FALSE,
      node_retry_policy = node_retry_policy,
      cache_enabled = isTRUE(langgraph_cache_enabled)
    )
  }

  agent
}

#' Get the Current Agent
#'
#' Returns the currently initialized agent, or NULL if not initialized.
#'
#' @return An asa_agent object or NULL
#'
#' @examples
#' \dontrun{
#' agent <- get_agent()
#' if (is.null(agent)) {
#'   agent <- initialize_agent()
#' }
#' }
#'
#' @export
get_agent <- function() {
  if (!.is_initialized()) {
    return(NULL)
  }

  asa_agent(
    python_agent = asa_env$agent,
    backend = asa_env$config$backend,
    model = asa_env$config$model,
    config = asa_env$config,
    llm = asa_env$llm,
    tools = asa_env$tools
  )
}

#' Reset the Agent
#'
#' Clears the initialized agent state, forcing reinitialization on next use.
#' Also closes any open HTTP clients to prevent resource leaks.
#'
#' @return Invisibly returns NULL
#'
#' @export
reset_agent <- function() {
  # Close HTTP clients to prevent resource leak (BUG-007 fix)
  .close_http_clients()
  .runtime_control_reset()

  asa_env$initialized <- FALSE
  asa_env$agent <- NULL
  asa_env$llm <- NULL
  asa_env$tools <- NULL
  asa_env$config <- NULL
  invisible(NULL)
}


# ============================================================================
# Internal helper functions
# ============================================================================

#' Get Local IP Address (Cross-Platform)
#'
#' Returns the local IP address for use with Exo backend.
#' Works on Windows, macOS, and Linux.
#'
#' @return Character string with the local IP address, or "127.0.0.1" on failure.
#' @keywords internal
.get_local_ip <- function() {
  os <- Sys.info()["sysname"]

  ip <- tryCatch({
    if (os == "Windows") {
      # Windows: parse ipconfig output
      output <- system("ipconfig", intern = TRUE, ignore.stderr = TRUE)
      ipv4_lines <- grep("IPv4", output, value = TRUE)
      if (length(ipv4_lines) > 0) {
        # Extract IP from "IPv4 Address. . . . . . . . . . . : 192.168.1.100"
        ip <- gsub(".*:\\s*", "", ipv4_lines[1])
        trimws(ip)
      } else {
        NULL
      }
    } else if (os == "Darwin") {
      # macOS: use ipconfig getifaddr
      ip <- system("ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null",
                   intern = TRUE, ignore.stderr = TRUE)
      if (length(ip) == 0 || ip == "") {
        # Fallback to ifconfig
        ip <- system("ifconfig | grep 'inet ' | grep -v '127.0.0.1' | awk '{print $2}' | head -1",
                     intern = TRUE, ignore.stderr = TRUE)
      }
      if (length(ip) > 0 && ip != "") ip[1] else NULL
    } else {
      # Linux: try hostname -I first, then ifconfig
      ip <- system("hostname -I 2>/dev/null | awk '{print $1}'",
                   intern = TRUE, ignore.stderr = TRUE)
      if (length(ip) == 0 || ip == "") {
        ip <- system("ifconfig 2>/dev/null | grep 'inet ' | grep -v '127.0.0.1' | awk '{print $2}' | head -1",
                     intern = TRUE, ignore.stderr = TRUE)
      }
      if (length(ip) > 0 && ip != "") ip[1] else NULL
    }
  }, error = function(e) NULL)

  # Fallback to localhost
  if (is.null(ip) || length(ip) == 0 || ip == "") {
    return("127.0.0.1")
  }
  ip
}
