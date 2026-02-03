#' Initialize the ASA Search Agent
#'
#' Initializes the Python environment and creates the LangGraph agent with
#' search tools (Wikipedia, DuckDuckGo). The agent can use multiple LLM
#' backends and supports DeepAgent-style memory folding.
#'
#' @param backend LLM backend to use. One of: "openai", "groq", "xai", "gemini", "exo", "openrouter"
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
#' @param use_memory_folding Enable DeepAgent-style memory compression (default: TRUE)
#' @param memory_threshold Number of messages before folding triggers (default: 4)
#' @param memory_keep_recent Number of recent messages to preserve after folding (default: 2)
#' @param rate_limit Requests per second for rate limiting (default: 0.1)
#' @param timeout Request timeout in seconds (default: 120)
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
                             use_memory_folding = ASA_DEFAULT_MEMORY_FOLDING,
                             memory_threshold = ASA_DEFAULT_MEMORY_THRESHOLD,
                             memory_keep_recent = ASA_DEFAULT_MEMORY_KEEP_RECENT,
                             rate_limit = ASA_DEFAULT_RATE_LIMIT,
                             timeout = ASA_DEFAULT_TIMEOUT,
                             tor = tor_options(),
                             verbose = TRUE) {

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
    rate_limit = rate_limit,
    timeout = timeout,
    verbose = verbose,
    tor = tor
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

  # Register cleanup finalizer on first initialization (belt-and-suspenders with .onUnload)
  if (asa_env$init_count == 1L) {
    .register_cleanup_finalizer()
  }

  # Initialize proactive rate limiter with configured rate
  .rate_limiter_init(rate = rate_limit)

  # Initialize adaptive rate limiting
  .adaptive_rate_init()

  # Activate conda environment
  if (verbose) message("  Activating conda environment: ", conda_env)
  reticulate::use_condaenv(conda_env, required = TRUE)

  # Import Python packages
  if (verbose) message("  Importing Python packages...")
  .import_python_packages()

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
  ddg_module <- asa_env$custom_ddg %||% reticulate::import("custom_ddg_production")
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

  # Create LLM instance
  if (verbose) message("  Creating LLM (", backend, "/", model, ")...")
  llm <- .create_llm(backend, model, clients, rate_limit)

  # Create search tools
  if (verbose) message("  Creating search tools...")
  tools <- .create_tools(proxy, use_browser = use_browser)

  # Create agent
  if (verbose) message("  Creating agent (memory_folding=", use_memory_folding, ")...")
  agent <- .create_agent(
    llm = llm,
    tools = tools,
    use_memory_folding = use_memory_folding,
    memory_threshold = memory_threshold,
    memory_keep_recent = memory_keep_recent
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
    use_memory_folding = use_memory_folding,
    memory_folding = use_memory_folding,
    memory_threshold = memory_threshold,
    memory_keep_recent = memory_keep_recent,
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

  # Import custom DDG module from package
  python_path <- .get_python_path()
  if (python_path != "" && dir.exists(python_path)) {
    asa_env$custom_ddg <- reticulate::import_from_path(
      "custom_ddg_production",
      path = python_path
    )
    # Optional tool: webpage reader (disabled by default via Python config)
    asa_env$webpage_tool <- tryCatch(
      reticulate::import_from_path("webpage_tool", path = python_path),
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
#' @keywords internal
.create_llm <- function(backend, model, clients, rate_limit) {
  # Create rate limiter
  rate_limiter <- asa_env$RateLimit$InMemoryRateLimiter(
    requests_per_second = rate_limit,
    check_every_n_seconds = 0.1,
    max_bucket_size = 1L
  )

  if (backend == "openai") {
    chat_models <- reticulate::import("langchain_openai")
    llm <- chat_models$ChatOpenAI(
      model_name = model,
      openai_api_key = Sys.getenv("OPENAI_API_KEY"),
      openai_api_base = Sys.getenv("OPENAI_API_BASE", unset = "https://api.openai.com/v1"),
      temperature = 0.5,
      streaming = TRUE,
      http_client = clients$direct,  # Use direct client (no proxy) - OpenAI fails through Tor
      # http_async_client omitted: LangChain manages its own (R-CRIT-001 fix)
      include_response_headers = FALSE,
      rate_limiter = rate_limiter
    )
  } else if (backend == "groq") {
    chat_models <- reticulate::import("langchain_groq")
    llm <- chat_models$ChatGroq(
      groq_api_key = Sys.getenv("GROQ_API_KEY"),
      model = model,
      temperature = 0.01,
      streaming = TRUE,
      rate_limiter = rate_limiter
    )
  } else if (backend == "xai") {
    # xAI uses OpenAI-compatible API
    chat_models <- reticulate::import("langchain_openai")
    llm <- chat_models$ChatOpenAI(
      model_name = model,
      openai_api_key = Sys.getenv("XAI_API_KEY"),
      openai_api_base = "https://api.x.ai/v1",
      temperature = 0.5,
      streaming = TRUE,
      rate_limiter = rate_limiter
    )
  } else if (backend == "exo") {
    # Exo is a local LLM server
    chat_models <- reticulate::import("langchain_openai")
    local_ip <- .get_local_ip()
    base_url <- sprintf("http://%s:52415/v1", local_ip)

    llm <- chat_models$ChatOpenAI(
      model_name = model,
      openai_api_base = base_url,
      temperature = 0.01,
      streaming = TRUE
    )
  } else if (backend == "openrouter") {
    # OpenRouter uses OpenAI-compatible API with provider/model format
    chat_models <- reticulate::import("langchain_openai")
    llm <- chat_models$ChatOpenAI(
      model_name = model,
      openai_api_key = Sys.getenv("OPENROUTER_API_KEY"),
      openai_api_base = "https://openrouter.ai/api/v1",
      temperature = 0.5,
      streaming = TRUE,
      http_client = clients$direct,  # Use direct client (no proxy) - avoid Tor for API calls
      # http_async_client omitted: LangChain manages its own (R-CRIT-001 fix)
      include_response_headers = FALSE,
      rate_limiter = rate_limiter,
      default_headers = list(
        "HTTP-Referer" = "https://github.com/cjerzak/asa-software",
        "X-Title" = "asa"
      )
    )
  } else if (backend == "gemini") {
    chat_models <- reticulate::import("langchain_google_genai")
    api_key <- Sys.getenv("GOOGLE_API_KEY", unset = "")
    if (!nzchar(api_key)) {
      api_key <- Sys.getenv("GEMINI_API_KEY", unset = "")
    }

    args <- list(
      model = model,
      temperature = 1.0,
      rate_limiter = rate_limiter
    )
    if (nzchar(api_key)) {
      args$api_key <- api_key
    }

    llm <- do.call(chat_models$ChatGoogleGenerativeAI, args)
  }

  llm
}

#' Create Search Tools
#' @param proxy Proxy URL or NULL
#' @param use_browser Enable Selenium browser tier for DuckDuckGo search
#' @keywords internal
.create_tools <- function(proxy, use_browser = ASA_DEFAULT_USE_BROWSER) {
  # Wikipedia tool
  wiki <- asa_env$community_tools$WikipediaQueryRun(
    api_wrapper = asa_env$community_utils$WikipediaAPIWrapper(
      top_k_results = 5L,
      doc_content_chars_max = 1000L
    ),
    verbose = FALSE,
    timeout = 90L,
    proxy = proxy,
    max_concurrency = 1L
  )

  # DuckDuckGo search with custom patched wrapper
  search <- asa_env$custom_ddg$PatchedDuckDuckGoSearchRun(
    name = "Search",
    description = "DuckDuckGo web search",
    api_wrapper = asa_env$custom_ddg$PatchedDuckDuckGoSearchAPIWrapper(
      proxy = proxy,
      use_browser = isTRUE(use_browser),
      max_results = 10L,
      safesearch = "moderate",
      time = "none"
    ),
    doc_content_chars_max = 500L,
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
#' @param memory_keep_recent Messages to keep
#' @keywords internal
.create_agent <- function(llm, tools, use_memory_folding,
                          memory_threshold, memory_keep_recent) {
  if (use_memory_folding) {
    # Use custom memory folding agent with unified API
    agent <- asa_env$custom_ddg$create_memory_folding_agent(
      model = llm,
      tools = tools,
      checkpointer = asa_env$MemorySaver(),
      message_threshold = as.integer(memory_threshold),
      keep_recent = as.integer(memory_keep_recent),
      debug = FALSE
    )
  } else {
    # Use standard ReAct agent with RemainingSteps guard
    agent <- asa_env$custom_ddg$create_standard_agent(
      model = llm,
      tools = tools,
      checkpointer = asa_env$MemorySaver(),
      debug = FALSE
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
