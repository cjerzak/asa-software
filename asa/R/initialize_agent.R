#' Initialize the ASA Search Agent
#'
#' Initializes the Python environment and creates the LangGraph agent with
#' search tools (Wikipedia, DuckDuckGo). The agent can use multiple LLM
#' backends and supports DeepAgent-style memory folding.
#'
#' @param backend LLM backend to use. One of: "openai", "groq", "xai", "exo", "openrouter"
#' @param model Model identifier (e.g., "gpt-4.1-mini", "llama-3.3-70b-versatile")
#' @param conda_env Name of the conda environment with Python dependencies
#' @param proxy SOCKS5 proxy URL for Tor (default: "socks5h://127.0.0.1:9050").
#'   Set to NULL to disable proxy.
#' @param use_memory_folding Enable DeepAgent-style memory compression (default: TRUE)
#' @param memory_threshold Number of messages before folding triggers (default: 4)
#' @param memory_keep_recent Number of recent messages to preserve after folding (default: 2)
#' @param rate_limit Requests per second for rate limiting (default: 0.2)
#' @param timeout Request timeout in seconds (default: 120)
#' @param verbose Print status messages (default: TRUE)
#'
#' @return An object of class \code{asa_agent} containing the initialized agent
#'   and configuration.
#'
#' @details
#' The agent is created with two tools:
#' \itemize{
#'   \item Wikipedia: For looking up encyclopedic information
#'   \item DuckDuckGo Search: For web searches with a 4-tier fallback system
#'     (PRIMP -> Selenium -> DDGS library -> raw requests)
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
#' @seealso \code{\link{run_agent}}, \code{\link{run_task}}
#'
#' @export
initialize_agent <- function(backend = "openai",
                             model = "gpt-4.1-mini",
                             conda_env = "asa_env",
                             proxy = "socks5h://127.0.0.1:9050",
                             use_memory_folding = TRUE,
                             memory_threshold = 4L,
                             memory_keep_recent = 2L,
                             rate_limit = 0.2,
                             timeout = 120L,
                             verbose = TRUE) {

  # Validate backend
  backend <- match.arg(backend, c("openai", "groq", "xai", "exo", "openrouter"))

  # Validate OpenRouter model format
  if (backend == "openrouter" && !grepl("/", model)) {
    warning("OpenRouter models should be in 'provider/model-name' format ",
            "(e.g., 'openai/gpt-4o', 'anthropic/claude-3-sonnet'). ",
            "Got: '", model, "'", call. = FALSE)
  }

  if (verbose) message("Initializing ASA agent...")

  # Activate conda environment
  if (verbose) message("  Activating conda environment: ", conda_env)
  reticulate::use_condaenv(conda_env, required = TRUE)

  # Import Python packages
  if (verbose) message("  Importing Python packages...")
  .import_python_packages()

  # Set up proxy from environment if not specified
  if (is.null(proxy)) {
    proxy <- Sys.getenv("HTTP_PROXY", unset = "")
    if (proxy == "") proxy <- NULL
  }

  # Close any existing HTTP clients before creating new ones (prevents resource leak)
  .close_http_clients()

  # Create HTTP clients for backends that need them (OpenAI-compatible APIs)
  clients <- NULL
  if (backend %in% c("openai", "openrouter")) {
    clients <- .create_http_clients(proxy, timeout)
    # Store clients in asa_env for cleanup on reset/unload
    asa_env$http_clients <- clients
  }

  # Create LLM instance
  if (verbose) message("  Creating LLM (", backend, "/", model, ")...")
  llm <- .create_llm(backend, model, clients, rate_limit)

  # Create search tools
  if (verbose) message("  Creating search tools...")
  tools <- .create_tools(proxy)

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
    use_memory_folding = use_memory_folding,
    memory_threshold = memory_threshold,
    memory_keep_recent = memory_keep_recent
  )

  if (verbose) message("  Done!")

  # Return S3 object
  asa_agent(
    python_agent = agent,
    backend = backend,
    model = model,
    config = asa_env$config
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
  } else {
    stop("Python module not found. Package may not be installed correctly.",
         call. = FALSE)
  }

  invisible(NULL)
}

#' Create HTTP Clients for API Calls
#' @param proxy Proxy URL or NULL
#' @param timeout Timeout in seconds
#' @keywords internal
.create_http_clients <- function(proxy, timeout) {
  # Generate fake headers
  headers <- asa_env$fake_headers$Headers(headers = TRUE)$generate()

  sync_client <- asa_env$httpx$Client(
    proxy = proxy,
    timeout = as.integer(timeout),
    http2 = FALSE,
    headers = headers,
    follow_redirects = TRUE
  )

  async_client <- asa_env$httpx$AsyncClient(
    proxy = proxy,
    timeout = as.integer(timeout),
    http2 = FALSE,
    headers = headers,
    follow_redirects = TRUE
  )

  list(sync = sync_client, async = async_client)
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
      http_client = clients$sync,
      http_async_client = clients$async,
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
    base_url <- system("ifconfig | grep 'inet ' | awk '{print $2}'", intern = TRUE)
    base_url <- sprintf("http://%s:52415/v1", base_url[length(base_url)])

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
      http_client = clients$sync,
      http_async_client = clients$async,
      include_response_headers = FALSE,
      rate_limiter = rate_limiter,
      default_headers = list(
        "HTTP-Referer" = "https://github.com/cjerzak/asa-software",
        "X-Title" = "asa"
      )
    )
  }

  llm
}

#' Create Search Tools
#' @param proxy Proxy URL or NULL
#' @keywords internal
.create_tools <- function(proxy) {
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
      use_browser = TRUE,
      max_results = 10L,
      safesearch = "moderate",
      time = "none"
    ),
    doc_content_chars_max = 500L,
    timeout = 90L,
    verbose = FALSE,
    max_concurrency = 1L
  )

  list(wiki, search)
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
    # Use custom memory folding agent
    agent <- asa_env$custom_ddg$create_memory_folding_agent_with_checkpointer(
      model = llm,
      tools = tools,
      checkpointer = asa_env$MemorySaver(),
      message_threshold = as.integer(memory_threshold),
      keep_recent = as.integer(memory_keep_recent),
      debug = FALSE
    )
  } else {
    # Use standard prebuilt ReAct agent
    agent <- asa_env$langgraph$prebuilt$create_react_agent(
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
    config = asa_env$config
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
