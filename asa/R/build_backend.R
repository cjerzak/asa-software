#' Build the Python Backend Environment
#'
#' Creates a conda environment with all required Python dependencies for the
#' asa search agent, including LangChain, LangGraph, and search tools.
#'
#' @param conda_env Name of the conda environment (default: "asa_env")
#' @param conda Path to conda executable (default: "auto")
#' @param python_version Python version to use (default: "3.13")
#'
#' @details
#' This function creates a new conda environment and installs the following

#' Python packages:
#' \itemize{
#'   \item langchain_groq, langchain_community, langchain_openai
#'   \item langgraph
#'   \item ddgs (DuckDuckGo search)
#'   \item selenium, primp (browser automation)
#'   \item beautifulsoup4, requests
#'   \item fake_headers, httpx
#'   \item pysocks, socksio (proxy support)
#' }
#'
#' @return Invisibly returns NULL; called for side effects.
#'
#' @examples
#' \dontrun{
#' # Create the default environment
#' build_backend()
#'
#' # Create with a custom name
#' build_backend(conda_env = "my_asa_env")
#' }
#'
#' @export
build_backend <- function(conda_env = "asa_env",
                          conda = "auto",
                          python_version = "3.13") {

  # Check reticulate is available
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("Package 'reticulate' is required. Install with: install.packages('reticulate')",
         call. = FALSE)
  }

  msg <- function(...) message(sprintf(...))

  # Create conda environment

  msg("Creating conda environment '%s' with Python %s...", conda_env, python_version)
  reticulate::conda_create(
    envname = conda_env,
    conda = conda,
    python_version = python_version
  )

  # Helper for pip installation
  pip_install <- function(pkgs) {
    reticulate::py_install(
      packages = pkgs,
      envname = conda_env,
      conda = conda,
      pip = TRUE
    )
  }

  # Install packages in order
  msg("Installing Python dependencies...")

  # Core packages
  pip_install(c("uv"))

  # LangChain ecosystem
  pip_install(c(
    "langchain_groq",
    "langchain_community",
    "langchain_openai",
    "langgraph",
    "langchain-tavily"
  ))

  # Search and scraping tools
  pip_install(c(
    "ddgs",
    "selenium",
    "primp",
    "beautifulsoup4",
    "requests",
    "fake_headers"
  ))

  # HTTP and proxy support
  pip_install(c(
    "httpx",
    "pysocks",
    "socksio"
  ))

  # Additional utilities
  pip_install(c(
    "python-dotenv",
    "arxiv",
    "wikipedia"
  ))

  msg("Environment '%s' is ready.", conda_env)
  msg("You can now use: asa::initialize_agent(conda_env = '%s')", conda_env)

  invisible(NULL)
}

#' Check Python Environment Availability
#'
#' Checks if the required Python environment and packages are available.
#'
#' @param conda_env Name of the conda environment to check
#'
#' @return A list with components:
#' \itemize{
#'   \item available: Logical, TRUE if environment is ready
#'   \item conda_env: Name of the environment checked
#'   \item python_version: Python version if available
#'   \item missing_packages: Character vector of missing packages (if any)
#' }
#'
#' @examples
#' \dontrun{
#' status <- check_backend()
#' if (!status$available) {
#'   build_backend()
#' }
#' }
#'
#' @export
check_backend <- function(conda_env = "asa_env") {

  result <- list(
    available = FALSE,
    conda_env = conda_env,
    python_version = NULL,
    missing_packages = character(0)
  )

  # Check if conda env exists
  tryCatch({
    envs <- reticulate::conda_list()
    if (!conda_env %in% envs$name) {
      result$missing_packages <- "Environment not found"
      return(result)
    }

    # Activate environment
    reticulate::use_condaenv(conda_env, required = TRUE)

    # Check Python version
    result$python_version <- reticulate::py_config()$version

    # Check required packages
    required_packages <- c(
      "langchain_community",
      "langchain_openai",
      "langgraph",
      "ddgs",
      "selenium",
      "primp",
      "httpx",
      "fake_headers"
    )

    missing <- character(0)
    for (pkg in required_packages) {
      if (!reticulate::py_module_available(pkg)) {
        missing <- c(missing, pkg)
      }
    }

    result$missing_packages <- missing
    result$available <- length(missing) == 0

  }, error = function(e) {
    result$missing_packages <- paste("Error:", e$message)
  })

  result
}
