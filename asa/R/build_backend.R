#' Build the Python Backend Environment
#'
#' Creates a conda environment with all required Python dependencies for the
#' asa search agent, including LangChain, LangGraph, and search tools.
#'
#' @param conda_env Name of the conda environment (default: "asa_env")
#' @param conda Path to conda executable (default: "auto")
#' @param python_version Python version to use (default: "3.11")
#' @param force If TRUE, delete and recreate the conda environment if it already exists.
#'
#' @details
#' This function creates a new conda environment and installs the following

#' Python packages:
#' \itemize{
#'   \item langchain_groq, langchain_community, langchain_openai
#'   \item langgraph
#'   \item ddgs (DuckDuckGo search)
#'   \item selenium, primp (browser automation)
#'   \item undetected-chromedriver (stealth Chrome)
#'   \item beautifulsoup4, requests
#'   \item fake_headers, httpx
#'   \item stem (Tor control)
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
#' # Rebuild from scratch
#' build_backend(force = TRUE)
#'
#' # Create with a custom name
#' build_backend(conda_env = "my_asa_env")
#' }
#'
#' @export
build_backend <- function(conda_env = "asa_env",
                          conda = "auto",
                          python_version = "3.11",
                          force = FALSE) {

  # Check reticulate is available
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("Package 'reticulate' is required. Install with: install.packages('reticulate')",
         call. = FALSE)
  }

  # Validate inputs
  .validate_build_backend(
    conda_env = conda_env,
    conda = conda,
    python_version = python_version,
    force = force
  )

  msg <- function(...) message(sprintf(...))

  # Create / reuse conda environment

  envs <- tryCatch(reticulate::conda_list(), error = function(e) NULL)
  env_exists <- !is.null(envs) && conda_env %in% envs$name

  if (env_exists && isTRUE(force)) {
    msg("Removing existing conda environment '%s' (force=TRUE)...", conda_env)
    reticulate::conda_remove(envname = conda_env, conda = conda)
    env_exists <- FALSE
  }

  if (!env_exists) {
    msg("Creating conda environment '%s' with Python %s...", conda_env, python_version)
    reticulate::conda_create(
      envname = conda_env,
      conda = conda,
      python_version = python_version
    )
  } else {
    msg("Conda environment '%s' already exists; installing/upgrading dependencies...", conda_env)
  }

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
    "undetected-chromedriver",
    "primp",
    "beautifulsoup4",
    "requests",
    "fake_headers"
  ))

  # HTTP and proxy support
  pip_install(c(
    "httpx",
    "pysocks",
    "socksio",
    "stem"
  ))

  # Additional utilities
  pip_install(c(
    "python-dotenv",
    "arxiv",
    "wikipedia"
  ))

  # Verify installation (strict: build_backend installs full dependency set)

  msg("Verifying installation...")
  status <- check_backend(conda_env = conda_env, strict = TRUE)

  if (!status$available) {
    all_missing <- unique(c(status$missing_packages, status$missing_optional_packages))
    if (length(all_missing) > 0) {
      warning(
        sprintf(
          "Installation completed but verification found issues:\n  Missing packages: %s\n  Run check_backend('%s') for details.",
          paste(all_missing, collapse = ", "),
          conda_env
        ),
        call. = FALSE
      )
    }
    msg("Environment '%s' created but may have issues. See warnings above.", conda_env)
  } else {
    msg("Verification passed: All required packages available.")
    msg("Environment '%s' is ready.", conda_env)
  }

  msg("You can now use: asa::initialize_agent(conda_env = '%s')", conda_env)

  invisible(NULL)
}

#' Check Python Environment Availability
#'
#' Checks if the required Python environment and packages are available.
#'
#' @param conda_env Name of the conda environment to check
#' @param strict If TRUE, also require optional packages used for Tor control and
#'   stealth Chrome support (e.g., `stem`, `undetected_chromedriver`).
#'
#' @return A list with components:
#' \itemize{
#'   \item available: Logical, TRUE if environment is ready
#'   \item conda_env: Name of the environment checked
#'   \item python_version: Python version if available
#'   \item missing_packages: Character vector of missing required packages (if any)
#'   \item missing_optional_packages: Character vector of missing optional packages (if any)
#'   \item strict: Logical, whether optional packages were required
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
check_backend <- function(conda_env = "asa_env", strict = FALSE) {

  result <- list(
    available = FALSE,
    conda_env = conda_env,
    python_version = NULL,
    missing_packages = character(0),
    missing_optional_packages = character(0),
    strict = isTRUE(strict)
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

    # Check packages
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

    optional_packages <- c(
      "undetected_chromedriver",  # stealth Chrome (optional)
      "stem"                      # Tor control (optional)
    )

    missing_required <- character(0)
    for (pkg in required_packages) {
      if (!reticulate::py_module_available(pkg)) {
        missing_required <- c(missing_required, pkg)
      }
    }

    missing_optional <- character(0)
    for (pkg in optional_packages) {
      if (!reticulate::py_module_available(pkg)) {
        missing_optional <- c(missing_optional, pkg)
      }
    }

    result$missing_packages <- missing_required
    result$missing_optional_packages <- missing_optional
    result$available <- length(missing_required) == 0 && (!isTRUE(strict) || length(missing_optional) == 0)

  }, error = function(e) {
    result$missing_packages <- paste("Error:", e$message)
  })

  result
}
