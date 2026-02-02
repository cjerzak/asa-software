#' Build the Python Backend Environment
#'
#' Creates a conda environment with all required Python dependencies for the
#' asa search agent, including LangChain, LangGraph, and search tools.
#'
#' @param conda_env Name of the conda environment. Defaults to the package
#'   option \code{asa.default_conda_env} (or \code{"asa_env"} if unset).
#' @param conda Path to conda executable (default: "auto")
#' @param python_version Python version to use (default: "3.11")
#' @param force If TRUE, delete and recreate the conda environment if it already exists.
#'
#' @details
#' This function creates a new conda environment and installs the following

#' Python packages:
#' \itemize{
#'   \item langchain_groq, langchain_community, langchain_openai, langchain_google_genai
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
build_backend <- function(conda_env = NULL,
                          conda = "auto",
                          python_version = "3.11",
                          force = FALSE,
                          check_browser = TRUE) {

  conda_env <- conda_env %||% .get_default_conda_env()

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
    "langchain-google-genai",
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

  # Optional: check system browser/driver compatibility for Selenium tier.
  if (isTRUE(check_browser)) {
    .check_browser_stack(verbose = TRUE)
  }

  invisible(NULL)
}

.parse_major_version <- function(version_str) {
  if (!is.character(version_str) || length(version_str) != 1 || is.na(version_str) || !nzchar(version_str)) {
    return(NA_integer_)
  }
  m <- regexec("([0-9]+)\\.", version_str)
  mm <- regmatches(version_str, m)[[1]]
  if (length(mm) < 2) {
    return(NA_integer_)
  }
  suppressWarnings(as.integer(mm[2]))
}

.run_version_cmd <- function(path, args = "--version") {
  if (!is.character(path) || length(path) != 1 || is.na(path) || !nzchar(path)) {
    return("")
  }
  out <- tryCatch(
    system2(path, args, stdout = TRUE, stderr = TRUE),
    error = function(e) character(0)
  )
  out <- paste(out, collapse = " ")
  trimws(out)
}

.find_chrome_executable <- function() {
  # Prefer explicit env var when set.
  chrome_env <- Sys.getenv("ASA_CHROME_BIN", unset = Sys.getenv("CHROME_BIN", unset = ""))
  candidates <- character(0)
  if (nzchar(chrome_env)) {
    candidates <- c(candidates, chrome_env)
  }

  sysname <- Sys.info()[["sysname"]] %||% ""
  if (identical(sysname, "Darwin")) {
    candidates <- c(candidates,
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
      "/Applications/Google Chrome Beta.app/Contents/MacOS/Google Chrome Beta",
      "/Applications/Chromium.app/Contents/MacOS/Chromium"
    )
  }

  # Common PATH names across Linux/macOS.
  path_hits <- unname(Sys.which(c(
    "google-chrome",
    "google-chrome-stable",
    "chromium",
    "chromium-browser",
    "chrome"
  )))
  candidates <- c(candidates, path_hits)
  candidates <- unique(candidates[nzchar(candidates)])

  for (p in candidates) {
    if (file.exists(p) && file.access(p, 1) == 0) {
      return(p)
    }
  }
  NULL
}

.find_chromedriver_executable <- function() {
  driver_env <- Sys.getenv(
    "ASA_CHROMEDRIVER_BIN",
    unset = Sys.getenv("CHROMEDRIVER_BIN", unset = "")
  )
  candidates <- character(0)
  if (nzchar(driver_env)) {
    candidates <- c(candidates, driver_env)
  }
  candidates <- c(candidates, unname(Sys.which("chromedriver")))
  candidates <- unique(candidates[nzchar(candidates)])
  for (p in candidates) {
    if (file.exists(p) && file.access(p, 1) == 0) {
      return(p)
    }
  }
  NULL
}

.check_browser_stack <- function(verbose = TRUE) {
  # This check is best-effort and never errors.
  chrome_path <- .find_chrome_executable()
  driver_path <- .find_chromedriver_executable()

  if (isTRUE(verbose)) {
    message("Checking system browser/driver for Selenium tier...")
  }

  if (is.null(chrome_path) || is.null(driver_path)) {
    if (isTRUE(verbose)) {
      if (is.null(chrome_path)) message("  Chrome not found (Selenium tier may fall back).")
      if (is.null(driver_path)) message("  chromedriver not found (Selenium tier may fall back).")
    }
    return(invisible(list(
      ok = FALSE,
      chrome_path = chrome_path,
      chromedriver_path = driver_path,
      chrome_version = NA_character_,
      chromedriver_version = NA_character_,
      chrome_major = NA_integer_,
      chromedriver_major = NA_integer_,
      mismatch = NA
    )))
  }

  chrome_version <- .run_version_cmd(chrome_path, "--version")
  chromedriver_version <- .run_version_cmd(driver_path, "--version")
  chrome_major <- .parse_major_version(chrome_version)
  chromedriver_major <- .parse_major_version(chromedriver_version)
  mismatch <- !is.na(chrome_major) && !is.na(chromedriver_major) && chrome_major != chromedriver_major

  if (isTRUE(verbose)) {
    message("  Chrome:      ", chrome_version, " (", chrome_path, ")")
    message("  chromedriver:", chromedriver_version, " (", driver_path, ")")
    if (isTRUE(mismatch)) {
      warning(
        paste0(
          "Chrome/chromedriver major versions differ (Chrome ", chrome_major,
          " vs chromedriver ", chromedriver_major, "). Selenium browser tier may fail ",
          "with SessionNotCreatedException. Align the major versions by updating Chrome ",
          "or installing the matching chromedriver."
        ),
        call. = FALSE
      )
    }
  }

  invisible(list(
    ok = !isTRUE(mismatch),
    chrome_path = chrome_path,
    chromedriver_path = driver_path,
    chrome_version = chrome_version,
    chromedriver_version = chromedriver_version,
    chrome_major = chrome_major,
    chromedriver_major = chromedriver_major,
    mismatch = mismatch
  ))
}

#' Check Python Environment Availability
#'
#' Checks if the required Python environment and packages are available.
#'
#' @param conda_env Name of the conda environment to check. Defaults to the
#'   package option \code{asa.default_conda_env} (or \code{"asa_env"} if unset).
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
check_backend <- function(conda_env = NULL, strict = FALSE) {

  conda_env <- conda_env %||% .get_default_conda_env()

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
      "langchain_groq",
      "langchain_openai",
      "langchain_google_genai",
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
