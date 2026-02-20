#' Build the Python Backend Environment
#'
#' Creates a conda environment with all required Python dependencies for the
#' asa search agent, including LangChain, LangGraph, and search tools.
#'
#' @param conda_env Name of the conda environment. Defaults to the package
#'   option \code{asa.default_conda_env} (or \code{"asa_env"} if unset).
#' @param conda Path to conda executable (default: "auto")
#' @param python_version Python version to use (default: "3.12")
#' @param force If TRUE, delete and recreate the conda environment if it already exists.
#' @param check_browser If TRUE, performs a best-effort check for a system Chrome/Chromium
#'   binary and `chromedriver`, warning when major versions are incompatible. Set to
#'   FALSE to skip this check.
#' @param fix_browser If TRUE and `check_browser = TRUE`, attempt to download a
#'   matching chromedriver for the detected Chrome version and prepend it to `PATH`
#'   for the current R session. The driver is stored under `chromedriver_dir`.
#' @param chromedriver_dir Optional directory to store downloaded chromedriver
#'   binaries. Defaults to `~/.asa/chromedriver` when `fix_browser = TRUE`.
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
#'   \item beautifulsoup4, curl_cffi, requests
#'   \item fake_headers, httpx
#'   \item stem (Tor control)
#'   \item pysocks, socksio (proxy support)
#' }
#'
#' Python compatibility note:
#' Current LangChain releases still import parts of `pydantic.v1`, which emits
#' warnings on Python 3.14+. Prefer Python 3.12 (or 3.13) until upstream
#' dependencies remove that compatibility layer.
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
                          python_version = "3.12",
                          force = FALSE,
                          check_browser = TRUE,
                          fix_browser = FALSE,
                          chromedriver_dir = NULL) {

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

  # Upstream LangChain modules still hit pydantic.v1 compatibility paths.
  # On Python 3.14+, pydantic emits a warning for this path.
  version_parts <- suppressWarnings(as.integer(strsplit(python_version, ".", fixed = TRUE)[[1]]))
  if (length(version_parts) == 2 && !any(is.na(version_parts))) {
    is_python_314_plus <- (version_parts[1] > 3L) || (version_parts[1] == 3L && version_parts[2] >= 14L)
    if (is_python_314_plus) {
      warning(
        paste0(
          "Python 3.14+ currently triggers upstream LangChain/Pydantic V1 compatibility warnings. ",
          "Prefer python_version = \"3.12\" (or \"3.13\") until upstream migration is complete."
        ),
        call. = FALSE
      )
    }
  }

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

  # Helper for pip installation with error handling
  pip_install <- function(pkgs) {
    tryCatch({
      reticulate::py_install(
        packages = pkgs,
        envname = conda_env,
        conda = conda,
        pip = TRUE
      )
    }, error = function(e) {
      warning(
        sprintf("Failed to install packages [%s]: %s",
                paste(pkgs, collapse = ", "), e$message),
        call. = FALSE
      )
    })
  }

  # Install packages in order
  msg("Installing Python dependencies...")

  # Core packages
  pip_install(c("uv", "setuptools"))

  # LangChain ecosystem
  pip_install(c(
    "langchain_groq>=0.2,<1.0",
    "langchain_community>=0.3,<1.0",
    "langchain_openai>=0.3,<1.0",
    "langchain-google-genai>=2.1,<3.0",
    "langchain-anthropic>=0.3,<1.0",
    "langchain-aws>=0.2,<1.0",
    "langgraph>=0.2,<1.0",
    "langchain-tavily"
  ))

  # Search and scraping tools
  pip_install(c(
    "ddgs>=8,<10",
    "selenium",
    "undetected-chromedriver",
    "primp",
    "beautifulsoup4",
    "curl_cffi",
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
    browser_status <- .check_browser_stack(verbose = TRUE)
    if (isTRUE(fix_browser) && isTRUE(browser_status$mismatch)) {
      .install_matching_chromedriver(
        chrome_version = browser_status$chrome_version,
        chromedriver_dir = chromedriver_dir,
        verbose = TRUE
      )
      .check_browser_stack(verbose = TRUE)
    }
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

.first_executable_candidate <- function(candidates) {
  candidates <- unique(candidates[nzchar(candidates)])
  for (p in candidates) {
    if (file.exists(p) && file.access(p, 1) == 0) {
      return(p)
    }
  }
  NULL
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
  .first_executable_candidate(candidates)
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
  .first_executable_candidate(candidates)
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
          "or installing the matching chromedriver. You can also run ",
          "build_backend(check_browser=TRUE, fix_browser=TRUE) to auto-download a ",
          "matching driver, or set ASA_IGNORE_PATH_CHROMEDRIVER=1 to let Selenium ",
          "Manager fetch a driver instead of using PATH."
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

.extract_full_version <- function(version_str) {
  if (!is.character(version_str) || length(version_str) != 1 || is.na(version_str) || !nzchar(version_str)) {
    return(NA_character_)
  }
  m <- regexpr("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", version_str)
  if (m[1] == -1) {
    return(NA_character_)
  }
  regmatches(version_str, m)[[1]]
}

.chromedriver_platform <- function() {
  sysname <- Sys.info()[["sysname"]] %||% ""
  arch <- Sys.info()[["machine"]] %||% ""
  if (identical(sysname, "Darwin")) {
    if (grepl("arm|aarch64", arch, ignore.case = TRUE)) {
      return("mac-arm64")
    }
    return("mac-x64")
  }
  if (identical(sysname, "Linux")) {
    return("linux64")
  }
  if (identical(sysname, "Windows")) {
    return("win64")
  }
  NULL
}

.install_matching_chromedriver <- function(chrome_version,
                                           chromedriver_dir = NULL,
                                           verbose = TRUE) {
  msg <- function(...) if (isTRUE(verbose)) message(...)

  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    msg("jsonlite is required to fetch Chrome for Testing metadata. Install it and retry.")
    return(invisible(list(ok = FALSE, reason = "jsonlite_missing")))
  }

  chrome_full <- .extract_full_version(chrome_version)
  if (is.na(chrome_full)) {
    msg("Unable to parse Chrome version from: ", chrome_version)
    return(invisible(list(ok = FALSE, reason = "chrome_version_unparsed")))
  }

  chrome_major <- .parse_major_version(chrome_full)
  platform <- .chromedriver_platform()
  if (is.null(platform)) {
    msg("Unsupported platform for chromedriver auto-install.")
    return(invisible(list(ok = FALSE, reason = "platform_unsupported")))
  }

  if (is.null(chromedriver_dir) || !nzchar(chromedriver_dir)) {
    chromedriver_dir <- Sys.getenv("ASA_CHROMEDRIVER_DIR", unset = "")
    if (!nzchar(chromedriver_dir)) {
      chromedriver_dir <- file.path(path.expand("~"), ".asa", "chromedriver")
    }
  }

  json_url <- "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json"
  payload <- tryCatch(
    jsonlite::fromJSON(json_url, simplifyDataFrame = FALSE),
    error = function(e) NULL
  )
  if (is.null(payload) || is.null(payload$versions)) {
    msg("Failed to fetch Chrome for Testing metadata.")
    return(invisible(list(ok = FALSE, reason = "metadata_fetch_failed")))
  }

  versions <- payload$versions
  majors <- vapply(versions, function(v) .parse_major_version(v$version), integer(1))
  candidates <- versions[majors == chrome_major]
  if (length(candidates) == 0) {
    msg("No Chrome for Testing build found for major ", chrome_major)
    return(invisible(list(ok = FALSE, reason = "no_matching_major")))
  }

  exact <- candidates[vapply(candidates, function(v) identical(v$version, chrome_full), logical(1))]
  if (length(exact) > 0) {
    selected <- exact[[1]]
  } else {
    ver_mat <- do.call(rbind, lapply(candidates, function(v) {
      parts <- as.integer(strsplit(v$version, "\\.")[[1]])
      c(parts, rep(0L, 4L - length(parts)))
    }))
    ord <- order(ver_mat[, 1], ver_mat[, 2], ver_mat[, 3], ver_mat[, 4])
    selected <- candidates[[tail(ord, 1)]]
  }

  downloads <- selected$downloads$chromedriver
  if (is.null(downloads) || length(downloads) == 0) {
    msg("No chromedriver downloads listed for version ", selected$version)
    return(invisible(list(ok = FALSE, reason = "no_downloads")))
  }

  pick <- NULL
  for (d in downloads) {
    if (identical(d$platform, platform)) {
      pick <- d
      break
    }
  }
  if (is.null(pick) || is.null(pick$url)) {
    msg("No chromedriver download available for platform ", platform)
    return(invisible(list(ok = FALSE, reason = "platform_missing")))
  }

  target_dir <- file.path(chromedriver_dir, selected$version, platform)
  dir.create(target_dir, recursive = TRUE, showWarnings = FALSE)

  zip_path <- file.path(target_dir, paste0("chromedriver-", platform, ".zip"))
  msg("Downloading chromedriver ", selected$version, " (", platform, ")...")
  ok <- tryCatch({
    utils::download.file(pick$url, zip_path, mode = "wb", quiet = !verbose)
    TRUE
  }, error = function(e) {
    msg("Download failed: ", e$message)
    FALSE
  })
  if (!ok) {
    return(invisible(list(ok = FALSE, reason = "download_failed")))
  }

  unzip_ok <- tryCatch({
    utils::unzip(zip_path, exdir = target_dir)
    TRUE
  }, error = function(e) {
    msg("Unzip failed: ", e$message)
    FALSE
  })
  if (!unzip_ok) {
    return(invisible(list(ok = FALSE, reason = "unzip_failed")))
  }

  sysname <- Sys.info()[["sysname"]] %||% ""
  exe_name <- if (identical(sysname, "Windows")) "chromedriver.exe" else "chromedriver"
  driver_path <- file.path(target_dir, paste0("chromedriver-", platform), exe_name)
  if (!file.exists(driver_path)) {
    hits <- list.files(target_dir, pattern = paste0(exe_name, "$"), recursive = TRUE, full.names = TRUE)
    if (length(hits) > 0) {
      driver_path <- hits[1]
    }
  }
  if (!file.exists(driver_path)) {
    msg("chromedriver binary not found after unzip.")
    return(invisible(list(ok = FALSE, reason = "binary_missing")))
  }

  if (!identical(sysname, "Windows")) {
    try(Sys.chmod(driver_path, mode = "0755"), silent = TRUE)
  }

  Sys.setenv(ASA_CHROMEDRIVER_BIN = driver_path)
  Sys.setenv(PATH = paste(dirname(driver_path), Sys.getenv("PATH"), sep = .Platform$path.sep))

  msg("Installed chromedriver to: ", driver_path)
  msg("Prepending chromedriver directory to PATH for this session.")
  msg("To persist, add the path to PATH or set ASA_CHROMEDRIVER_BIN in .Renviron.")

  invisible(list(
    ok = TRUE,
    chromedriver_path = driver_path,
    chromedriver_version = selected$version,
    platform = platform
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
      "langchain_anthropic",
      "langchain_aws",
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
