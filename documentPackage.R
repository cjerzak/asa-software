#!/usr/bin/env Rscript
# Script: documentPackage.R
args <- commandArgs(trailingOnly = TRUE)

opts <- list(
  package_path = NULL,
  build_vignettes = TRUE,
  build_pdf = TRUE,
  run_tests = TRUE,
  run_check = TRUE,
  install = FALSE
)

install.packages( "~/Documents/asa-software/asa",
                  repos = NULL, type = "source",force = F);

for (arg in args) {
  if (grepl("^--package=", arg)) {
    opts$package_path <- sub("^--package=", "", arg)
  } else if (arg == "--no-vignettes") {
    opts$build_vignettes <- FALSE
  } else if (arg == "--no-pdf") {
    opts$build_pdf <- FALSE
  } else if (arg == "--no-tests") {
    opts$run_tests <- FALSE
  } else if (arg == "--no-check") {
    opts$run_check <- FALSE
  } else if (arg == "--install") {
    opts$install <- TRUE
  } else if (!startsWith(arg, "--") && is.null(opts$package_path)) {
    opts$package_path <- arg
  }
}

if (is.null(opts$package_path) || !nzchar(opts$package_path)) {
  opts$package_path <- file.path(getwd(), "asa")
}

package_path <- normalizePath(opts$package_path, mustWork = TRUE)
desc_path <- file.path(package_path, "DESCRIPTION")
if (!file.exists(desc_path)) {
  stop("DESCRIPTION not found at: ", desc_path)
}

desc <- read.dcf(desc_path)
pkg <- desc[1, "Package"]
ver <- desc[1, "Version"]

cat("Documenting package:", pkg, ver, "\n")
cat("Path:", package_path, "\n")

if (!requireNamespace("devtools", quietly = TRUE)) {
  stop("Package 'devtools' is required. Install with install.packages('devtools').")
}

devtools::document(package_path)

if (isTRUE(opts$build_vignettes)) {
  devtools::build_vignettes(package_path)
}

if (isTRUE(opts$run_tests) && requireNamespace("testthat", quietly = TRUE)) {
  devtools::test(package_path, stop_on_failure = TRUE)
}

if (isTRUE(opts$build_pdf)) {
  # Build a PDF manual into the package's parent directory.
  try(tools::Rd2pdf(package_path), silent = TRUE)
}

if (isTRUE(opts$run_check)) {
  devtools::check(package_path)
}

if (isTRUE(opts$install)) {
  devtools::install_local(package_path, upgrade = "never")
}
