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
package_parent <- normalizePath(dirname(package_path), mustWork = TRUE)
desc_path <- file.path(package_path, "DESCRIPTION")
if (!file.exists(desc_path)) {
  stop("DESCRIPTION not found at: ", desc_path)
}

desc <- read.dcf(desc_path)
pkg <- desc[1, "Package"]
ver <- desc[1, "Version"]
pdf_path <- file.path(package_parent, paste0(pkg, ".pdf"))

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
  r_bin <- file.path(R.home("bin"), "R")
  rd2pdf_args <- c(
    "CMD", "Rd2pdf", "--force",
    paste0("--output=", pdf_path),
    package_path
  )
  rd2pdf_cmd <- paste(c(shQuote(r_bin), shQuote(rd2pdf_args)), collapse = " ")

  if (file.exists(pdf_path) && !isTRUE(file.remove(pdf_path))) {
    stop("Unable to remove existing PDF manual: ", pdf_path)
  }

  cat("Building PDF manual:", pdf_path, "\n")
  rd2pdf_output <- tryCatch(
    suppressWarnings(system2(
      r_bin,
      args = rd2pdf_args,
      stdout = TRUE,
      stderr = TRUE
    )),
    error = function(e) {
      stop(
        "Failed to run PDF manual build.\n",
        "Command: ", rd2pdf_cmd, "\n",
        "Output path: ", pdf_path, "\n",
        "Error: ", conditionMessage(e),
        call. = FALSE
      )
    }
  )
  rd2pdf_status <- attr(rd2pdf_output, "status")
  if (is.null(rd2pdf_status)) {
    rd2pdf_status <- 0L
  }

  if (rd2pdf_status != 0L || !file.exists(pdf_path)) {
    stop(
      paste0(
        "PDF manual generation failed.\n",
        "Command: ", rd2pdf_cmd, "\n",
        "Output path: ", pdf_path, "\n",
        if (length(rd2pdf_output) > 0) {
          paste0("Output:\n", paste(rd2pdf_output, collapse = "\n"))
        } else {
          "No command output captured."
        }
      ),
      call. = FALSE
    )
  }

  cat("PDF manual created:", pdf_path, "\n")
}

if (isTRUE(opts$run_check)) {
  devtools::check(package_path)
}

if (isTRUE(opts$install)) {
  devtools::install_local(package_path, upgrade = "never")
}
