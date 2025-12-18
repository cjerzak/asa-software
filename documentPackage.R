{
  rm(list = ls())
  options(error = NULL)


  # Set path and specify package name
  package_name <- "asa"
  setwd(sprintf("~/Documents/%s-software", package_name))

  # Version number (should match DESCRIPTION)
  versionNumber <- "0.1.0"

  # Package path
  package_path <- sprintf("~/Documents/%s-software/%s", package_name, package_name)

  # Add data list if package has data
  tools::add_datalist(package_path, force = TRUE, small.size = 1L)

  # Build vignettes
  devtools::build_vignettes(package_path)

  # Generate documentation from roxygen comments
  devtools::document(package_path)

  # Remove old PDF manual
  try(file.remove(sprintf("./%s.pdf", package_name)), silent = TRUE)

  # Create new PDF manual
  system(sprintf("R CMD Rd2pdf %s", package_path))

  # Show object sizes in environment (for debugging memory usage)
  log(sort(sapply(ls(), function(l_) { object.size(eval(parse(text = l_))) })))

  # Check package to ensure it meets CRAN standards
  devtools::check(package_path)

  # Build tar.gz
  system(paste(
    shQuote(file.path(R.home("bin"), "R")),
    "CMD build --resave-data",
    shQuote(package_path)
  ))

  # Check as CRAN
  system(paste(
    shQuote(file.path(R.home("bin"), "R")),
    "CMD check --as-cran",
    shQuote(paste0(package_name, "_", versionNumber, ".tar.gz"))
  ))

  # Manual commands for reference:
  # R CMD build --resave-data ~/Documents/asa-software/asa
  # R CMD check --as-cran ~/Documents/asa_0.1.0.tar.gz
}
