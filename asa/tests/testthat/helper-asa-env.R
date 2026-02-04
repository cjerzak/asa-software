# Ensure tests use the standard asa conda environment when available.
options(asa.default_conda_env = "asa_env")

if (requireNamespace("reticulate", quietly = TRUE)) {
  try(
    suppressWarnings(reticulate::use_condaenv("asa_env", required = FALSE)),
    silent = TRUE
  )
}
