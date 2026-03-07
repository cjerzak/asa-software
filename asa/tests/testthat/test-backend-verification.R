test_that("check_backend uses requested conda for environment discovery", {
  conda_seen <- NULL

  testthat::local_mocked_bindings(
    conda_list = function(conda = "auto") {
      conda_seen <<- conda
      data.frame(name = "other_env", stringsAsFactors = FALSE)
    },
    .package = "reticulate"
  )

  status <- asa::check_backend(
    conda_env = "custom_env",
    conda = "/opt/miniconda/bin/conda"
  )

  expect_identical(conda_seen, "/opt/miniconda/bin/conda")
  expect_false(isTRUE(status$available))
  expect_identical(status$missing_packages, "Environment not found")
})

test_that("check_backend probes backend without initializing reticulate", {
  conda_seen <- NULL
  python_seen <- NULL
  probe_command <- NULL
  probe_args <- NULL

  testthat::local_mocked_bindings(
    conda_list = function(conda = "auto") {
      conda_seen <<- conda
      data.frame(name = "custom_env", stringsAsFactors = FALSE)
    },
    conda_python = function(envname = NULL, conda = "auto", all = FALSE) {
      python_seen <<- list(envname = envname, conda = conda, all = all)
      "/tmp/custom-python"
    },
    use_condaenv = function(...) {
      stop("check_backend should not initialize reticulate")
    },
    py_config = function(...) {
      stop("check_backend should not call py_config")
    },
    py_module_available = function(...) {
      stop("check_backend should not call py_module_available")
    },
    .package = "reticulate"
  )

  testthat::local_mocked_bindings(
    .run_command = function(command,
                            args = character(),
                            timeout = NULL,
                            stdin = NULL,
                            echo = FALSE) {
      probe_command <<- command
      probe_args <<- args
      list(
        status = 0L,
        stdout = '{"python_version":"3.12.7","missing_packages":[],"missing_optional_packages":[]}',
        stderr = ""
      )
    },
    .package = "asa"
  )

  status <- asa::check_backend(
    conda_env = "custom_env",
    conda = "/opt/miniconda/bin/conda",
    strict = TRUE
  )

  expect_identical(conda_seen, "/opt/miniconda/bin/conda")
  expect_identical(python_seen$envname, "custom_env")
  expect_identical(python_seen$conda, "/opt/miniconda/bin/conda")
  expect_false(isTRUE(python_seen$all))
  expect_identical(probe_command, "/tmp/custom-python")
  expect_identical(probe_args[[1]], "-c")
  expect_true(isTRUE(status$available))
  expect_identical(status$python_version, "3.12.7")
  expect_identical(status$missing_packages, character(0))
  expect_identical(status$missing_optional_packages, character(0))
})

test_that("build_backend threads custom conda into verification", {
  conda_list_calls <- character(0)
  check_backend_call <- NULL

  testthat::local_mocked_bindings(
    conda_list = function(conda = "auto") {
      conda_list_calls <<- c(conda_list_calls, conda)
      data.frame(name = "asa_env", stringsAsFactors = FALSE)
    },
    py_install = function(...) invisible(NULL),
    .package = "reticulate"
  )

  testthat::local_mocked_bindings(
    .build_backend_python_dependency_groups = function() {
      list(core = c("setuptools"))
    },
    check_backend = function(conda_env = NULL, conda = "auto", strict = FALSE) {
      check_backend_call <<- list(conda_env = conda_env, conda = conda, strict = strict)
      list(
        available = TRUE,
        conda_env = conda_env,
        python_version = "3.12.7",
        missing_packages = character(0),
        missing_optional_packages = character(0),
        strict = strict
      )
    },
    .package = "asa"
  )

  expect_invisible(
    asa::build_backend(
      conda_env = "asa_env",
      conda = "/opt/miniconda/bin/conda",
      check_browser = FALSE
    )
  )

  expect_true("/opt/miniconda/bin/conda" %in% conda_list_calls)
  expect_identical(check_backend_call$conda_env, "asa_env")
  expect_identical(check_backend_call$conda, "/opt/miniconda/bin/conda")
  expect_true(isTRUE(check_backend_call$strict))
})
