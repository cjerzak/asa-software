test_that("Tor egress is active for the configured proxy", {
  readiness <- asa_test_require_tor_ready()
  probe <- asa_test_tor_probe(proxy = readiness$proxy, timeout = 45L)

  expect_true(
    isTRUE(probe$is_tor),
    info = sprintf("Expected IsTor=true from Tor probe via %s", readiness$proxy)
  )
  expect_true(
    nzchar(as.character(probe$ip %||% "")),
    info = "Tor probe did not return an IP value."
  )
})

test_that("get_tor_ip resolves a non-empty IP through Tor", {
  readiness <- asa_test_require_tor_ready()
  ip <- asa::get_tor_ip(proxy = readiness$proxy, timeout = 45L)

  expect_false(is.na(ip), info = sprintf("get_tor_ip returned NA via %s", readiness$proxy))
  expect_true(nzchar(as.character(ip)), info = "get_tor_ip returned an empty IP value.")
  expect_match(as.character(ip), "^[0-9A-Fa-f:.]+$")
})

test_that("Tor control-port rotation path works (sanity)", {
  readiness <- asa_test_require_tor_ready()

  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("reticulate is required for Tor rotation integration tests.", call. = FALSE)
  }

  conda_env <- tryCatch(asa:::.get_default_conda_env(), error = function(e) NULL)
  if (!is.null(conda_env) && is.character(conda_env) && nzchar(conda_env)) {
    .asa_test_bind_conda_once(conda_env)
  }

  if (!isTRUE(reticulate::py_available(initialize = TRUE))) {
    stop("Python is unavailable for Tor rotation integration tests.", call. = FALSE)
  }

  required_modules <- c(
    "langchain_community",
    "pydantic",
    "requests",
    "ddgs",
    "bs4",
    "selenium",
    "primp"
  )
  missing_modules <- required_modules[
    !vapply(required_modules, reticulate::py_module_available, logical(1))
  ]
  if (length(missing_modules) > 0L) {
    stop(
      sprintf(
        "Missing Python modules for Tor rotation test: %s",
        paste(missing_modules, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  python_path <- asa_test_python_path(required_files = "asa_backend/search/ddg_transport.py")
  if (!nzchar(python_path) || !dir.exists(python_path)) {
    stop("Could not locate asa_backend/search/ddg_transport.py for Tor rotation test.", call. = FALSE)
  }

  ddg_transport <- reticulate::import_from_path(
    "asa_backend.search.ddg_transport",
    path = python_path
  )

  ddg_transport$configure_tor(
    control_port = as.integer(readiness$control_port),
    control_password = NULL,
    min_rotation_interval = 0
  )

  rotation_ok <- ddg_transport$`_rotate_tor_circuit`(
    force = TRUE,
    proxy = readiness$proxy,
    verify = FALSE
  )

  expect_true(
    isTRUE(rotation_ok),
    info = sprintf(
      "Expected Tor rotation sanity check to succeed on control port %d.",
      as.integer(readiness$control_port)
    )
  )
})
