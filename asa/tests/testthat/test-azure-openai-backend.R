test_that("Azure OpenAI URL normalization appends v1 path and rejects direct OpenAI", {
  expect_equal(
    asa:::.normalize_azure_openai_base_url("https://example.openai.azure.com"),
    "https://example.openai.azure.com/openai/v1"
  )
  expect_equal(
    asa:::.normalize_azure_openai_base_url("https://example.services.ai.azure.com/openai"),
    "https://example.services.ai.azure.com/openai/v1"
  )
  expect_error(
    asa:::.normalize_azure_openai_base_url("https://api.openai.com/v1"),
    "api.openai.com",
    fixed = TRUE
  )
})

test_that("Azure OpenAI validation requires key and Azure endpoint", {
  withr::local_envvar(c(
    AZURE_OPENAI_API_KEY = "azkey_abcdefghijklmnopqrstuvwxyz",
    AZURE_OPENAI_ENDPOINT = "https://example.openai.azure.com",
    AZURE_OPENAI_API_BASE = NA
  ))
  expect_error(asa:::.validate_api_key("azure-openai"), NA)

  withr::local_envvar(c(
    AZURE_OPENAI_API_KEY = "azkey_abcdefghijklmnopqrstuvwxyz",
    AZURE_OPENAI_ENDPOINT = NA,
    AZURE_OPENAI_API_BASE = NA
  ))
  expect_error(
    asa:::.validate_api_key("azure-openai"),
    "azure_openai_endpoint",
    fixed = TRUE
  )

  withr::local_envvar(c(
    AZURE_OPENAI_API_KEY = NA,
    AZURE_OPENAI_ENDPOINT = "https://example.openai.azure.com",
    AZURE_OPENAI_API_BASE = NA
  ))
  expect_error(
    asa:::.validate_api_key("azure-openai"),
    "AZURE_OPENAI_API_KEY",
    fixed = TRUE
  )
})

test_that("Azure OpenAI defaults and asa_config preserve deployment-oriented model", {
  withr::local_envvar(c(AZURE_OPENAI_DEPLOYMENT = "deployment-from-env"))
  expect_equal(asa:::.get_default_model_for_backend("azure-openai"), "deployment-from-env")

  cfg <- asa_config(
    backend = "azure-openai",
    model = "gpt-5-mini-deployment",
    workers = 8,
    proxy = NULL
  )
  expect_equal(cfg$backend, "azure-openai")
  expect_equal(cfg$model, "gpt-5-mini-deployment")
  expect_equal(cfg$workers, 8L)
})

test_that("Azure OpenAI ChatOpenAI config uses Azure base, direct client, deployment headers", {
  direct_client <- structure(list(name = "direct"), class = "fake_direct_client")
  rate_limiter <- structure(list(name = "limiter"), class = "fake_rate_limiter")

  withr::local_envvar(c(
    AZURE_OPENAI_ENDPOINT = "https://example.openai.azure.com",
    AZURE_OPENAI_API_BASE = NA
  ))
  cfg <- asa:::.azure_openai_llm_config(
    clients = list(direct = direct_client),
    rate_limiter = rate_limiter,
    temperature = 0.2
  )

  expect_equal(cfg$env, "AZURE_OPENAI_API_KEY")
  expect_equal(cfg$base, "https://example.openai.azure.com/openai/v1")
  expect_identical(cfg$http_client, direct_client)
  expect_true(isTRUE(cfg$include_response_headers))
  expect_identical(cfg$rate_limiter, rate_limiter)
})

test_that("Azure OpenAI is accepted as an explicit webpage embedding provider", {
  search <- search_options(webpage_embedding_provider = "azure-openai")
  expect_equal(search$webpage_embedding_provider, "azure-openai")

  cfg <- asa_config(
    backend = "azure-openai",
    model = "dep",
    search = search,
    proxy = NULL
  )
  runtime <- asa:::.resolve_runtime_inputs(
    config = cfg,
    allow_read_webpages = TRUE,
    webpage_embedding_provider = NULL
  )$runtime
  expect_equal(runtime$config_backend, "azure-openai")
  expect_equal(runtime$embedding_provider, "azure-openai")
})

test_that("Azure shared limiter coordinates leases through SQLite", {
  asa_test_skip_if_no_python(required_files = "shared/azure_rate_limit.py")
  python_path <- asa_test_python_path(required_files = "shared/azure_rate_limit.py")
  db_path <- tempfile("asa_azure_rate_limit_", fileext = ".sqlite")
  withr::local_envvar(c(
    ASA_AZURE_RATE_LIMIT_DB = db_path,
    ASA_AZURE_RATE_LIMIT_MODE = "shared",
    ASA_AZURE_RPM = "120",
    ASA_AZURE_TPM = "100000",
    ASA_AZURE_MAX_CONCURRENT_REQUESTS = "1"
  ))

  mod <- reticulate::import_from_path("shared.azure_rate_limit", path = python_path)
  lim1 <- mod$AzureOpenAISharedRateLimiter(
    endpoint = "https://example.openai.azure.com",
    deployment = "dep",
    db_path = db_path,
    mode = "shared",
    rpm = 120,
    tpm = 100000,
    max_concurrent_requests = 1L
  )
  lim2 <- mod$AzureOpenAISharedRateLimiter(
    endpoint = "https://example.openai.azure.com/openai/v1",
    deployment = "dep",
    db_path = db_path,
    mode = "shared",
    rpm = 120,
    tpm = 100000,
    max_concurrent_requests = 1L
  )

  lease1 <- lim1$acquire_lease(blocking = FALSE)
  expect_true(is.character(lease1) && nzchar(lease1))
  expect_null(lim2$acquire_lease(blocking = FALSE))

  lim1$release(lease1)
  lease2 <- lim2$acquire_lease(blocking = FALSE)
  expect_true(is.character(lease2) && nzchar(lease2))
  lim2$release(lease2)
})

test_that("free-code gateway constructs Azure OpenAI model from Azure env vars", {
  asa_test_skip_if_no_python(required_files = "asa_backend/free_code/anthropic_gateway.py")
  python_path <- asa_test_python_path(required_files = "asa_backend/free_code/anthropic_gateway.py")
  gateway <- tryCatch(
    reticulate::import_from_path("asa_backend.free_code.anthropic_gateway", path = python_path),
    error = function(e) testthat::skip(paste("free-code gateway dependencies unavailable:", conditionMessage(e)))
  )

  main <- reticulate::import_main(convert = FALSE)
  main$gateway <- gateway
  reticulate::py_run_string("
class FakeChatOpenAI:
    last_kwargs = None
    def __init__(self, **kwargs):
        FakeChatOpenAI.last_kwargs = kwargs
    def bind_tools(self, *args, **kwargs):
        return self
    def bind(self, *args, **kwargs):
        return self
    def invoke(self, *args, **kwargs):
        return None
gateway.ChatOpenAI = FakeChatOpenAI
")

  reticulate::py_run_string("
import os
os.environ['AZURE_OPENAI_API_KEY'] = 'azkey_abcdefghijklmnopqrstuvwxyz'
os.environ['AZURE_OPENAI_ENDPOINT'] = 'https://example.openai.azure.com'
os.environ.pop('AZURE_OPENAI_API_BASE', None)
os.environ['ASA_AZURE_RATE_LIMIT_MODE'] = 'off'
")
  model <- gateway$`_create_model`(
    "azure-openai",
    "dep",
    max_tokens = NULL,
    timeout_s = 12
  )
  kwargs <- reticulate::py_to_r(main$FakeChatOpenAI$last_kwargs)

  expect_equal(kwargs$model, "dep")
  expect_equal(kwargs$api_key, "azkey_abcdefghijklmnopqrstuvwxyz")
  expect_equal(kwargs$base_url, "https://example.openai.azure.com/openai/v1")
  expect_equal(kwargs$timeout, 12)
  expect_equal(reticulate::py_to_r(model$asa_backend), "azure-openai")

  reticulate::py_run_string("
import os
os.environ['AZURE_OPENAI_API_KEY'] = 'azkey_abcdefghijklmnopqrstuvwxyz'
os.environ.pop('AZURE_OPENAI_ENDPOINT', None)
os.environ.pop('AZURE_OPENAI_API_BASE', None)
os.environ['ASA_AZURE_RATE_LIMIT_MODE'] = 'off'
")
  expect_error(
    gateway$`_create_model`("azure-openai", "dep", max_tokens = NULL, timeout_s = NULL),
    "AZURE_OPENAI_ENDPOINT",
    fixed = TRUE
  )
})
