# test-research.R
# Tests for multi-agent enumeration functionality

# ============================================================================
# Input Validation Tests
# ============================================================================

test_that("asa_enumerate validates query parameter", {
  expect_error(
    asa_enumerate(query = NULL),
    regexp = "query"
  )

  expect_error(
    asa_enumerate(query = ""),
    regexp = "query"
  )

  expect_error(
    asa_enumerate(query = 123),
    regexp = "query"
  )
})

test_that("asa_enumerate validates workers parameter", {
  expect_error(
    asa_enumerate(query = "test", workers = 0),
    regexp = "workers"
  )

  expect_error(
    asa_enumerate(query = "test", workers = 20),
    regexp = "workers"
  )

  expect_error(
    asa_enumerate(query = "test", workers = "four"),
    regexp = "workers"
  )
})

test_that("asa_enumerate validates max_rounds parameter", {
  expect_error(
    asa_enumerate(query = "test", max_rounds = 0),
    regexp = "max_rounds"
  )

  expect_error(
    asa_enumerate(query = "test", max_rounds = 150),
    regexp = "max_rounds"
  )
})

test_that("asa_enumerate validates budget parameter", {
  expect_error(
    asa_enumerate(query = "test", budget = "high"),
    regexp = "budget"
  )

  expect_error(
    asa_enumerate(query = "test", budget = c(queries = 50)),
    regexp = "budget"
  )
})

test_that("asa_enumerate validates stop_policy parameter", {
  expect_error(
    asa_enumerate(query = "test", stop_policy = "aggressive"),
    regexp = "stop_policy"
  )
})

test_that("asa_enumerate validates sources parameter", {
  expect_error(
    asa_enumerate(query = "test", sources = "all"),
    regexp = "sources"
  )
})

test_that("asa_enumerate validates allow_read_webpages parameter", {
  expect_error(
    asa_enumerate(query = "test", allow_read_webpages = "yes"),
    regexp = "allow_read_webpages"
  )

  # "auto" should not produce a validation error (it may fail for other reasons)
  expect_error(
    asa_enumerate(query = "test", allow_read_webpages = "auto",
                  checkpoint_dir = "/nonexistent/path/12345"),
    regexp = "checkpoint_dir"  # Fails at checkpoint_dir, not allow_read_webpages
  )
})

test_that("asa_enumerate validates webpage embedding options", {
  expect_error(
    asa_enumerate(query = "test", webpage_relevance_mode = "bad"),
    regexp = "webpage_relevance_mode"
  )
  expect_error(
    asa_enumerate(query = "test", webpage_embedding_provider = "bad"),
    regexp = "webpage_embedding_provider"
  )
})

test_that("asa_enumerate validates checkpoint_dir parameter", {
  expect_error(
    asa_enumerate(query = "test", checkpoint_dir = "/nonexistent/path/12345"),
    regexp = "checkpoint_dir"
  )
})

test_that("asa_enumerate validates resume_from parameter", {
  expect_error(
    asa_enumerate(query = "test", resume_from = "/nonexistent/checkpoint.rds"),
    regexp = "resume_from"
  )
})

# ============================================================================
# Schema Normalization Tests
# ============================================================================

test_that(".normalize_schema handles named character vector", {
  schema <- c(name = "character", age = "numeric", active = "logical")
  result <- asa:::.normalize_schema(schema, "test query", FALSE)

  expect_type(result, "list")
  expect_equal(names(result), c("name", "age", "active"))
})

test_that(".normalize_schema handles list input", {
  schema <- list(name = "character", count = "integer")
  result <- asa:::.normalize_schema(schema, "test query", FALSE)

  expect_type(result, "list")
  expect_equal(result$name, "character")
})

test_that(".normalize_schema handles NULL/auto", {
  result_null <- asa:::.normalize_schema(NULL, "test query", FALSE)
  result_auto <- asa:::.normalize_schema("auto", "test query", FALSE)

  expect_type(result_null, "list")
  expect_type(result_auto, "list")
})

# ============================================================================
# Enumeration Config Tests
# ============================================================================

test_that(".create_research_config creates proper structure", {
  config <- asa:::.create_research_config(
    workers = 4,
    max_rounds = 8,
    budget = list(queries = 50, tokens = 200000, time_sec = 300),
    stop_policy = list(target_items = 100, plateau_rounds = 2),
    sources = list(web = TRUE, wikipedia = TRUE, wikidata = TRUE)
  )

  expect_type(config, "list")
  expect_equal(config$max_workers, 4L)  # Python side still uses max_workers
  expect_equal(config$max_rounds, 8L)
  expect_equal(config$budget_queries, 50L)
  expect_equal(config$target_items, 100L)
  expect_true(config$use_wikidata)
})

test_that(".create_research_config handles NULL target_items", {
  config <- asa:::.create_research_config(
    workers = 2,
    max_rounds = 5,
    budget = list(queries = 25),
    stop_policy = list(target_items = NULL),
    sources = list(web = TRUE)
  )

  expect_null(config$target_items)
})

test_that(".create_research_config uses defaults for missing values", {
  config <- asa:::.create_research_config(
    workers = 2,
    max_rounds = 5,
    budget = list(),  # Empty budget
    stop_policy = list(),  # Empty stop_policy
    sources = list()  # Empty sources
  )

  expect_equal(config$budget_queries, 50L)
  expect_equal(config$budget_tokens, 200000L)
  expect_equal(config$plateau_rounds, 2L)
})

# ============================================================================
# S3 Class Tests
# ============================================================================

test_that("asa_enumerate_result constructor works", {
  result <- asa_enumerate_result(
    data = data.frame(name = c("A", "B"), value = c(1, 2)),
    status = "complete",
    stop_reason = "target_reached",
    metrics = list(round_number = 3, queries_used = 15),
    provenance = NULL,
    plan = list(),
    query = "test query"
  )

  expect_s3_class(result, "asa_enumerate_result")
  expect_equal(result$status, "complete")
  expect_equal(nrow(result$data), 2)
})

test_that("print.asa_enumerate_result works", {
  result <- asa_test_mock_enumerate_result(
    data = data.frame(name = c("A", "B", "C")),
    stop_reason = "novelty_plateau",
    metrics = list(round_number = 5, novelty_rate = 0.02)
  )

  output <- capture.output(print(result))
  expect_true(any(grepl("ASA Enumeration Result", output)))
  expect_true(any(grepl("complete", output)))
  expect_true(any(grepl("Items.*3|3.*items|\\b3\\b", output, ignore.case = TRUE)))  # Items found
})

test_that("summary.asa_enumerate_result works", {
  result <- asa_test_mock_enumerate_result(
    status = "partial",
    stop_reason = "budget_queries",
    metrics = list(round_number = 8, queries_used = 50),
    query = "Find all items"
  )

  output <- capture.output(summary(result))
  expect_true(any(grepl("Summary", output)))
  expect_true(any(grepl("partial", output)))
})

test_that("as.data.frame.asa_enumerate_result returns data", {
  original_df <- data.frame(name = c("X", "Y", "Z"), value = 1:3)
  result <- asa_test_mock_enumerate_result(
    data = original_df,
    stop_reason = NULL,
    metrics = list()
  )

  extracted_df <- as.data.frame(result)
  expect_equal(extracted_df, original_df)
})

test_that("asa_enumerate_result handles empty data", {
  result <- asa_test_mock_enumerate_result(
    data = data.frame(),
    status = "failed",
    stop_reason = "planning_error",
    metrics = list()
  )

  expect_equal(nrow(result$data), 0)
  output <- capture.output(print(result))
  expect_true(any(grepl("No data", output)))
})

test_that("asa_enumerate_result handles provenance", {
  result <- asa_enumerate_result(
    data = data.frame(name = c("A", "B")),
    status = "complete",
    stop_reason = "target_reached",
    metrics = list(),
    provenance = data.frame(
      row_id = 1:2,
      source_url = c("http://a.com", "http://b.com"),
      confidence = c(0.9, 0.8)
    )
  )

  expect_equal(nrow(result$provenance), 2)

  # Summary should mention provenance
  output <- capture.output(summary(result))
  expect_true(any(grepl("Provenance", output)) || any(grepl("sources", output)))
})

# ============================================================================
# Checkpoint Tests
# ============================================================================

test_that("checkpoint save and resume works", {
  skip_if_not(dir.exists(tempdir()))

  # Create a mock result
  mock_result <- list(
    results = list(
      list(fields = list(name = "Test1"), source_url = "http://test.com", confidence = 0.9),
      list(fields = list(name = "Test2"), source_url = "http://test2.com", confidence = 0.8)
    ),
    status = "complete",
    stop_reason = "test",
    metrics = list(round_number = 2),
    plan = list()
  )

  checkpoint_file <- tempfile(fileext = ".rds")
  asa:::.save_checkpoint(
    result = mock_result,
    query = "test query",
    schema_dict = list(name = "character"),
    config_dict = list(max_workers = 4),
    checkpoint_file = checkpoint_file
  )

  expect_true(file.exists(checkpoint_file))

  # Read and verify checkpoint
  checkpoint <- readRDS(checkpoint_file)
  expect_equal(checkpoint$query, "test query")
  expect_equal(checkpoint$result$status, "complete")

  # Clean up
  unlink(checkpoint_file)
})

# ============================================================================
# Result Processing Tests
# ============================================================================

test_that(".process_research_results handles empty results", {
  result <- list(results = list())
  schema <- list(name = "character", value = "numeric")

  processed <- asa:::.process_research_results(result, schema, FALSE)

  expect_equal(nrow(processed$data), 0)
  expect_equal(names(processed$data), c("name", "value"))
})

test_that(".process_research_results handles valid results", {
  result <- list(
    results = list(
      list(fields = list(name = "Alice", age = 30), source_url = "http://a.com", confidence = 0.9),
      list(fields = list(name = "Bob", age = 25), source_url = "http://b.com", confidence = 0.85)
    )
  )
  schema <- list(name = "character", age = "numeric")

  processed <- asa:::.process_research_results(result, schema, FALSE)

  expect_equal(nrow(processed$data), 2)
  expect_equal(processed$data$name, c("Alice", "Bob"))
  expect_null(processed$provenance)
})

test_that(".process_research_results includes provenance when requested", {
  result <- list(
    results = list(
      list(fields = list(name = "Test"), source_url = "http://test.com",
           confidence = 0.9, worker_id = "w1", extraction_timestamp = 12345)
    )
  )
  schema <- list(name = "character")

  processed <- asa:::.process_research_results(result, schema, TRUE)

  expect_false(is.null(processed$provenance))
  expect_equal(nrow(processed$provenance), 1)
  expect_equal(processed$provenance$source_url, "http://test.com")
})

# ============================================================================
# Integration Tests (skipped without API keys)
# ============================================================================

test_that("asa_enumerate integration test with OpenAI", {
  skip_if(Sys.getenv("OPENAI_API_KEY") == "", "OPENAI_API_KEY not set")
  skip_if(!reticulate::py_module_available("langchain"), "LangChain not available")
  skip_on_cran()

  # Simple test with minimal resources
  result <- asa_enumerate(
    query = "Find the 3 largest countries by area",
    schema = c(name = "character", area = "character"),
    workers = 1,
    max_rounds = 2,
    budget = list(queries = 5, tokens = 10000, time_sec = 60),
    stop_policy = list(target_items = 3, plateau_rounds = 1),
    sources = list(web = FALSE, wikipedia = TRUE, wikidata = TRUE),
    progress = FALSE,
    checkpoint = FALSE,
    verbose = FALSE
  )

  expect_s3_class(result, "asa_enumerate_result")
  expect_true(result$status %in% c("complete", "partial", "failed"))
})
