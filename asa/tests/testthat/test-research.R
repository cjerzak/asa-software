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

test_that("asa_config enforces recursion_limit >= 4 for research graphs", {
  expect_error(
    asa_config(recursion_limit = 3L),
    regexp = "recursion_limit"
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

test_that(".process_research_results prefers top-level provenance when available", {
  input <- list(
    results = list(
      list(
        row_id = "wd_0",
        fields = list(name = "Alice", state = "CA"),
        source_url = "https://fallback.example/a",
        confidence = 0.7,
        worker_id = "web_search",
        extraction_timestamp = 1700000000
      )
    ),
    provenance = list(
      list(
        row_id = "wd_0",
        source_url = "https://wikidata.org/wiki/Q1",
        confidence = 0.95,
        worker_id = "wikidata",
        extraction_timestamp = 1700000001
      )
    )
  )
  out <- asa:::.process_research_results(
    result = input,
    schema_dict = list(name = "character", state = "character"),
    include_provenance = TRUE
  )

  expect_equal(nrow(out$data), 1)
  expect_equal(nrow(out$provenance), 1)
  expect_equal(out$provenance$source_url[[1]], "https://wikidata.org/wiki/Q1")
  expect_equal(out$provenance$worker_id[[1]], "wikidata")
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

test_that(".run_research_streaming writes round checkpoints even when progress is FALSE", {
  skip_if_not_installed("reticulate")

  asa_ns <- asNamespace("asa")
  had_research_graph <- exists("research_graph", envir = asa_ns$asa_env, inherits = FALSE)
  old_research_graph <- if (had_research_graph) {
    get("research_graph", envir = asa_ns$asa_env, inherits = FALSE)
  } else {
    NULL
  }
  withr::defer({
    if (had_research_graph) {
      assign("research_graph", old_research_graph, envir = asa_ns$asa_env)
    } else if (exists("research_graph", envir = asa_ns$asa_env, inherits = FALSE)) {
      rm("research_graph", envir = asa_ns$asa_env)
    }
  })

  reticulate::py_run_string("
class _MockResearchGraph:
    def stream_research(self, graph, query, schema, config_dict, thread_id=None):
        def _gen():
            yield {
                'event_type': 'node_update',
                'node': 'planner',
                'status': 'searching',
                'items_found': 0,
                'elapsed': 0.1,
                'thread_id': thread_id or 'planner-thread',
                'state_snapshot': {
                    'query': query,
                    'schema': schema,
                    'config': config_dict,
                    'plan': {'search_queries': ['query a']},
                    'results': [],
                    'new_results': [],
                    'seen_hashes': {},
                    'novelty_history': [],
                    'round_number': 0,
                    'queries_used': 1,
                    'tokens_used': 10,
                    'input_tokens': 5,
                    'output_tokens': 5,
                    'token_trace': [],
                    'start_time': 1.0,
                    'elapsed_carry_sec': 1.0,
                    'status': 'searching',
                    'stop_reason': None,
                    'resume_stage': 'searcher',
                    'completion_gate': {},
                    'errors': []
                }
            }
        return _gen()
mock_research_graph = _MockResearchGraph()
")
  assign("research_graph", reticulate::py$mock_research_graph, envir = asa_ns$asa_env)

  checkpoint_file <- tempfile(fileext = ".rds")
  out <- asa_ns$.run_research_streaming(
    graph = "graph",
    query = "test query",
    schema_dict = list(name = "character"),
    config_dict = list(max_workers = 1L),
    checkpoint_file = checkpoint_file,
    agent_config = asa_config(backend = "openai", model = "gpt-4.1-mini"),
    progress = FALSE,
    verbose = FALSE
  )

  expect_true(file.exists(checkpoint_file))
  checkpoint <- readRDS(checkpoint_file)
  expect_equal(checkpoint$version, "2.0")
  expect_equal(checkpoint$status, "searching")
  expect_equal(checkpoint$resume_stage, "searcher")
  expect_equal(checkpoint$metrics$round_number, 0L)
  expect_equal(out$stop_reason, "no_result_from_stream")
})

test_that("resume_from accepts v2 nonterminal checkpoint without query", {
  checkpoint_file <- tempfile(fileext = ".rds")
  checkpoint <- list(
    version = "2.0",
    query = "find test entities",
    schema = list(name = "character"),
    config = list(
      max_workers = 1L,
      max_rounds = 3L,
      budget_queries = 10L,
      budget_tokens = 1000L,
      budget_time_sec = 60L,
      target_items = NULL,
      plateau_rounds = 2L,
      novelty_min = 0.05,
      novelty_window = 20L,
      use_wikidata = FALSE,
      use_web = TRUE,
      use_wikipedia = FALSE,
      allow_read_webpages = FALSE
    ),
    agent_config = asa_config(backend = "openai", model = "gpt-4.1-mini"),
    thread_id = "checkpoint-thread",
    resume_stage = "searcher",
    status = "searching",
    stop_reason = NULL,
    state_snapshot = list(
      query = "find test entities",
      schema = list(name = "character"),
      config = list(max_workers = 1L),
      plan = list(search_queries = list("query a")),
      results = list(
        list(
          fields = list(name = "Existing"),
          source_url = "https://example.com/existing",
          confidence = 0.8,
          worker_id = "web_search",
          extraction_timestamp = 1
        )
      ),
      new_results = list(),
      seen_hashes = list(hash_existing = TRUE),
      novelty_history = list(1),
      round_number = 1L,
      queries_used = 2L,
      tokens_used = 20L,
      input_tokens = 10L,
      output_tokens = 10L,
      token_trace = list(),
      start_time = 1,
      elapsed_carry_sec = 5,
      status = "searching",
      stop_reason = NULL,
      resume_stage = "searcher",
      completion_gate = list(),
      errors = list()
    ),
    metrics = list(round_number = 1L, items_found = 1L, active_elapsed_sec = 5),
    updated_at = Sys.time()
  )
  saveRDS(checkpoint, checkpoint_file)

  captured <- new.env(parent = emptyenv())
  captured$called <- FALSE

  testthat::local_mocked_bindings(
    .ensure_research_agent = function(...) {
      list(
        agent = structure(
          list(backend = "openai", model = "gpt-4.1-mini", config = checkpoint$agent_config),
          class = "asa_agent"
        ),
        config = checkpoint$agent_config
      )
    },
    .import_research_modules = function() invisible(NULL),
    .resolve_runtime_inputs = function(...) list(runtime = list(), temporal = NULL, allow_rw = FALSE),
    .create_research_graph = function(...) "graph",
    .with_runtime_wrappers = function(runtime, conda_env, agent, fn) fn(),
    .run_research_streaming = function(graph, query, schema_dict, config_dict, checkpoint_file,
                                       agent_config, progress, verbose, thread_id, state_snapshot) {
      captured$called <- TRUE
      captured$query <- query
      captured$thread_id <- thread_id
      captured$state_snapshot <- state_snapshot
      list(
        results = list(
          list(
            fields = list(name = "Existing"),
            source_url = "https://example.com/existing",
            confidence = 0.8,
            worker_id = "web_search",
            extraction_timestamp = 1
          ),
          list(
            fields = list(name = "New"),
            source_url = "https://example.com/new",
            confidence = 0.9,
            worker_id = "web_search",
            extraction_timestamp = 2
          )
        ),
        provenance = list(),
        metrics = list(round_number = 2L, queries_used = 3L, active_elapsed_sec = 12),
        status = "complete",
        stop_reason = "target_reached",
        plan = list(done = TRUE),
        completion_gate = list(completion_status = "complete"),
        thread_id = "checkpoint-thread"
      )
    },
    .package = "asa"
  )

  out <- asa_enumerate(
    resume_from = checkpoint_file,
    include_provenance = TRUE,
    progress = FALSE,
    verbose = FALSE
  )

  expect_true(captured$called)
  expect_equal(captured$query, "find test entities")
  expect_equal(captured$thread_id, "checkpoint-thread")
  expect_equal(captured$state_snapshot$round_number, 1L)
  expect_equal(nrow(out$data), 2)
  expect_true(isTRUE(out$metrics$resumed_from_checkpoint))
  expect_equal(out$metrics$checkpoint_version, 2L)
  expect_equal(out$metrics$checkpoint_round, 1L)
})

test_that("resume_from warns on ignored execution-shaping arguments", {
  checkpoint_file <- tempfile(fileext = ".rds")
  saveRDS(
    list(
      version = "2.0",
      query = "find test entities",
      schema = list(name = "character"),
      config = list(max_workers = 1L),
      agent_config = asa_config(backend = "openai", model = "gpt-4.1-mini"),
      thread_id = "checkpoint-thread",
      resume_stage = "searcher",
      status = "complete",
      state_snapshot = list(
        results = list(),
        completion_gate = list(),
        errors = list(),
        plan = list(),
        elapsed_carry_sec = 1
      ),
      metrics = list(round_number = 0L, active_elapsed_sec = 1),
      updated_at = Sys.time()
    ),
    checkpoint_file
  )

  expect_warning(
    asa_enumerate(
      query = "fresh query",
      schema = c(other = "character"),
      resume_from = checkpoint_file,
      verbose = FALSE
    ),
    regexp = "Ignoring execution-shaping arguments"
  )
})

test_that("resume_from terminal v2 checkpoint returns without executing python", {
  checkpoint_file <- tempfile(fileext = ".rds")
  saveRDS(
    list(
      version = "2.0",
      query = "find test entities",
      schema = list(name = "character"),
      config = list(max_workers = 1L),
      thread_id = "checkpoint-thread",
      resume_stage = "searcher",
      status = "complete",
      stop_reason = "target_reached",
      state_snapshot = list(
        results = list(
          list(
            fields = list(name = "Saved"),
            source_url = "https://example.com/saved",
            confidence = 0.95,
            worker_id = "wikidata",
            extraction_timestamp = 1
          )
        ),
        completion_gate = list(completion_status = "complete"),
        errors = list(),
        plan = list(),
        elapsed_carry_sec = 8
      ),
      metrics = list(round_number = 2L, active_elapsed_sec = 8),
      updated_at = Sys.time()
    ),
    checkpoint_file
  )

  testthat::local_mocked_bindings(
    .ensure_research_agent = function(...) stop("should not initialize agent"),
    .package = "asa"
  )

  out <- asa_enumerate(resume_from = checkpoint_file, verbose = FALSE)

  expect_equal(out$status, "complete")
  expect_equal(nrow(out$data), 1)
  expect_true(isTRUE(out$metrics$resumed_from_checkpoint))
  expect_equal(out$metrics$thread_id, "checkpoint-thread")
})

test_that("resume_from legacy checkpoint warns and returns saved result", {
  checkpoint_file <- tempfile(fileext = ".rds")
  mock_result <- list(
    results = list(
      list(fields = list(name = "Legacy"), source_url = "http://legacy.test", confidence = 0.9)
    ),
    status = "complete",
    stop_reason = "legacy_saved",
    metrics = list(round_number = 2L, active_elapsed_sec = 4),
    plan = list()
  )

  asa:::.save_checkpoint(
    result = mock_result,
    query = "legacy query",
    schema_dict = list(name = "character"),
    config_dict = list(max_workers = 4L),
    checkpoint_file = checkpoint_file
  )

  expect_warning(
    out <- asa_enumerate(resume_from = checkpoint_file, verbose = FALSE),
    regexp = "Legacy checkpoint format does not support continuation"
  )

  expect_equal(out$data$name[[1]], "Legacy")
  expect_true(isTRUE(out$metrics$resumed_from_checkpoint))
  expect_equal(out$metrics$checkpoint_version, 1L)
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
