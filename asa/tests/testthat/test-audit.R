# test-audit.R
#
# Tests for the asa_audit function and asa_audit_result S3 class.

# ============================================================================
# Input Validation Tests
# ============================================================================

test_that("asa_audit rejects invalid result types", {
  expect_error(
    asa_audit(result = "not a data frame"),
    "result must be"
  )

  expect_error(
    asa_audit(result = list(a = 1, b = 2)),
    "result must be"
  )
})

test_that("asa_audit rejects empty data frames", {
  empty_df <- data.frame()
  expect_error(
    asa_audit(result = empty_df),
    "Cannot audit empty"
  )
})

test_that("asa_audit validates confidence_threshold", {
  df <- data.frame(name = c("A", "B", "C"), value = 1:3)

  expect_error(
    asa_audit(df, confidence_threshold = -0.5),
    "confidence_threshold.*must be"
  )

  expect_error(
    asa_audit(df, confidence_threshold = 1.5),
    "confidence_threshold.*must be"
  )

  expect_error(
    asa_audit(df, confidence_threshold = "high"),
    "confidence_threshold.*must be"
  )
})

test_that("asa_audit validates timeout", {
  df <- data.frame(name = c("A", "B", "C"), value = 1:3)

  expect_error(
    asa_audit(df, timeout = -10),
    "timeout.*must be"
  )

  expect_error(
    asa_audit(df, timeout = 0),
    "timeout.*must be"
  )
})

test_that("asa_audit validates checks parameter", {
  df <- data.frame(name = c("A", "B", "C"), value = 1:3)

  expect_error(
    asa_audit(df, checks = "invalid_check"),
    "'arg' should be one of"
  )
})

test_that("asa_audit validates backend parameter", {
  df <- data.frame(name = c("A", "B", "C"), value = 1:3)

  expect_error(
    asa_audit(df, backend = "invalid_backend"),
    "'arg' should be one of"
  )
})


# ============================================================================
# S3 Class Tests
# ============================================================================

test_that("asa_audit_result constructor creates valid object", {
  df <- data.frame(
    name = c("A", "B", "C"),
    value = 1:3,
    `_audit_flag` = c("ok", "warning", "ok"),
    `_audit_notes` = c(NA, "Low confidence", NA),
    check.names = FALSE
  )

  result <- asa_audit_result(
    data = df,
    audit_summary = "Test audit",
    issues = list(
      list(severity = "medium", description = "Test issue", affected_rows = c(2))
    ),
    recommendations = c("Check row 2"),
    completeness_score = 0.95,
    consistency_score = 0.90,
    backend_used = "claude_code",
    elapsed_time = 5.5,
    query = "Test query",
    checks = c("completeness", "consistency")
  )

  expect_s3_class(result, "asa_audit_result")
  expect_equal(nrow(result$data), 3)
  expect_equal(result$completeness_score, 0.95)
  expect_equal(result$consistency_score, 0.90)
  expect_equal(result$backend_used, "claude_code")
  expect_equal(length(result$issues), 1)
  expect_equal(length(result$recommendations), 1)
})

test_that("print.asa_audit_result produces output", {
  result <- asa_test_mock_audit_result()

  output <- capture.output(print(result))
  expect_true(any(grepl("ASA Audit Result", output)))
  expect_true(any(grepl("Completeness:", output)))
  expect_true(any(grepl("Consistency:", output)))
})

test_that("summary.asa_audit_result produces output", {
  result <- asa_test_mock_audit_result(
    issues = list(
      list(severity = "high", description = "Critical issue")
    ),
    recommendations = c("Fix it"),
    completeness_score = 0.8,
    consistency_score = 0.9,
    backend_used = "langgraph",
    elapsed_time = 2.0,
    checks = c("completeness")
  )

  output <- capture.output(summary(result))
  expect_true(any(grepl("Audit Result Summary", output)))
  expect_true(any(grepl("Backend:", output)))
  expect_true(any(grepl("Scores:", output)))
})

test_that("as.data.frame.asa_audit_result returns data", {
  df <- data.frame(
    name = c("A", "B", "C"),
    value = 1:3,
    `_audit_flag` = c("ok", "ok", "warning"),
    check.names = FALSE
  )

  result <- asa_test_mock_audit_result(data = df)

  extracted <- as.data.frame(result)
  expect_equal(nrow(extracted), 3)
  expect_true("_audit_flag" %in% names(extracted))
})


# ============================================================================
# Helper Function Tests
# ============================================================================

test_that(".extract_audit_data handles asa_enumerate_result", {
  # Create mock asa_enumerate_result
  mock_result <- structure(
    list(
      data = data.frame(name = c("A", "B"), state = c("CA", "TX")),
      query = "Find all items",
      schema = c(name = "character", state = "character")
    ),
    class = "asa_enumerate_result"
  )

  extracted <- asa:::.extract_audit_data(mock_result)

  expect_equal(nrow(extracted$data), 2)
  expect_equal(extracted$query, "Find all items")
  expect_equal(extracted$schema, c(name = "character", state = "character"))
})

test_that(".extract_audit_data handles plain data.frame", {
  df <- data.frame(name = c("A", "B"), value = 1:2)

  extracted <- asa:::.extract_audit_data(df)

  expect_equal(nrow(extracted$data), 2)
  expect_null(extracted$query)
  expect_null(extracted$schema)
})

test_that(".annotate_results adds audit columns", {
  df <- data.frame(name = c("A", "B", "C"), value = 1:3)

  flagged <- list(
    list(index = 0, flag = "warning", note = "Test note 1"),
    list(index = 2, flag = "suspect", note = "Test note 2", confidence = 0.5)
  )

  annotated <- asa:::.annotate_results(df, flagged)

  expect_true("_audit_flag" %in% names(annotated))
  expect_true("_audit_notes" %in% names(annotated))
  expect_true("_confidence_adjusted" %in% names(annotated))

  # Check that flags were applied correctly (Python 0-indexed → R 1-indexed)
  # flagged index=0 → R row 1, flagged index=2 → R row 3
  expect_equal(annotated[["_audit_flag"]][1], "warning")
  expect_equal(annotated[["_audit_flag"]][2], "ok")
  expect_equal(annotated[["_audit_flag"]][3], "suspect")

  expect_equal(annotated[["_audit_notes"]][1], "Test note 1")
  expect_true(is.na(annotated[["_audit_notes"]][2]))
  expect_equal(annotated[["_audit_notes"]][3], "Test note 2")

  expect_equal(annotated[["_confidence_adjusted"]][3], 0.5)
})

test_that(".validate_audit_inputs passes for valid inputs", {
  df <- data.frame(name = c("A", "B", "C"), value = 1:3)

  expect_true(
    asa:::.validate_audit_inputs(
      result = df,
      query = "Test query",
      known_universe = c("A", "B", "C", "D"),
      checks = c("completeness"),
      backend = "claude_code",
      confidence_threshold = 0.8,
      timeout = 120
    )
  )
})

test_that(".build_audit_prompt creates valid prompt", {
  df <- data.frame(
    name = c("Alice", "Bob"),
    party = c("D", "R")
  )
  schema <- c(name = "character", party = "character")

  prompt <- asa:::.build_audit_prompt(
    query = "Find senators",
    data = df,
    schema = schema,
    checks = c("completeness", "consistency"),
    known_universe = c("Alice", "Bob", "Charlie"),
    confidence_threshold = 0.8
  )

  expect_true(grepl("Find senators", prompt))
  expect_true(grepl("completeness", prompt))
  expect_true(grepl("Known Universe", prompt))
  expect_true(grepl("Alice", prompt))
})

asa_test_audit_context_fixture <- function() {
  schema <- list(
    name = "character",
    category = "character",
    value = "numeric",
    confidence = "numeric",
    observed_at = "character"
  )

  data <- lapply(seq_len(35), function(i) {
    list(
      name = sprintf("Item %02d", i),
      category = "common",
      value = i,
      confidence = 0.95,
      observed_at = sprintf("2024-01-%02d", ((i - 1L) %% 28L) + 1L)
    )
  })

  data[[5]]$name <- "Missing Category"
  data[[5]]$category <- ""

  data[[12]]$name <- "Low Confidence"
  data[[12]]$confidence <- 0.21

  data[[28]] <- data[[8]]

  data[[35]]$name <- "Tail Rare"
  data[[35]]$category <- "rare"
  data[[35]]$value <- 999

  list(data = data, schema = schema)
}

test_that(".audit_with_langgraph forwards checks to run_audit", {
  ns_asa <- asNamespace("asa")
  asa_env <- get("asa_env", envir = ns_asa)
  audit_with_langgraph <- get(".audit_with_langgraph", envir = ns_asa)
  captured_checks <- NULL
  had_audit_graph <- exists("audit_graph", envir = asa_env, inherits = FALSE)
  old_audit_graph <- if (had_audit_graph) get("audit_graph", envir = asa_env, inherits = FALSE) else NULL

  on.exit({
    if (had_audit_graph) {
      assign("audit_graph", old_audit_graph, envir = asa_env)
    } else if (exists("audit_graph", envir = asa_env, inherits = FALSE)) {
      rm(list = "audit_graph", envir = asa_env)
    }
  }, add = TRUE)

  assign("audit_graph", new.env(parent = emptyenv()), envir = asa_env)
  asa_env$audit_graph$create_audit_graph <- function(llm, checks) {
    expect_equal(unlist(checks), "consistency")
    "fake_graph"
  }
  asa_env$audit_graph$run_audit <- function(graph, data, query, schema, known_universe, confidence_threshold, checks) {
    captured_checks <<- checks
    list(
      summary = "ok",
      issues = list(),
      recommendations = character(),
      completeness_score = 1.0,
      consistency_score = 1.0,
      flagged_rows = list(),
      status = "complete"
    )
  }

  local_mocked_bindings(
    .import_audit_modules = function() NULL,
    .env = ns_asa
  )

  audit_with_langgraph(
    data = data.frame(name = "A", value = 1),
    query = "test query",
    schema = c(name = "character", value = "numeric"),
    checks = "consistency",
    known_universe = NULL,
    confidence_threshold = 0.8,
    timeout = 60,
    model = "test-model",
    agent = list(llm = "fake-llm"),
    verbose = FALSE
  )

  expect_equal(unlist(captured_checks), "consistency")
})

test_that("audit dataset context includes tail, missing, low-confidence, and duplicate evidence", {
  audit <- asa_test_import_langgraph_module(
    "workflows.audit_graph_workflow",
    required_files = "workflows/audit_graph_workflow.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )
  fixture <- asa_test_audit_context_fixture()

  context <- reticulate::py_to_r(audit$`_build_audit_dataset_context`(
    data = fixture$data,
    schema = fixture$schema,
    confidence_threshold = 0.8
  ))

  representative_rows <- context$representative_rows
  row_names <- vapply(representative_rows, function(entry) {
    if (is.null(entry$row$name)) "" else as.character(entry$row$name)
  }, character(1))
  buckets <- vapply(representative_rows, function(entry) {
    as.character(entry$bucket)
  }, character(1))

  expect_equal(as.integer(context$row_count), 35L)
  expect_true("Tail Rare" %in% row_names)
  expect_true("missing_fields" %in% buckets)
  expect_true("low_confidence" %in% buckets)
  expect_true("duplicate" %in% buckets)
  expect_true(as.numeric(context$field_summaries$category$missing_count) >= 1)
  expect_true(as.numeric(context$field_summaries$value$unique_count) >= 30)
})

test_that("audit LLM prompts use representative dataset context instead of head-only slices", {
  audit <- asa_test_import_langgraph_module(
    "workflows.audit_graph_workflow",
    required_files = "workflows/audit_graph_workflow.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )
  fixture <- asa_test_audit_context_fixture()

  reticulate::py_run_string(paste0(
    "class _AuditPromptResponse:\n",
    "    def __init__(self, content):\n",
    "        self.content = content\n",
    "\n",
    "class _RecordingAuditLLM:\n",
    "    def __init__(self, response_content):\n",
    "        self.response_content = response_content\n",
    "        self.last_prompt = None\n",
    "    def invoke(self, messages):\n",
    "        self.last_prompt = messages[-1].content if messages else None\n",
    "        return _AuditPromptResponse(self.response_content)\n",
    "\n",
    "audit_prompt_llm_completeness = _RecordingAuditLLM('{\"completeness_score\": 0.9, \"issues\": [], \"recommendations\": []}')\n",
    "audit_prompt_llm_gaps = _RecordingAuditLLM('{\"gaps_found\": [], \"recommendations\": []}')\n"
  ))

  completeness_node <- audit$create_completeness_node(reticulate::py$audit_prompt_llm_completeness)
  gaps_node <- audit$create_gaps_node(reticulate::py$audit_prompt_llm_gaps)

  completeness_node(list(
    data = fixture$data,
    query = "Find all items",
    schema = fixture$schema,
    checks = list("completeness"),
    known_universe = NULL,
    confidence_threshold = 0.8
  ))
  gaps_node(list(
    data = fixture$data,
    query = "Find all items",
    schema = fixture$schema,
    checks = list("gaps"),
    confidence_threshold = 0.8
  ))

  completeness_prompt <- reticulate::py_to_r(reticulate::py$audit_prompt_llm_completeness$last_prompt)
  gaps_prompt <- reticulate::py_to_r(reticulate::py$audit_prompt_llm_gaps$last_prompt)

  expect_match(completeness_prompt, "Dataset context:", fixed = TRUE)
  expect_match(completeness_prompt, "\"representative_rows\"", fixed = TRUE)
  expect_match(completeness_prompt, "Tail Rare", fixed = TRUE)
  expect_match(gaps_prompt, "Dataset context:", fixed = TRUE)
  expect_match(gaps_prompt, "\"representative_rows\"", fixed = TRUE)
  expect_match(gaps_prompt, "Tail Rare", fixed = TRUE)
})


# ============================================================================
# Integration Tests (require Claude Code CLI)
# ============================================================================

test_that("asa_audit works with claude_code backend (skip if CLI not available)", {
  skip_if_not(Sys.which("claude") != "", "Claude Code CLI not installed")

  # Simple test with mock data
  df <- data.frame(
    name = c("Senator A", "Senator B", "Senator C"),
    state = c("CA", "TX", "NY"),
    party = c("D", "R", "D")
  )

  # This would actually call Claude Code - skip in automated tests
  skip("Skipping Claude Code integration test in automated testing")

  result <- asa_audit(
    result = df,
    query = "Find US senators",
    backend = "claude_code",
    timeout = 60
  )

  expect_s3_class(result, "asa_audit_result")
  expect_true("_audit_flag" %in% names(result$data))
})

test_that("asa_audit works with langgraph backend (skip if not initialized)", {
  skip("Skipping LangGraph integration test - requires initialized agent")

  # This would require an initialized agent
  df <- data.frame(
    name = c("Item A", "Item B"),
    value = c(1, 2)
  )

  # Would need: agent <- initialize_agent(...)
  # result <- asa_audit(df, backend = "langgraph", agent = agent)
})
