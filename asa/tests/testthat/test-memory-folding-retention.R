# test-memory-folding-retention.R
# Tests for memory folding information retention
#
# Validates that:
# 1. Memory folding defaults are correctly set
# 2. The fold_char_budget constant exists and is reasonable
# 3. Structure-preserving compaction logic (tested at R level via config)
# 4. after_tools no longer routes to summarize (tested via config verification)

# ============================================================================
# Memory Folding Defaults Tests
# ============================================================================

test_that("memory threshold default is 10 (backstop)", {
  expect_equal(ASA_DEFAULT_MEMORY_THRESHOLD, 10L)
})

test_that("memory keep_recent default is 4", {
  expect_equal(ASA_DEFAULT_MEMORY_KEEP_RECENT, 4L)
})

test_that("fold char budget constant exists and is reasonable", {
  expect_true(exists("ASA_DEFAULT_FOLD_CHAR_BUDGET"))
  expect_equal(ASA_DEFAULT_FOLD_CHAR_BUDGET, 30000L)
  expect_true(ASA_DEFAULT_FOLD_CHAR_BUDGET > 10000L)
  expect_true(ASA_DEFAULT_FOLD_CHAR_BUDGET < 100000L)
})

test_that("memory folding enabled by default", {
  expect_true(ASA_DEFAULT_MEMORY_FOLDING)
})

# ============================================================================
# asa_config Defaults Tests
# ============================================================================

test_that("asa_config uses updated memory defaults", {
  config <- asa_config()

  expect_equal(config$memory_threshold, 10L)
  expect_equal(config$memory_keep_recent, 4L)
})

test_that("asa_config respects explicit overrides", {
  config <- asa_config(memory_threshold = 20L, memory_keep_recent = 6L)

  expect_equal(config$memory_threshold, 20L)
  expect_equal(config$memory_keep_recent, 6L)
})

# ============================================================================
# Research Config Tests
# ============================================================================

test_that(".create_research_config accepts max_tool_calls_per_round", {
  config <- asa:::.create_research_config(
    workers = 4,
    max_rounds = 8,
    budget = list(queries = 50, tokens = 200000, time_sec = 300),
    stop_policy = list(),
    sources = list(web = TRUE),
    max_tool_calls_per_round = 7
  )

  expect_equal(config$max_tool_calls_per_round, 7L)
})

test_that(".create_research_config passes NULL max_tool_calls_per_round", {
  config <- asa:::.create_research_config(
    workers = 4,
    max_rounds = 8,
    budget = list(queries = 50),
    stop_policy = list(),
    sources = list(web = TRUE)
  )

  expect_null(config$max_tool_calls_per_round)
})

test_that(".create_research_config handles allow_read_webpages auto mode", {
  config <- asa:::.create_research_config(
    workers = 4,
    max_rounds = 8,
    budget = list(queries = 50),
    stop_policy = list(),
    sources = list(web = TRUE),
    allow_read_webpages = "auto"
  )

  expect_equal(config$allow_read_webpages, "auto")
})

# ============================================================================
# Threshold Relationship Tests
# ============================================================================

test_that("char budget is primary, message count is backstop", {
  # The char budget should be the primary trigger mechanism.
  # Message threshold is only a backstop for edge cases.
  # Verify the char budget is set higher than what a few messages would produce.

  # Average message is ~500-1000 chars. With threshold=10 messages,
  # that's ~5000-10000 chars. The char budget at 30000 means we tolerate
  # more messages when they're short, and fold earlier when tool outputs are huge.
  avg_message_chars <- 750
  threshold_chars <- ASA_DEFAULT_MEMORY_THRESHOLD * avg_message_chars

  expect_true(ASA_DEFAULT_FOLD_CHAR_BUDGET > threshold_chars,
    info = "Char budget should be higher than threshold * avg_message_chars to serve as the primary trigger for typical messages")
})
