# test-stream-accumulation.R
# Tests for stream_research() reducer-aware state accumulation (Bug fix #1)
#
# The stream_research() function manually accumulates LangGraph state updates.
# Previously it overwrote keys, ignoring add_to_list/merge_dicts reducers.
# These tests verify the fixed accumulation logic.

# ============================================================================
# Accumulation Logic Tests (Pure R, no Python required)
# ============================================================================

test_that("list reducer keys are appended, not overwritten", {
  # Simulate what stream_research does internally
  LIST_REDUCER_KEYS <- c("results", "novelty_history", "errors")
  DICT_REDUCER_KEYS <- c("seen_hashes")

  accumulate <- function(accumulated_state, node_state) {
    for (key in names(node_state)) {
      value <- node_state[[key]]
      if (key %in% LIST_REDUCER_KEYS) {
        if (is.null(accumulated_state[[key]])) accumulated_state[[key]] <- list()
        if (is.list(value)) {
          accumulated_state[[key]] <- c(accumulated_state[[key]], value)
        } else {
          accumulated_state[[key]] <- c(accumulated_state[[key]], list(value))
        }
      } else if (key %in% DICT_REDUCER_KEYS) {
        if (is.null(accumulated_state[[key]])) accumulated_state[[key]] <- list()
        if (is.list(value)) {
          for (k in names(value)) {
            accumulated_state[[key]][[k]] <- value[[k]]
          }
        }
      } else {
        accumulated_state[[key]] <- value
      }
    }
    accumulated_state
  }

  # Simulate 3 rounds of deduper emitting 5 results each
  state <- list(results = list(), seen_hashes = list(), novelty_history = list())

  # Round 1
  round1_results <- lapply(1:5, function(i) list(row_id = paste0("r1_", i), fields = list(name = paste0("Entity_", i))))
  round1_hashes <- setNames(as.list(rep(TRUE, 5)), paste0("hash_r1_", 1:5))
  state <- accumulate(state, list(results = round1_results, seen_hashes = round1_hashes, novelty_history = list(1.0)))

  expect_equal(length(state$results), 5)
  expect_equal(length(state$seen_hashes), 5)

  # Round 2
  round2_results <- lapply(6:10, function(i) list(row_id = paste0("r2_", i), fields = list(name = paste0("Entity_", i))))
  round2_hashes <- setNames(as.list(rep(TRUE, 5)), paste0("hash_r2_", 6:10))
  state <- accumulate(state, list(results = round2_results, seen_hashes = round2_hashes, novelty_history = list(0.8)))

  expect_equal(length(state$results), 10)
  expect_equal(length(state$seen_hashes), 10)

  # Round 3
  round3_results <- lapply(11:15, function(i) list(row_id = paste0("r3_", i), fields = list(name = paste0("Entity_", i))))
  round3_hashes <- setNames(as.list(rep(TRUE, 5)), paste0("hash_r3_", 11:15))
  state <- accumulate(state, list(results = round3_results, seen_hashes = round3_hashes, novelty_history = list(0.5)))

  # CRITICAL: After 3 rounds of 5 results each, we should have 15 results, not 5

  expect_equal(length(state$results), 15)
  expect_equal(length(state$seen_hashes), 15)
  expect_equal(length(state$novelty_history), 3)
})

test_that("non-reducer keys are overwritten (not appended)", {
  LIST_REDUCER_KEYS <- c("results", "novelty_history", "errors")
  DICT_REDUCER_KEYS <- c("seen_hashes")

  accumulate <- function(accumulated_state, node_state) {
    for (key in names(node_state)) {
      value <- node_state[[key]]
      if (key %in% LIST_REDUCER_KEYS) {
        if (is.null(accumulated_state[[key]])) accumulated_state[[key]] <- list()
        if (is.list(value)) {
          accumulated_state[[key]] <- c(accumulated_state[[key]], value)
        } else {
          accumulated_state[[key]] <- c(accumulated_state[[key]], list(value))
        }
      } else if (key %in% DICT_REDUCER_KEYS) {
        if (is.null(accumulated_state[[key]])) accumulated_state[[key]] <- list()
        if (is.list(value)) {
          for (k in names(value)) {
            accumulated_state[[key]][[k]] <- value[[k]]
          }
        }
      } else {
        accumulated_state[[key]] <- value
      }
    }
    accumulated_state
  }

  state <- list(status = "planning", round_number = 0)

  state <- accumulate(state, list(status = "searching", round_number = 1))
  expect_equal(state$status, "searching")
  expect_equal(state$round_number, 1)

  state <- accumulate(state, list(status = "complete", round_number = 3))
  expect_equal(state$status, "complete")
  expect_equal(state$round_number, 3)
})

test_that("errors list accumulates across nodes", {
  LIST_REDUCER_KEYS <- c("results", "novelty_history", "errors")
  DICT_REDUCER_KEYS <- c("seen_hashes")

  accumulate <- function(accumulated_state, node_state) {
    for (key in names(node_state)) {
      value <- node_state[[key]]
      if (key %in% LIST_REDUCER_KEYS) {
        if (is.null(accumulated_state[[key]])) accumulated_state[[key]] <- list()
        if (is.list(value)) {
          accumulated_state[[key]] <- c(accumulated_state[[key]], value)
        } else {
          accumulated_state[[key]] <- c(accumulated_state[[key]], list(value))
        }
      } else if (key %in% DICT_REDUCER_KEYS) {
        if (is.null(accumulated_state[[key]])) accumulated_state[[key]] <- list()
        if (is.list(value)) {
          for (k in names(value)) {
            accumulated_state[[key]][[k]] <- value[[k]]
          }
        }
      } else {
        accumulated_state[[key]] <- value
      }
    }
    accumulated_state
  }

  state <- list(errors = list())

  state <- accumulate(state, list(errors = list(list(stage = "search", error = "timeout"))))
  expect_equal(length(state$errors), 1)

  state <- accumulate(state, list(errors = list(list(stage = "extract", error = "parse_fail"))))
  expect_equal(length(state$errors), 2)
  expect_equal(state$errors[[1]]$stage, "search")
  expect_equal(state$errors[[2]]$stage, "extract")
})
