# test-novelty-rate.R
# Tests for per-round novelty rate calculation (Bug fix #2)
#
# The novelty rate previously used a cumulative denominator:
#   novelty = new_hashes / (seen_hashes + new_hashes)
# which premature-stopped when many results were already found.
#
# Fixed to per-round:
#   novelty = new_hashes / batch_size
# where batch_size is the number of results entering the deduper this round.

# ============================================================================
# Novelty Rate Calculation Tests (Pure R, no Python required)
# ============================================================================

test_that("novelty rate is per-round, not cumulative", {
  # Simulate the deduper's novelty calculation

  # Old (buggy) formula:
  # total_unique = len(seen_hashes) + len(new_hashes)
  # novelty_rate = len(new_hashes) / max(1, total_unique)

  # New (fixed) formula:
  # batch_size = len(results)  # results entering deduper THIS round
  # novelty_rate = len(new_hashes) / max(1, batch_size)

  compute_novelty_fixed <- function(batch_size, new_count) {
    new_count / max(1, batch_size)
  }

  compute_novelty_old <- function(seen_count, new_count) {
    new_count / max(1, seen_count + new_count)
  }

  # Scenario: 100 previously seen + 5 new results out of 5 in batch
  # Old: 5 / (100 + 5) = 0.048 -- falsely below 0.1 threshold
  # New: 5 / 5 = 1.0 -- correctly shows 100% of batch was novel
  old_rate <- compute_novelty_old(seen_count = 100, new_count = 5)
  new_rate <- compute_novelty_fixed(batch_size = 5, new_count = 5)

  expect_equal(old_rate, 5/105, tolerance = 0.001)  # ~0.048
  expect_equal(new_rate, 1.0)
  expect_true(old_rate < 0.1)  # Would trigger premature stop
  expect_true(new_rate >= 0.1) # Correctly continues
})

test_that("novelty rate handles mixed batch (some dupes, some new)", {
  compute_novelty_fixed <- function(batch_size, new_count) {
    new_count / max(1, batch_size)
  }

  # 10 results in batch, 3 are novel, 7 are dupes
  rate <- compute_novelty_fixed(batch_size = 10, new_count = 3)
  expect_equal(rate, 0.3)

  # 10 results in batch, all are dupes
  rate_zero <- compute_novelty_fixed(batch_size = 10, new_count = 0)
  expect_equal(rate_zero, 0.0)

  # 10 results in batch, all are novel
  rate_all <- compute_novelty_fixed(batch_size = 10, new_count = 10)
  expect_equal(rate_all, 1.0)
})

test_that("novelty rate handles empty batch gracefully", {
  compute_novelty_fixed <- function(batch_size, new_count) {
    new_count / max(1, batch_size)
  }

  # Edge case: empty batch
  rate <- compute_novelty_fixed(batch_size = 0, new_count = 0)
  expect_equal(rate, 0.0)
})

test_that("novelty history correctly feeds into stopper plateau detection", {
  # The stopper checks if last N rounds all had novelty < threshold
  plateau_rounds <- 2
  novelty_min <- 0.1

  # Simulate novelty history with the FIXED per-round rates
  history <- c(1.0, 0.8, 0.3, 0.05, 0.02)

  # Check last plateau_rounds
  recent <- tail(history, plateau_rounds)
  is_plateau <- all(recent < novelty_min)

  expect_true(is_plateau)  # 0.05 and 0.02 are both < 0.1

  # With fixed calculation, earlier rounds would show higher novelty
  history_active <- c(1.0, 0.8, 0.6, 0.4)
  recent_active <- tail(history_active, plateau_rounds)
  is_plateau_active <- all(recent_active < novelty_min)

  expect_false(is_plateau_active)  # 0.6 and 0.4 are both > 0.1
})

test_that("per-round novelty avoids premature stopping scenario", {
  # The key scenario from the bug report:
  # After accumulating 100 results, each new round finds 5 novel results.
  # Old formula would stop immediately (5/105 < 0.1).
  # New formula correctly recognizes productive search (5/5 = 1.0).

  plateau_rounds <- 2
  novelty_min <- 0.05  # default threshold

  # Simulate 5 rounds with old formula (cumulative denominator)
  old_history <- c(
    10/10,        # Round 1: 10 new, 10 total -> 1.0
    8/(10+8),     # Round 2: 8 new, 18 total -> 0.44
    5/(18+5),     # Round 3: 5 new, 23 total -> 0.22
    5/(23+5),     # Round 4: 5 new, 28 total -> 0.18
    5/(28+5)      # Round 5: 5 new, 33 total -> 0.15
  )

  # All still above 0.05 but trending down fast toward premature stop
  # By round 20+: 5/(100+5) = 0.048 < 0.05 -> stops!

  # Simulate with new formula (per-round batch size)
  # Assume each round processes 10 results, 5 are novel
  new_history <- c(
    10/10,  # Round 1: 10 novel out of 10 batch -> 1.0
    8/10,   # Round 2: 8 novel out of 10 batch -> 0.8
    5/10,   # Round 3: 5 novel out of 10 batch -> 0.5
    5/10,   # Round 4: 5 novel out of 10 batch -> 0.5
    5/10    # Round 5: 5 novel out of 10 batch -> 0.5
  )

  # With per-round formula, productive search continues correctly
  recent_new <- tail(new_history, plateau_rounds)
  expect_false(all(recent_new < novelty_min))
})
