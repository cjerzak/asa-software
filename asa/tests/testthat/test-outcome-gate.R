test_that("evaluate_schema_outcome reports complete when all fields resolved", {
  gate <- asa_test_import_module("shared.schema_outcome_gate", required_file = "shared/schema_outcome_gate.py")

  schema <- list(
    status = "complete|partial",
    items = list(list(name = "string", state = "string|null"))
  )
  field_status <- list(
    status = list(status = "found", value = "partial"),
    name = list(status = "found", value = "Alice"),
    state = list(status = "unknown", value = NULL)
  )

  report <- gate$evaluate_schema_outcome(
    expected_schema = schema,
    field_status = field_status,
    budget_state = list(budget_exhausted = FALSE)
  )

  expect_true(isTRUE(report$done))
  expect_equal(report$completion_status, "complete")
  expect_equal(as.integer(report$total_fields), 3L)
  expect_equal(length(report$missing_fields), 0L)
})

test_that("evaluate_schema_outcome reports partial when budget exhausts first", {
  gate <- asa_test_import_module("shared.schema_outcome_gate", required_file = "shared/schema_outcome_gate.py")

  schema <- list(name = "string", birth_year = "integer|null")
  field_status <- list(
    name = list(status = "found", value = "Ada Lovelace")
  )

  report <- gate$evaluate_schema_outcome(
    expected_schema = schema,
    field_status = field_status,
    budget_state = list(budget_exhausted = TRUE)
  )

  expect_false(isTRUE(report$done))
  expect_equal(report$completion_status, "partial")
  expect_true("birth_year" %in% unlist(report$missing_fields))
})

test_that("evaluate_research_outcome distinguishes complete vs partial stops", {
  gate <- asa_test_import_module("shared.schema_outcome_gate", required_file = "shared/schema_outcome_gate.py")

  complete_report <- gate$evaluate_research_outcome(
    round_number = 2L,
    queries_used = 4L,
    tokens_used = 1000L,
    elapsed_sec = 2,
    items_found = 10L,
    novelty_history = list(0.6, 0.4),
    target_items = 10L,
    max_rounds = 8L
  )
  expect_true(isTRUE(complete_report$should_stop))
  expect_equal(complete_report$stop_reason, "target_reached")
  expect_equal(complete_report$completion_status, "complete")

  partial_report <- gate$evaluate_research_outcome(
    round_number = 5L,
    queries_used = 20L,
    tokens_used = 4000L,
    elapsed_sec = 20,
    items_found = 3L,
    novelty_history = list(0.01, 0.03),
    target_items = 25L,
    plateau_rounds = 2L,
    novelty_min = 0.05
  )
  expect_true(isTRUE(partial_report$should_stop))
  expect_equal(partial_report$stop_reason, "novelty_plateau")
  expect_equal(partial_report$completion_status, "partial")
  expect_true(any(grepl("^target_items:", unlist(partial_report$missing_constraints))))
})
