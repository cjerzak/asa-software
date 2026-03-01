test_that("_finalize_plan_statuses keeps pending steps and completes only in-progress steps", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")

  in_plan <- list(
    goal = "G",
    steps = list(
      list(id = 1L, description = "S1", status = "pending", findings = ""),
      list(id = "2", description = "S2", status = "in_progress", findings = "working"),
      list(id = 3L, description = "S3", status = "completed", findings = "done"),
      list(id = 4L, description = "S4", status = "skipped", findings = "n/a")
    ),
    version = 2L,
    current_step = 2L
  )

  out <- reticulate::py_to_r(custom_ddg$`_finalize_plan_statuses`(in_plan))
  expect_true(is.list(out))
  expect_equal(length(out$steps), 4L)
  expect_equal(as.character(out$steps[[1]]$status), "pending")
  expect_equal(as.character(out$steps[[2]]$status), "completed")
  expect_equal(as.character(out$steps[[3]]$status), "completed")
  expect_equal(as.character(out$steps[[4]]$status), "skipped")
  expect_equal(as.integer(out$current_step), 1L)
  expect_equal(as.integer(out$version), 3L)
})

test_that("_finalize_plan_statuses preserves version when no step status changes", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")

  in_plan <- list(
    goal = "G",
    steps = list(
      list(id = 1L, description = "S1", status = "pending", findings = ""),
      list(id = 2L, description = "S2", status = "completed", findings = "done")
    ),
    version = 5L,
    current_step = 1L
  )

  out <- reticulate::py_to_r(custom_ddg$`_finalize_plan_statuses`(in_plan))
  expect_true(is.list(out))
  expect_equal(as.integer(out$version), 5L)
  expect_equal(as.character(out$steps[[1]]$status), "pending")
  expect_equal(as.character(out$steps[[2]]$status), "completed")
  expect_equal(as.integer(out$current_step), 1L)
})

test_that("_finalize_plan_statuses closes pending steps when close_pending is TRUE", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")

  in_plan <- list(
    goal = "G",
    steps = list(
      list(id = 1L, description = "S1", status = "pending", findings = ""),
      list(id = 2L, description = "S2", status = "in_progress", findings = "working"),
      list(id = 3L, description = "S3", status = "completed", findings = "done")
    ),
    version = 3L,
    current_step = 1L
  )

  out <- reticulate::py_to_r(custom_ddg$`_finalize_plan_statuses`(
    in_plan,
    close_pending = TRUE
  ))
  expect_true(is.list(out))
  expect_equal(as.character(out$steps[[1]]$status), "skipped")
  expect_equal(as.character(out$steps[[2]]$status), "completed")
  expect_equal(as.character(out$steps[[3]]$status), "completed")
  expect_true(is.null(out$current_step))
  expect_equal(as.integer(out$version), 4L)
})

test_that("_finalized_plan_history_entry emits finalize snapshot metadata", {
  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")

  in_plan <- list(
    goal = "G",
    steps = list(
      list(id = 1L, description = "S1", status = "in_progress", findings = "")
    ),
    version = 7L,
    current_step = 1L
  )

  entry <- reticulate::py_to_r(custom_ddg$`_finalized_plan_history_entry`(in_plan))
  expect_true(is.list(entry))
  expect_equal(as.integer(entry$version), 8L)
  expect_equal(as.character(entry$source), "finalize")
  expect_true(is.list(entry$plan))
  expect_equal(as.character(entry$plan$steps[[1]]$status), "completed")
  expect_true(is.null(entry$plan$current_step))
})

test_that(".extract_plan_steps unwraps nested plan payloads from plan_history entries", {
  steps <- asa:::.extract_plan_steps(
    list(
      version = 2L,
      plan = list(
        goal = "Sample",
        steps = list(
          list(step = "Locate profile", status = "completed"),
          list(step = "Extract fields", status = "pending")
        )
      )
    )
  )

  expect_true(is.list(steps))
  expect_equal(length(steps), 2L)
  expect_equal(as.character(steps[[1]]$status), "completed")
  expect_equal(as.character(steps[[2]]$status), "pending")
})

test_that(".summarize_plan_history reports concrete status counts for wrapped plan payloads", {
  summary <- asa:::.summarize_plan_history(
    plan_history = list(
      list(
        version = 2L,
        plan = list(
          steps = list(
            list(step = "Locate profile", status = "completed"),
            list(step = "Extract fields", status = "pending")
          )
        )
      )
    )
  )

  expect_true(length(summary) >= 1L)
  expect_match(summary[[1]], "2 total \\(1 completed, 0 in_progress, 1 pending\\)")
  expect_false(grepl("status metadata not yet populated", summary[[1]], fixed = TRUE))
})
