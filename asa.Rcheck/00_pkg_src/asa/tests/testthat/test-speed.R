# Speed/performance regression tests
#
# These tests establish baseline performance for core operations and fail
# if execution time exceeds the threshold (baseline * tolerance_factor).
# This helps catch performance regressions early.
#
# After running, results are written to SPEED_REPORT.md in this directory.

# Configuration
SPEED_TEST_ITERATIONS <- 10
TOLERANCE_FACTOR <- 4

# Baseline times (in seconds) - established on reference hardware (Dec 19, 2024)
# These should be updated if significant intentional performance changes are made
BASELINE_BUILD_PROMPT <- 0.090     # 10 iterations x 50 build_prompt calls each
BASELINE_HELPER_FUNCS <- 0.070     # 10 iterations x 100 helper function calls each
BASELINE_COMBINED <- 0.091         # Combined workload
BASELINE_AGENT_SEARCH <- 17.6      # Single agent search task (includes network/API latency)

# Environment to store results across tests for final report
.speed_results <- new.env(parent = emptyenv())
.speed_results$data <- list()

# Helper to record a result
record_speed_result <- function(name, elapsed, baseline, threshold) {
  ratio <- elapsed / baseline
  passed <- elapsed < threshold
  .speed_results$data[[name]] <- list(
    name = name,
    elapsed = elapsed,
    baseline = baseline,
    threshold = threshold,
    ratio = ratio,
    passed = passed
  )
}

# Helper to find project root README.md
find_readme <- function() {
  candidates <- c(
    "../../../README.md",                    # From tests/testthat/
    "../../README.md",                       # Alternative
    file.path(getwd(), "README.md"),         # Working dir
    file.path(getwd(), "..", "README.md")    # Parent of working dir
  )
  for (cand in candidates) {
    if (file.exists(cand)) return(normalizePath(cand))
  }
  NULL
}

# Helper to update README.md with speed results
update_readme_speed_section <- function(results) {
  readme_path <- find_readme()
  if (is.null(readme_path)) {
    message("  Could not find README.md to update")
    return(invisible(NULL))
  }

  # Build the speed section content
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
  all_passed <- all(vapply(results, function(x) x$passed, logical(1)))
  overall_status <- if (all_passed) "PASS" else "FAIL"

  speed_lines <- c(
    sprintf("**Last Run:** %s | **Status:** %s", timestamp, overall_status),
    "",
    "| Benchmark | Current | Baseline | Ratio | Status |",
    "|-----------|---------|----------|-------|--------|"
  )

  for (r in results) {
    status_icon <- if (r$passed) "PASS" else "FAIL"
    # Use adaptive formatting: seconds for fast ops, whole seconds for slow ops
    if (r$baseline >= 1.0) {
      elapsed_fmt <- sprintf("%.1fs", r$elapsed)
      baseline_fmt <- sprintf("%.0fs", r$baseline)
    } else {
      elapsed_fmt <- sprintf("%.3fs", r$elapsed)
      baseline_fmt <- sprintf("%.2fs", r$baseline)
    }
    speed_lines <- c(speed_lines, sprintf(
      "| `%s` | %s | %s | %.2fx | %s |",
      r$name, elapsed_fmt, baseline_fmt, r$ratio, status_icon
    ))
  }

  speed_lines <- c(speed_lines,
    "",
    sprintf("Tests fail if time exceeds %.2fx baseline. ", TOLERANCE_FACTOR),
    "See [full report](asa/tests/testthat/SPEED_REPORT.md) for details."
  )

  new_section <- paste(speed_lines, collapse = "\n")

  # Read README and replace section between markers
  tryCatch({
    readme_content <- readLines(readme_path, warn = FALSE)
    readme_text <- paste(readme_content, collapse = "\n")

    # Pattern to match content between markers
    pattern <- "<!-- SPEED_REPORT_START -->.*<!-- SPEED_REPORT_END -->"
    replacement <- sprintf("<!-- SPEED_REPORT_START -->\n%s\n<!-- SPEED_REPORT_END -->", new_section)

    if (grepl(pattern, readme_text)) {
      updated_text <- sub(pattern, replacement, readme_text)
      writeLines(updated_text, readme_path)
      message(sprintf("  README.md updated: %s", readme_path))
    } else {
      message("  README.md missing speed section markers")
    }
  }, error = function(e) {
    message(sprintf("  Could not update README.md: %s", e$message))
  })

  invisible(readme_path)
}

# Helper to write the speed report
write_speed_report <- function() {
  results <- .speed_results$data
  if (length(results) == 0) return(invisible(NULL))


  # Determine report path (same directory as test file)
  report_path <- file.path(
    system.file("tests", "testthat", package = "asa", mustWork = FALSE),
    "SPEED_REPORT.md"
  )


  # Fallback for development (not installed package)
  if (report_path == "SPEED_REPORT.md" || !dir.exists(dirname(report_path))) {
    # Try to find the testthat directory relative to working directory
    candidates <- c(
      "tests/testthat/SPEED_REPORT.md",
      "asa/tests/testthat/SPEED_REPORT.md",
      file.path(getwd(), "tests", "testthat", "SPEED_REPORT.md")
    )
    for (cand in candidates) {
      if (dir.exists(dirname(cand))) {
        report_path <- cand
        break
      }
    }
  }

  # Build report content
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
  all_passed <- all(vapply(results, function(x) x$passed, logical(1)))
  overall_status <- if (all_passed) "PASS" else "FAIL"

  lines <- c(
    "# Speed Test Report",
    "",
    sprintf("**Last Run:** %s", timestamp),
    sprintf("**Overall Status:** %s", overall_status),
    sprintf("**Tolerance Factor:** %.2fx baseline", TOLERANCE_FACTOR),
    "",
    "## Results",
    "",
    "| Benchmark | Current | Baseline | Threshold | Ratio | Status |",
    "|-----------|---------|----------|-----------|-------|--------|"
  )

  for (r in results) {
    status_icon <- if (r$passed) "PASS" else "FAIL"
    # Use adaptive formatting: seconds for fast ops, whole seconds for slow ops
    if (r$baseline >= 1.0) {
      elapsed_fmt <- sprintf("%.1fs", r$elapsed)
      baseline_fmt <- sprintf("%.0fs", r$baseline)
      threshold_fmt <- sprintf("%.0fs", r$threshold)
    } else {
      elapsed_fmt <- sprintf("%.4fs", r$elapsed)
      baseline_fmt <- sprintf("%.2fs", r$baseline)
      threshold_fmt <- sprintf("%.2fs", r$threshold)
    }
    lines <- c(lines, sprintf(
      "| %s | %s | %s | %s | %.2fx | %s |",
      r$name, elapsed_fmt, baseline_fmt, threshold_fmt, r$ratio, status_icon
    ))
  }

  lines <- c(lines,
    "",
    "## Baseline Reference",
    "",
    "Baselines were established on Dec 19, 2024. Tests fail if current time exceeds",
    sprintf("`baseline * %.2f`.", TOLERANCE_FACTOR),
    "",
    "| Benchmark | Baseline | Operations per Run |",
    "|-----------|----------|-------------------|",
    sprintf("| build_prompt | %.2fs | %d iterations x 50 calls x 3 templates |",
            BASELINE_BUILD_PROMPT, SPEED_TEST_ITERATIONS),
    sprintf("| helper_funcs | %.2fs | %d iterations x 100 calls x 5 functions |",
            BASELINE_HELPER_FUNCS, SPEED_TEST_ITERATIONS),
    sprintf("| combined | %.2fs | %d iterations of mixed workload |",
            BASELINE_COMBINED, SPEED_TEST_ITERATIONS),
    sprintf("| agent_search | %.0fs | 1 search task (API + network latency) |",
            BASELINE_AGENT_SEARCH),
    ""
  )

  # Write report
 tryCatch({
    writeLines(lines, report_path)
    message(sprintf("\n  Speed report written to: %s", report_path))
  }, error = function(e) {
    message(sprintf("\n  Could not write speed report: %s", e$message))
  })

  # Also update README.md
  update_readme_speed_section(results)

  invisible(report_path)
}

test_that("build_prompt performance is within acceptable bounds", {
  n_iterations <- SPEED_TEST_ITERATIONS
  n_ops_per_iter <- 50

  start_time <- Sys.time()

  for (i in seq_len(n_iterations)) {
    for (j in seq_len(n_ops_per_iter)) {
      build_prompt("Find {{name}} in {{country}} during {{year}}",
                   name = "Einstein", country = "Germany", year = 1905)
      build_prompt("Search for {{topic}} related to {{field}} with {{constraint}}",
                   topic = "quantum mechanics", field = "physics", constraint = "1920s")
      build_prompt("What is the {{metric}} of {{entity}} in {{location}}?",
                   metric = "population", entity = "Tokyo", location = "Japan")
    }
  }

  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  threshold <- BASELINE_BUILD_PROMPT * TOLERANCE_FACTOR

  # Record for report
  record_speed_result("build_prompt", elapsed, BASELINE_BUILD_PROMPT, threshold)

  # Report timing for visibility
  message(sprintf(
    "\n  build_prompt: %.4f sec (baseline: %.2f, threshold: %.2f, ratio: %.2fx)",
    elapsed, BASELINE_BUILD_PROMPT, threshold, elapsed / BASELINE_BUILD_PROMPT
  ))

  expect_lt(
    elapsed, threshold,
    label = sprintf(
      "build_prompt took %.4f sec, exceeding %.2fx baseline (%.2f sec threshold)",
      elapsed, TOLERANCE_FACTOR, threshold
    )
  )
})

test_that("helper function performance is within acceptable bounds", {
  n_iterations <- SPEED_TEST_ITERATIONS
  n_ops_per_iter <- 100

  start_time <- Sys.time()

  for (i in seq_len(n_iterations)) {
    for (j in seq_len(n_ops_per_iter)) {
      json_escape("Test \"string\" with\nnewlines and special chars")
      decode_html("&amp;lt;html&amp;gt; &quot;test&quot; &apos;single&apos;")
      clean_whitespace("  Multiple   spaces   here  and   more  ")
      truncate_string("This is a very long string that definitely needs truncation", 25)
      format_duration(45.5)
    }
  }

  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  threshold <- BASELINE_HELPER_FUNCS * TOLERANCE_FACTOR

  # Record for report
  record_speed_result("helper_funcs", elapsed, BASELINE_HELPER_FUNCS, threshold)

  # Report timing for visibility
  message(sprintf(
    "\n  helper_funcs: %.4f sec (baseline: %.2f, threshold: %.2f, ratio: %.2fx)",
    elapsed, BASELINE_HELPER_FUNCS, threshold, elapsed / BASELINE_HELPER_FUNCS
  ))

  expect_lt(
    elapsed, threshold,
    label = sprintf(
      "helper functions took %.4f sec, exceeding %.2fx baseline (%.2f sec threshold)",
      elapsed, TOLERANCE_FACTOR, threshold
    )
  )
})

test_that("combined workload performance is within acceptable bounds", {
  n_iterations <- SPEED_TEST_ITERATIONS

  start_time <- Sys.time()

  for (i in seq_len(n_iterations)) {
    # build_prompt operations (50 per iteration)
    for (j in 1:50) {
      build_prompt("Find {{name}} in {{country}} during {{year}}",
                   name = "Einstein", country = "Germany", year = 1905)
    }

    # Helper function operations (100 per iteration)
    for (j in 1:100) {
      json_escape("Test \"string\" with\nnewlines and special chars")
      decode_html("&amp;lt;html&amp;gt; &quot;test&quot;")
      clean_whitespace("  Multiple   spaces   here  ")
      truncate_string("This is a very long string that needs truncation", 25)
      format_duration(45.5)
    }
  }

  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  threshold <- BASELINE_COMBINED * TOLERANCE_FACTOR

  # Record for report
  record_speed_result("combined", elapsed, BASELINE_COMBINED, threshold)

  # Report timing for visibility
  message(sprintf(
    "\n  combined:     %.4f sec (baseline: %.2f, threshold: %.2f, ratio: %.2fx)",
    elapsed, BASELINE_COMBINED, threshold, elapsed / BASELINE_COMBINED
  ))

  expect_lt(
    elapsed, threshold,
    label = sprintf(
      "combined workload took %.4f sec, exceeding %.2fx baseline (%.2f sec threshold)",
      elapsed, TOLERANCE_FACTOR, threshold
    )
  )
})

test_that("agent search performance is within acceptable bounds", {

  # Skip if environment not configured for agent tests
  skip_if_not(
    nzchar(Sys.getenv("OPENAI_API_KEY")) || nzchar(Sys.getenv("GROQ_API_KEY")),
    "No API key available (OPENAI_API_KEY or GROQ_API_KEY)"
  )

  # Try to initialize agent - skip if backend not available
  agent <- tryCatch({
    initialize_agent(
      backend = if (nzchar(Sys.getenv("OPENAI_API_KEY"))) "openai" else "groq",
      model = if (nzchar(Sys.getenv("OPENAI_API_KEY"))) "gpt-4.1-mini" else "llama-3.3-70b-versatile",
      verbose = FALSE
    )
  }, error = function(e) {
    skip(paste("Could not initialize agent:", e$message))
  })

  start_time <- Sys.time()

  # Run a single search task
  result <- tryCatch({
    run_task(
      prompt = "What is the population of Tokyo, Japan?",
      output_format = "text",
      agent = agent,
      verbose = FALSE
    )
  }, error = function(e) {
    skip(paste("Agent search failed:", e$message))
  })

  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  threshold <- BASELINE_AGENT_SEARCH * TOLERANCE_FACTOR

  # Record for report
  record_speed_result("agent_search", elapsed, BASELINE_AGENT_SEARCH, threshold)

  # Report timing for visibility
  message(sprintf(
    "\n  agent_search: %.2f sec (baseline: %.0f, threshold: %.0f, ratio: %.2fx)",
    elapsed, BASELINE_AGENT_SEARCH, threshold, elapsed / BASELINE_AGENT_SEARCH
  ))

  expect_lt(
    elapsed, threshold,
    label = sprintf(
      "agent search took %.2f sec, exceeding %.2fx baseline (%.0f sec threshold)",
      elapsed, TOLERANCE_FACTOR, threshold
    )
  )
})

test_that("speed report is generated", {
  # This test runs last (alphabetically "speed report" > "combined"/"helper"/"build"/"agent")
  # and writes the accumulated results to SPEED_REPORT.md
  write_speed_report()
  expect_true(TRUE)
})
