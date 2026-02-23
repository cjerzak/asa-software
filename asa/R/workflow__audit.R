#' Audit Enumeration Results for Completeness and Quality
#'
#' Validates enumeration results for completeness, consistency, and data quality
#' using either Claude Code (CLI) or a LangGraph-based audit pipeline.
#'
#' @param result An \code{asa_enumerate_result} object or a data.frame to audit
#' @param query The original enumeration query (inferred from result if NULL)
#' @param known_universe Optional vector of expected items for completeness check
#' @param checks Character vector of checks to perform. Options: "completeness",
#'   "consistency", "gaps", "anomalies". Default runs all checks.
#' @param backend Backend to use for auditing: "claude_code" (CLI) or "langgraph"
#' @param claude_model Model to use with Claude Code backend
#' @param llm_model Model to use with LangGraph backend
#' @param interactive If TRUE and using claude_code backend, spawn an interactive
#'   Claude Code session instead of programmatic invocation
#' @param confidence_threshold Flag items with confidence below this threshold
#' @param timeout Timeout in seconds for the audit operation
#' @param verbose Print progress messages
#' @param agent Existing asa_agent for LangGraph backend (optional)
#'
#' @return An \code{asa_audit_result} object containing:
#'   \item{data}{Original data with audit columns added (_audit_flag, _audit_notes)}
#'   \item{audit_summary}{High-level summary of findings}
#'   \item{issues}{List of identified issues with severity and descriptions}
#'   \item{recommendations}{Suggested remediation queries}
#'   \item{completeness_score}{0-1 score for data completeness}
#'   \item{consistency_score}{0-1 score for data consistency}
#'
#' @details
#' The audit function adds three columns to the data:
#' \itemize{
#'   \item \code{_audit_flag}: "ok", "warning", or "suspect"
#'   \item \code{_audit_notes}: Explanation of any issues
#'   \item \code{_confidence_adjusted}: Revised confidence after audit
#' }
#'
#' ## Audit Checks
#'
#' \strong{completeness}: Checks for missing items by comparing against
#'   known_universe (if provided) or using domain knowledge.
#'
#' \strong{consistency}: Validates data types, patterns, and value ranges.
#'
#' \strong{gaps}: Identifies systematic patterns of missing data
#'   (geographic, temporal, categorical gaps).
#'
#' \strong{anomalies}: Detects duplicates, outliers, and suspicious patterns.
#'
#' @examples
#' \dontrun{
#' # Audit enumeration results with Claude Code
#' senators <- asa_enumerate(
#'   query = "Find all current US senators",
#'   schema = c(name = "character", state = "character", party = "character")
#' )
#' audit <- asa_audit(senators, backend = "claude_code")
#' print(audit)
#'
#' # Audit with known universe for precise completeness check
#' audit <- asa_audit(senators, known_universe = state.abb)
#'
#' # Interactive mode for complex audits
#' asa_audit(senators, backend = "claude_code", interactive = TRUE)
#'
#' # Use LangGraph backend
#' audit <- asa_audit(senators, backend = "langgraph", agent = agent)
#' }
#'
#' @export
asa_audit <- function(result,
                      query = NULL,
                      known_universe = NULL,
                      checks = c("completeness", "consistency", "gaps", "anomalies"),
                      backend = c("claude_code", "langgraph"),
                      claude_model = "claude-sonnet-4-20250514",
                      llm_model = "gpt-4.1-mini",
                      interactive = FALSE,
                      confidence_threshold = 0.8,
                      timeout = 120,
                      verbose = TRUE,
                      agent = NULL) {

  # Match arguments

backend <- match.arg(backend)
  checks <- match.arg(checks, c("completeness", "consistency", "gaps", "anomalies"),
                      several.ok = TRUE)

  # Validate inputs
  .validate_audit_inputs(result, query, known_universe, checks, backend,
                         confidence_threshold, timeout)

  # Extract data and query from result
  audit_data <- .extract_audit_data(result)
  data <- audit_data$data
  query <- query %||% audit_data$query

  if (is.null(query) || query == "") {
    query <- "[No query provided - performing general audit]"
  }

  # Get schema from result or infer from data
  schema <- audit_data$schema
  if (is.null(schema)) {
    schema <- sapply(data, class)
  }

  start_time <- Sys.time()

  if (verbose) {
    message("Starting audit with ", backend, " backend...")
    message("  Data rows: ", nrow(data))
    message("  Checks: ", paste(checks, collapse = ", "))
  }

  # Dispatch to appropriate backend
  if (backend == "claude_code") {
    audit_result <- .audit_with_claude_code(
      data = data,
      query = query,
      schema = schema,
      checks = checks,
      known_universe = known_universe,
      confidence_threshold = confidence_threshold,
      timeout = timeout,
      model = claude_model,
      interactive = interactive,
      verbose = verbose
    )
  } else {
    audit_result <- .audit_with_langgraph(
      data = data,
      query = query,
      schema = schema,
      checks = checks,
      known_universe = known_universe,
      confidence_threshold = confidence_threshold,
      timeout = timeout,
      model = llm_model,
      agent = agent,
      verbose = verbose
    )
  }

  elapsed_time <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))

  # Annotate original data with audit results
  annotated_data <- .annotate_results(data, audit_result$flagged_rows)

  # Build and return result
  asa_audit_result(
    data = annotated_data,
    audit_summary = audit_result$summary %||% "",
    issues = audit_result$issues %||% list(),
    recommendations = audit_result$recommendations %||% character(),
    completeness_score = audit_result$completeness_score %||% 1.0,
    consistency_score = audit_result$consistency_score %||% 1.0,
    backend_used = backend,
    elapsed_time = elapsed_time,
    query = query,
    checks = checks
  )
}


# ============================================================================
# Claude Code Backend
# ============================================================================

#' Audit Using Claude Code CLI
#'
#' @noRd
.audit_with_claude_code <- function(data, query, schema, checks, known_universe,
                                    confidence_threshold, timeout, model,
                                    interactive, verbose) {

  # Check if claude CLI is available
  claude_path <- Sys.which("claude")
  if (!nzchar(claude_path)) {
    stop("Claude Code CLI not found. Please install it from: ",
         "https://github.com/anthropics/claude-code",
         call. = FALSE)
  }

  # Serialize data to JSON file (for large datasets or file attachment)
  data_json <- jsonlite::toJSON(data, auto_unbox = TRUE, pretty = TRUE)
  data_file <- tempfile(fileext = ".json")
  writeLines(data_json, data_file)
  on.exit(unlink(data_file), add = TRUE)

  # Check whether the CLI supports file attachments
  claude_help <- tryCatch(.run_command("claude", "--help"), error = function(e) NULL)
  supports_file <- !is.null(claude_help) &&
    claude_help$status == 0 &&
    grepl("--file", claude_help$stdout, fixed = TRUE)

  # Build audit prompt
  prompt <- .build_audit_prompt(query, data, schema, checks, known_universe,
                                 confidence_threshold)
  file_args <- character()
  if (supports_file) {
    prompt <- paste0(prompt, "\n\nUse the attached JSON file for the full dataset.")
    file_args <- c("--file", data_file)
  } else {
    prompt <- paste0(prompt, "\n\nFull dataset (JSON):\n", data_json)
  }

  if (interactive) {
    # Spawn interactive Claude Code session
    if (verbose) message("Spawning interactive Claude Code session...")
    message("\n", strrep("=", 60))
    message("INTERACTIVE AUDIT SESSION")
    message("The data has been loaded. Review the audit and copy results.")
    message(strrep("=", 60), "\n")

    # Write prompt to temp file for piping
    prompt_file <- tempfile(fileext = ".txt")
    writeLines(prompt, prompt_file)
    on.exit(unlink(prompt_file), add = TRUE)

    # Run interactively - user will see the session
    tryCatch({
      .run_command(
        command = "claude",
        args = c("--model", model, file_args),
        stdin = prompt_file,
        echo = TRUE
      )
    }, error = function(e) {
      warning("Interactive session ended: ", e$message)
    })

    # Return placeholder result for interactive mode
    return(list(
      summary = "Interactive audit session completed. Review output manually.",
      issues = list(),
      recommendations = character(),
      completeness_score = NA_real_,
      consistency_score = NA_real_,
      flagged_rows = list()
    ))
  }

  # Programmatic invocation
  if (verbose) message("Running Claude Code audit...")

  result <- tryCatch({
    .run_command(
      command = "claude",
      args = c(
        "--model", model,
        "--output-format", "json",
        file_args,
        "-p", prompt
      ),
      timeout = timeout
    )
  }, error = function(e) {
    stop("Claude Code invocation failed: ", e$message, call. = FALSE)
  })

  if (result$status != 0) {
    warning("Claude Code returned non-zero status: ", result$status)
    if (nzchar(result$stderr)) {
      warning("Stderr: ", result$stderr)
    }
  }

  # Parse JSON response
  parsed <- tryCatch({
    jsonlite::fromJSON(result$stdout, simplifyVector = FALSE)
  }, error = function(e) {
    # Try to extract JSON from mixed output using shared parser
    .parse_json_response(result$stdout)
  })

  if (is.null(parsed)) {
    warning("Could not parse Claude Code response as JSON")
    return(list(
      summary = result$stdout,
      issues = list(),
      recommendations = character(),
      completeness_score = 0.5,
      consistency_score = 0.5,
      flagged_rows = list()
    ))
  }

  # Normalize response structure
  list(
    summary = parsed$summary %||% parsed$audit_summary %||% "",
    issues = parsed$issues %||% list(),
    recommendations = parsed$recommendations %||% character(),
    completeness_score = parsed$completeness_score %||% 1.0,
    consistency_score = parsed$consistency_score %||% 1.0,
    flagged_rows = parsed$flagged_rows %||% list()
  )
}


# ============================================================================
# LangGraph Backend
# ============================================================================

#' Audit Using LangGraph Pipeline
#'
#' @noRd
.audit_with_langgraph <- function(data, query, schema, checks, known_universe,
                                  confidence_threshold, timeout, model,
                                  agent, verbose) {

  # Import audit graph module
  .import_audit_modules()

  # Create or reuse agent
  if (is.null(agent)) {
    if (verbose) message("Creating new agent for audit...")
    agent <- initialize_agent(
      backend = "openai",
      model = model,
      verbose = verbose
    )
  }

  # Convert data to Python-compatible format
  data_list <- lapply(seq_len(nrow(data)), function(i) {
    as.list(data[i, , drop = FALSE])
  })

  schema_dict <- as.list(schema)
  known_universe_list <- if (!is.null(known_universe)) as.list(known_universe) else NULL

  llm <- if (!is.null(agent) && !is.null(agent$llm)) agent$llm else asa_env$llm

  # Create audit graph
  audit_graph <- asa_env$audit_graph$create_audit_graph(
    llm = llm,
    checks = as.list(checks)
  )

  # Run audit
  if (verbose) message("Running LangGraph audit...")

  result <- tryCatch({
    asa_env$audit_graph$run_audit(
      graph = audit_graph,
      data = data_list,
      query = query,
      schema = schema_dict,
      known_universe = known_universe_list,
      confidence_threshold = confidence_threshold
    )
  }, error = function(e) {
    stop("LangGraph audit failed: ", e$message, call. = FALSE)
  })

  # Convert Python result to R
  list(
    summary = result$summary %||% "",
    issues = .py_to_r_list(result$issues),
    recommendations = as.character(result$recommendations %||% character()),
    completeness_score = result$completeness_score %||% 1.0,
    consistency_score = result$consistency_score %||% 1.0,
    flagged_rows = .py_to_r_list(result$flagged_rows)
  )
}


# ============================================================================
# Helper Functions
# ============================================================================

#' Import Audit Python Modules
#'
#' @noRd
.import_audit_modules <- function() {
  .import_python_module(
    module_name = "workflows.audit_graph_workflow",
    env_name = "audit_graph"
  )
}

#' Build Audit Prompt for Claude Code
#'
#' @noRd
.build_audit_prompt <- function(query, data, schema, checks, known_universe,
                                 confidence_threshold) {

  # Format schema
  schema_str <- paste(
    names(schema), "(",
    sapply(schema, function(x) if (is.character(x)) x else class(x)[1]),
    ")",
    sep = "", collapse = ", "
  )

  # Format data preview (first 50 rows max for prompt)
  n_preview <- min(50, nrow(data))
  data_preview <- jsonlite::toJSON(
    utils::head(data, n_preview),
    auto_unbox = TRUE,
    pretty = TRUE
  )

  # Format known universe if provided
  universe_str <- ""
  if (!is.null(known_universe)) {
    universe_str <- sprintf(
      "\n\nKnown Universe (%d expected items):\n%s",
      length(known_universe),
      paste(utils::head(known_universe, 20), collapse = ", ")
    )
    if (length(known_universe) > 20) {
      universe_str <- paste0(universe_str, ", ... and ",
                             length(known_universe) - 20, " more")
    }
  }

  # Build prompt
  sprintf('You are a data quality auditor. Analyze the following enumeration results.

Query: "%s"
Schema: %s
Total Rows: %d

Data (first %d rows):
%s%s

Perform these checks: %s

Check descriptions:
1. COMPLETENESS: Are there obvious gaps? Expected entities that are missing?
   Consider: geographic coverage, temporal coverage, categorical coverage.
2. CONSISTENCY: Do values match expected patterns and formats?
   Consider: data types, value ranges, naming conventions.
3. GAPS: What systematic patterns of missing data exist?
   Consider: missing regions, missing time periods, missing categories.
4. ANOMALIES: Any suspicious entries?
   Consider: duplicates, outliers, impossible values, low-confidence entries.

Flag items with confidence below %.2f as requiring review.

Return your analysis as JSON with this exact structure:
{
  "summary": "Brief overall assessment (1-2 sentences)",
  "completeness_score": 0.95,
  "consistency_score": 0.98,
  "issues": [
    {"severity": "high|medium|low", "description": "...", "affected_rows": [1, 5, 10]}
  ],
  "recommendations": ["Search for X to fill gap", "Verify rows Y and Z"],
  "flagged_rows": [
    {"index": 0, "flag": "ok|warning|suspect", "note": "explanation"}
  ]
}

Be specific about row indices (0-based) when flagging issues.
If the data looks complete and consistent, say so with high scores.',
    query, schema_str, nrow(data), n_preview, data_preview, universe_str,
    paste(checks, collapse = ", "), confidence_threshold
  )
}

#' Extract Data and Query from Result Object
#'
#' @noRd
.extract_audit_data <- function(result) {
  if (inherits(result, "asa_enumerate_result")) {
    list(
      data = result$data,
      query = result$query,
      schema = result$schema
    )
  } else if (is.data.frame(result)) {
    list(
      data = result,
      query = NULL,
      schema = NULL
    )
  } else {
    stop("result must be an asa_enumerate_result or data.frame",
         call. = FALSE)
  }
}

#' Annotate Results with Audit Flags
#'
#' @noRd
.annotate_results <- function(data, flagged_rows) {
  # Initialize audit columns
  n <- nrow(data)
  data[["_audit_flag"]] <- rep("ok", n)
  data[["_audit_notes"]] <- rep(NA_character_, n)
  data[["_confidence_adjusted"]] <- rep(NA_real_, n)

  # Apply flags from audit
  if (length(flagged_rows) > 0) {
    for (flag_info in flagged_rows) {
      idx <- flag_info$index
      if (!is.null(idx) && is.numeric(idx)) {
        # Convert 0-based index to 1-based
        row_idx <- idx + 1
        if (row_idx >= 1 && row_idx <= n) {
          data[["_audit_flag"]][row_idx] <- flag_info$flag %||% "warning"
          data[["_audit_notes"]][row_idx] <- flag_info$note %||% NA_character_
          if (!is.null(flag_info$confidence)) {
            data[["_confidence_adjusted"]][row_idx] <- flag_info$confidence
          }
        }
      }
    }
  }

  data
}

#' Validate Audit Inputs
#'
#' @noRd
.validate_audit_inputs <- function(result, query, known_universe, checks,
                                   backend, confidence_threshold, timeout) {

  # Validate result
  if (!inherits(result, "asa_enumerate_result") && !is.data.frame(result)) {
    stop("result must be an asa_enumerate_result object or data.frame",
         call. = FALSE)
  }

  if (is.data.frame(result) && nrow(result) == 0) {
    stop("Cannot audit empty data.frame", call. = FALSE)
  }

  # Validate query
  if (!is.null(query)) {
    .validate_string(query, "query")
  }

  # Validate known_universe
  if (!is.null(known_universe)) {
    if (!is.vector(known_universe)) {
      stop("known_universe must be a vector", call. = FALSE)
    }
  }

  # Validate confidence_threshold
  .validate_positive(confidence_threshold, "confidence_threshold", allow_zero = TRUE)
  if (confidence_threshold > 1) {
    .stop_validation("confidence_threshold", "be between 0 and 1",
                     actual = confidence_threshold,
                     fix = "Provide a value between 0 and 1")
  }

  # Validate timeout
  .validate_positive(timeout, "timeout")

  invisible(TRUE)
}

# NOTE: .extract_json_from_output() has been replaced by the shared
# .parse_json_response() from helpers.R, which is a strict superset
# (handles markdown fences, brace counting, and embedded JSON).

#' Convert Python List to R List
#'
#' @noRd
.py_to_r_list <- function(py_obj) {
  if (is.null(py_obj)) return(list())

  tryCatch({
    if (inherits(py_obj, "python.builtin.list")) {
      reticulate::py_to_r(py_obj)
    } else {
      as.list(py_obj)
    }
  }, error = function(e) {
    list()
  })
}
