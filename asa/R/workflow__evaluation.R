#' Evaluate an ASA Agent Against a Benchmark
#'
#' Runs a benchmark suite against an ASA agent using deterministic local scoring.
#'
#' @param benchmark An \code{asa_benchmark} object or built-in benchmark name
#' @param agent Optional initialized \code{asa_agent}
#' @param config Optional \code{asa_config} used to initialize an agent when
#'   \code{agent} is not supplied
#' @param runs Number of repeated benchmark runs (default: 1)
#' @param fail_fast If TRUE, stop evaluation on first execution failure
#' @param keep_outputs If TRUE, retain raw \code{asa_result} objects in the
#'   returned evaluation result
#' @param verbose Print progress messages
#'
#' @return An object of class \code{asa_evaluation_result}
#'
#' @export
evaluate_agent <- function(benchmark,
                           agent = NULL,
                           config = NULL,
                           runs = 1L,
                           fail_fast = FALSE,
                           keep_outputs = FALSE,
                           verbose = TRUE) {
  benchmark <- .coerce_benchmark_input(benchmark)
  .validate_asa_config(config)
  .validate_positive(runs, "runs", integer_only = TRUE)
  .validate_logical(fail_fast, "fail_fast")
  .validate_logical(keep_outputs, "keep_outputs")
  .validate_logical(verbose, "verbose")

  boot <- .evaluation_boot_agent(agent = agent, config = config, verbose = verbose)
  eval_agent <- boot$agent
  eval_config <- boot$config

  tasks <- benchmark$tasks %||% list()
  started_at <- Sys.time()
  task_rows <- list()
  raw_outputs <- if (isTRUE(keep_outputs)) list() else NULL
  row_index <- 0L

  for (run_idx in seq_len(as.integer(runs))) {
    if (verbose) {
      message("Evaluation run ", run_idx, "/", runs, "...")
    }
    for (task in tasks) {
      row_index <- row_index + 1L
      row <- .evaluate_single_benchmark_task(
        task = task,
        benchmark = benchmark,
        run_index = run_idx,
        agent = eval_agent,
        keep_output = keep_outputs,
        verbose = verbose
      )
      task_rows[[row_index]] <- row$record
      if (isTRUE(keep_outputs)) {
        raw_outputs[[paste0("run_", run_idx, "__", task$id)]] <- row$output
      }
      if (isTRUE(fail_fast) && !isTRUE(row$record$pass[[1]]) && identical(as.character(row$record$status[[1]]), "failed")) {
        stop("Benchmark evaluation aborted because fail_fast = TRUE and a task failed.", call. = FALSE)
      }
    }
  }

  task_results <- .evaluation_bind_record_rows(task_rows)
  run_summaries <- .evaluation_run_summaries(task_results)
  metrics <- .evaluation_aggregate_metrics(task_results, benchmark, runs)
  agent_spec <- list(
    backend = eval_config$backend %||% NA_character_,
    model = eval_config$model %||% NA_character_,
    conda_env = eval_config$conda_env %||% NA_character_
  )

  asa_evaluation_result(
    benchmark = benchmark,
    task_results = task_results,
    run_summaries = run_summaries,
    metrics = metrics,
    agent_spec = agent_spec,
    config_snapshot = eval_config,
    outputs = raw_outputs,
    elapsed_time = as.numeric(difftime(Sys.time(), started_at, units = "mins"))
  )
}

.coerce_benchmark_input <- function(benchmark) {
  if (inherits(benchmark, "asa_benchmark")) {
    return(benchmark)
  }
  if (is.character(benchmark) && length(benchmark) == 1L) {
    return(load_benchmark(benchmark))
  }
  .stop_validation(
    "benchmark",
    "be an asa_benchmark object or built-in benchmark name",
    actual = class(benchmark)[1],
    fix = "Use create_benchmark(...), benchmark_from_yaml(...), or load_benchmark(\"FactRetrieval2024\")"
  )
}

.evaluation_boot_agent <- function(agent = NULL, config = NULL, verbose = TRUE) {
  if (!is.null(agent) && !inherits(agent, "asa_agent")) {
    .stop_validation(
      "agent",
      "be an asa_agent object",
      actual = class(agent)[1],
      fix = "Create an agent with initialize_agent() before evaluation"
    )
  }

  base_config <- if (!is.null(config)) {
    config
  } else if (!is.null(agent) && inherits(agent, "asa_agent")) {
    .agent_config_from_agent(agent)
  } else if (.is_initialized()) {
    .agent_config_from_agent(get_agent())
  } else {
    asa_config()
  }
  base_config$om_cross_thread_memory <- FALSE

  .ensure_research_agent(
    agent = NULL,
    config = base_config,
    backend = base_config$backend %||% NULL,
    model = base_config$model %||% NULL,
    conda_env = base_config$conda_env %||% NULL,
    verbose = verbose
  )
}

.evaluate_single_benchmark_task <- function(task,
                                            benchmark,
                                            run_index,
                                            agent,
                                            keep_output = FALSE,
                                            verbose = FALSE) {
  thread_id <- .evaluation_thread_id(benchmark$name, benchmark$version, run_index, task$id)
  invoke_args <- c(
    list(
      prompt = task$prompt,
      output_format = task$output_format,
      agent = agent,
      thread_id = thread_id,
      verbose = FALSE
    ),
    task$run_args %||% list()
  )

  result <- tryCatch(
    do.call(run_task, invoke_args),
    error = function(e) e
  )

  scored <- .score_benchmark_result(task = task, result = result)
  record <- .evaluation_record_row(
    task = task,
    run_index = run_index,
    thread_id = thread_id,
    result = result,
    scored = scored
  )

  if (verbose) {
    message(
      "  [run ", run_index, "] ",
      task$id, ": ",
      if (isTRUE(record$pass[[1]])) "pass" else "fail",
      " (score=", sprintf("%.2f", as.numeric(record$score[[1]] %||% 0)), ")"
    )
  }

  list(record = record, output = if (inherits(result, "error")) NULL else if (isTRUE(keep_output)) result else NULL)
}

.evaluation_thread_id <- function(benchmark_name, benchmark_version, run_index, task_id) {
  paste0(
    "eval_",
    substr(
      digest::digest(paste(benchmark_name, benchmark_version, run_index, task_id, sep = "::")),
      1,
      16
    )
  )
}

.score_benchmark_result <- function(task, result) {
  if (inherits(result, "error")) {
    return(list(
      score = 0,
      pass = FALSE,
      details = list(reason = conditionMessage(result), matcher = task$match),
      actual_summary = paste0("ERROR: ", conditionMessage(result))
    ))
  }

  if (identical(task$match, "fields")) {
    actual_fields <- .evaluation_actual_fields(result$parsed %||% NULL)
    field_names <- names(task$expected) %||% character(0)
    detail_rows <- lapply(field_names, function(field_name) {
      matcher <- task$field_matchers[[field_name]] %||% "exact"
      expected_value <- task$expected[[field_name]]
      actual_value <- actual_fields[[field_name]] %||% NULL
      field_score <- .evaluation_match_value(
        actual = actual_value,
        expected = expected_value,
        matcher = matcher
      )
      list(
        field = field_name,
        matcher = matcher,
        expected = expected_value,
        actual = actual_value,
        pass = isTRUE(field_score$pass),
        score = field_score$score
      )
    })
    passes <- vapply(detail_rows, function(x) isTRUE(x$pass), logical(1))
    scores <- vapply(detail_rows, function(x) as.numeric(x$score %||% 0), numeric(1))
    actual_summary <- .evaluation_value_summary(actual_fields)
    return(list(
      score = if (length(scores) == 0L) 0 else mean(scores, na.rm = TRUE),
      pass = length(passes) > 0L && all(passes),
      details = detail_rows,
      actual_summary = actual_summary
    ))
  }

  actual_text <- as.character(result$message %||% "")
  matched <- .evaluation_match_value(
    actual = actual_text,
    expected = task$expected,
    matcher = task$match
  )
  list(
    score = matched$score,
    pass = matched$pass,
    details = list(matcher = task$match, expected = task$expected, actual = actual_text),
    actual_summary = truncate_string(actual_text %||% "", 160)
  )
}

.evaluation_actual_fields <- function(parsed) {
  if (is.null(parsed)) {
    return(list())
  }
  if (is.data.frame(parsed)) {
    if (nrow(parsed) == 0L) {
      return(list())
    }
    return(as.list(parsed[1, , drop = FALSE]))
  }
  if (is.list(parsed)) {
    return(parsed)
  }
  list(value = parsed)
}

.evaluation_match_value <- function(actual, expected, matcher) {
  if (is.list(matcher)) {
    matcher_type <- matcher$type %||% matcher$match %||% NULL
    if (identical(matcher_type, "numeric_tolerance")) {
      actual_num <- suppressWarnings(as.numeric(actual))
      expected_num <- suppressWarnings(as.numeric(expected))
      tol <- suppressWarnings(as.numeric(matcher$tol %||% matcher$tolerance %||% NA_real_))
      pass <- is.finite(actual_num) && is.finite(expected_num) && is.finite(tol) &&
        abs(actual_num - expected_num) <= tol
      return(list(score = if (isTRUE(pass)) 1 else 0, pass = isTRUE(pass)))
    }
    matcher <- matcher_type
  }

  matcher <- as.character(matcher %||% "exact")
  actual_chr <- .evaluation_normalize_text(actual)
  expected_chr <- .evaluation_normalize_text(expected)

  pass <- switch(
    matcher,
    exact = identical(actual_chr, expected_chr),
    contains = nzchar(expected_chr) && grepl(expected_chr, actual_chr, fixed = TRUE),
    regex = nzchar(expected_chr) && grepl(expected_chr, as.character(actual %||% ""), perl = TRUE, ignore.case = TRUE),
    FALSE
  )

  list(score = if (isTRUE(pass)) 1 else 0, pass = isTRUE(pass))
}

.evaluation_normalize_text <- function(x) {
  if (is.null(x)) {
    return("")
  }
  text <- if (length(x) > 1L) paste(as.character(x), collapse = " ") else as.character(x)
  tolower(clean_whitespace(text))
}

.evaluation_value_summary <- function(x, max_chars = 160L) {
  if (is.null(x)) {
    return("")
  }
  txt <- if (is.list(x) || is.data.frame(x)) {
    .try_or(
      jsonlite::toJSON(x, auto_unbox = TRUE, null = "null", digits = NA),
      paste(capture.output(str(x, max.level = 1)), collapse = " ")
    )
  } else {
    as.character(x)
  }
  truncate_string(txt, max_length = max_chars)
}

.evaluation_record_row <- function(task,
                                   run_index,
                                   thread_id,
                                   result,
                                   scored) {
  status_value <- if (inherits(result, "error")) "failed" else as.character(result$status %||% "unknown")
  error_value <- if (inherits(result, "error")) {
    conditionMessage(result)
  } else if (!identical(status_value, "success")) {
    as.character(result$message %||% NA_character_)
  } else {
    NA_character_
  }
  elapsed_minutes <- if (inherits(result, "error")) NA_real_ else as.numeric(result$elapsed_time %||% NA_real_)
  token_stats <- if (inherits(result, "error")) list() else result$token_stats %||% list()
  input_tokens <- .as_scalar_int(token_stats$input_tokens %||% if (!inherits(result, "error")) result$execution$input_tokens else NA_integer_)
  output_tokens <- .as_scalar_int(token_stats$output_tokens %||% if (!inherits(result, "error")) result$execution$output_tokens else NA_integer_)
  tokens_used <- .as_scalar_int(token_stats$tokens_used %||% if (!inherits(result, "error")) result$execution$tokens_used else NA_integer_)

  data.frame(
    run = as.integer(run_index),
    task_id = as.character(task$id),
    category = as.character(task$category %||% "uncategorized"),
    match = as.character(task$match),
    output_format = paste(task$output_format, collapse = "|"),
    status = status_value,
    thread_id = as.character(thread_id),
    error = error_value %||% NA_character_,
    score = as.numeric(scored$score %||% 0),
    pass = isTRUE(scored$pass),
    elapsed_minutes = elapsed_minutes,
    elapsed_seconds = if (is.finite(elapsed_minutes)) elapsed_minutes * 60 else NA_real_,
    input_tokens = input_tokens,
    output_tokens = output_tokens,
    tokens_used = tokens_used,
    expected_summary = .evaluation_value_summary(task$expected),
    actual_summary = as.character(scored$actual_summary %||% ""),
    score_details = I(list(scored$details %||% list())),
    stringsAsFactors = FALSE
  )
}

.evaluation_bind_record_rows <- function(rows) {
  if (length(rows) == 0L) {
    return(data.frame(
      run = integer(0),
      task_id = character(0),
      category = character(0),
      match = character(0),
      output_format = character(0),
      status = character(0),
      thread_id = character(0),
      error = character(0),
      score = numeric(0),
      pass = logical(0),
      elapsed_minutes = numeric(0),
      elapsed_seconds = numeric(0),
      input_tokens = integer(0),
      output_tokens = integer(0),
      tokens_used = integer(0),
      expected_summary = character(0),
      actual_summary = character(0),
      score_details = I(list()),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

.evaluation_run_summaries <- function(task_results) {
  if (!is.data.frame(task_results) || nrow(task_results) == 0L) {
    return(data.frame(
      run = integer(0),
      task_count = integer(0),
      pass_rate = numeric(0),
      mean_task_score = numeric(0),
      error_rate = numeric(0),
      elapsed_seconds_total = numeric(0),
      tokens_used_total = integer(0),
      stringsAsFactors = FALSE
    ))
  }

  runs <- sort(unique(task_results$run))
  rows <- lapply(runs, function(run_idx) {
    chunk <- task_results[task_results$run == run_idx, , drop = FALSE]
    data.frame(
      run = as.integer(run_idx),
      task_count = nrow(chunk),
      pass_rate = mean(chunk$pass %||% FALSE, na.rm = TRUE),
      mean_task_score = mean(chunk$score %||% 0, na.rm = TRUE),
      error_rate = mean(chunk$status != "success", na.rm = TRUE),
      elapsed_seconds_total = sum(chunk$elapsed_seconds, na.rm = TRUE),
      tokens_used_total = sum(chunk$tokens_used, na.rm = TRUE),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

.evaluation_aggregate_metrics <- function(task_results, benchmark, runs) {
  if (!is.data.frame(task_results) || nrow(task_results) == 0L) {
    return(list(
      benchmark_name = benchmark$name,
      benchmark_version = benchmark$version,
      runs = as.integer(runs),
      task_count = length(benchmark$tasks %||% list()),
      evaluations = 0L,
      pass_rate = NA_real_,
      mean_task_score = NA_real_,
      error_rate = NA_real_,
      latency_seconds_mean = NA_real_,
      latency_seconds_p50 = NA_real_,
      latency_seconds_p95 = NA_real_,
      input_tokens_total = 0L,
      output_tokens_total = 0L,
      tokens_used_total = 0L,
      tokens_used_mean = NA_real_,
      pass_rate_by_category = data.frame(
        category = character(0),
        pass_rate = numeric(0),
        mean_task_score = numeric(0),
        n = integer(0),
        stringsAsFactors = FALSE
      ),
      status_counts = list()
    ))
  }

  latency <- task_results$elapsed_seconds
  latency <- latency[is.finite(latency)]
  status_table <- table(task_results$status)
  categories <- sort(unique(task_results$category))
  category_rows <- lapply(categories, function(category_name) {
    chunk <- task_results[task_results$category == category_name, , drop = FALSE]
    data.frame(
      category = as.character(category_name),
      pass_rate = mean(chunk$pass, na.rm = TRUE),
      mean_task_score = mean(chunk$score, na.rm = TRUE),
      n = nrow(chunk),
      stringsAsFactors = FALSE
    )
  })

  list(
    benchmark_name = benchmark$name,
    benchmark_version = benchmark$version,
    runs = as.integer(runs),
    task_count = length(benchmark$tasks %||% list()),
    evaluations = nrow(task_results),
    pass_rate = mean(task_results$pass, na.rm = TRUE),
    mean_task_score = mean(task_results$score, na.rm = TRUE),
    error_rate = mean(task_results$status != "success", na.rm = TRUE),
    latency_seconds_mean = if (length(latency) > 0L) mean(latency) else NA_real_,
    latency_seconds_p50 = if (length(latency) > 0L) as.numeric(stats::quantile(latency, probs = 0.50, names = FALSE, na.rm = TRUE)) else NA_real_,
    latency_seconds_p95 = if (length(latency) > 0L) as.numeric(stats::quantile(latency, probs = 0.95, names = FALSE, na.rm = TRUE)) else NA_real_,
    input_tokens_total = sum(task_results$input_tokens, na.rm = TRUE),
    output_tokens_total = sum(task_results$output_tokens, na.rm = TRUE),
    tokens_used_total = sum(task_results$tokens_used, na.rm = TRUE),
    tokens_used_mean = mean(task_results$tokens_used, na.rm = TRUE),
    pass_rate_by_category = do.call(rbind, category_rows),
    status_counts = as.list(stats::setNames(as.integer(status_table), names(status_table)))
  )
}
