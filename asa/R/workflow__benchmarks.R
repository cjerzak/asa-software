#' Create an ASA Benchmark Object
#'
#' Creates a structured benchmark specification for agent evaluation.
#'
#' @param name Benchmark name
#' @param tasks List of benchmark task definitions
#' @param description Optional benchmark description
#' @param version Benchmark version string (default: "1.0")
#' @param citation Optional citation string
#' @param metadata Optional named list of additional benchmark metadata
#'
#' @return An object of class \code{asa_benchmark}
#'
#' @export
create_benchmark <- function(name,
                             tasks,
                             description = NULL,
                             version = "1.0",
                             citation = NULL,
                             metadata = list()) {
  .validate_string(name, "name")
  if (!is.null(description)) {
    .validate_string(description, "description", allow_empty = TRUE)
  }
  .validate_string(version, "version")
  if (!is.null(citation)) {
    .validate_string(citation, "citation", allow_empty = TRUE)
  }
  if (!is.list(tasks) || length(tasks) == 0L) {
    .stop_validation(
      "tasks",
      "be a non-empty list of task definitions",
      actual = class(tasks)[1],
      fix = "Provide one or more task lists with at least id, prompt, expected, and match"
    )
  }
  if (!is.list(metadata)) {
    .stop_validation(
      "metadata",
      "be a named list",
      actual = class(metadata)[1],
      fix = "Provide metadata = list(key = value)"
    )
  }

  normalized_tasks <- lapply(seq_along(tasks), function(i) {
    .normalize_benchmark_task(tasks[[i]], index = i)
  })
  task_ids <- vapply(normalized_tasks, function(task) task$id, character(1))
  duplicated_ids <- unique(task_ids[duplicated(task_ids)])
  if (length(duplicated_ids) > 0L) {
    .stop_validation(
      "tasks",
      "contain unique task ids",
      actual = paste(duplicated_ids, collapse = ", "),
      fix = "Rename duplicated task ids so every task has a stable unique identifier"
    )
  }

  structure(
    list(
      name = name,
      description = description %||% "",
      version = version,
      citation = citation %||% "",
      metadata = metadata,
      tasks = normalized_tasks,
      created_at = Sys.time()
    ),
    class = "asa_benchmark"
  )
}

#' Create an ASA Benchmark from a Data Frame
#'
#' Converts a tabular benchmark definition into an \code{asa_benchmark}.
#'
#' @param df data.frame containing benchmark rows
#' @param prompt_col Column containing prompts
#' @param expected_col Column containing expected values
#' @param id_col Optional column containing task ids
#' @param category_col Optional column containing task categories
#' @param output_format_col Optional column containing per-task output formats
#' @param match_col Optional column containing per-task match modes
#' @param run_args_col Optional column containing per-task run arguments
#' @param field_matchers_col Optional column containing per-task field matcher specs
#' @param name Benchmark name (default: "CustomBenchmark")
#' @param description Optional benchmark description
#' @param version Benchmark version string (default: "1.0")
#' @param citation Optional citation string
#' @param metadata Optional benchmark metadata
#' @param output_format Default output format when \code{output_format_col} is NULL
#' @param match Default matcher when \code{match_col} is NULL
#'
#' @return An object of class \code{asa_benchmark}
#'
#' @export
benchmark_from_df <- function(df,
                              prompt_col,
                              expected_col,
                              id_col = NULL,
                              category_col = NULL,
                              output_format_col = NULL,
                              match_col = NULL,
                              run_args_col = NULL,
                              field_matchers_col = NULL,
                              name = "CustomBenchmark",
                              description = NULL,
                              version = "1.0",
                              citation = NULL,
                              metadata = list(),
                              output_format = "text",
                              match = "exact") {
  if (!is.data.frame(df) || nrow(df) == 0L) {
    .stop_validation(
      "df",
      "be a non-empty data.frame",
      actual = class(df)[1],
      fix = "Provide a data frame with one row per benchmark task"
    )
  }
  .validate_string(prompt_col, "prompt_col")
  .validate_string(expected_col, "expected_col")
  if (!is.null(id_col)) {
    .validate_string(id_col, "id_col")
  }
  if (!is.null(category_col)) {
    .validate_string(category_col, "category_col")
  }
  if (!is.null(output_format_col)) {
    .validate_string(output_format_col, "output_format_col")
  }
  if (!is.null(match_col)) {
    .validate_string(match_col, "match_col")
  }
  if (!is.null(run_args_col)) {
    .validate_string(run_args_col, "run_args_col")
  }
  if (!is.null(field_matchers_col)) {
    .validate_string(field_matchers_col, "field_matchers_col")
  }

  required_cols <- c(prompt_col, expected_col)
  optional_cols <- c(id_col, category_col, output_format_col, match_col, run_args_col, field_matchers_col)
  missing_cols <- setdiff(c(required_cols, optional_cols[!is.null(optional_cols)]), names(df))
  if (length(missing_cols) > 0L) {
    .stop_validation(
      "df",
      "contain all requested benchmark columns",
      actual = paste(missing_cols, collapse = ", "),
      fix = "Check column names passed to benchmark_from_df()"
    )
  }

  default_output_format <- .normalize_benchmark_output_format(output_format, "output_format")
  default_match <- .normalize_task_matcher(match, "match")
  tasks <- lapply(seq_len(nrow(df)), function(i) {
    row_id <- if (!is.null(id_col)) df[[id_col]][[i]] else paste0("task_", i)
    row_category <- if (!is.null(category_col)) df[[category_col]][[i]] else NULL
    row_output_format <- if (!is.null(output_format_col)) df[[output_format_col]][[i]] else default_output_format
    row_match <- if (!is.null(match_col)) df[[match_col]][[i]] else default_match
    row_expected <- .evaluation_parse_listish_value(df[[expected_col]][[i]])
    row_run_args <- if (!is.null(run_args_col)) .evaluation_parse_listish_value(df[[run_args_col]][[i]]) else NULL
    row_field_matchers <- if (!is.null(field_matchers_col)) .evaluation_parse_listish_value(df[[field_matchers_col]][[i]]) else NULL
    list(
      id = as.character(row_id),
      prompt = as.character(df[[prompt_col]][[i]]),
      expected = row_expected,
      output_format = row_output_format,
      match = row_match,
      category = if (is.null(row_category) || is.na(row_category)) NULL else as.character(row_category),
      run_args = row_run_args,
      field_matchers = row_field_matchers
    )
  })

  create_benchmark(
    name = name,
    tasks = tasks,
    description = description,
    version = version,
    citation = citation,
    metadata = metadata
  )
}

#' Load an ASA Benchmark from YAML
#'
#' Reads a benchmark definition from a YAML file.
#'
#' @param path Path to YAML benchmark file
#'
#' @return An object of class \code{asa_benchmark}
#'
#' @export
benchmark_from_yaml <- function(path) {
  .validate_string(path, "path")
  if (!file.exists(path)) {
    .stop_validation(
      "path",
      "point to an existing YAML file",
      actual = path,
      fix = "Pass a valid .yml or .yaml path"
    )
  }
  if (!grepl("\\.ya?ml$", path, ignore.case = TRUE)) {
    .stop_validation(
      "path",
      "end in .yml or .yaml",
      actual = path,
      fix = "Use a YAML file extension for benchmark definitions"
    )
  }

  parsed <- yaml::read_yaml(path)
  if (!is.list(parsed)) {
    .stop_validation(
      "path",
      "contain a YAML object at the top level",
      actual = class(parsed)[1],
      fix = "Ensure the YAML file starts with named keys like name, version, and tasks"
    )
  }

  reserved_keys <- c("name", "description", "version", "citation", "tasks", "metadata")
  metadata <- parsed$metadata %||% list()
  if (!is.list(metadata)) {
    metadata <- list(metadata = metadata)
  }
  extra_keys <- setdiff(names(parsed), reserved_keys)
  if (length(extra_keys) > 0L) {
    for (key in extra_keys) {
      metadata[[key]] <- parsed[[key]]
    }
  }

  create_benchmark(
    name = parsed$name,
    tasks = parsed$tasks,
    description = parsed$description %||% NULL,
    version = parsed$version %||% "1.0",
    citation = parsed$citation %||% NULL,
    metadata = metadata
  )
}

#' List Built-In ASA Benchmarks
#'
#' Lists benchmark definitions bundled with the package.
#'
#' @return data.frame of benchmark metadata
#'
#' @export
list_benchmarks <- function() {
  benchmark_dir <- .benchmark_builtin_dir()
  if (!nzchar(benchmark_dir) || !dir.exists(benchmark_dir)) {
    return(data.frame(
      name = character(0),
      version = character(0),
      description = character(0),
      citation = character(0),
      n_tasks = integer(0),
      file = character(0),
      stringsAsFactors = FALSE
    ))
  }

  files <- list.files(benchmark_dir, pattern = "\\.ya?ml$", full.names = TRUE)
  files <- sort(files)
  rows <- lapply(files, function(file) {
    benchmark <- benchmark_from_yaml(file)
    data.frame(
      name = benchmark$name,
      version = benchmark$version,
      description = benchmark$description %||% "",
      citation = benchmark$citation %||% "",
      n_tasks = length(benchmark$tasks %||% list()),
      file = basename(file),
      stringsAsFactors = FALSE
    )
  })
  if (length(rows) == 0L) {
    return(data.frame(
      name = character(0),
      version = character(0),
      description = character(0),
      citation = character(0),
      n_tasks = integer(0),
      file = character(0),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

#' Load a Built-In ASA Benchmark
#'
#' Loads a benchmark bundled under \code{inst/benchmarks/}.
#'
#' @param name Benchmark name
#'
#' @return An object of class \code{asa_benchmark}
#'
#' @export
load_benchmark <- function(name) {
  .validate_string(name, "name")
  benchmark_dir <- .benchmark_builtin_dir()
  if (!nzchar(benchmark_dir) || !dir.exists(benchmark_dir)) {
    stop("No built-in benchmark directory found.", call. = FALSE)
  }

  files <- list.files(benchmark_dir, pattern = "\\.ya?ml$", full.names = TRUE)
  if (length(files) == 0L) {
    stop("No built-in benchmarks are available.", call. = FALSE)
  }

  normalized_name <- tolower(trimws(name))
  for (file in files) {
    benchmark <- benchmark_from_yaml(file)
    file_name <- tolower(tools::file_path_sans_ext(basename(file)))
    if (tolower(benchmark$name) == normalized_name || file_name == normalized_name) {
      return(benchmark)
    }
  }

  stop(
    "Unknown benchmark: ", name, ". Available benchmarks: ",
    paste(list_benchmarks()$name, collapse = ", "),
    call. = FALSE
  )
}

.benchmark_builtin_dir <- function() {
  candidates <- c(
    system.file("benchmarks", package = "asa"),
    file.path(getwd(), "asa", "inst", "benchmarks"),
    file.path(getwd(), "inst", "benchmarks")
  )
  candidates <- unique(normalizePath(candidates, winslash = "/", mustWork = FALSE))
  for (path in candidates) {
    if (nzchar(path) && dir.exists(path)) {
      return(path)
    }
  }
  ""
}

.normalize_benchmark_task <- function(task, index = NULL) {
  if (!is.list(task)) {
    .stop_validation(
      sprintf("tasks[[%d]]", index %||% 1L),
      "be a named list",
      actual = class(task)[1],
      fix = "Use list(id = ..., prompt = ..., expected = ..., match = ...)"
    )
  }

  task_name <- sprintf("tasks[[%d]]", index %||% 1L)
  id_value <- task$id %||% if (!is.null(index)) paste0("task_", index) else NULL
  .validate_string(id_value, paste0(task_name, "$id"))
  .validate_string(task$prompt %||% "", paste0(task_name, "$prompt"))

  match_value <- .normalize_task_matcher(task$match %||% "exact", paste0(task_name, "$match"))
  output_format_value <- .normalize_benchmark_output_format(
    task$output_format %||% "text",
    paste0(task_name, "$output_format")
  )
  run_args_value <- .normalize_benchmark_run_args(task$run_args %||% list(), paste0(task_name, "$run_args"))
  field_matchers_value <- .normalize_field_matchers(
    task$field_matchers %||% list(),
    paste0(task_name, "$field_matchers")
  )
  expected_value <- .normalize_benchmark_expected(
    task$expected,
    match = match_value,
    param_name = paste0(task_name, "$expected")
  )

  if (identical(match_value, "fields")) {
    if (identical(output_format_value, "text") || identical(output_format_value, "raw")) {
      .stop_validation(
        paste0(task_name, "$output_format"),
        'not be "text" or "raw" when match = "fields"',
        actual = paste(output_format_value, collapse = ", "),
        fix = 'Use "json" or a character vector of field names for structured tasks'
      )
    }
    if (length(names(expected_value)) == 0L) {
      .stop_validation(
        paste0(task_name, "$expected"),
        "be a named list when match = \"fields\"",
        actual = class(expected_value)[1],
        fix = "Provide expected = list(field = value, ...)"
      )
    }
  }

  category_value <- task$category %||% NULL
  if (!is.null(category_value)) {
    .validate_string(as.character(category_value), paste0(task_name, "$category"), allow_empty = TRUE)
  }

  metadata_value <- task$metadata %||% list()
  if (!is.list(metadata_value)) {
    metadata_value <- list(value = metadata_value)
  }

  list(
    id = id_value,
    prompt = task$prompt,
    expected = expected_value,
    output_format = output_format_value,
    match = match_value,
    category = if (is.null(category_value) || is.na(category_value) || !nzchar(trimws(as.character(category_value)))) {
      NULL
    } else {
      as.character(category_value)
    },
    run_args = run_args_value,
    field_matchers = field_matchers_value,
    metadata = metadata_value
  )
}

.normalize_benchmark_expected <- function(expected, match, param_name) {
  parsed <- .evaluation_parse_listish_value(expected)
  if (identical(match, "fields")) {
    if (is.data.frame(parsed)) {
      if (nrow(parsed) == 0L) {
        return(list())
      }
      parsed <- as.list(parsed[1, , drop = FALSE])
    }
    if (!is.list(parsed)) {
      .stop_validation(
        param_name,
        "be a named list or JSON object for field-matching tasks",
        actual = class(parsed)[1],
        fix = "Provide expected = list(field = value, ...) or a JSON object string"
      )
    }
    parsed_names <- names(parsed) %||% character(0)
    if (length(parsed_names) == 0L || any(!nzchar(parsed_names))) {
      .stop_validation(
        param_name,
        "contain named expected fields",
        actual = paste(parsed_names, collapse = ", "),
        fix = "Ensure each expected field has a non-empty name"
      )
    }
    return(parsed)
  }

  if (is.list(parsed) || length(parsed) != 1L) {
    .stop_validation(
      param_name,
      "be a scalar expected value for text-matching tasks",
      actual = class(parsed)[1],
      fix = "Use a single string, number, or logical expected value"
    )
  }

  parsed
}

.normalize_task_matcher <- function(match, param_name) {
  choices <- c("exact", "contains", "regex", "fields")
  .validate_choice(as.character(match), param_name, choices)
  as.character(match)
}

.normalize_benchmark_output_format <- function(output_format, param_name) {
  if (is.list(output_format) && !is.null(names(output_format)) && length(output_format) == 1L &&
      names(output_format)[[1]] == "value") {
    output_format <- output_format[[1]]
  }
  if (is.list(output_format) && is.null(names(output_format))) {
    output_format <- unlist(output_format, use.names = FALSE)
  }
  if (!is.character(output_format) || length(output_format) == 0L || any(is.na(output_format))) {
    .stop_validation(
      param_name,
      'be a character value like "text", "json", or a character vector of fields',
      actual = class(output_format)[1],
      fix = 'Use output_format = "text", output_format = "json", or output_format = c("field_a", "field_b")'
    )
  }
  if (length(output_format) == 1L && !output_format %in% c("text", "json")) {
    .validate_string(output_format, param_name)
  }
  if (length(output_format) > 1L) {
    .validate_string_vector(output_format, param_name)
  }
  if (identical(output_format, "raw")) {
    .stop_validation(
      param_name,
      'not be "raw" for benchmark tasks',
      actual = output_format,
      fix = 'Use "text", "json", or a vector of field names'
    )
  }
  output_format
}

.normalize_benchmark_run_args <- function(run_args, param_name) {
  parsed <- .evaluation_parse_listish_value(run_args)
  if (is.null(parsed) || (is.list(parsed) && length(parsed) == 0L)) {
    return(list())
  }
  if (!is.list(parsed)) {
    .stop_validation(
      param_name,
      "be a named list",
      actual = class(parsed)[1],
      fix = "Use list(temporal = ..., expected_schema = ..., allow_read_webpages = TRUE)"
    )
  }
  allowed_names <- c(
    "temporal",
    "expected_fields",
    "expected_schema",
    "allow_read_webpages",
    "use_plan_mode"
  )
  arg_names <- names(parsed) %||% character(0)
  if (length(arg_names) == 0L || any(!nzchar(arg_names))) {
    .stop_validation(
      param_name,
      "contain only named entries",
      actual = paste(arg_names, collapse = ", "),
      fix = paste("Use only named entries:", paste(allowed_names, collapse = ", "))
    )
  }
  invalid_names <- setdiff(arg_names, allowed_names)
  if (length(invalid_names) > 0L) {
    .stop_validation(
      param_name,
      sprintf("only use supported keys (%s)", paste(allowed_names, collapse = ", ")),
      actual = paste(invalid_names, collapse = ", "),
      fix = "Remove unsupported keys from task-level run_args"
    )
  }
  parsed
}

.normalize_field_matchers <- function(field_matchers, param_name) {
  parsed <- .evaluation_parse_listish_value(field_matchers)
  if (is.null(parsed) || (is.list(parsed) && length(parsed) == 0L)) {
    return(list())
  }
  if (!is.list(parsed)) {
    .stop_validation(
      param_name,
      "be a named list",
      actual = class(parsed)[1],
      fix = "Use list(field_a = \"exact\", field_b = list(type = \"numeric_tolerance\", tol = 0.1))"
    )
  }
  matcher_names <- names(parsed) %||% character(0)
  if (length(matcher_names) == 0L || any(!nzchar(matcher_names))) {
    .stop_validation(
      param_name,
      "contain named field matcher entries",
      actual = paste(matcher_names, collapse = ", "),
      fix = "Provide names matching the expected field names"
    )
  }
  out <- list()
  for (name in matcher_names) {
    value <- parsed[[name]]
    if (is.character(value) && length(value) == 1L) {
      out[[name]] <- .normalize_task_matcher(value, paste0(param_name, "$", name))
      next
    }
    if (is.list(value)) {
      matcher_type <- value$type %||% value$match %||% NULL
      .validate_string(as.character(matcher_type %||% ""), paste0(param_name, "$", name, "$type"))
      matcher_type <- as.character(matcher_type)
      if (!identical(matcher_type, "numeric_tolerance")) {
        .stop_validation(
          paste0(param_name, "$", name, "$type"),
          'be "numeric_tolerance" when using a matcher object',
          actual = matcher_type,
          fix = 'Use "numeric_tolerance" or a scalar matcher like "exact"'
        )
      }
      tol <- suppressWarnings(as.numeric(value$tol %||% value$tolerance %||% NA_real_))
      if (!is.finite(tol) || tol < 0) {
        .stop_validation(
          paste0(param_name, "$", name, "$tol"),
          "be a non-negative number",
          actual = value$tol %||% value$tolerance %||% NA,
          fix = "Provide a tolerance like tol = 0.1"
        )
      }
      out[[name]] <- list(type = matcher_type, tol = tol)
      next
    }
    .stop_validation(
      paste0(param_name, "$", name),
      'be a scalar matcher name or list(type = "numeric_tolerance", tol = ...)',
      actual = class(value)[1],
      fix = 'Use "exact", "contains", "regex", or a numeric tolerance spec'
    )
  }
  out
}

.evaluation_parse_listish_value <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (is.data.frame(value)) {
    return(value)
  }
  if (is.list(value)) {
    return(value)
  }
  if (length(value) == 1L && is.character(value) && !is.na(value)) {
    txt <- trimws(value)
    if (!nzchar(txt)) {
      return(NULL)
    }
    if ((startsWith(txt, "{") && endsWith(txt, "}")) || (startsWith(txt, "[") && endsWith(txt, "]"))) {
      parsed <- safe_json_parse(txt)
      if (!is.null(parsed)) {
        return(parsed)
      }
    }
  }
  value
}
