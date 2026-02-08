# ============================================================================
# Data Quality-Check System for asa Package
# ============================================================================
#
# This file provides comprehensive input validation with fail-fast behavior
# and actionable error messages. All validators are internal (not exported).
#
# Error Message Format:
#   `{param_name}` must {requirement}.
#     Got: {actual_value}
#     Fix: {actionable_suggestion}
#
# ============================================================================

# ============================================================================
# CORE VALIDATION PRIMITIVES
# ============================================================================

#' Stop with Formatted Validation Error
#'
#' Creates a standardized error message with Got/Fix sections.
#'
#' @param param_name Name of the parameter that failed validation
#' @param requirement What the parameter should be
#' @param actual What was actually received (optional, auto-formatted)
#' @param fix Actionable fix suggestion
#' @keywords internal
.stop_validation <- function(param_name, requirement, actual = NULL, fix = NULL) {
  msg <- sprintf("`%s` must %s.", param_name, requirement)

  if (!is.null(actual)) {
    actual_str <- if (is.character(actual) && length(actual) == 1) {
      if (nchar(actual) == 0) '""' else sprintf('"%s"', actual)
    } else if (is.null(actual)) {
      "NULL"
    } else if (length(actual) > 3) {
      sprintf("<%s of length %d>", class(actual)[1], length(actual))
    } else if (length(actual) > 1) {
      paste0("c(", paste(utils::head(actual, 3), collapse = ", "), ")")
    } else {
      as.character(actual)
    }
    msg <- paste0(msg, sprintf("\n  Got: %s", actual_str))
  }

  if (!is.null(fix)) {
    msg <- paste0(msg, sprintf("\n  Fix: %s", fix))
  }

  stop(msg, call. = FALSE)
}

#' Validate Required Argument Presence
#' @param x Value to check
#' @param param_name Name for error message
#' @keywords internal
.validate_required <- function(x, param_name) {
  if (missing(x) || is.null(x)) {
    .stop_validation(
      param_name,
      "be provided (required argument)",
      actual = if (missing(x)) "<missing>" else "NULL",
      fix = sprintf("Provide a value for %s", param_name)
    )
  }
  invisible(TRUE)
}

#' Validate Non-Empty String
#' @param x Value to check
#' @param param_name Name for error message
#' @param allow_empty Allow empty strings (default: FALSE)
#' @param allow_na Allow NA values (default: FALSE)
#' @keywords internal
.validate_string <- function(x, param_name, allow_empty = FALSE, allow_na = FALSE) {
  if (!is.character(x) || length(x) != 1) {
    .stop_validation(
      param_name,
      "be a single character string",
      actual = x,
      fix = sprintf("Provide a single string value (e.g., %s = \"example\")", param_name)
    )
  }

  if (!allow_na && is.na(x)) {
    .stop_validation(
      param_name,
      "not be NA",
      actual = "NA",
      fix = sprintf("Provide a non-NA string value for %s", param_name)
    )
  }

  if (!allow_empty && !is.na(x) && nchar(trimws(x)) == 0) {
    .stop_validation(
      param_name,
      "not be empty or whitespace-only",
      actual = x,
      fix = sprintf("Provide a non-empty string for %s", param_name)
    )
  }

  invisible(TRUE)
}

#' Validate Positive Number
#' @param x Value to check
#' @param param_name Name for error message
#' @param allow_zero Allow zero values (default: FALSE)
#' @param integer_only Require integer values (default: FALSE)
#' @keywords internal
.validate_positive <- function(x, param_name, allow_zero = FALSE, integer_only = FALSE) {
  if (!is.numeric(x) || length(x) != 1 || is.na(x)) {
    .stop_validation(
      param_name,
      sprintf("be a single %s", if (integer_only) "integer" else "number"),
      actual = x,
      fix = sprintf("Provide a numeric value (e.g., %s = 10)", param_name)
    )
  }

  if (integer_only && x != as.integer(x)) {
    .stop_validation(
      param_name,
      "be an integer (whole number)",
      actual = x,
      fix = sprintf("Use an integer value (e.g., %s = %dL)", param_name, round(x))
    )
  }

  if (allow_zero) {
    if (x < 0) {
      .stop_validation(
        param_name,
        "be non-negative (>= 0)",
        actual = x,
        fix = sprintf("Provide a non-negative value (e.g., %s = 0 or %s = 1)", param_name, param_name)
      )
    }
  } else {
    if (x <= 0) {
      .stop_validation(
        param_name,
        "be positive (> 0)",
        actual = x,
        fix = sprintf("Provide a positive value (e.g., %s = 1)", param_name)
      )
    }
  }

  invisible(TRUE)
}

#' Validate Boolean
#' @param x Value to check
#' @param param_name Name for error message
#' @keywords internal
.validate_logical <- function(x, param_name) {
  if (!is.logical(x) || length(x) != 1 || is.na(x)) {
    .stop_validation(
      param_name,
      "be TRUE or FALSE",
      actual = x,
      fix = sprintf("Use TRUE or FALSE (e.g., %s = TRUE)", param_name)
    )
  }
  invisible(TRUE)
}

#' Validate Choice from Set
#' @param x Value to check
#' @param param_name Name for error message
#' @param choices Valid choices
#' @keywords internal
.validate_choice <- function(x, param_name, choices) {
  if (!is.character(x) || length(x) != 1 || !x %in% choices) {
    .stop_validation(
      param_name,
      sprintf("be one of: %s", paste0('"', choices, '"', collapse = ", ")),
      actual = x,
      fix = sprintf("Choose from: %s", paste(choices, collapse = ", "))
    )
  }
  invisible(TRUE)
}

#' Validate Proxy URL Format
#' @param x Value to check (NULL disables proxy; NA enables auto-detection)
#' @param param_name Name for error message
#' @keywords internal
.validate_proxy_url <- function(x, param_name) {
  if (is.null(x)) {
    return(invisible(TRUE))  # NULL is valid (no proxy)
  }

  if (isTRUE(is.na(x))) {
    return(invisible(TRUE))  # NA is valid (auto proxy)
  }

  .validate_string(x, param_name)

  # Check proxy URL format: socks5(h)://host:port or http(s)://host:port
  # Allow optional basic auth (user:pass@) for corporate proxies.
  if (!grepl("^(socks5h?|https?)://([^/@]+@)?[^:]+:\\d+$", x)) {
    .stop_validation(
      param_name,
      'be a valid proxy URL (format: "socks5h://host:port" or "http://host:port")',
      actual = x,
      fix = 'Use e.g. "socks5h://127.0.0.1:9050" (Tor), "http://proxy:8080", NA for auto, or NULL to disable'
    )
  }

  invisible(TRUE)
}

#' Validate Conda Environment Name
#' @param x Value to check
#' @param param_name Name for error message
#' @keywords internal
.validate_conda_env <- function(x, param_name) {
  .validate_string(x, param_name)

  # Conda env names: start with letter, contain only letters, numbers, underscores, hyphens
  if (!grepl("^[a-zA-Z][a-zA-Z0-9_-]*$", x)) {
    .stop_validation(
      param_name,
      "be a valid conda environment name (start with letter, contain only letters, numbers, underscores, hyphens)",
      actual = x,
      fix = 'Use a name like "asa_env" or "my-agent-env"'
    )
  }

  invisible(TRUE)
}

#' Validate Character Vector (Non-Empty)
#' @param x Value to check
#' @param param_name Name for error message
#' @param min_length Minimum required length (default: 1)
#' @keywords internal
.validate_string_vector <- function(x, param_name, min_length = 1L) {
  if (!is.character(x) || length(x) < min_length) {
    .stop_validation(
      param_name,
      sprintf("be a character vector with at least %d element(s)", min_length),
      actual = x,
      fix = sprintf('Provide one or more strings (e.g., %s = c("a", "b"))', param_name)
    )
  }

  # Check for NA values
  if (any(is.na(x))) {
    .stop_validation(
      param_name,
      "not contain NA values",
      actual = sprintf("<%d NAs found>", sum(is.na(x))),
      fix = "Remove or replace NA values in the vector"
    )
  }

  # Check for empty strings
  if (any(nchar(trimws(x)) == 0)) {
    .stop_validation(
      param_name,
      "not contain empty or whitespace-only strings",
      actual = sprintf("<%d empty strings found>", sum(nchar(trimws(x)) == 0)),
      fix = "Remove or replace empty strings in the vector"
    )
  }

  invisible(TRUE)
}

#' Validate Data Frame with Required Columns
#' @param x Value to check
#' @param param_name Name for error message
#' @param required_cols Required column names (optional)
#' @keywords internal
.validate_dataframe <- function(x, param_name, required_cols = NULL) {
  if (!is.data.frame(x)) {
    .stop_validation(
      param_name,
      "be a data frame",
      actual = class(x)[1],
      fix = "Provide a data.frame object"
    )
  }

  if (!is.null(required_cols)) {
    missing_cols <- setdiff(required_cols, names(x))
    if (length(missing_cols) > 0) {
      .stop_validation(
        param_name,
        sprintf("have required column(s): %s", paste(required_cols, collapse = ", ")),
        actual = sprintf("<missing: %s>", paste(missing_cols, collapse = ", ")),
        fix = sprintf("Add missing column(s): %s", paste(missing_cols, collapse = ", "))
      )
    }
  }

  invisible(TRUE)
}

#' Validate S3 Class
#' @param x Value to check
#' @param param_name Name for error message
#' @param expected_class Expected S3 class name
#' @keywords internal
.validate_s3_class <- function(x, param_name, expected_class) {
  if (!inherits(x, expected_class)) {
    .stop_validation(
      param_name,
      sprintf("be an object of class '%s'", expected_class),
      actual = class(x)[1],
      fix = sprintf("Use an object created by the appropriate constructor (e.g., %s())",
                    gsub("^asa_", "", expected_class))
    )
  }
  invisible(TRUE)
}

#' Validate Range
#' @param x Value to check (must already be validated as numeric)
#' @param param_name Name for error message
#' @param min Minimum allowed value (optional)
#' @param max Maximum allowed value (optional)
#' @keywords internal
.validate_range <- function(x, param_name, min = NULL, max = NULL) {
  if (!is.null(min) && x < min) {
    .stop_validation(
      param_name,
      sprintf("be >= %s", min),
      actual = x,
      fix = sprintf("Use a value of at least %s", min)
    )
  }

  if (!is.null(max) && x > max) {
    .stop_validation(
      param_name,
      sprintf("be <= %s", max),
      actual = x,
      fix = sprintf("Use a value of at most %s", max)
    )
  }

  invisible(TRUE)
}

#' Validate Optional Recursion Limit
#' @param recursion_limit Optional recursion limit value
#' @param param_name Parameter name for error messages
#' @keywords internal
.validate_recursion_limit <- function(recursion_limit, param_name = "recursion_limit") {
  if (is.null(recursion_limit)) {
    return(invisible(TRUE))
  }

  .validate_positive(recursion_limit, param_name, integer_only = TRUE)
  .validate_range(recursion_limit, param_name, min = 4, max = 500)

  invisible(TRUE)
}

#' Validate Logical Consistency Between Parameters
#' @param condition Condition that must be TRUE
#' @param message Error message if condition is FALSE
#' @param fix How to fix the issue
#' @keywords internal
.validate_consistency <- function(condition, message, fix) {
  if (!condition) {
    stop(sprintf("%s\n  Fix: %s", message, fix), call. = FALSE)
  }
  invisible(TRUE)
}

# ============================================================================
# SHARED CROSS-FUNCTION VALIDATORS
# ============================================================================

#' Validate Optional asa_config Argument
#' @param config Value to check (NULL or asa_config)
#' @param param_name Name for error message
#' @keywords internal
.validate_asa_config <- function(config, param_name = "config") {
  if (!is.null(config) && !inherits(config, "asa_config")) {
    stop(sprintf("`%s` must be an asa_config object or NULL", param_name), call. = FALSE)
  }
  invisible(TRUE)
}

#' Validate output_format Argument
#' @param output_format Output format argument
#' @param param_name Name for error message
#' @param fix Hint text for invalid non-character values
#' @keywords internal
.validate_output_format <- function(
    output_format,
    param_name = "output_format",
    fix = 'Use output_format = "json"'
) {
  # output_format: "text", "json", "raw", or character vector of field names
  if (is.character(output_format)) {
    if (length(output_format) == 1) {
      .validate_choice(output_format, param_name, c("text", "json", "raw"))
    }
    return(invisible(TRUE))
  }

  .stop_validation(
    param_name,
    'be "text", "json", "raw", or a character vector of field names',
    actual = output_format,
    fix = fix
  )
}

#' Validate expected_schema Argument
#' @param expected_schema Value to validate
#' @param param_name Name for error messages
#' @param allow_empty_list Whether an empty list is allowed
#' @param invalid_fix Fix text when schema is wrong type
#' @param empty_fix Fix text when schema is an empty list
#' @keywords internal
.validate_expected_schema <- function(
    expected_schema,
    param_name = "expected_schema",
    allow_empty_list = TRUE,
    invalid_fix = "Pass a nested list schema or set expected_schema = NULL",
    empty_fix = "Provide at least one key/element in the schema, or set expected_schema = NULL"
) {
  if (is.null(expected_schema)) {
    return(invisible(TRUE))
  }

  is_py <- FALSE
  try({
    is_py <- reticulate::is_py_object(expected_schema)
  }, silent = TRUE)

  is_list_schema <- is.list(expected_schema) && !is.data.frame(expected_schema)
  if (!is_py && !is_list_schema) {
    .stop_validation(
      param_name,
      "be a nested list (or Python dict/list) describing the required JSON shape",
      actual = expected_schema,
      fix = invalid_fix
    )
  }
  if (!allow_empty_list && is_list_schema && length(expected_schema) == 0) {
    .stop_validation(
      param_name,
      "not be an empty list",
      actual = expected_schema,
      fix = empty_fix
    )
  }

  invisible(TRUE)
}

# ============================================================================
# FUNCTION-LEVEL VALIDATORS
# ============================================================================

#' Validate initialize_agent() Parameters
#' @keywords internal
.validate_initialize_agent <- function(backend, model, conda_env, proxy,
                                        use_browser, use_memory_folding,
                                        memory_threshold, memory_keep_recent,
                                        rate_limit, timeout, verbose,
                                        tor = NULL,
                                        recursion_limit = NULL) {
  # backend is validated by match.arg() in the calling function
  .validate_string(model, "model")
  .validate_conda_env(conda_env, "conda_env")
  .validate_proxy_url(proxy, "proxy")  # NULL is allowed
  .validate_logical(use_browser, "use_browser")
  .validate_logical(use_memory_folding, "use_memory_folding")
  .validate_positive(memory_threshold, "memory_threshold", integer_only = TRUE)
  .validate_positive(memory_keep_recent, "memory_keep_recent", integer_only = TRUE)
  .validate_positive(rate_limit, "rate_limit")
  .validate_positive(timeout, "timeout", integer_only = TRUE)
  .validate_recursion_limit(recursion_limit, "recursion_limit")
  .validate_logical(verbose, "verbose")
  .validate_tor_options(tor, "tor")

  # memory_keep_recent counts exchanges, so there is no direct consistency check
  # against message-based memory_threshold.

  invisible(TRUE)
}

#' Validate run_task() Parameters
#' @keywords internal
.validate_run_task <- function(prompt, output_format, agent, verbose, thread_id = NULL,
                               expected_schema = NULL,
                               recursion_limit = NULL,
                               field_status = NULL,
                               budget_state = NULL,
                               search_budget_limit = NULL,
                               unknown_after_searches = NULL,
                               finalize_on_all_fields_resolved = NULL,
                               allow_read_webpages = NULL,
                               webpage_relevance_mode = NULL,
                               webpage_embedding_provider = NULL,
                               webpage_embedding_model = NULL) {
  .validate_string(prompt, "prompt")

  .validate_output_format(
    output_format,
    fix = 'Use output_format = "json" or output_format = c("field1", "field2")'
  )

  # agent: NULL or asa_agent
  if (!is.null(agent)) {
    .validate_s3_class(agent, "agent", "asa_agent")
  }

  .validate_logical(verbose, "verbose")

  # thread_id: NULL or single string (opt-in stable sessions)
  if (!is.null(thread_id)) {
    .validate_string(thread_id, "thread_id")
  }

  .validate_expected_schema(
    expected_schema,
    allow_empty_list = FALSE,
    invalid_fix = "Pass a nested list schema (e.g., list(status=\"complete|partial\", items=list(list(name=\"string\")))) or set expected_schema = NULL"
  )
  .validate_expected_schema(
    field_status,
    param_name = "field_status",
    allow_empty_list = TRUE,
    invalid_fix = "Pass a named list or Python dict for field_status, or set field_status = NULL",
    empty_fix = "Pass field_status = NULL or include at least one status entry"
  )
  .validate_expected_schema(
    budget_state,
    param_name = "budget_state",
    allow_empty_list = TRUE,
    invalid_fix = "Pass a named list or Python dict for budget_state, or set budget_state = NULL",
    empty_fix = "Pass budget_state = NULL or include at least one budget key"
  )

  .validate_recursion_limit(recursion_limit, "recursion_limit")
  if (!is.null(search_budget_limit)) {
    .validate_positive(search_budget_limit, "search_budget_limit", integer_only = TRUE)
  }
  if (!is.null(unknown_after_searches)) {
    .validate_positive(unknown_after_searches, "unknown_after_searches", integer_only = TRUE)
  }
  if (!is.null(finalize_on_all_fields_resolved)) {
    .validate_logical(finalize_on_all_fields_resolved, "finalize_on_all_fields_resolved")
  }

  # allow_read_webpages: NULL or TRUE/FALSE
  if (!is.null(allow_read_webpages)) {
    .validate_logical(allow_read_webpages, "allow_read_webpages")
  }

  # Webpage reader tuning (optional)
  if (!is.null(webpage_relevance_mode)) {
    .validate_choice(
      webpage_relevance_mode, "webpage_relevance_mode",
      c("auto", "lexical", "embeddings")
    )
  }
  if (!is.null(webpage_embedding_provider)) {
    .validate_choice(
      webpage_embedding_provider, "webpage_embedding_provider",
      c("auto", "openai", "sentence_transformers")
    )
  }
  if (!is.null(webpage_embedding_model)) {
    .validate_string(webpage_embedding_model, "webpage_embedding_model")
  }

  invisible(TRUE)
}

#' Validate run_task_batch() Parameters
#' @keywords internal
.validate_run_task_batch <- function(prompts, output_format, agent,
                                      parallel, workers, progress) {
  # prompts: character vector or data frame with 'prompt' column
  if (is.data.frame(prompts)) {
    .validate_dataframe(prompts, "prompts", required_cols = "prompt")
    # Also validate the prompt column contents
    if (any(is.na(prompts$prompt))) {
      .stop_validation(
        "prompts$prompt",
        "not contain NA values",
        actual = sprintf("<%d NAs found>", sum(is.na(prompts$prompt))),
        fix = "Remove rows with NA prompts or replace them"
      )
    }
    if (any(nchar(trimws(as.character(prompts$prompt))) == 0)) {
      .stop_validation(
        "prompts$prompt",
        "not contain empty or whitespace-only values",
        actual = sprintf("<%d empty prompts found>", sum(nchar(trimws(as.character(prompts$prompt))) == 0)),
        fix = "Remove rows with empty prompts or provide valid text"
      )
    }
  } else {
    .validate_string_vector(prompts, "prompts")
  }

  .validate_output_format(output_format)

  if (!is.null(agent)) {
    .validate_s3_class(agent, "agent", "asa_agent")
  }

  .validate_logical(parallel, "parallel")
  .validate_positive(workers, "workers", integer_only = TRUE)
  .validate_logical(progress, "progress")

  invisible(TRUE)
}

#' Validate run_agent() Parameters
#' @keywords internal
.validate_run_agent <- function(prompt, agent, recursion_limit, verbose,
                                thread_id = NULL, expected_schema = NULL,
                                field_status = NULL, budget_state = NULL,
                                search_budget_limit = NULL,
                                unknown_after_searches = NULL,
                                finalize_on_all_fields_resolved = NULL) {
  .validate_string(prompt, "prompt")

  if (!is.null(agent)) {
    .validate_s3_class(agent, "agent", "asa_agent")
  }

  .validate_recursion_limit(recursion_limit, "recursion_limit")

  .validate_expected_schema(expected_schema)
  .validate_expected_schema(field_status, param_name = "field_status", allow_empty_list = TRUE)
  .validate_expected_schema(budget_state, param_name = "budget_state", allow_empty_list = TRUE)

  if (!is.null(search_budget_limit)) {
    .validate_positive(search_budget_limit, "search_budget_limit", integer_only = TRUE)
  }
  if (!is.null(unknown_after_searches)) {
    .validate_positive(unknown_after_searches, "unknown_after_searches", integer_only = TRUE)
  }
  if (!is.null(finalize_on_all_fields_resolved)) {
    .validate_logical(finalize_on_all_fields_resolved, "finalize_on_all_fields_resolved")
  }

  .validate_logical(verbose, "verbose")

  if (!is.null(thread_id)) {
    .validate_string(thread_id, "thread_id")
  }

  invisible(TRUE)
}

#' Validate build_prompt() Parameters
#' @keywords internal
.validate_build_prompt <- function(template) {
  .validate_string(template, "template")
  invisible(TRUE)
}

#' Validate configure_search() Parameters
#' @keywords internal
.validate_configure_search <- function(max_results, timeout, max_retries,
                                        retry_delay, backoff_multiplier,
                                        captcha_backoff_base, page_load_wait,
                                        inter_search_delay, conda_env) {
  # All parameters are optional (NULL allowed), but if provided must be valid
  if (!is.null(max_results)) {
    .validate_positive(max_results, "max_results", integer_only = TRUE)
    .validate_range(max_results, "max_results", min = 1, max = 100)
  }

  if (!is.null(timeout)) {
    .validate_positive(timeout, "timeout")
    .validate_range(timeout, "timeout", min = 1, max = 300)
  }

  if (!is.null(max_retries)) {
    .validate_positive(max_retries, "max_retries", allow_zero = TRUE, integer_only = TRUE)
    .validate_range(max_retries, "max_retries", max = 10)
  }

  if (!is.null(retry_delay)) {
    .validate_positive(retry_delay, "retry_delay", allow_zero = TRUE)
  }

  if (!is.null(backoff_multiplier)) {
    .validate_positive(backoff_multiplier, "backoff_multiplier")
    .validate_range(backoff_multiplier, "backoff_multiplier", min = 1.0, max = 5.0)
  }

  if (!is.null(captcha_backoff_base)) {
    .validate_positive(captcha_backoff_base, "captcha_backoff_base")
  }

  if (!is.null(page_load_wait)) {
    .validate_positive(page_load_wait, "page_load_wait", allow_zero = TRUE)
  }

  if (!is.null(inter_search_delay)) {
    .validate_positive(inter_search_delay, "inter_search_delay", allow_zero = TRUE)
  }

  .validate_conda_env(conda_env, "conda_env")

  invisible(TRUE)
}

#' Validate tor_options() Parameters
#' @keywords internal
.validate_tor_options <- function(tor, param_name = "tor") {
  if (is.null(tor)) return(invisible(TRUE))

  if (!inherits(tor, "asa_tor")) {
    stop(sprintf("`%s` must be created with tor_options() or be a list", param_name),
         call. = FALSE)
  }

  .validate_string(tor$registry_path, sprintf("%s$registry_path", param_name))
  .validate_logical(tor$dirty_tor_exists, sprintf("%s$dirty_tor_exists", param_name))
  .validate_positive(tor$bad_ttl, sprintf("%s$bad_ttl", param_name), allow_zero = FALSE)
  .validate_positive(tor$good_ttl, sprintf("%s$good_ttl", param_name), allow_zero = FALSE)
  .validate_positive(tor$overuse_threshold, sprintf("%s$overuse_threshold", param_name),
                     integer_only = TRUE)
  .validate_positive(tor$overuse_decay, sprintf("%s$overuse_decay", param_name))
  .validate_positive(tor$max_rotation_attempts, sprintf("%s$max_rotation_attempts", param_name),
                     integer_only = TRUE, allow_zero = FALSE)
  .validate_positive(tor$ip_cache_ttl, sprintf("%s$ip_cache_ttl", param_name))

  invisible(TRUE)
}

#' Validate configure_tor_registry() Parameters
#' @keywords internal
.validate_configure_tor_registry <- function(registry_path, enable,
                                             bad_ttl, good_ttl,
                                             overuse_threshold, overuse_decay,
                                             max_rotation_attempts, ip_cache_ttl,
                                             conda_env) {
  if (!is.null(registry_path)) {
    .validate_string(registry_path, "registry_path")
  }
  .validate_logical(enable, "enable")
  .validate_positive(bad_ttl, "bad_ttl", allow_zero = FALSE)
  .validate_positive(good_ttl, "good_ttl", allow_zero = FALSE)
  .validate_positive(overuse_threshold, "overuse_threshold", integer_only = TRUE)
  .validate_positive(overuse_decay, "overuse_decay")
  .validate_positive(max_rotation_attempts, "max_rotation_attempts", integer_only = TRUE)
  .validate_positive(ip_cache_ttl, "ip_cache_ttl")
  .validate_conda_env(conda_env, "conda_env")
  invisible(TRUE)
}

#' Validate build_backend() Parameters
#' @keywords internal
.validate_build_backend <- function(conda_env, conda, python_version, force = FALSE) {
  .validate_conda_env(conda_env, "conda_env")

  if (conda != "auto") {
    .validate_string(conda, "conda")
  }

  .validate_string(python_version, "python_version")
  # Validate python version format (e.g., "3.12", "3.13")
  if (!grepl("^\\d+\\.\\d+$", python_version)) {
    .stop_validation(
      "python_version",
      'be in format "X.Y" (e.g., "3.12", "3.13")',
      actual = python_version,
      fix = 'Use format like python_version = "3.12"'
    )
  }

  .validate_logical(force, "force")

  invisible(TRUE)
}

#' Validate process_outputs() Parameters
#' @keywords internal
.validate_process_outputs <- function(df, parallel, workers) {
  .validate_dataframe(df, "df", required_cols = "raw_output")
  .validate_logical(parallel, "parallel")
  .validate_positive(workers, "workers", integer_only = TRUE)

  invisible(TRUE)
}

# ============================================================================
# S3 CONSTRUCTOR VALIDATORS
# ============================================================================

#' Validate S3 Constructor: asa_agent
#' @keywords internal
.validate_asa_agent <- function(python_agent, backend, model, config) {
  # python_agent can be NULL for testing, but normally should be a Python object
  .validate_string(backend, "backend")
  .validate_string(model, "model")

  if (!is.list(config)) {
    .stop_validation(
      "config",
      "be a list",
      actual = class(config)[1],
      fix = "Provide a configuration list"
    )
  }

  invisible(TRUE)
}

#' Validate S3 Constructor: asa_response
#' @keywords internal
.validate_asa_response <- function(message, status_code, raw_response, trace,
                                    elapsed_time, prompt,
                                    fold_stats = list()) {
  # message can be NA
  if (!is.na(message)) {
    .validate_string(message, "message", allow_empty = TRUE)
  }

  .validate_positive(status_code, "status_code", integer_only = TRUE)

  # trace can be empty string
  if (!is.character(trace) || length(trace) != 1) {
    .stop_validation("trace", "be a character string", actual = trace,
                     fix = "Provide a character string for trace")
  }

  .validate_positive(elapsed_time, "elapsed_time", allow_zero = TRUE)
  .validate_string(prompt, "prompt")

  if (!is.list(fold_stats)) {
    .stop_validation("fold_stats", "be a list", actual = class(fold_stats)[1],
                     fix = "Provide a list for fold_stats")
  }

  # fold_stats$fold_count, if present, must be integer or character (error message)
  fc <- fold_stats$fold_count
  if (!is.null(fc) && !is.integer(fc) && !is.numeric(fc) && !is.character(fc)) {
    .stop_validation("fold_stats$fold_count", "be an integer, numeric, or character string",
                     actual = class(fc)[1],
                     fix = "Provide an integer fold count or a character error message")
  }

  invisible(TRUE)
}

#' Validate S3 Constructor: asa_result
#' @keywords internal
.validate_asa_result <- function(prompt, message, parsed, raw_output,
                                  elapsed_time, status) {
  .validate_string(prompt, "prompt")

  # message can be NA
  if (!is.na(message)) {
    .validate_string(message, "message", allow_empty = TRUE)
  }

  # parsed can be NULL or a list
  if (!is.null(parsed) && !is.list(parsed)) {
    .stop_validation("parsed", "be NULL or a list", actual = class(parsed)[1],
                     fix = "Provide NULL or a list for parsed output")
  }

  # raw_output can be empty
  if (!is.character(raw_output) || length(raw_output) != 1) {
    .stop_validation("raw_output", "be a character string", actual = raw_output,
                     fix = "Provide a character string for raw_output")
  }

  .validate_positive(elapsed_time, "elapsed_time", allow_zero = TRUE)
  .validate_choice(status, "status", c("success", "error"))

  invisible(TRUE)
}


# ============================================================================
# TEMPORAL FILTERING VALIDATORS
# ============================================================================

#' Validate Temporal Filtering Parameters
#'
#' Validates and normalizes temporal filtering parameters used by run_task()
#' and asa_enumerate(). Returns a normalized list or NULL if input is NULL.
#'
#' @param temporal Named list with temporal filtering options, or NULL
#' @param param_name Name for error messages (default: "temporal")
#' @return Normalized temporal list or NULL
#' @keywords internal
.validate_temporal <- function(temporal, param_name = "temporal") {
  if (is.null(temporal)) {
    return(NULL)
  }

  if (!is.list(temporal)) {
    .stop_validation(
      param_name,
      "be a list or NULL",
      actual = class(temporal)[1],
      fix = "Use temporal = list(time_filter = 'y') or temporal = NULL"
    )
  }

  # Validate time_filter if present
  if (!is.null(temporal$time_filter)) {
    valid_filters <- c("d", "w", "m", "y")
    if (!temporal$time_filter %in% valid_filters) {
      .stop_validation(
        paste0(param_name, "$time_filter"),
        sprintf("be one of: %s", paste0('"', valid_filters, '"', collapse = ", ")),
        actual = temporal$time_filter,
        fix = 'Use "d" (day), "w" (week), "m" (month), or "y" (year)'
      )
    }
  }

  # Validate after date if present
  if (!is.null(temporal$after)) {
    if (!is.character(temporal$after) || length(temporal$after) != 1) {
      .stop_validation(
        paste0(param_name, "$after"),
        "be a single character string in ISO 8601 format",
        actual = temporal$after,
        fix = 'Use format "YYYY-MM-DD" (e.g., "2020-01-01")'
      )
    }
    tryCatch(
      as.Date(temporal$after),
      error = function(e) {
        .stop_validation(
          paste0(param_name, "$after"),
          "be a valid ISO 8601 date",
          actual = temporal$after,
          fix = 'Use format "YYYY-MM-DD" (e.g., "2020-01-01")'
        )
      }
    )
  }

  # Validate before date if present
  if (!is.null(temporal$before)) {
    if (!is.character(temporal$before) || length(temporal$before) != 1) {
      .stop_validation(
        paste0(param_name, "$before"),
        "be a single character string in ISO 8601 format",
        actual = temporal$before,
        fix = 'Use format "YYYY-MM-DD" (e.g., "2024-01-01")'
      )
    }
    tryCatch(
      as.Date(temporal$before),
      error = function(e) {
        .stop_validation(
          paste0(param_name, "$before"),
          "be a valid ISO 8601 date",
          actual = temporal$before,
          fix = 'Use format "YYYY-MM-DD" (e.g., "2024-01-01")'
        )
      }
    )
  }

  # Validate date ordering if both present
  if (!is.null(temporal$after) && !is.null(temporal$before)) {
    after_date <- as.Date(temporal$after)
    before_date <- as.Date(temporal$before)
    if (after_date >= before_date) {
      .stop_validation(
        param_name,
        "have after < before when both are specified",
        actual = sprintf("after=%s, before=%s", temporal$after, temporal$before),
        fix = "Ensure the 'after' date is earlier than the 'before' date"
      )
    }
  }

  # Check for contradictions between time_filter and date range
  time_filter <- temporal$time_filter
  if (!is.null(time_filter) && !is.na(time_filter) && time_filter != "none") {
    if (!is.null(temporal$after)) {
      # Calculate implied start date from time_filter
      today <- Sys.Date()
      filter_start <- switch(time_filter,
        "d" = today - 1,
        "w" = today - 7,
        "m" = today - 30,
        "y" = today - 365,
        today  # fallback
      )
      after_date <- as.Date(temporal$after)

      if (after_date < filter_start) {
        filter_desc <- switch(time_filter,
          "d" = "past day",
          "w" = "past week",
          "m" = "past month",
          "y" = "past year"
        )
        warning(
          "Temporal conflict: time_filter='", time_filter, "' limits DuckDuckGo to ",
          filter_desc, ", but after='", temporal$after, "' suggests broader range.\n",
          "DuckDuckGo will only return results from the ", filter_desc,
          ". Consider removing time_filter for the full date range.",
          call. = FALSE
        )
      }
    }
  }

  # Validate strictness if present
  if (!is.null(temporal$strictness)) {
    valid_strictness <- c("best_effort", "strict")
    if (!temporal$strictness %in% valid_strictness) {
      .stop_validation(
        paste0(param_name, "$strictness"),
        sprintf("be one of: %s", paste0('"', valid_strictness, '"', collapse = ", ")),
        actual = temporal$strictness,
        fix = 'Use "best_effort" (default) or "strict"'
      )
    }
  }

  # Validate use_wayback if present
  if (!is.null(temporal$use_wayback)) {
    if (!is.logical(temporal$use_wayback) || length(temporal$use_wayback) != 1) {
      .stop_validation(
        paste0(param_name, "$use_wayback"),
        "be TRUE or FALSE",
        actual = temporal$use_wayback,
        fix = "Use TRUE or FALSE"
      )
    }
  }

  invisible(temporal)
}


# ============================================================================
# API KEY VALIDATORS
# ============================================================================

#' Validate API Key for Backend
#'
#' Checks that the required API key environment variable is set for the
#' specified backend. Throws an informative error if missing.
#'
#' @param backend LLM backend name
#' @return Invisibly returns TRUE if valid
#' @keywords internal
.validate_api_key <- function(backend) {
  # Map backends to their API key environment variables
  key_vars <- list(
    openai = "OPENAI_API_KEY",
    groq = "GROQ_API_KEY",
    xai = "XAI_API_KEY",
    gemini = c("GOOGLE_API_KEY", "GEMINI_API_KEY"),
    openrouter = "OPENROUTER_API_KEY"
  )

  # Exo is local, no API key needed
  if (backend == "exo") {
    return(invisible(TRUE))
  }

  env_vars <- key_vars[[backend]]
  if (is.null(env_vars)) {
    stop("Unknown backend: ", backend, call. = FALSE)
  }

  # Gemini can also be used via Vertex AI with ADC credentials. If the user has
  # explicitly enabled Vertex mode, allow missing API key (best-effort).
  use_vertex <- identical(backend, "gemini") &&
    tolower(Sys.getenv("GOOGLE_GENAI_USE_VERTEXAI", unset = "")) %in% c("true", "1", "yes", "t")

  api_key <- ""
  env_var <- NULL
  for (v in env_vars) {
    val <- Sys.getenv(v, unset = "")
    if (nzchar(val)) {
      api_key <- val
      env_var <- v
      break
    }
  }

  if (api_key == "" || is.na(api_key)) {
    if (use_vertex) {
      warning(
        "Gemini backend: No GOOGLE_API_KEY/GEMINI_API_KEY found. ",
        "Assuming Vertex AI credentials are configured (GOOGLE_GENAI_USE_VERTEXAI=true). ",
        "If not, set GOOGLE_API_KEY or GEMINI_API_KEY.",
        call. = FALSE
      )
      return(invisible(TRUE))
    }

    .stop_validation(
      "api_key",
      sprintf(
        "be set in environment variable %s for backend '%s'",
        paste(env_vars, collapse = " or "),
        backend
      ),
      actual = "<not set>",
      fix = sprintf(
        "Set the API key via: Sys.setenv(%s = \"your-api-key\")\n         Or in your .Renviron file: %s=your-api-key",
        env_vars[[1]], env_vars[[1]]
      )
    )
  }

  # Warn if key looks suspicious (too short or placeholder)
  if (nchar(api_key) < 10) {
    warning(sprintf("%s appears to be too short. Is it a valid API key?", env_var),
            call. = FALSE)
  }

  if (grepl("your[_-]?api[_-]?key|placeholder|test|example", api_key, ignore.case = TRUE)) {
    warning(sprintf("%s appears to be a placeholder value. Is it a real API key?", env_var),
            call. = FALSE)
  }

  invisible(TRUE)
}
