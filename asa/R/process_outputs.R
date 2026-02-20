#' Parse Structured Trace JSON (asa_trace_v1)
#' @keywords internal
.parse_trace_json_messages <- function(text) {
  if (!is.character(text) || length(text) != 1 || is.na(text) || !nzchar(text)) {
    return(NULL)
  }
  txt <- trimws(text)
  if (!startsWith(txt, "{") || !grepl("\"format\"\\s*:\\s*\"asa_trace_v1\"", txt)) {
    return(NULL)
  }
  parsed <- .try_or(jsonlite::fromJSON(txt, simplifyVector = FALSE))
  if (is.null(parsed) || !identical(parsed$format, "asa_trace_v1")) {
    return(NULL)
  }
  if (!is.list(parsed$messages)) {
    return(NULL)
  }
  parsed$messages
}

#' Clip trace/action text to an intuitive preview
#' @keywords internal
.clip_action_text <- function(text, max_chars = 88L) {
  max_chars <- as.integer(max_chars)
  if (is.na(max_chars) || max_chars < 8L) {
    max_chars <- 8L
  }

  if (is.null(text)) {
    return("")
  }

  if (!is.character(text)) {
    text <- .try_or(jsonlite::toJSON(text, auto_unbox = TRUE, null = "null"), "")
  }

  if (length(text) == 0) {
    return("")
  }

  txt <- text[[1]]
  if (is.na(txt) || !nzchar(txt)) {
    return("")
  }

  txt <- trimws(gsub("\\s+", " ", txt))
  if (!nzchar(txt)) {
    return("")
  }

  if (nchar(txt) <= max_chars) {
    return(txt)
  }

  # For URLs/identifiers, preserve both prefix and suffix so investigators
  # can still disambiguate records like IDs and query tails.
  if (grepl("(https?://|www\\.|\\?|&|\\bid_[a-z_]+\\b|[[:alnum:]_%-]+=)", txt, ignore.case = TRUE)) {
    keep_chars <- max_chars - 3L
    head_chars <- max(8L, floor(keep_chars * 0.65))
    tail_chars <- max(5L, keep_chars - head_chars)
    if ((head_chars + tail_chars + 3L) < nchar(txt)) {
      return(paste0(
        substr(txt, 1L, head_chars),
        "...",
        substr(txt, nchar(txt) - tail_chars + 1L, nchar(txt))
      ))
    }
  }

  paste0(substr(txt, 1L, max_chars - 3L), "...")
}

#' Normalize tool_calls payload into an R list of call records
#' @keywords internal
.normalize_tool_calls <- function(tool_calls) {
  if (is.null(tool_calls) || length(tool_calls) == 0) {
    return(list())
  }

  normalized <- tool_calls
  if (is.data.frame(normalized)) {
    normalized <- split(normalized, seq_len(nrow(normalized)))
  }

  if (!is.list(normalized)) {
    return(list())
  }

  out <- list()
  for (tc in normalized) {
    tc <- .try_or(reticulate::py_to_r(tc), tc)
    if (is.list(tc)) {
      out[[length(out) + 1L]] <- tc
    }
  }
  out
}

#' Normalize token usage payload into scalar integer fields
#' @keywords internal
.normalize_token_usage <- function(token_usage) {
  if (is.null(token_usage)) {
    return(NULL)
  }
  token_usage <- .try_or(reticulate::py_to_r(token_usage), token_usage)
  if (!is.list(token_usage)) {
    return(NULL)
  }

  get_first <- function(x, default = NA_integer_) {
    if (is.null(x)) return(default)
    if (length(x) == 0L) return(default)
    .as_scalar_int(x[[1]] %||% x)
  }

  input_tokens <- get_first(
    token_usage$input_tokens %||%
      token_usage$prompt_tokens %||%
      token_usage$input
  )
  output_tokens <- get_first(
    token_usage$output_tokens %||%
      token_usage$completion_tokens %||%
      token_usage$output
  )
  total_tokens <- get_first(
    token_usage$total_tokens %||%
      token_usage$total
  )

  if (is.na(total_tokens)) {
    if (!is.na(input_tokens) || !is.na(output_tokens)) {
      total_tokens <- sum(c(input_tokens, output_tokens), na.rm = TRUE)
    } else {
      return(NULL)
    }
  }
  if (is.na(input_tokens)) input_tokens <- 0L
  if (is.na(output_tokens)) output_tokens <- 0L

  list(
    input_tokens = input_tokens,
    output_tokens = output_tokens,
    total_tokens = total_tokens
  )
}

#' Normalize tool-quality events emitted by runtime diagnostics
#' @keywords internal
.normalize_tool_quality_events <- function(tool_quality_events = list()) {
  events <- .try_or(reticulate::py_to_r(tool_quality_events), tool_quality_events)
  if (is.null(events)) {
    return(list())
  }
  if (is.data.frame(events)) {
    events <- split(events, seq_len(nrow(events)))
  }
  if (!is.list(events) || length(events) == 0L) {
    return(list())
  }

  as_bool <- function(x, default = FALSE) {
    if (is.null(x) || length(x) == 0L) return(default)
    val <- .try_or(as.logical(x[[1]] %||% x), NA)
    if (is.na(val)) default else isTRUE(val)
  }

  out <- list()
  for (event in events) {
    event <- .try_or(reticulate::py_to_r(event), event)
    if (!is.list(event)) next

    msg_index <- .as_scalar_int(event$message_index_in_round)
    if (is.na(msg_index) || msg_index < 1L) {
      msg_index <- NA_integer_
    }

    tool_name <- as.character(event$tool_name %||% "unknown")
    if (length(tool_name) == 0L || is.na(tool_name[[1]]) || !nzchar(tool_name[[1]])) {
      tool_name <- "unknown"
    } else {
      tool_name <- tool_name[[1]]
    }

    quality_version <- as.character(event$quality_version %||% "v1")
    if (length(quality_version) == 0L || is.na(quality_version[[1]]) || !nzchar(quality_version[[1]])) {
      quality_version <- "v1"
    } else {
      quality_version <- quality_version[[1]]
    }

    out[[length(out) + 1L]] <- list(
      message_index_in_round = msg_index,
      tool_name = tool_name,
      is_empty = as_bool(event$is_empty),
      is_off_target = as_bool(event$is_off_target),
      quality_version = quality_version
    )
  }

  out
}

#' Extract message-level token usage if present
#' @keywords internal
.extract_message_token_usage <- function(message) {
  if (!is.list(message)) {
    return(NULL)
  }

  candidates <- list(
    message$usage_metadata,
    message$token_usage,
    message$response_metadata$token_usage %||% NULL,
    message$additional_kwargs$token_usage %||% NULL
  )

  for (cand in candidates) {
    usage <- .normalize_token_usage(cand)
    if (!is.null(usage)) {
      return(usage)
    }
  }
  NULL
}

#' Render one tool call as a compact action snippet
#' @keywords internal
.summarize_tool_call <- function(tool_call, max_chars = 88L) {
  if (!is.list(tool_call)) {
    return("?()")
  }

  tool_name <- as.character(tool_call$name %||% tool_call$tool %||% "?")
  if (!length(tool_name) || !nzchar(tool_name[[1]])) {
    tool_name <- "?"
  } else {
    tool_name <- tool_name[[1]]
  }

  args <- tool_call$args %||% list()
  args <- .try_or(reticulate::py_to_r(args), args)

  preview_bits <- character(0)
  if (is.list(args) && length(args) > 0) {
    key_priority <- c("query", "url", "finding", "step", "status", "step_id", "category")
    keys <- intersect(key_priority, names(args))
    keys <- keys[seq_len(min(length(keys), 2L))]

    for (key in keys) {
      val <- args[[key]]
      val <- if (is.list(val)) {
        .try_or(jsonlite::toJSON(val, auto_unbox = TRUE, null = "null"), "")
      } else {
        .try_or(as.character(val[[1]]), "")
      }
      if (!is.null(val) && nzchar(val)) {
        preview_bits <- c(
          preview_bits,
          paste0(key, "=", .clip_action_text(val, max_chars = max(20L, max_chars %/% 2L)))
        )
      }
    }

    if (length(preview_bits) == 0) {
      arg_names <- names(args)
      if (!is.null(arg_names) && length(arg_names) > 0) {
        shown <- arg_names[seq_len(min(length(arg_names), 3L))]
        suffix <- if (length(arg_names) > 3L) ", ..." else ""
        preview_bits <- paste0("args{", paste(shown, collapse = ", "), suffix, "}")
      }
    }
  } else if (is.character(args) && length(args) > 0 && nzchar(args[[1]])) {
    preview_bits <- .clip_action_text(args[[1]], max_chars = max(20L, max_chars %/% 2L))
  }

  if (length(preview_bits) == 0 || !nzchar(paste0(preview_bits, collapse = ""))) {
    return(paste0(tool_name, "()"))
  }

  paste0(
    tool_name,
    "(",
    .clip_action_text(paste(preview_bits, collapse = ", "), max_chars = max_chars),
    ")"
  )
}

#' Parse high-level action events from legacy raw trace text
#' @keywords internal
.parse_trace_raw_events <- function(raw_trace, max_preview_chars = 88L) {
  if (!is.character(raw_trace) || length(raw_trace) != 1 || is.na(raw_trace) || !nzchar(raw_trace)) {
    return(list())
  }

  blocks <- strsplit(raw_trace, "\\n\\s*\\n", perl = TRUE)[[1]]
  if (length(blocks) == 0) {
    return(list())
  }

  events <- list()
  for (block in blocks) {
    lines <- strsplit(block, "\n", fixed = TRUE)[[1]]
    if (length(lines) == 0) next

    header <- trimws(lines[[1]])
    if (!grepl("^\\[.*\\]$", header)) next

    header_inner <- sub("^\\[", "", sub("\\]$", "", header))
    header_parts <- strsplit(header_inner, ":", fixed = TRUE)[[1]]
    msg_type <- tolower(trimws(header_parts[[1]] %||% "message"))
    msg_name <- if (length(header_parts) > 1L) {
      trimws(paste(header_parts[-1], collapse = ":"))
    } else {
      ""
    }

    actor <- if (msg_type %in% c("human", "humanmessage", "user")) {
      "Human"
    } else if (msg_type %in% c("ai", "aimessage", "assistant")) {
      "AI"
    } else if (msg_type %in% c("tool", "toolmessage", "function")) {
      paste0("Tool ", if (nzchar(msg_name)) msg_name else "?")
    } else {
      paste0("Message ", msg_type)
    }

    summary <- if (grepl("^Tool\\s", actor)) "Returned result" else "Message"
    content <- if (length(lines) > 1L) paste(lines[-1], collapse = " ") else ""
    events[[length(events) + 1L]] <- list(
      type = msg_type,
      actor = actor,
      summary = summary,
      preview = .clip_action_text(content, max_chars = max_preview_chars)
    )
  }

  events
}

#' Convert structured trace messages into compact action events
#' @keywords internal
.trace_messages_to_action_events <- function(messages, max_preview_chars = 88L) {
  events <- list()

  add_event <- function(type, actor, summary, preview = "", token_usage = NULL) {
    token_usage <- .normalize_token_usage(token_usage)
    events[[length(events) + 1L]] <<- list(
      type = type,
      actor = actor,
      summary = summary,
      preview = .clip_action_text(preview, max_chars = max_preview_chars),
      input_tokens = .as_scalar_int(token_usage$input_tokens),
      output_tokens = .as_scalar_int(token_usage$output_tokens),
      total_tokens = .as_scalar_int(token_usage$total_tokens)
    )
  }

  if (!is.list(messages)) {
    return(events)
  }

  for (m in messages) {
    if (!is.list(m)) next

    m_type <- tolower(as.character(m$message_type %||% ""))
    m_name <- as.character(m$name %||% "")
    if (length(m_name) == 0L || is.na(m_name[[1]])) {
      m_name <- ""
    } else {
      m_name <- m_name[[1]]
    }
    m_content <- as.character(m$content %||% "")
    if (length(m_content) == 0L || is.na(m_content[[1]])) {
      m_content <- ""
    } else {
      m_content <- m_content[[1]]
    }

    if (m_type %in% c("humanmessage", "human", "user")) {
      add_event("human", "Human", "Submitted task prompt", m_content)
      next
    }

    if (m_type %in% c("aimessage", "ai", "assistant")) {
      usage <- .extract_message_token_usage(m)
      tool_calls <- .normalize_tool_calls(m$tool_calls)
      if (length(tool_calls) > 0L) {
        tool_names <- unique(vapply(tool_calls, function(tc) {
          nm <- as.character(tc$name %||% tc$tool %||% "?")
          if (length(nm) > 0L && nzchar(nm[[1]])) nm[[1]] else "?"
        }, character(1)))

        summary <- sprintf(
          "Requested %d tool call%s (%s)",
          length(tool_calls),
          if (length(tool_calls) == 1L) "" else "s",
          paste(tool_names, collapse = ", ")
        )
        call_preview <- paste(
          vapply(tool_calls, .summarize_tool_call, character(1), max_chars = max_preview_chars),
          collapse = " | "
        )
        add_event("ai_tool_calls", "AI", summary, call_preview, token_usage = usage)
      }

      if (nzchar(trimws(m_content))) {
        answer_type <- if (grepl("^\\s*\\{", m_content)) {
          "Produced structured answer"
        } else {
          "Produced answer"
        }
        add_event("ai_response", "AI", answer_type, m_content, token_usage = usage)
      }
      next
    }

    if (m_type %in% c("toolmessage", "tool", "function")) {
      actor <- paste0("Tool ", if (nzchar(m_name)) m_name else "?")
      add_event("tool_result", actor, "Returned result", m_content)
      next
    }

    if (nzchar(trimws(m_content))) {
      add_event("message", paste0("Message ", m_type), "Emitted message", m_content)
    }
  }

  events
}

#' Summarize plan history for action timelines
#' @keywords internal
.normalize_progress_status <- function(x) {
  x <- tolower(trimws(as.character(x %||% "unspecified")))
  if (!nzchar(x)) return("unspecified")
  if (x %in% c("found", "resolved", "verified", "confirmed")) return("found")
  if (x %in% c("unknown", "not_found", "unavailable", "undetermined")) return("unknown")
  if (x %in% c("completed", "complete", "done", "finished", "success")) return("completed")
  if (x %in% c("in_progress", "in progress", "active", "ongoing", "running")) return("in_progress")
  if (x %in% c("pending", "todo", "to_do", "not_started", "not started", "planned", "queued")) return("pending")
  "unspecified"
}

#' Extract steps list from plan payload
#' @keywords internal
.extract_plan_steps <- function(plan_state) {
  if (!is.list(plan_state)) {
    return(NULL)
  }
  steps <- plan_state$steps %||% plan_state$plan %||% NULL
  if (is.null(steps) || !is.list(steps) || length(steps) == 0L) {
    return(NULL)
  }
  steps
}

#' Summarize plan history for action timelines
#' @keywords internal
.summarize_plan_history <- function(plan_history, plan = list(), max_chars = 88L) {
  steps <- NULL

  if (is.list(plan_history) && length(plan_history) > 0L) {
    for (idx in seq(length(plan_history), 1L)) {
      candidate <- .extract_plan_steps(plan_history[[idx]])
      if (!is.null(candidate)) {
        steps <- candidate
        break
      }
    }
  }

  if (is.null(steps)) {
    steps <- .extract_plan_steps(plan)
  }
  if (is.null(steps)) {
    return(character(0))
  }

  statuses <- vapply(steps, function(step) {
    if (!is.list(step)) {
      return("unspecified")
    }
    status <- as.character(step$status %||% "unspecified")
    if (length(status) == 0L || !nzchar(status[[1]])) {
      "unspecified"
    } else {
      .normalize_progress_status(status[[1]])
    }
  }, character(1))

  completed <- sum(statuses == "completed")
  in_progress <- sum(statuses == "in_progress")
  pending <- sum(statuses == "pending")
  unspecified <- length(statuses) - completed - in_progress - pending

  if (completed == 0L && in_progress == 0L && pending == 0L && unspecified > 0L) {
    summary_line <- sprintf(
      "Plan steps: %d total (status metadata not yet populated)",
      length(steps)
    )
  } else {
    summary_line <- sprintf(
      "Plan steps: %d total (%d completed, %d in_progress, %d pending%s)",
      length(steps),
      completed,
      in_progress,
      pending,
      if (unspecified > 0L) paste0(", ", unspecified, " unspecified") else ""
    )
  }

  step_labels <- vapply(steps, function(step) {
    if (!is.list(step)) return("")
    label <- as.character(step$step %||% step$title %||% "")
    if (length(label) == 0L || is.na(label[[1]])) "" else label[[1]]
  }, character(1))
  step_labels <- step_labels[nzchar(step_labels)]

  if (length(step_labels) == 0L) {
    return(summary_line)
  }

  c(
    summary_line,
    paste0(
      "Step flow: ",
      .clip_action_text(paste(step_labels, collapse = " -> "), max_chars = max_chars)
    )
  )
}

#' Coerce a value to scalar minutes (double)
#' @keywords internal
.as_scalar_minutes <- function(value, default = NA_real_) {
  if (is.null(value) || length(value) == 0L) {
    return(default)
  }
  candidate <- suppressWarnings(as.numeric(value[[1]] %||% value))
  if (is.na(candidate) || !is.finite(candidate)) {
    return(default)
  }
  if (candidate < 0) {
    return(default)
  }
  as.numeric(candidate)
}

#' Normalize one token/timing trace entry
#' @keywords internal
.normalize_token_trace_entry <- function(entry) {
  entry <- .try_or(reticulate::py_to_r(entry), entry)
  if (!is.list(entry)) return(NULL)
  node <- as.character(entry$node %||% "")
  node <- if (length(node) > 0L && !is.na(node[[1]])) tolower(trimws(node[[1]])) else ""
  elapsed_minutes <- .as_scalar_minutes(
    entry$elapsed_minutes %||%
      entry$duration_minutes %||%
      {
        elapsed_seconds <- .as_scalar_minutes(entry$elapsed_seconds)
        if (!is.na(elapsed_seconds)) elapsed_seconds / 60 else NA_real_
      }
  )
  list(
    node = node,
    input_tokens = .as_scalar_int(entry$input_tokens),
    output_tokens = .as_scalar_int(entry$output_tokens),
    total_tokens = .as_scalar_int(entry$total_tokens),
    elapsed_minutes = elapsed_minutes
  )
}

#' Extract normalized LangGraph step timing rows from token_trace
#' @keywords internal
.extract_langgraph_step_timings <- function(token_trace = list()) {
  if (!is.list(token_trace) || length(token_trace) == 0L) {
    return(list())
  }
  out <- list()
  for (entry in token_trace) {
    norm <- .normalize_token_trace_entry(entry)
    if (is.null(norm) || !nzchar(norm$node)) next
    out[[length(out) + 1L]] <- list(
      step_index = length(out) + 1L,
      node = norm$node,
      elapsed_minutes = norm$elapsed_minutes,
      input_tokens = norm$input_tokens %||% 0L,
      output_tokens = norm$output_tokens %||% 0L,
      total_tokens = norm$total_tokens %||% 0L
    )
  }
  out
}

#' Format step minutes for ASCII/action reporting
#' @keywords internal
.format_step_minutes <- function(value, digits = 4L, na_label = "n/a") {
  minutes <- .as_scalar_minutes(value)
  if (is.na(minutes)) {
    return(na_label)
  }
  sprintf(paste0("%.", as.integer(digits), "f"), minutes)
}

#' Format step minutes with "m" suffix only when numeric
#' @keywords internal
.format_step_minutes_with_unit <- function(value, digits = 4L, na_label = "n/a") {
  min_txt <- .format_step_minutes(value, digits = digits, na_label = na_label)
  if (!is.character(min_txt) || length(min_txt) == 0L) {
    return(na_label)
  }
  min_txt <- min_txt[[1]]
  if (!nzchar(min_txt) || identical(min_txt, na_label)) {
    return(na_label)
  }
  paste0(min_txt, "m")
}

#' Build field-status metrics used for investigator summaries and flags
#' @keywords internal
.field_status_metrics <- function(field_status = list()) {
  empty_metrics <- list(
    all = list(total = 0L, found = 0L, unknown = 0L, pending = 0L, unspecified = 0L),
    core = list(total = 0L, found = 0L, unknown = 0L, pending = 0L, unspecified = 0L),
    source_backed_found = 0L,
    unresolved_core_fields = character(0),
    low_evidence = FALSE
  )

  if (!is.list(field_status) || length(field_status) == 0L) {
    return(empty_metrics)
  }

  field_names <- names(field_status)
  if (is.null(field_names)) {
    field_names <- rep("", length(field_status))
  }

  statuses <- character(length(field_status))
  has_source_url <- logical(length(field_status))

  for (i in seq_along(field_status)) {
    entry <- field_status[[i]]
    if (is.list(entry)) {
      status_raw <- as.character(entry$status %||% "unspecified")
      statuses[[i]] <- if (length(status_raw) == 0L || !nzchar(status_raw[[1]])) {
        "unspecified"
      } else {
        .normalize_progress_status(status_raw[[1]])
      }

      src <- as.character(entry$source_url %||% "")
      has_source_url[[i]] <- length(src) > 0L && !is.na(src[[1]]) && nzchar(trimws(src[[1]]))
    } else {
      statuses[[i]] <- .normalize_progress_status(entry)
      has_source_url[[i]] <- FALSE
    }
  }

  summarize_statuses <- function(mask) {
    sub_status <- statuses[mask]
    list(
      total = as.integer(length(sub_status)),
      found = as.integer(sum(sub_status == "completed" | sub_status == "found")),
      unknown = as.integer(sum(sub_status == "unknown")),
      pending = as.integer(sum(sub_status == "pending" | sub_status == "in_progress")),
      unspecified = as.integer(sum(sub_status == "unspecified"))
    )
  }

  # Normalize any non-plan status values that may leak through.
  statuses <- ifelse(statuses == "completed", "found", statuses)
  statuses[statuses == "in_progress"] <- "pending"

  is_source_field <- nzchar(field_names) & grepl("_source$", field_names, ignore.case = TRUE)
  is_meta_field <- field_names %in% c("confidence", "justification")
  is_core_field <- !(is_source_field | is_meta_field)
  if (!any(is_core_field)) {
    is_core_field <- rep(TRUE, length(field_status))
  }

  all_metrics <- summarize_statuses(rep(TRUE, length(field_status)))
  core_metrics <- summarize_statuses(is_core_field)
  source_backed_found <- sum((statuses == "found") & has_source_url)
  unresolved_core_fields <- field_names[
    is_core_field &
      (statuses %in% c("unknown", "pending", "unspecified")) &
      nzchar(field_names)
  ]

  unresolved_core <- core_metrics$unknown + core_metrics$pending + core_metrics$unspecified
  low_evidence <- isTRUE(core_metrics$total >= 3L) &&
    ((core_metrics$found == 0L) || ((unresolved_core / core_metrics$total) >= 0.60))

  list(
    all = all_metrics,
    core = core_metrics,
    source_backed_found = as.integer(source_backed_found),
    unresolved_core_fields = unresolved_core_fields,
    low_evidence = isTRUE(low_evidence)
  )
}

#' Summarize field resolution status for investigators
#' @keywords internal
.summarize_field_status <- function(field_status = list(), max_chars = 88L) {
  metrics <- .field_status_metrics(field_status)
  if (metrics$core$total <= 0L) {
    return(character(0))
  }

  core <- metrics$core
  unresolved <- core$unknown + core$pending + core$unspecified
  unresolved_label <- if (core$unspecified > 0L) {
    paste0(", ", core$unspecified, " unspecified")
  } else {
    ""
  }

  lines <- c(
    sprintf(
      "Field resolution: %d found, %d unknown, %d pending%s (%d core fields)",
      core$found, core$unknown, core$pending, unresolved_label, core$total
    ),
    sprintf(
      "Source-backed found values: %d (across %d tracked fields)",
      metrics$source_backed_found, metrics$all$total
    )
  )

  if (unresolved > 0L && length(metrics$unresolved_core_fields) > 0L) {
    unresolved_fields <- metrics$unresolved_core_fields
    shown <- unresolved_fields[seq_len(min(length(unresolved_fields), 4L))]
    extra_n <- length(unresolved_fields) - length(shown)
    lines <- c(lines, paste0(
      "Unresolved focus: ",
      paste(shown, collapse = ", "),
      if (extra_n > 0L) paste0(" (+", extra_n, " more)") else ""
    ))
  }

  if (isTRUE(metrics$low_evidence)) {
    lines <- c(lines, "Risk signal: LOW_EVIDENCE_FINAL_ANSWER")
  }

  vapply(lines, .clip_action_text, character(1), max_chars = max_chars, USE.NAMES = FALSE)
}

#' Tokenize action text for coarse relevance checks
#' @keywords internal
.tokenize_action_text <- function(text) {
  if (!is.character(text) || length(text) == 0L || is.na(text[[1]]) || !nzchar(text[[1]])) {
    return(character(0))
  }
  txt <- tolower(text[[1]])
  txt <- gsub("https?://", " ", txt)
  txt <- gsub("[^[:alnum:]_]+", " ", txt)
  parts <- strsplit(txt, "\\s+", perl = TRUE)[[1]]
  parts <- parts[nzchar(parts)]
  if (length(parts) == 0L) {
    return(character(0))
  }
  stop_words <- c(
    "http", "https", "www", "com", "org", "net", "html",
    "query", "url", "tool", "search", "openwebpage", "save", "finding",
    "result", "results", "returned", "call", "calls",
    "the", "and", "for", "with", "from", "this", "that", "are",
    "del", "las", "los", "con", "por", "para", "una", "uno", "que"
  )
  unique(parts[nchar(parts) >= 3L & !(parts %in% stop_words)])
}

#' Extract candidate query tokens from an AI Search tool-call preview
#' @keywords internal
.extract_search_query_terms <- function(preview) {
  if (!is.character(preview) || length(preview) == 0L || is.na(preview[[1]]) || !nzchar(preview[[1]])) {
    return(character(0))
  }
  txt <- preview[[1]]
  capture <- regmatches(
    txt,
    regexec("query\\s*=\\s*([^\\)]+)", txt, ignore.case = TRUE, perl = TRUE)
  )[[1]]
  query_txt <- if (length(capture) >= 2L) capture[[2]] else txt
  .tokenize_action_text(query_txt)
}

#' Attach best-effort token counts to action steps
#' @keywords internal
.attach_step_token_counts <- function(steps, token_trace = list()) {
  if (!is.list(steps) || length(steps) == 0L) {
    return(steps)
  }

  # Ensure token fields exist for all steps.
  for (i in seq_along(steps)) {
    if (!is.list(steps[[i]])) next
    if (is.na(.as_scalar_int(steps[[i]]$input_tokens))) steps[[i]]$input_tokens <- 0L
    if (is.na(.as_scalar_int(steps[[i]]$output_tokens))) steps[[i]]$output_tokens <- 0L
    if (is.na(.as_scalar_int(steps[[i]]$total_tokens))) steps[[i]]$total_tokens <- 0L
    if (is.na(.as_scalar_minutes(steps[[i]]$elapsed_minutes))) steps[[i]]$elapsed_minutes <- NA_real_
  }

  ai_indices <- which(vapply(steps, function(step) {
    if (!is.list(step)) return(FALSE)
    st <- as.character(step$type %||% "")
    if (length(st) == 0L || is.na(st[[1]])) return(FALSE)
    st <- st[[1]]
    st %in% c("ai_tool_calls", "ai_response")
  }, logical(1)))
  if (length(ai_indices) == 0L) {
    return(steps)
  }

  # Prefer explicit message-level usage when present.
  unresolved_ai <- ai_indices[vapply(ai_indices, function(idx) {
    tok <- .as_scalar_int(steps[[idx]]$total_tokens)
    tm <- .as_scalar_minutes(steps[[idx]]$elapsed_minutes)
    is.na(tok) || tok <= 0L || is.na(tm)
  }, logical(1))]

  token_entries <- list()
  if (is.list(token_trace) && length(token_trace) > 0L) {
    for (entry in token_trace) {
      norm <- .normalize_token_trace_entry(entry)
      if (is.null(norm)) next
      has_tokens <- !is.na(norm$total_tokens) && norm$total_tokens > 0L
      has_time <- !is.na(norm$elapsed_minutes)
      if (has_tokens || has_time) {
        token_entries[[length(token_entries) + 1L]] <- norm
      }
    }
  }
  if (length(token_entries) == 0L || length(unresolved_ai) == 0L) {
    return(steps)
  }

  # Map only model-generation nodes; exclude summarize/tool bookkeeping nodes.
  model_nodes <- token_entries[vapply(token_entries, function(entry) {
    !(entry$node %in% c("summarize", "tools", "tool", "nudge", "planner", "deduper", "stopper", "unknown", ""))
  }, logical(1))]
  if (length(model_nodes) == 0L) {
    return(steps)
  }

  assign_n <- min(length(unresolved_ai), length(model_nodes))
  target_idx <- tail(unresolved_ai, assign_n)
  source_entries <- tail(model_nodes, assign_n)
  for (j in seq_len(assign_n)) {
    steps[[target_idx[[j]]]]$input_tokens <- source_entries[[j]]$input_tokens %||% 0L
    steps[[target_idx[[j]]]]$output_tokens <- source_entries[[j]]$output_tokens %||% 0L
    steps[[target_idx[[j]]]]$total_tokens <- source_entries[[j]]$total_tokens %||% 0L
    steps[[target_idx[[j]]]]$elapsed_minutes <- source_entries[[j]]$elapsed_minutes %||% NA_real_
  }

  steps
}

#' Collapse repetitive consecutive action events for readability
#' @keywords internal
.collapse_action_events <- function(events, max_preview_chars = 88L) {
  if (!is.list(events) || length(events) == 0L) {
    return(list())
  }

  collapsed <- list()
  i <- 1L
  n <- length(events)

  while (i <= n) {
    ev <- events[[i]]
    if (!is.list(ev)) {
      collapsed[[length(collapsed) + 1L]] <- ev
      i <- i + 1L
      next
    }

    ev_type <- as.character(ev$type %||% "")
    ev_actor <- as.character(ev$actor %||% "")
    if (length(ev_type) == 0L || is.na(ev_type[[1]])) ev_type <- "" else ev_type <- ev_type[[1]]
    if (length(ev_actor) == 0L || is.na(ev_actor[[1]])) ev_actor <- "" else ev_actor <- ev_actor[[1]]

    if (!identical(ev_type, "tool_result") || !nzchar(ev_actor)) {
      collapsed[[length(collapsed) + 1L]] <- ev
      i <- i + 1L
      next
    }

    j <- i
    while (j + 1L <= n) {
      next_ev <- events[[j + 1L]]
      if (!is.list(next_ev)) break
      next_type <- as.character(next_ev$type %||% "")
      next_actor <- as.character(next_ev$actor %||% "")
      if (length(next_type) == 0L || is.na(next_type[[1]])) next_type <- "" else next_type <- next_type[[1]]
      if (length(next_actor) == 0L || is.na(next_actor[[1]])) next_actor <- "" else next_actor <- next_actor[[1]]
      if (!identical(next_type, ev_type) || !identical(next_actor, ev_actor)) break
      j <- j + 1L
    }

    run_len <- j - i + 1L
    if (run_len == 1L) {
      ev$collapsed_event_count <- .as_scalar_int(ev$collapsed_event_count)
      if (is.na(ev$collapsed_event_count) || ev$collapsed_event_count < 1L) {
        ev$collapsed_event_count <- 1L
      }
      collapsed[[length(collapsed) + 1L]] <- ev
      i <- i + 1L
      next
    }

    previews <- vapply(events[i:j], function(x) {
      p <- as.character(x$preview %||% "")
      if (length(p) == 0L || is.na(p[[1]])) "" else p[[1]]
    }, character(1))
    previews <- previews[nzchar(previews)]
    preview_show <- previews[seq_len(min(length(previews), 3L))]
    preview_text <- paste(preview_show, collapse = " | ")
    if (length(previews) > 3L) {
      preview_text <- paste0(preview_text, " | ... (+", length(previews) - 3L, " more)")
    }

    # Carry elapsed_minutes from the group (use max: all tool results in a
    # collapsed group come from the same tools-node invocation).
    group_minutes <- vapply(events[i:j], function(x) {
      .as_scalar_minutes(x$elapsed_minutes)
    }, numeric(1))
    best_minutes <- if (all(is.na(group_minutes))) NA_real_ else max(group_minutes, na.rm = TRUE)

    collapsed[[length(collapsed) + 1L]] <- list(
      type = ev_type,
      actor = ev_actor,
      summary = sprintf("Returned %d results", run_len),
      preview = .clip_action_text(preview_text, max_chars = max_preview_chars),
      elapsed_minutes = best_minutes,
      collapsed_event_count = run_len
    )
    i <- j + 1L
  }

  collapsed
}

#' Attach anomaly flags to action steps
#' @keywords internal
.attach_step_anomaly_flags <- function(steps, field_status = list(), field_metrics = NULL,
                                       tool_quality_events = list()) {
  if (!is.list(steps) || length(steps) == 0L) {
    return(steps)
  }

  if (is.null(field_metrics)) {
    field_metrics <- .field_status_metrics(field_status)
  }

  for (i in seq_along(steps)) {
    if (!is.list(steps[[i]])) next
    if (is.null(steps[[i]]$flags) || !is.character(steps[[i]]$flags)) {
      steps[[i]]$flags <- character(0)
    }
  }

  quality_events <- .normalize_tool_quality_events(tool_quality_events)
  quality_cursor <- 1L

  for (i in seq_along(steps)) {
    step <- steps[[i]]
    if (!is.list(step)) next

    step_type <- as.character(step$type %||% "")
    step_actor <- as.character(step$actor %||% "")
    step_preview <- as.character(step$preview %||% "")
    if (length(step_type) == 0L || is.na(step_type[[1]])) step_type <- "" else step_type <- step_type[[1]]
    if (length(step_actor) == 0L || is.na(step_actor[[1]])) step_actor <- "" else step_actor <- step_actor[[1]]
    if (length(step_preview) == 0L || is.na(step_preview[[1]])) step_preview <- "" else step_preview <- step_preview[[1]]

    if (identical(step_type, "tool_result")) {
      grouped_count <- .as_scalar_int(step$collapsed_event_count)
      if (is.na(grouped_count) || grouped_count < 1L) {
        grouped_count <- 1L
      }
      event_end <- min(length(quality_events), quality_cursor + grouped_count - 1L)
      step_events <- if (quality_cursor <= event_end) {
        quality_events[quality_cursor:event_end]
      } else {
        list()
      }
      quality_cursor <- quality_cursor + grouped_count

      if (length(step_events) > 0L) {
        if (any(vapply(step_events, function(ev) isTRUE(ev$is_empty), logical(1)))) {
          steps[[i]]$flags <- unique(c(steps[[i]]$flags, "EMPTY_RESULT"))
        }
        if (any(vapply(step_events, function(ev) isTRUE(ev$is_off_target), logical(1)))) {
          steps[[i]]$flags <- unique(c(steps[[i]]$flags, "OFF_TOPIC_RESULT"))
        }
      } else if (!nzchar(trimws(step_preview))) {
        steps[[i]]$flags <- unique(c(steps[[i]]$flags, "EMPTY_RESULT"))
      }

      if (length(step_events) == 0L &&
          identical(step_actor, "Tool Search") &&
          nzchar(step_preview) &&
          i > 1L) {
        prev_ai_idx <- which(vapply(steps[seq_len(i - 1L)], function(s) {
          if (!is.list(s)) return(FALSE)
          s_type <- as.character(s$type %||% "")
          s_preview <- as.character(s$preview %||% "")
          if (length(s_type) == 0L || is.na(s_type[[1]])) return(FALSE)
          if (length(s_preview) == 0L || is.na(s_preview[[1]])) return(FALSE)
          identical(s_type[[1]], "ai_tool_calls") && grepl("Search\\(", s_preview[[1]])
        }, logical(1)))

        if (length(prev_ai_idx) > 0L) {
          last_ai <- prev_ai_idx[length(prev_ai_idx)]
          query_terms <- .extract_search_query_terms(steps[[last_ai]]$preview %||% "")
          result_terms <- .tokenize_action_text(step_preview)
          if (length(query_terms) >= 3L) {
            overlap <- intersect(query_terms, result_terms)
            if (length(overlap) < 2L) {
              steps[[i]]$flags <- unique(c(steps[[i]]$flags, "OFF_TOPIC_RESULT"))
            }
          }
        }
      }
    }
  }

  if (isTRUE(field_metrics$low_evidence)) {
    final_ai <- which(vapply(steps, function(s) {
      if (!is.list(s)) return(FALSE)
      s_type <- as.character(s$type %||% "")
      if (length(s_type) == 0L || is.na(s_type[[1]])) return(FALSE)
      identical(s_type[[1]], "ai_response")
    }, logical(1)))
    if (length(final_ai) > 0L) {
      final_idx <- final_ai[length(final_ai)]
      steps[[final_idx]]$flags <- unique(c(steps[[final_idx]]$flags, "LOW_EVIDENCE_FINAL_ANSWER"))
    }
  }

  steps
}

#' Summarize anomaly flags across action steps
#' @keywords internal
.summarize_step_anomalies <- function(steps, diagnostics = list(), max_chars = 88L) {
  if (!is.list(steps) || length(steps) == 0L) {
    return(character(0))
  }
  diag_empty <- .as_scalar_int(diagnostics$empty_tool_results_count)
  diag_off_target <- .as_scalar_int(diagnostics$off_target_tool_results_count)
  diag_labels <- character(0)
  if (!is.na(diag_empty)) {
    diag_labels <- c(diag_labels, paste0("EMPTY_RESULT=", diag_empty))
  }
  if (!is.na(diag_off_target)) {
    diag_labels <- c(diag_labels, paste0("OFF_TOPIC_RESULT=", diag_off_target))
  }
  diag_suffix <- if (length(diag_labels) > 0L) {
    paste0("; diagnostics(", paste(diag_labels, collapse = ", "), ")")
  } else {
    ""
  }

  per_step_flags <- lapply(steps, function(step) {
    if (!is.list(step)) return(character(0))
    flags <- as.character(step$flags %||% character(0))
    flags <- flags[!is.na(flags) & nzchar(flags)]
    unique(flags)
  })
  flagged_step_count <- sum(vapply(per_step_flags, function(x) length(x) > 0L, logical(1)))
  if (flagged_step_count == 0L) {
    if (length(diag_labels) == 0L) {
      return(character(0))
    }
    return(.clip_action_text(
      sprintf("Anomalies: %d flagged step(s)%s", flagged_step_count, diag_suffix),
      max_chars = max_chars
    ))
  }

  all_flags <- unlist(per_step_flags, use.names = FALSE)
  if (length(all_flags) == 0L) {
    if (length(diag_labels) == 0L) {
      return(character(0))
    }
    return(.clip_action_text(
      sprintf("Anomalies: %d flagged step(s)%s", flagged_step_count, diag_suffix),
      max_chars = max_chars
    ))
  }
  counts <- sort(table(all_flags), decreasing = TRUE)
  labels <- paste0(names(counts), "=", as.integer(counts))
  .clip_action_text(
    sprintf(
      "Anomalies: %d flagged step(s) (%s)%s",
      flagged_step_count,
      paste(labels, collapse = ", "),
      diag_suffix
    ),
    max_chars = max_chars
  )
}

#' Build high-level "what happened overall" lines for action maps
#' @keywords internal
.summarize_action_overall <- function(raw_steps, steps = list(), plan_summary = character(0),
                                      diagnostics = list(), max_chars = 88L) {
  if (!is.list(raw_steps) || length(raw_steps) == 0L) {
    return(c("No action events available."))
  }

  get_type <- function(step) {
    t <- as.character(step$type %||% "")
    if (length(t) == 0L || is.na(t[[1]])) "" else t[[1]]
  }
  get_actor <- function(step) {
    a <- as.character(step$actor %||% "")
    if (length(a) == 0L || is.na(a[[1]])) "" else a[[1]]
  }
  get_summary <- function(step) {
    s <- as.character(step$summary %||% "")
    if (length(s) == 0L || is.na(s[[1]])) "" else s[[1]]
  }

  types <- vapply(raw_steps, get_type, character(1))
  actors <- vapply(raw_steps, get_actor, character(1))
  summaries <- vapply(raw_steps, get_summary, character(1))

  human_prompts <- sum(types == "human")
  ai_tool_turns <- sum(types == "ai_tool_calls")
  ai_answer_turns <- sum(types == "ai_response")
  tool_results <- sum(types == "tool_result")

  tool_actor_counts <- table(actors[types == "tool_result"])
  tool_actor_counts <- sort(tool_actor_counts, decreasing = TRUE)
  tool_actor_labels <- if (length(tool_actor_counts) > 0L) {
    paste(
      paste0(names(tool_actor_counts), "=", as.integer(tool_actor_counts)),
      collapse = ", "
    )
  } else {
    "none"
  }

  final_answer <- "No terminal AI answer detected"
  ai_idx <- which(types == "ai_response")
  if (length(ai_idx) > 0L) {
    last_summary <- summaries[ai_idx[length(ai_idx)]]
    if (grepl("structured", tolower(last_summary), fixed = TRUE)) {
      final_answer <- "Structured terminal answer emitted"
    } else {
      final_answer <- "Terminal answer emitted"
    }
  }

  lines <- c(
    sprintf("Human prompts: %d", human_prompts),
    sprintf("AI turns: %d tool-call turn(s), %d answer turn(s)", ai_tool_turns, ai_answer_turns),
    sprintf("Tool results: %d total (%s)", tool_results, tool_actor_labels),
    final_answer
  )

  if (length(plan_summary) > 0L) {
    lines <- c(lines, paste0("Plan status: ", plan_summary[[1]]))
  }
  anomaly_line <- .summarize_step_anomalies(
    steps,
    diagnostics = diagnostics,
    max_chars = max_chars
  )
  if (length(anomaly_line) > 0L && nzchar(anomaly_line[[1]])) {
    lines <- c(lines, anomaly_line[[1]])
  }

  vapply(lines, .clip_action_text, character(1), max_chars = max_chars, USE.NAMES = FALSE)
}

#' Build compact action trace metadata from run output
#' @keywords internal
.extract_action_trace <- function(trace_json = "", raw_trace = "", plan_history = list(),
                                  plan = list(),
                                  field_status = list(),
                                  tool_quality_events = list(),
                                  diagnostics = list(),
                                  max_preview_chars = 88L, max_steps = 200L,
                                  token_trace = list(),
                                  wall_time_minutes = NA_real_) {
  # Defensive coercion: trace_json / raw_trace may arrive as jsonlite "json"

  # class objects or reticulate wrappers; force plain character.
  trace_json <- as.character(trace_json %||% "")
  if (length(trace_json) != 1L) trace_json <- paste0(trace_json, collapse = "")
  raw_trace <- as.character(raw_trace %||% "")
  if (length(raw_trace) != 1L) raw_trace <- paste0(raw_trace, collapse = "")
  max_preview_chars <- as.integer(max_preview_chars)
  if (is.na(max_preview_chars) || max_preview_chars < 20L) {
    max_preview_chars <- 88L
  }
  max_steps <- as.integer(max_steps)
  if (is.na(max_steps) || max_steps < 1L) {
    max_steps <- 200L
  }
  field_metrics <- tryCatch(
    .field_status_metrics(field_status),
    error = function(e) {
      warning("[action_trace:field_metrics] ", conditionMessage(e), call. = FALSE)
      .field_status_metrics(list())
    }
  )

  messages <- tryCatch(
    .parse_trace_json_messages(trace_json),
    error = function(e) {
      warning("[action_trace:parse_json] ", conditionMessage(e), call. = FALSE)
      NULL
    }
  )
  raw_steps <- tryCatch(
    if (!is.null(messages)) {
      .trace_messages_to_action_events(messages, max_preview_chars = max_preview_chars)
    } else {
      .parse_trace_raw_events(raw_trace, max_preview_chars = max_preview_chars)
    },
    error = function(e) {
      warning("[action_trace:build_events] ", conditionMessage(e), call. = FALSE)
      list()
    }
  )
  steps <- tryCatch(
    .collapse_action_events(raw_steps, max_preview_chars = max_preview_chars),
    error = function(e) {
      warning("[action_trace:collapse] ", conditionMessage(e), call. = FALSE)
      raw_steps
    }
  )
  steps <- tryCatch(
    .attach_step_token_counts(steps, token_trace = token_trace),
    error = function(e) {
      warning("[action_trace:token_counts] ", conditionMessage(e), call. = FALSE)
      steps
    }
  )
  steps <- tryCatch(
    .attach_step_anomaly_flags(
      steps,
      field_status = field_status,
      field_metrics = field_metrics,
      tool_quality_events = tool_quality_events
    ),
    error = function(e) {
      warning("[action_trace:flags] ", conditionMessage(e), call. = FALSE)
      steps
    }
  )
  langgraph_step_timings <- tryCatch(
    .extract_langgraph_step_timings(token_trace),
    error = function(e) {
      warning("[action_trace:lg_timings] ", conditionMessage(e), call. = FALSE)
      list()
    }
  )
  plan_summary <- tryCatch(
    .summarize_plan_history(plan_history, plan = plan, max_chars = max_preview_chars),
    error = function(e) {
      warning("[action_trace:plan_summary] ", conditionMessage(e), call. = FALSE)
      character(0)
    }
  )
  investigator_summary <- tryCatch(
    .summarize_field_status(field_status, max_chars = max_preview_chars),
    error = function(e) {
      warning("[action_trace:investigator_summary] ", conditionMessage(e), call. = FALSE)
      character(0)
    }
  )
  overall_summary <- tryCatch(
    .summarize_action_overall(
      raw_steps,
      steps = steps,
      plan_summary = plan_summary,
      diagnostics = diagnostics,
      max_chars = max_preview_chars
    ),
    error = function(e) {
      warning("[action_trace:overall_summary] ", conditionMessage(e), call. = FALSE)
      character(0)
    }
  )

  total_steps <- length(steps)
  omitted_steps <- 0L
  if (total_steps > max_steps) {
    omitted_steps <- total_steps - max_steps
    steps <- steps[seq_len(max_steps)]
  }

  out <- list(
    step_count = total_steps,
    omitted_steps = omitted_steps,
    steps = steps,
    langgraph_step_timings = langgraph_step_timings,
    plan_summary = plan_summary,
    investigator_summary = investigator_summary,
    field_metrics = field_metrics,
    overall_summary = overall_summary,
    wall_time_minutes = .as_scalar_minutes(wall_time_minutes)
  )
  out$ascii <- tryCatch(
    .render_action_ascii(out),
    error = function(e) {
      warning("[action_trace:ascii_render] ", conditionMessage(e), call. = FALSE)
      "=== AGENT ACTION MAP ===\n[Rendering failed]\n"
    }
  )
  out
}

#' Aggregate LangGraph timings by node for compact reporting
#' @keywords internal
.aggregate_langgraph_timings <- function(langgraph_timings = list()) {
  if (!is.list(langgraph_timings) || length(langgraph_timings) == 0L) {
    return(list())
  }

  buckets <- list()
  for (timing in langgraph_timings) {
    if (!is.list(timing)) next
    node <- as.character(timing$node %||% "unknown")
    if (length(node) == 0L || is.na(node[[1]]) || !nzchar(node[[1]])) {
      node <- "unknown"
    } else {
      node <- node[[1]]
    }
    elapsed <- .as_scalar_minutes(timing$elapsed_minutes, default = 0)
    tok_total <- .as_scalar_int(timing$total_tokens)
    if (is.na(tok_total)) tok_total <- 0L

    if (is.null(buckets[[node]])) {
      buckets[[node]] <- list(
        node = node,
        elapsed_minutes = 0,
        total_tokens = 0L,
        count = 0L
      )
    }
    buckets[[node]]$elapsed_minutes <- buckets[[node]]$elapsed_minutes + elapsed
    buckets[[node]]$total_tokens <- as.integer(buckets[[node]]$total_tokens + tok_total)
    buckets[[node]]$count <- as.integer(buckets[[node]]$count + 1L)
  }

  if (length(buckets) == 0L) {
    return(list())
  }
  out <- unname(buckets)
  ord <- order(
    vapply(out, function(x) x$elapsed_minutes %||% 0, numeric(1)),
    decreasing = TRUE
  )
  out[ord]
}

#' Return top slowest individual LangGraph timing rows
#' @keywords internal
.top_langgraph_timings <- function(langgraph_timings = list(), n = 5L) {
  if (!is.list(langgraph_timings) || length(langgraph_timings) == 0L) {
    return(list())
  }
  n <- max(1L, as.integer(n))
  times <- vapply(langgraph_timings, function(t) {
    if (!is.list(t)) return(NA_real_)
    .as_scalar_minutes(t$elapsed_minutes)
  }, numeric(1))
  valid <- which(!is.na(times))
  if (length(valid) == 0L) {
    return(list())
  }
  ord <- order(times[valid], decreasing = TRUE)
  pick <- valid[ord][seq_len(min(length(valid), n))]
  langgraph_timings[pick]
}

#' Render action timeline as high-level ASCII flow
#' @keywords internal
.render_action_ascii <- function(action_trace, max_line_chars = 100L) {
  max_line_chars <- as.integer(max_line_chars)
  if (is.na(max_line_chars) || max_line_chars < 30L) {
    max_line_chars <- 100L
  }

  clip_line <- function(x) {
    if (!is.character(x) || length(x) == 0L) {
      return("")
    }
    line <- x[[1]]
    if (is.na(line) || !nzchar(line)) {
      return("")
    }
    leading <- regmatches(line, regexpr("^\\s*", line))
    core <- sub("^\\s*", "", line)
    core <- gsub("\\s+", " ", core)
    merged <- paste0(leading, core)
    if (nchar(merged) <= max_line_chars) {
      return(merged)
    }
    paste0(substr(merged, 1L, max_line_chars - 3L), "...")
  }

  steps <- action_trace$steps %||% list()
  step_count <- .as_scalar_int(action_trace$step_count)
  if (is.na(step_count)) {
    step_count <- length(steps)
  }
  omitted <- .as_scalar_int(action_trace$omitted_steps)
  if (is.na(omitted)) {
    omitted <- 0L
  }
  langgraph_timings <- action_trace$langgraph_step_timings %||% list()
  investigator_summary <- action_trace$investigator_summary %||% character(0)
  overview <- action_trace$overall_summary %||% character(0)

  lines <- c(
    "AGENT ACTION MAP",
    "================",
    paste0("Steps captured: ", step_count),
    paste0("LangGraph timed steps: ", length(langgraph_timings)),
    "",
    "INVESTIGATOR SIGNALS",
    "--------------------",
    if (length(investigator_summary) == 0L) {
      clip_line("  +-- none")
    } else {
      c(
        clip_line("  +-- Summary"),
        vapply(investigator_summary, function(line) {
          clip_line(paste0("      ", line))
        }, character(1))
      )
    },
    "",
    "WHAT HAPPENED OVERALL",
    "---------------------",
    clip_line("  +-- Summary"),
    {
      if (length(overview) == 0L) {
        clip_line("      No summary available")
      } else {
        vapply(overview, function(line) clip_line(paste0("      ", line)), character(1))
      }
    },
    "",
    "START"
  )
  lines <- unlist(lines, use.names = FALSE)

  plan_summary <- action_trace$plan_summary %||% character(0)
  if (length(plan_summary) > 0L) {
    for (line in plan_summary) {
      lines <- c(lines, clip_line(paste0("  +-- [PLAN] ", line)))
    }
  }

  langgraph_agg <- .aggregate_langgraph_timings(langgraph_timings)
  lines <- c(lines, "  +-- [LANGGRAPH] Node aggregates (minutes, tokens, count)")
  if (length(langgraph_agg) == 0L) {
    lines <- c(lines, clip_line("      none"))
  } else {
    for (timing in langgraph_agg) {
      if (!is.list(timing)) next
      node <- as.character(timing$node %||% "unknown")
      if (length(node) == 0L || is.na(node[[1]]) || !nzchar(node[[1]])) node <- "unknown" else node <- node[[1]]
      invocations <- .as_scalar_int(timing$count)
      if (is.na(invocations) || invocations < 1L) invocations <- 1L
      tok_total <- .as_scalar_int(timing$total_tokens)
      lines <- c(
        lines,
        clip_line(
          paste0(
            "      ",
            node,
            ": ",
            .format_step_minutes_with_unit(timing$elapsed_minutes),
            " (tok=",
            if (is.na(tok_total)) 0L else tok_total,
            ", n=",
            invocations,
            ")"
          )
        )
      )
    }
  }

  slowest <- .top_langgraph_timings(langgraph_timings, n = 5L)
  lines <- c(lines, "  +-- [LANGGRAPH] Slowest timed invocations")
  if (length(slowest) == 0L) {
    lines <- c(lines, clip_line("      none"))
  } else {
    for (timing in slowest) {
      if (!is.list(timing)) next
      node <- as.character(timing$node %||% "unknown")
      if (length(node) == 0L || is.na(node[[1]]) || !nzchar(node[[1]])) node <- "unknown" else node <- node[[1]]
      idx <- .as_scalar_int(timing$step_index)
      if (is.na(idx) || idx < 1L) idx <- 0L
      tok_total <- .as_scalar_int(timing$total_tokens)
      if (is.na(tok_total)) tok_total <- 0L
      lines <- c(
        lines,
        clip_line(
          paste0(
            "      #",
            sprintf("%02d", idx),
            " ",
            node,
            ": ",
            .format_step_minutes_with_unit(timing$elapsed_minutes),
            " (tok=",
            tok_total,
            ")"
          )
        )
      )
    }
  }

  # Timing sanity check: compare global wall time to sum of node timings
  node_sum_min <- sum(vapply(langgraph_timings, function(t) {
    m <- .as_scalar_minutes(t$elapsed_minutes)
    if (is.na(m)) 0 else m
  }, numeric(1)))
  wall_min <- .as_scalar_minutes(action_trace$wall_time_minutes)
  if (!is.na(wall_min) && wall_min > 0) {
    delta_min <- wall_min - node_sum_min
    delta_note <- if (delta_min < 0) {
      "overlap likely"
    } else {
      "overhead"
    }
    lines <- c(lines, clip_line(sprintf(
      "  +-- [TIMING] wall=%.4fm nodes=%.4fm delta=%+.4fm (%s)",
      wall_min, node_sum_min, delta_min, delta_note
    )))
  }

  if (length(steps) == 0L) {
    lines <- c(lines, "  +-- [No action trace available]")
  } else {
    for (i in seq_along(steps)) {
      step <- steps[[i]]
      actor <- as.character(step$actor %||% "Step")
      summary <- as.character(step$summary %||% "Action")
      preview <- as.character(step$preview %||% "")

      if (length(actor) == 0L || is.na(actor[[1]]) || !nzchar(actor[[1]])) {
        actor <- "Step"
      } else {
        actor <- actor[[1]]
      }
      if (length(summary) == 0L || is.na(summary[[1]]) || !nzchar(summary[[1]])) {
        summary <- "Action"
      } else {
        summary <- summary[[1]]
      }
      if (length(preview) == 0L || is.na(preview[[1]])) {
        preview <- ""
      } else {
        preview <- preview[[1]]
      }

      lines <- c(
        lines,
        clip_line(paste0("  +-- [", sprintf("%02d", i), "] ", summary)),
        clip_line(paste0("      by: ", actor))
      )
      tok_in <- .as_scalar_int(step$input_tokens)
      tok_out <- .as_scalar_int(step$output_tokens)
      tok_total <- .as_scalar_int(step$total_tokens)
      token_line <- if (!is.na(tok_total) && tok_total > 0L) {
        paste0("      tok: in=", tok_in %||% 0L, " out=", tok_out %||% 0L, " total=", tok_total)
      } else {
        "      tok: in=0 out=0 total=0"
      }
      lines <- c(lines, clip_line(token_line))
      lines <- c(
        lines,
        clip_line(
          paste0("      min: ", .format_step_minutes_with_unit(step$elapsed_minutes))
        )
      )
      step_flags <- as.character(step$flags %||% character(0))
      step_flags <- unique(step_flags[!is.na(step_flags) & nzchar(step_flags)])
      if (length(step_flags) > 0L) {
        lines <- c(lines, clip_line(paste0("      flag: ", paste(step_flags, collapse = ", "))))
      }
      if (nzchar(preview)) {
        lines <- c(lines, clip_line(paste0("      detail: ", preview)))
      }
    }
  }

  if (omitted > 0L) {
    lines <- c(lines, paste0("  +-- [... ] ", omitted, " additional step(s) omitted"))
  }

  lines <- c(lines, "END")
  paste(lines, collapse = "\n")
}

#' Extract tool message contents from either text trace or asa_trace_v1 JSON.
#' @keywords internal
.extract_tool_contents <- function(text, tool_name) {
  msgs <- .parse_trace_json_messages(text)
  if (is.null(msgs)) {
    return(NULL)
  }
  tool_name <- tolower(tool_name)
  contents <- character(0)
  for (m in msgs) {
    if (!is.list(m)) next
    m_type <- tolower(as.character(m$message_type %||% ""))
    m_name <- tolower(as.character(m$name %||% ""))
    if (!(m_type %in% c("toolmessage", "tool"))) next
    if (m_name != tool_name) next
    contents <- c(contents, as.character(m$content %||% ""))
  }
  contents
}

#' Decode legacy ToolMessage content (Python repr-like escapes)
#' @keywords internal
.decode_legacy_toolmessage_content <- function(x) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    return("")
  }

  decoded <- .decode_trace_escaped_string(x)
  if (is.character(decoded) && length(decoded) == 1L && !is.null(decoded)) {
    return(decoded)
  }

  # Backward-compatible fallback: manual unescaping for common sequences.
  out <- x
  out <- gsub("\\\\n", " ", out)
  out <- gsub("\\\\'", "'", out)
  out <- gsub('\\\\"', '"', out)
  out <- gsub("\\\\\\\\", "\\\\", out)
  out
}

#' Extract tool contents from legacy (non-JSON) ToolMessage(...) traces
#' @keywords internal
.extract_tool_contents_legacy <- function(text, tool_name) {
  if (!is.character(text) || length(text) != 1 || is.na(text) || !nzchar(text)) {
    return(character(0))
  }

  tool_name <- as.character(tool_name)
  tool_name_esc <- gsub("([\\\\.^$|()\\[\\]{}*+?])", "\\\\\\1", tool_name, perl = TRUE)

  tool_pattern <- paste0(
    "ToolMessage\\(content=(?:'([^']*(?:\\\\'[^']*)*)'|",
    "\"([^\"]*(?:\\\\\"[^\"]*)*)\"), ",
    "name=(?:'(?i:", tool_name_esc, ")'|\"(?i:", tool_name_esc, ")\").*?\\)"
  )

  tool_matches <- gregexpr(tool_pattern, text, perl = TRUE)[[1]]
  if (tool_matches[1] == -1) {
    return(character(0))
  }

  match_texts <- regmatches(text, list(tool_matches))[[1]]
  contents <- character(0)

  for (match_text in match_texts) {
    content_match <- regmatches(match_text, regexec(tool_pattern, match_text, perl = TRUE))[[1]]

    # Content is in group 2 or 3 (single-quoted vs double-quoted).
    content <- ifelse(nchar(content_match[2]) > 0, content_match[2], content_match[3])
    if (is.na(content) || !nzchar(content)) next

    contents <- c(contents, .decode_legacy_toolmessage_content(content))
  }

  contents
}

#' Extract tool contents from either asa_trace_v1 JSON or legacy ToolMessage traces.
#' @keywords internal
.extract_tool_contents_any <- function(text, tool_name) {
  tool_contents <- .extract_tool_contents(text, tool_name)
  if (!is.null(tool_contents)) {
    return(tool_contents)
  }
  .extract_tool_contents_legacy(text, tool_name)
}

#' Select sources from an ordered vector (1-indexed)
#' @keywords internal
.select_sources <- function(result, source = NULL) {
  if (is.null(source)) {
    return(result)
  }

  source <- as.integer(source)
  source <- source[!is.na(source) & source > 0]
  if (length(source) == 0) {
    return(character(0))
  }

  result[source]
}

#' Extract per-source search fields (content or URL) from Search tool traces
#' @keywords internal
.extract_search_sources <- function(text, field = c("content", "url"), source = NULL) {
  field <- match.arg(field)

  tool_contents <- .extract_tool_contents_any(text, "search")
  if (length(tool_contents) == 0) {
    return(character(0))
  }

  source_values <- list()

  source_pattern <- if (field == "content") {
    "(?s)__START_OF_SOURCE (\\d+)__.*?<CONTENT>(.*?)</CONTENT>.*?__END_OF_SOURCE.*?__"
  } else {
    "(?s)__START_OF_SOURCE (\\d+)__.*?<URL>(.*?)</URL>.*?__END_OF_SOURCE.*?__"
  }

  for (content in tool_contents) {
    if (is.na(content) || !nzchar(content)) next

    source_matches <- gregexpr(source_pattern, content, perl = TRUE)[[1]]
    if (source_matches[1] == -1) next

    source_match_texts <- regmatches(content, list(source_matches))[[1]]
    for (src_match in source_match_texts) {
      m <- regmatches(src_match, regexec(source_pattern, src_match, perl = TRUE))[[1]]
      if (length(m) < 3) next

      source_num <- suppressWarnings(as.integer(m[2]))
      if (is.na(source_num) || source_num <= 0) next

      value <- m[3]
      if (field == "content") {
        value <- trimws(gsub("\\s+", " ", value))
      } else {
        value <- trimws(value)
      }

      if (!nzchar(value)) next

      key <- as.character(source_num)
      if (is.null(source_values[[key]])) {
        source_values[[key]] <- character(0)
      }

      if (field == "url") {
        source_values[[key]] <- unique(c(source_values[[key]], value))
      } else {
        source_values[[key]] <- c(source_values[[key]], value)
      }
    }
  }

  if (length(source_values) == 0) {
    return(character(0))
  }

  max_source <- max(as.integer(names(source_values)))
  result <- character(max_source)
  for (i in seq_len(max_source)) {
    source_key <- as.character(i)
    if (source_key %in% names(source_values)) {
      result[i] <- paste(source_values[[source_key]], collapse = " ")
    } else {
      result[i] <- ""
    }
  }

  .select_sources(result, source)
}

#' Extract Search Snippets by Source Number
#'
#' Extracts content from Search tool messages in the agent trace.
#'
#' @param text Raw agent trace text, or a structured JSON trace (asa_trace_v1)
#' @param source Optional integer vector of source numbers to return (1-indexed).
#'   If NULL (default), returns a character vector for all sources encountered
#'   (with empty strings for missing indices).
#'
#' @return Character vector of search snippets, ordered by source number
#'
#' @examples
#' \dontrun{
#' snippets <- extract_search_snippets(response$trace)
#' }
#'
#' @export
extract_search_snippets <- function(text, source = NULL) {
  .extract_search_sources(text, field = "content", source = source)
}

#' Extract Wikipedia Content
#'
#' Extracts content from Wikipedia tool messages in the agent trace.
#'
#' @param text Raw agent trace text, or a structured JSON trace (asa_trace_v1)
#'
#' @return Character vector of Wikipedia snippets
#'
#' @examples
#' \dontrun{
#' wiki <- extract_wikipedia_content(response$trace)
#' }
#'
#' @export
extract_wikipedia_content <- function(text) {
  snippets <- character(0)

  tool_contents <- .extract_tool_contents_any(text, "wikipedia")
  for (content in tool_contents) {
    if (is.na(content) || !nzchar(content)) next
    if ((grepl("Page:", content) || grepl("Summary:", content)) &&
        !grepl("No good Wikipedia Search Result", content, ignore.case = TRUE)) {
      cleaned <- trimws(gsub("\\s+", " ", content))
      if (nchar(cleaned) > 0) {
        snippets <- c(snippets, cleaned)
      }
    }
  }

  snippets
}

#' Extract URLs by Source Number
#'
#' Extracts URLs from Search tool messages in the agent trace.
#'
#' @param text Raw agent trace text, or a structured JSON trace (asa_trace_v1)
#' @param source Optional integer vector of source numbers to return (1-indexed).
#'   If NULL (default), returns a character vector for all sources encountered
#'   (with empty strings for missing indices).
#'
#' @return Character vector of URLs, ordered by source number
#'
#' @examples
#' \dontrun{
#' urls <- extract_urls(response$trace)
#' }
#'
#' @export
extract_urls <- function(text, source = NULL) {
  .extract_search_sources(text, field = "url", source = source)
}

#' Extract Search Tier Information
#'
#' Extracts which search tier was used from the agent trace.
#' The search module uses a multi-tier fallback system:
#' \itemize{
#'   \item primp: Fast HTTP client with browser impersonation (Tier 0)
#'   \item selenium: Headless browser for JS-rendered content (Tier 1)
#'   \item ddgs: Standard DDGS Python library (Tier 2)
#'   \item requests: Raw POST to DuckDuckGo HTML endpoint (Tier 3)
#' }
#'
#' @param text Raw agent trace text, or a structured JSON trace (asa_trace_v1)
#'
#' @return Character vector of unique tier names encountered
#'   (e.g., "primp", "selenium", "ddgs", "requests")
#'
#' @examples
#' \dontrun{
#' tiers <- extract_search_tiers(response$trace)
#' print(tiers)  # e.g., "primp"
#' }
#'
#' @export
extract_search_tiers <- function(text) {
  if (is.null(text) || text == "") {
    return(character(0))
  }

  tool_contents <- .extract_tool_contents(text, "search")
  if (!is.null(tool_contents) && length(tool_contents) > 0) {
    tier_pattern <- "['\"]_tier['\"]:\\s*['\"]?(curl_cffi|primp|selenium|ddgs|requests)['\"]?"
    tiers <- character(0)
    for (content in tool_contents) {
      matches <- gregexpr(tier_pattern, content, perl = TRUE)[[1]]
      if (matches[1] == -1) next
      match_texts <- regmatches(content, list(matches))[[1]]
      tiers <- c(tiers, gsub(tier_pattern, "\\1", match_texts, perl = TRUE))
    }
    tiers <- unique(tiers)
    return(tiers)
  }

  # Match '_tier': 'primp' patterns in Python dict repr
  # Handle both single and double quotes
  tier_pattern <- "['\"]_tier['\"]:\\s*['\"]?(curl_cffi|primp|selenium|ddgs|requests)['\"]?"
  matches <- gregexpr(tier_pattern, text, perl = TRUE)[[1]]

  if (matches[1] == -1) {
    return(character(0))
  }

  match_texts <- regmatches(text, list(matches))[[1]]
  tiers <- gsub(tier_pattern, "\\1", match_texts, perl = TRUE)
  unique(tiers)
}

#' Make a string safe to wrap as a JSON string literal
#'
#' Ensures every double quote is escaped (i.e., preceded by an odd number of
#' backslashes), so `jsonlite::fromJSON(paste0('"', x, '"'))` can be used to
#' decode one layer of trace-escaped content.
#'
#' @param x Character scalar
#' @keywords internal
.make_json_string_wrapper_safe <- function(x) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    return("")
  }

  chars <- strsplit(x, "", fixed = TRUE)[[1]]
  out <- character(0)

  for (ch in chars) {
    if (ch == '"') {
      # Count consecutive backslashes immediately before this quote in the output
      # built so far. If the count is even, the quote would terminate a JSON
      # string literal; insert one more backslash to escape it.
      bs_count <- 0L
      j <- length(out)
      while (j >= 1L && out[j] == "\\") {
        bs_count <- bs_count + 1L
        j <- j - 1L
      }
      if (bs_count %% 2L == 0L) {
        out <- c(out, "\\")
      }
      out <- c(out, ch)
    } else {
      out <- c(out, ch)
    }
  }

  paste0(out, collapse = "")
}

#' Decode one layer of trace-escaped content into a string
#'
#' Many raw traces contain AI message content escaped as a Python-esque string
#' literal (e.g. `\"`, `\\n`). This function attempts to decode one layer of
#' escaping using JSON string decoding, while normalizing `\\'` (a Python escape
#' that is not valid JSON) back to `'`.
#'
#' @param x Character scalar
#' @return Character scalar, or NULL
#' @keywords internal
.decode_trace_escaped_string <- function(x) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    return(NULL)
  }

  # JSON doesn't support `\'`, but Python repr does (when single-quoting). If it
  # exists here, it's not valid JSON text yet; normalize before decoding.
  x <- gsub("\\'", "'", x, fixed = TRUE)

  safe <- .make_json_string_wrapper_safe(x)
  wrapped <- paste0('"', safe, '"')
  decoded <- .try_or(jsonlite::fromJSON(wrapped))

  if (is.character(decoded) && length(decoded) == 1L) {
    decoded
  } else {
    NULL
  }
}

#' Try to parse a JSON candidate extracted from a raw trace
#'
#' @param x Character scalar candidate (may be escaped)
#' @param max_decode_layers Maximum number of decode layers to attempt
#' @return Parsed JSON (list/data.frame), or NULL
#' @keywords internal
.parse_trace_json_candidate <- function(x, max_decode_layers = 3L) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    return(NULL)
  }

  current <- x

  unescape_quotes_only <- function(txt) {
    # Convert `\"` -> `"` without decoding other escapes (e.g. preserve `\\`).
    gsub("\\\"", "\"", txt, fixed = TRUE)
  }

  parse_one <- function(txt) {
    # Prefer the shared robust extractor (handles JSON embedded in text).
    parsed <- .try_or(.parse_json_response(txt))
    if (is.list(parsed) && length(parsed) > 0) {
      return(parsed)
    }
    NULL
  }

  parsed <- parse_one(current)
  if (!is.null(parsed)) {
    return(parsed)
  }

  max_decode_layers <- as.integer(max_decode_layers)
  if (is.na(max_decode_layers) || max_decode_layers < 1L) {
    return(NULL)
  }

  for (i in seq_len(max_decode_layers)) {
    decoded <- .decode_trace_escaped_string(current)
    if (is.null(decoded) || identical(decoded, current)) {
      break
    }
    current <- decoded

    parsed <- parse_one(current)
    if (!is.null(parsed)) {
      return(parsed)
    }

    parsed <- parse_one(unescape_quotes_only(current))
    if (!is.null(parsed)) {
      return(parsed)
    }
  }

  # Last resort: if decoding didn't help, try unescaping quotes only once on the
  # original string (useful when backslashes are already JSON-level).
  parsed <- parse_one(unescape_quotes_only(x))
  if (!is.null(parsed)) {
    return(parsed)
  }

  NULL
}

#' Detect parsed tool-message envelope payloads that are not task answers
#' @param x Parsed JSON candidate
#' @return TRUE when the object matches a ToolMessage-like envelope
#' @keywords internal
.is_tool_message_envelope <- function(x) {
  if (!is.list(x) || length(x) == 0L || is.null(names(x))) {
    return(FALSE)
  }
  if (!("message_type" %in% names(x))) {
    return(FALSE)
  }
  message_type <- tolower(as.character(x$message_type %||% ""))
  if (!nzchar(message_type) || !(message_type %in% c("toolmessage", "tool", "function"))) {
    return(FALSE)
  }
  has_content <- "content" %in% names(x)
  has_tool_name <- "name" %in% names(x) || "tool_name" %in% names(x)
  has_tool_calls <- "tool_calls" %in% names(x)
  isTRUE(has_content && (has_tool_name || has_tool_calls))
}

#' Count the ratio of Unknown/empty fields in a parsed JSON list
#' @param parsed A named list from JSON extraction
#' @return Numeric ratio between 0 and 1
#' @keywords internal
.count_unknown_ratio <- function(parsed) {
  if (!is.list(parsed) || length(parsed) == 0) return(1.0)
  total <- 0L
  unknown <- 0L
  for (nm in names(parsed)) {
    # Skip source/meta fields for this count
    if (endsWith(nm, "_source") || nm %in% c("confidence", "justification")) next
    total <- total + 1L
    val <- parsed[[nm]]
    if (is.null(val) || (is.character(val) && (tolower(val) == "unknown" || val == ""))) {
      unknown <- unknown + 1L
    }
  }
  if (total == 0L) return(1.0)
  unknown / total
}

#' Mine tool message text for biographical field values
#'
#' Scans all ToolMessage contents in a trace for biographical data
#' (birth year, prior occupation, class background) using regex patterns.
#' Used as a fallback when the final AIMessage returns mostly Unknown fields.
#'
#' @param text Raw trace text
#' @return Named list of extracted field values, or NULL if nothing found
#' @keywords internal
.mine_trace_for_fields <- function(text) {
  NULL
}

.trace_field_mining_enabled <- function() {
  opt <- getOption("asa.enable_trace_field_mining", default = NULL)
  if (is.logical(opt) && length(opt) == 1L && !is.na(opt)) {
    return(isTRUE(opt))
  }

  env <- Sys.getenv("ASA_ENABLE_TRACE_FIELD_MINING", unset = "")
  if (!nzchar(env)) {
    return(FALSE)
  }

  tolower(trimws(env)) %in% c("1", "true", "t", "yes", "y", "on")
}

#' Get unknown-ratio threshold for optional trace mining fallback
#' @keywords internal
.trace_field_mining_unknown_ratio <- function(default = 0.6) {
  threshold <- suppressWarnings(as.numeric(getOption(
    "asa.trace_field_mining_unknown_ratio",
    default = default
  )))
  if (is.na(threshold) || threshold < 0 || threshold > 1) {
    return(default)
  }
  threshold
}

#' Extract JSON from trace, with optional heuristic trace mining
#'
#' By default this returns canonical terminal JSON extracted from the trace.
#' Heuristic mining of tool messages is opt-in via option/env var or explicit
#' `allow_trace_mining = TRUE`.
#'
#' @param text Raw trace text
#' @param allow_trace_mining Optional logical override. When NULL, uses
#'   `getOption("asa.enable_trace_field_mining", FALSE)` (or corresponding env).
#' @return Parsed JSON data as a list, or NULL if no JSON found
#' @keywords internal
.extract_json_from_trace <- function(text, allow_trace_mining = NULL) {
  # Try the standard extraction first
  json_data <- .extract_json_from_trace_inner(text)
  if (is.null(json_data)) {
    return(NULL)
  }

  if (is.null(allow_trace_mining)) {
    allow_trace_mining <- .trace_field_mining_enabled()
  }
  if (!isTRUE(allow_trace_mining)) {
    return(json_data)
  }

  # If mostly Unknown, enhance with trace mining
  unknown_ratio <- .count_unknown_ratio(json_data)
  if (unknown_ratio > .trace_field_mining_unknown_ratio()) {
    mined <- .mine_trace_for_fields(text)
    if (!is.null(mined) && is.list(mined)) {
      for (field in names(mined)) {
        current_val <- json_data[[field]]
        if (is.null(current_val) ||
            (is.character(current_val) && tolower(current_val) %in% c("unknown", ""))) {
          json_data[[field]] <- mined[[field]]
        }
      }
    }
  }

  json_data
}

#' Inner extraction logic for JSON from Agent Traces
#'
#' Internal function to extract JSON data from raw agent traces.
#' Called by .extract_json_from_trace which wraps it with trace mining fallback.
#'
#' @param text Raw trace text
#' @return Parsed JSON data as a list, or NULL if no JSON found
#' @keywords internal
.extract_json_from_trace_inner <- function(text) {
  # Structured trace JSON path (asa_trace_v1)
  msgs <- .parse_trace_json_messages(text)
  if (!is.null(msgs)) {
    ai_contents <- character(0)
    for (m in msgs) {
      if (!is.list(m)) next
      m_type <- tolower(as.character(m$message_type %||% ""))
      if (!(m_type %in% c("aimessage", "ai"))) next
      content <- as.character(m$content %||% "")
      if (nzchar(content)) {
        ai_contents <- c(ai_contents, content)
      }
    }
    if (length(ai_contents) > 0) {
      # Prefer the most recent AI message
      for (i in rev(seq_along(ai_contents))) {
        parsed <- .try_or(.parse_json_response(ai_contents[i]))
        if (is.list(parsed) && length(parsed) > 0 && !isTRUE(.is_tool_message_envelope(parsed))) {
          return(parsed)
        }
      }
    }
    # Structured traces are authoritative: if no AI terminal JSON is found,
    # do not fall back to generic brace-matching over tool payloads.
    return(NULL)
  }

  tryCatch({
    # Extract JSON from AIMessage
    json_patterns <- c(
      "AIMessage\\(content='(\\{[^']*(?:\\\\'[^']*)*\\})'",
      "AIMessage\\(content=\"(\\{[^\"]*(?:\\\\\"[^\"]*)*\\})\"",
      "AIMessage\\(content=['\"]([\\s\\S]*?)['\"], additional_kwargs",
      "(\\{[^{}]*\\})"
    )

    for (pattern in json_patterns) {
      matches <- regmatches(text, gregexpr(pattern, text, perl = TRUE))[[1]]

      if (length(matches) > 0) {
        # Try matches from last to first (most recent response)
        for (i in rev(seq_along(matches))) {
          json_str <- gsub(pattern, "\\1", matches[i], perl = TRUE)

          data <- .parse_trace_json_candidate(json_str, max_decode_layers = 3L)
          if (is.list(data) && length(data) > 0 && !isTRUE(.is_tool_message_envelope(data))) {
            return(data)
          }
        }
      }
    }

    NULL
  }, error = function(e) {
    warning(paste("Error extracting JSON:", e$message), call. = FALSE)
    NULL
  })
}

#' Process Multiple Agent Outputs
#'
#' Processes a data frame of raw agent outputs, extracting structured data.
#'
#' @param df Data frame with a 'raw_output' column containing agent traces
#' @param parallel Use parallel processing
#' @param workers Number of workers
#'
#' @return The input data frame with additional extracted columns:
#'   search_count, wiki_count, and any JSON fields found
#'
#' @export
process_outputs <- function(df, parallel = FALSE, workers = 10L) {
  # Validate inputs
  .validate_process_outputs(
    df = df,
    parallel = parallel,
    workers = workers
  )

  # Process function
  process_one <- function(raw_out) {
    result <- extract_agent_results(raw_out)
    list(
      search_count = length(result$search_snippets),
      wiki_count = length(result$wikipedia_snippets),
      json_data = result$json_data
    )
  }

  # Process all rows
  if (parallel &&
      requireNamespace("future", quietly = TRUE) &&
      requireNamespace("future.apply", quietly = TRUE)) {
    future::plan(future::multisession(workers = workers))
    on.exit(future::plan(future::sequential), add = TRUE)

    results <- future.apply::future_lapply(df$raw_output, process_one)
  } else {
    results <- lapply(df$raw_output, process_one)
  }

  # Add basic columns
  df$search_count <- vapply(results, function(r) r$search_count, integer(1))
  df$wiki_count <- vapply(results, function(r) r$wiki_count, integer(1))

  # Collect JSON field names from all results
  all_json_fields <- unique(unlist(lapply(results, function(r) {
    if (!is.null(r$json_data) && is.list(r$json_data)) {
      names(r$json_data)
    } else {
      character(0)
    }
  })))

  # Add JSON fields as columns
  for (field in all_json_fields) {
    df[[field]] <- vapply(results, function(r) {
      if (!is.null(r$json_data) && field %in% names(r$json_data)) {
        val <- r$json_data[[field]]
        if (length(val) == 1) as.character(val) else NA_character_
      } else {
        NA_character_
      }
    }, character(1))
  }

  df
}
