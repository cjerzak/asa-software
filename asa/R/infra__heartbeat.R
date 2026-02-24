#' Normalize Heartbeat Field Tokens
#'
#' @param x Value to normalize.
#' @param default Fallback token when \code{x} is empty.
#' @param max_chars Maximum output characters.
#' @return Sanitized token.
#' @keywords internal
.heartbeat_token <- function(x, default = "na", max_chars = 72L) {
  txt <- as.character(x)
  if (length(txt) == 0L || is.na(txt[[1]]) || !nzchar(txt[[1]])) {
    txt <- default
  } else {
    txt <- txt[[1]]
  }
  txt <- gsub("[[:space:]]+", "_", txt)
  txt <- gsub("[|=]", "/", txt)
  txt <- gsub("[^A-Za-z0-9_./:-]", "", txt)
  if (!nzchar(txt)) {
    txt <- default
  }
  if (nchar(txt) > max_chars) {
    txt <- substr(txt, 1L, max_chars)
  }
  txt
}


#' Set or Clear Progress State File for R/Python Runtime
#'
#' @param path Progress file path; empty value clears the setting.
#' @return Invisible NULL.
#' @keywords internal
.heartbeat_set_progress_state_file <- function(path = "") {
  has_path <- is.character(path) && length(path) == 1L && !is.na(path) && nzchar(path)
  if (has_path) {
    Sys.setenv(ASA_PROGRESS_STATE_FILE = path)
  } else {
    Sys.unsetenv("ASA_PROGRESS_STATE_FILE")
  }

  if (isTRUE(reticulate::py_available(initialize = FALSE))) {
    suppressWarnings(try({
      py_os <- reticulate::import("os")
      if (has_path) {
        py_os$environ[["ASA_PROGRESS_STATE_FILE"]] <- path
      } else {
        py_os$environ$pop("ASA_PROGRESS_STATE_FILE", NULL)
      }
    }, silent = TRUE))
  }

  invisible(NULL)
}


#' Check if Heartbeat Process is Alive
#'
#' @param pid Process id.
#' @return TRUE when process exists, otherwise FALSE.
#' @keywords internal
.heartbeat_process_alive <- function(pid) {
  pid <- suppressWarnings(as.integer(pid)[1])
  if (is.na(pid) || pid <= 0L) {
    return(FALSE)
  }
  isTRUE(.try_or(tools::pskill(pid, signal = 0L), FALSE))
}


#' Wait for Heartbeat Process Exit
#'
#' @param pid Process id.
#' @param timeout_sec Max seconds to wait.
#' @return TRUE when process exited before timeout, otherwise FALSE.
#' @keywords internal
.heartbeat_wait_for_exit <- function(pid, timeout_sec = 2) {
  timeout_sec <- max(0, as.numeric(timeout_sec %||% 0))
  if (!.heartbeat_process_alive(pid)) {
    return(TRUE)
  }
  deadline <- as.numeric(Sys.time()) + timeout_sec
  while (as.numeric(Sys.time()) < deadline) {
    if (!.heartbeat_process_alive(pid)) {
      return(TRUE)
    }
    Sys.sleep(0.05)
  }
  !.heartbeat_process_alive(pid)
}


#' Build Background Heartbeat Shell Script
#'
#' @param stopfile Stop signal file.
#' @param phasefile File used by R to communicate phase transitions.
#' @param progress_file Backend progress state file.
#' @param pidfile File where heartbeat writes its own pid.
#' @param label Display label.
#' @param interval_sec Mid-speed cadence in seconds.
#' @param stall_after_sec Idle threshold for "stalled" status.
#' @param r_pid R process id to monitor.
#' @return Single shell script string.
#' @keywords internal
.heartbeat_build_script <- function(stopfile,
                                    phasefile,
                                    progress_file,
                                    pidfile,
                                    label,
                                    interval_sec,
                                    stall_after_sec,
                                    r_pid) {
  paste(
    "#!/bin/sh",
    sprintf("STOPFILE=%s", shQuote(stopfile)),
    sprintf("PHASEFILE=%s", shQuote(phasefile)),
    sprintf("PROGRESS_FILE=%s", shQuote(progress_file)),
    sprintf("PIDFILE=%s", shQuote(pidfile)),
    sprintf("RPID=%d", as.integer(r_pid)),
    sprintf("LABEL=%s", shQuote(label)),
    sprintf("MID_INTERVAL=%d", as.integer(interval_sec)),
    sprintf("STALL_AFTER=%d", as.integer(stall_after_sec)),
    "echo $$ > \"$PIDFILE\"",
    "START_TS=$(date +%s)",
    "LAST_EVENT_TS=$START_TS",
    "HAS_LSOF=0",
    "if command -v lsof >/dev/null 2>&1; then HAS_LSOF=1; fi",
    "format_age() {",
    "  T=$1",
    "  H=$((T/3600))",
    "  M=$(((T%3600)/60))",
    "  S=$((T%60))",
    "  if [ \"$H\" -gt 0 ]; then",
    "    printf \"%dh%02dm%02ds\" \"$H\" \"$M\" \"$S\"",
    "  else",
    "    printf \"%dm%02ds\" \"$M\" \"$S\"",
    "  fi",
    "}",
    "get_kv() {",
    "  KEY=\"$1\"",
    "  LINE=\"$2\"",
    "  printf \"%s\\n\" \"$LINE\" | awk -v key=\"$KEY\" '{for (i=1; i<=NF; i++) {split($i, a, \"=\"); if (a[1]==key) {print a[2]; exit}}}'",
    "}",
    "while [ ! -f \"$STOPFILE\" ] && kill -0 \"$RPID\" 2>/dev/null; do",
    "  NOW_TS=$(date +%s)",
    "  ELAPSED=$((NOW_TS-START_TS))",
    "  PHASE=bootstrap",
    "  DETAIL=none",
    "  PHASE_TS=$START_TS",
    "  if [ -f \"$PHASEFILE\" ]; then",
    "    PHASE_LINE=$(cat \"$PHASEFILE\" 2>/dev/null)",
    "    if [ -n \"$PHASE_LINE\" ]; then",
    "      PHASE=$(printf \"%s\" \"$PHASE_LINE\" | awk -F'|' '{print $1}')",
    "      DETAIL=$(printf \"%s\" \"$PHASE_LINE\" | awk -F'|' '{print $2}')",
    "      PHASE_TS=$(printf \"%s\" \"$PHASE_LINE\" | awk -F'|' '{print $3}')",
    "    fi",
    "  fi",
    "  PROGRESS_LINE=",
    "  if [ -f \"$PROGRESS_FILE\" ]; then",
    "    PROGRESS_LINE=$(cat \"$PROGRESS_FILE\" 2>/dev/null)",
    "  fi",
    "  NODE=$(get_kv node \"$PROGRESS_LINE\")",
    "  TOOL_USED=$(get_kv tool_used \"$PROGRESS_LINE\")",
    "  TOOL_LIMIT=$(get_kv tool_limit \"$PROGRESS_LINE\")",
    "  TOOL_REMAINING=$(get_kv tool_rem \"$PROGRESS_LINE\")",
    "  FIELDS_RESOLVED=$(get_kv resolved \"$PROGRESS_LINE\")",
    "  FIELDS_TOTAL=$(get_kv total \"$PROGRESS_LINE\")",
    "  FIELDS_UNKNOWN=$(get_kv unknown \"$PROGRESS_LINE\")",
    "  EVENT_TS=$(get_kv ts \"$PROGRESS_LINE\")",
    "  if [ -z \"$NODE\" ]; then NODE=na; fi",
    "  if [ -z \"$TOOL_USED\" ]; then TOOL_USED=na; fi",
    "  if [ -z \"$TOOL_LIMIT\" ]; then TOOL_LIMIT=na; fi",
    "  if [ -z \"$TOOL_REMAINING\" ]; then TOOL_REMAINING=na; fi",
    "  if [ -z \"$FIELDS_RESOLVED\" ]; then FIELDS_RESOLVED=na; fi",
    "  if [ -z \"$FIELDS_TOTAL\" ]; then FIELDS_TOTAL=na; fi",
    "  if [ -z \"$FIELDS_UNKNOWN\" ]; then FIELDS_UNKNOWN=na; fi",
    "  if [ -z \"$EVENT_TS\" ]; then EVENT_TS=$START_TS; fi",
    "  if [ \"$PHASE_TS\" -gt \"$EVENT_TS\" ] 2>/dev/null; then EVENT_TS=$PHASE_TS; fi",
    "  if [ \"$EVENT_TS\" -gt \"$LAST_EVENT_TS\" ] 2>/dev/null; then LAST_EVENT_TS=$EVENT_TS; fi",
    "  PY_PID=$(pgrep -P \"$RPID\" -f \"python|reticulate|asa_backend\" 2>/dev/null | head -n 1)",
    "  METRIC_PID=$RPID",
    "  if [ -n \"$PY_PID\" ]; then METRIC_PID=$PY_PID; fi",
    "  PS_LINE=$(ps -p \"$METRIC_PID\" -o %cpu= -o rss= 2>/dev/null | awk 'NR==1 {print $1 \" \" $2}')",
    "  CPU=$(printf \"%s\" \"$PS_LINE\" | awk '{if (NF>=1) printf(\"%.1f\", ($1+0)); else printf(\"0.0\")}')",
    "  RSS_MB=$(printf \"%s\" \"$PS_LINE\" | awk '{if (NF>=2) printf(\"%d\", int(($2+0)/1024)); else printf(\"0\")}')",
    "  TCP_COUNT=0",
    "  HOSTS=none",
    "  if [ \"$HAS_LSOF\" -eq 1 ]; then",
    "    TCP_COUNT=$(lsof -nP -a -p \"$METRIC_PID\" -iTCP -sTCP:ESTABLISHED 2>/dev/null | awk 'NR>1 {n++} END {print n+0}')",
    "    HOSTS=$(lsof -nP -a -p \"$METRIC_PID\" -iTCP -sTCP:ESTABLISHED 2>/dev/null | awk 'NR>1 {split($9,a,\"->\"); if (length(a)>1) {split(a[2],b,\":\"); h=b[1]; if (h!=\"\") c[h]++}} END {for (h in c) print c[h],h}' | sort -rn | head -n 3 | awk '{printf(\"%s%s(%s)\", sep, $2, $1); sep=\",\"}')",
    "    if [ -z \"$HOSTS\" ]; then HOSTS=none; fi",
    "  fi",
    "  CPU_ACTIVE=$(awk -v c=\"$CPU\" 'BEGIN {print ((c+0)>=3.0) ? 1 : 0}')",
    "  if [ \"$CPU_ACTIVE\" -eq 1 ] || [ \"$TCP_COUNT\" -gt 0 ]; then LAST_EVENT_TS=$NOW_TS; fi",
    "  IDLE_FOR=$((NOW_TS-LAST_EVENT_TS))",
    "  STATUS=active",
    "  SLEEP_SECS=10",
    "  if [ \"$IDLE_FOR\" -ge \"$STALL_AFTER\" ]; then",
    "    STATUS=stalled",
    "    SLEEP_SECS=30",
    "  elif [ \"$IDLE_FOR\" -ge 45 ]; then",
    "    STATUS=slow",
    "    SLEEP_SECS=$MID_INTERVAL",
    "  fi",
    "  WAIT_REASON=backend_wait",
    "  if [ \"$TCP_COUNT\" -gt 0 ]; then",
    "    WAIT_REASON=network_io",
    "  elif [ \"$CPU_ACTIVE\" -eq 1 ]; then",
    "    WAIT_REASON=local_compute",
    "  fi",
    "  PHASE_DISPLAY=$PHASE",
    "  if [ -n \"$DETAIL\" ] && [ \"$DETAIL\" != \"none\" ]; then PHASE_DISPLAY=\"${PHASE}(${DETAIL})\"; fi",
    "  ELAPSED_TXT=$(format_age \"$ELAPSED\")",
    "  IDLE_TXT=$(format_age \"$IDLE_FOR\")",
    "  printf \"[heartbeat] %s\\n\" \"$LABEL\"",
    "  printf \"  runtime  elapsed=%s    idle=%s    cadence=%ss\\n\" \"$ELAPSED_TXT\" \"$IDLE_TXT\" \"$SLEEP_SECS\"",
    "  printf \"  stage    phase=%s    status=%s    reason=%s    node=%s\\n\" \"$PHASE_DISPLAY\" \"$STATUS\" \"$WAIT_REASON\" \"$NODE\"",
    "  printf \"  budget   tools=%s/%s(rem=%s)    fields=%s/%s(unknown=%s)\\n\" \"$TOOL_USED\" \"$TOOL_LIMIT\" \"$TOOL_REMAINING\" \"$FIELDS_RESOLVED\" \"$FIELDS_TOTAL\" \"$FIELDS_UNKNOWN\"",
    "  printf \"  system   cpu=%s%%    rss=%sMB    tcp=%s    hosts=%s\\n\\n\" \"$CPU\" \"$RSS_MB\" \"$TCP_COUNT\" \"$HOSTS\"",
    "  sleep \"$SLEEP_SECS\"",
    "done",
    sep = "\n"
  )
}


#' Start Background Heartbeat
#'
#' @param label Label shown in heartbeat output.
#' @param interval_sec Mid-speed cadence in seconds.
#' @param stall_after_sec Idle threshold for "stalled" status.
#' @param verbose Emit local start diagnostics.
#' @return Heartbeat handle list.
#' @keywords internal
.heartbeat_start <- function(label = "run_task",
                             interval_sec = 20L,
                             stall_after_sec = 180L,
                             verbose = FALSE) {
  interval_sec <- max(10L, min(30L, as.integer(interval_sec %||% 20L)))
  stall_after_sec <- max(60L, as.integer(stall_after_sec %||% 180L))
  hb_label <- .heartbeat_token(label, default = "run_task", max_chars = 64L)

  stopfile <- tempfile("asa_heartbeat_stop_")
  phasefile <- tempfile("asa_heartbeat_phase_")
  progress_file <- tempfile("asa_heartbeat_progress_")
  scriptfile <- tempfile("asa_heartbeat_script_")
  pidfile <- tempfile("asa_heartbeat_pid_")
  old_progress_file <- Sys.getenv("ASA_PROGRESS_STATE_FILE", unset = "")

  hb <- structure(
    list(
      pid = NA_integer_,
      stopfile = stopfile,
      phasefile = phasefile,
      progress_file = progress_file,
      scriptfile = scriptfile,
      pidfile = pidfile,
      label = hb_label,
      old_progress_file = old_progress_file
    ),
    class = c("asa_heartbeat_handle", "list")
  )

  .heartbeat_set_phase(hb, phase = "bootstrap", detail = "init")

  script <- .heartbeat_build_script(
    stopfile = stopfile,
    phasefile = phasefile,
    progress_file = progress_file,
    pidfile = pidfile,
    label = hb_label,
    interval_sec = interval_sec,
    stall_after_sec = stall_after_sec,
    r_pid = Sys.getpid()
  )
  writeLines(script, con = scriptfile, useBytes = TRUE)
  Sys.chmod(scriptfile, mode = "700")

  .heartbeat_set_progress_state_file(progress_file)
  launch_status <- .try_or(
    suppressWarnings(system2("sh", c(scriptfile), wait = FALSE, stdout = "", stderr = "")),
    1L
  )
  if (isTRUE(verbose)) {
    message(sprintf("Heartbeat launch status=%s label=%s", as.character(launch_status), hb_label))
  }

  pid <- NA_integer_
  for (i in seq_len(50L)) {
    if (!file.exists(pidfile)) {
      Sys.sleep(0.05)
      next
    }
    pid_text <- .try_or(readLines(pidfile, n = 1L, warn = FALSE), character(0))
    if (length(pid_text) > 0L) {
      parsed <- suppressWarnings(as.integer(gsub("[^0-9]", "", pid_text[[1]])))
      if (length(parsed) > 0L && !is.na(parsed[[1]]) && parsed[[1]] > 0L) {
        pid <- parsed[[1]]
        break
      }
    }
    Sys.sleep(0.05)
  }

  hb$pid <- pid
  if (isTRUE(verbose)) {
    message(sprintf(
      "Heartbeat started label=%s pid=%s progress_file=%s",
      hb_label,
      if (is.na(pid)) "NA" else as.character(pid),
      progress_file
    ))
  }
  if (is.na(pid) || pid <= 0L) {
    warning(
      "Heartbeat process PID was not detected; cleanup will proceed via files/env only.",
      call. = FALSE
    )
  }

  hb
}


#' Update Heartbeat Phase File
#'
#' @param hb Heartbeat handle.
#' @param phase Phase token.
#' @param detail Additional detail token.
#' @return Invisible NULL.
#' @keywords internal
.heartbeat_set_phase <- function(hb, phase = "running", detail = NA_character_) {
  if (is.null(hb) || !is.list(hb)) {
    return(invisible(NULL))
  }
  phasefile <- hb$phasefile
  if (!is.character(phasefile) || length(phasefile) != 1L || !nzchar(phasefile)) {
    return(invisible(NULL))
  }
  phase_token <- .heartbeat_token(phase, default = "running")
  detail_token <- .heartbeat_token(detail, default = "none")
  ts_now <- as.integer(Sys.time())
  payload <- sprintf("%s|%s|%d", phase_token, detail_token, ts_now)
  suppressWarnings(try(writeLines(payload, con = phasefile, useBytes = TRUE), silent = TRUE))
  invisible(NULL)
}


#' Stop Background Heartbeat and Restore Environment
#'
#' @param hb Heartbeat handle.
#' @param wait_timeout_sec Graceful termination wait.
#' @param kill_timeout_sec Hard-kill wait.
#' @param verbose Emit local stop diagnostics.
#' @return Invisible NULL.
#' @keywords internal
.heartbeat_stop <- function(hb,
                            wait_timeout_sec = 2,
                            kill_timeout_sec = 2,
                            verbose = FALSE) {
  if (is.null(hb) || !is.list(hb)) {
    return(invisible(NULL))
  }

  stopfile <- hb$stopfile
  if (is.character(stopfile) && length(stopfile) == 1L && nzchar(stopfile)) {
    suppressWarnings(try(file.create(stopfile), silent = TRUE))
  }

  pid <- suppressWarnings(as.integer(hb$pid)[1])
  if (!is.na(pid) && pid > 0L && .heartbeat_process_alive(pid)) {
    suppressWarnings(try(tools::pskill(pid, signal = tools::SIGTERM), silent = TRUE))
    .heartbeat_wait_for_exit(pid, timeout_sec = wait_timeout_sec)
    if (.heartbeat_process_alive(pid)) {
      suppressWarnings(try(tools::pskill(pid, signal = tools::SIGKILL), silent = TRUE))
      .heartbeat_wait_for_exit(pid, timeout_sec = kill_timeout_sec)
    }
  }

  old_progress_file <- hb$old_progress_file
  if (is.character(old_progress_file) && length(old_progress_file) == 1L && nzchar(old_progress_file)) {
    .heartbeat_set_progress_state_file(old_progress_file)
  } else {
    .heartbeat_set_progress_state_file("")
  }

  cleanup_files <- c(
    hb$phasefile %||% "",
    hb$progress_file %||% "",
    hb$pidfile %||% "",
    hb$scriptfile %||% "",
    hb$stopfile %||% ""
  )
  cleanup_files <- as.character(cleanup_files)
  cleanup_files <- cleanup_files[!is.na(cleanup_files) & nzchar(cleanup_files)]
  if (length(cleanup_files) > 0L) {
    suppressWarnings(try(unlink(unique(cleanup_files), force = TRUE), silent = TRUE))
  }

  if (isTRUE(verbose)) {
    message(sprintf(
      "Heartbeat stopped label=%s pid=%s alive=%s",
      as.character(hb$label %||% "run_task"),
      if (is.na(pid)) "NA" else as.character(pid),
      if (is.na(pid)) "FALSE" else as.character(.heartbeat_process_alive(pid))
    ))
  }

  invisible(NULL)
}


#' Run a Function with Task-Scoped Heartbeat
#'
#' @param fn Function to execute.
#' @param label Label shown in heartbeat output.
#' @param interval_sec Mid-speed cadence in seconds.
#' @param stall_after_sec Idle threshold for "stalled" status.
#' @param start_phase Phase marker set before \code{fn()}.
#' @param start_detail Detail marker set before \code{fn()}.
#' @param complete_phase Phase marker set after successful \code{fn()}.
#' @param complete_detail Detail marker set after successful \code{fn()}.
#' @param error_phase Phase marker set when \code{fn()} errors.
#' @param verbose Emit local start/stop diagnostics.
#' @return Result of \code{fn()}.
#' @keywords internal
.with_heartbeat <- function(fn,
                            label = "run_task",
                            interval_sec = 20L,
                            stall_after_sec = 180L,
                            start_phase = "backend_invoke",
                            start_detail = "run_task",
                            complete_phase = "backend_complete",
                            complete_detail = "run_task_returned",
                            error_phase = "backend_error",
                            verbose = FALSE) {
  if (!is.function(fn)) {
    stop("`fn` must be a function.", call. = FALSE)
  }

  hb <- .heartbeat_start(
    label = label,
    interval_sec = interval_sec,
    stall_after_sec = stall_after_sec,
    verbose = verbose
  )
  on.exit(.heartbeat_stop(hb, verbose = verbose), add = TRUE)

  .heartbeat_set_phase(hb, phase = start_phase, detail = start_detail)
  tryCatch(
    {
      result <- fn()
      .heartbeat_set_phase(hb, phase = complete_phase, detail = complete_detail)
      result
    },
    error = function(e) {
      .heartbeat_set_phase(hb, phase = error_phase, detail = conditionMessage(e))
      stop(e)
    }
  )
}
