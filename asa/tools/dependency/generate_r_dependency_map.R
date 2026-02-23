#!/usr/bin/env Rscript

suppressWarnings(options(stringsAsFactors = FALSE))

args <- commandArgs(trailingOnly = TRUE)
repo_root <- if (length(args) >= 1L) normalizePath(args[[1]], winslash = "/", mustWork = TRUE) else normalizePath(getwd(), winslash = "/", mustWork = TRUE)
out_dir <- if (length(args) >= 2L) normalizePath(args[[2]], winslash = "/", mustWork = FALSE) else file.path(repo_root, "tracked_reports", "dependency")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

r_dir <- file.path(repo_root, "asa", "R")
if (!dir.exists(r_dir)) {
  stop("Could not find R directory: ", r_dir, call. = FALSE)
}

escape_regex <- function(x) {
  gsub("([][{}()+*^$|\\\\.?])", "\\\\\\1", x)
}

to_rel <- function(path) {
  path_norm <- normalizePath(path, winslash = "/", mustWork = FALSE)
  sub(paste0("^", escape_regex(repo_root), "/?"), "", path_norm, perl = TRUE)
}

node_id <- function(path) {
  paste0("r_", gsub("[^A-Za-z0-9_]", "_", path))
}

read_lines_safely <- function(path) {
  tryCatch(readLines(path, warn = FALSE), error = function(e) character())
}

extract_function_defs <- function(lines) {
  out <- character()
  for (ln in lines) {
    m <- regexec("^\\s*([.A-Za-z][A-Za-z0-9._]*)\\s*<-\\s*function\\s*\\(", ln, perl = TRUE)
    hit <- regmatches(ln, m)[[1]]
    if (length(hit) >= 2L) out <- c(out, hit[[2]])
  }
  unique(out)
}

extract_python_modules <- function(lines) {
  mods <- character()
  for (ln in lines) {
    m1 <- gregexpr("\\.import_python_module\\(\\s*['\"]([^'\"]+)['\"]", ln, perl = TRUE)
    hits1 <- regmatches(ln, m1)[[1]]
    if (length(hits1) > 0L) {
      vals <- sub("^.*\\(\\s*['\"]([^'\"]+)['\"].*$", "\\1", hits1, perl = TRUE)
      mods <- c(mods, vals)
    }
    m2 <- gregexpr("import_from_path\\(\\s*['\"]([^'\"]+)['\"]", ln, perl = TRUE)
    hits2 <- regmatches(ln, m2)[[1]]
    if (length(hits2) > 0L) {
      vals <- sub("^.*\\(\\s*['\"]([^'\"]+)['\"].*$", "\\1", hits2, perl = TRUE)
      mods <- c(mods, vals)
    }
  }
  unique(mods[nzchar(mods)])
}

files_abs <- sort(list.files(r_dir, pattern = "\\.R$", full.names = TRUE))
files_rel <- vapply(files_abs, to_rel, character(1))
names(files_rel) <- files_abs

file_lines <- lapply(files_abs, read_lines_safely)
file_lines_no_comments <- lapply(file_lines, function(lines) gsub("#.*$", "", lines))

function_to_file <- list()
for (i in seq_along(files_abs)) {
  defs <- extract_function_defs(file_lines[[i]])
  if (length(defs) == 0L) next
  rel <- files_rel[[i]]
  for (fn in defs) {
    if (is.null(function_to_file[[fn]])) function_to_file[[fn]] <- rel
  }
}

edge_from <- character()
edge_to <- character()
edge_call <- character()

for (i in seq_along(files_abs)) {
  from_rel <- files_rel[[i]]
  lines <- file_lines_no_comments[[i]]
  if (length(lines) == 0L) next
  for (fn in names(function_to_file)) {
    to_rel_path <- function_to_file[[fn]]
    if (identical(from_rel, to_rel_path)) next
    pat <- paste0("\\b", escape_regex(fn), "\\s*\\(")
    if (any(grepl(pat, lines, perl = TRUE))) {
      edge_from <- c(edge_from, from_rel)
      edge_to <- c(edge_to, to_rel_path)
      edge_call <- c(edge_call, fn)
    }
  }
}

edges_df <- unique(data.frame(
  from = edge_from,
  to = edge_to,
  call = edge_call,
  stringsAsFactors = FALSE
))

key_calls <- c(
  "run_task",
  "initialize_agent",
  ".run_agent",
  "asa_enumerate",
  "asa_audit",
  ".import_python_module",
  ".import_backend_api",
  ".with_runtime_wrappers"
)

key_df <- edges_df[edges_df$call %in% key_calls, , drop = FALSE]

py_module_from <- character()
py_module_to <- character()
py_module_kind <- character()
for (i in seq_along(files_abs)) {
  rel <- files_rel[[i]]
  modules <- extract_python_modules(file_lines_no_comments[[i]])
  if (length(modules) == 0L) next
  py_module_from <- c(py_module_from, rep(rel, length(modules)))
  py_module_to <- c(py_module_to, modules)
  py_module_kind <- c(py_module_kind, rep("python_module_import", length(modules)))
}
py_module_df <- unique(data.frame(
  from = py_module_from,
  to_module = py_module_to,
  kind = py_module_kind,
  stringsAsFactors = FALSE
))

node_lines <- vapply(files_rel, function(rel) {
  sprintf("  %s[\"%s\"]", node_id(rel), basename(rel))
}, character(1))

edge_lines <- character()
if (nrow(edges_df) > 0L) {
  edge_lines <- apply(edges_df, 1, function(row) {
    sprintf(
      "  %s -->|%s| %s",
      node_id(row[["from"]]),
      row[["call"]],
      node_id(row[["to"]])
    )
  })
}

r_graph_path <- file.path(out_dir, "r_file_graph.mmd")
writeLines(
  c("graph TD", sort(unique(node_lines)), sort(unique(edge_lines))),
  con = r_graph_path,
  useBytes = TRUE
)

manifest <- list(
  language = "R",
  generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  nodes = unname(lapply(files_rel, function(rel) list(id = rel, label = basename(rel)))),
  edges = if (nrow(edges_df) > 0L) unname(lapply(seq_len(nrow(edges_df)), function(i) {
    row <- edges_df[i, , drop = FALSE]
    list(
      from = row$from[[1]],
      to = row$to[[1]],
      kind = "function_call",
      call = row$call[[1]]
    )
  })) else list(),
  key_function_edges = if (nrow(key_df) > 0L) unname(lapply(seq_len(nrow(key_df)), function(i) {
    row <- key_df[i, , drop = FALSE]
    list(
      from_file = row$from[[1]],
      to_file = row$to[[1]],
      call = row$call[[1]]
    )
  })) else list(),
  python_module_edges = if (nrow(py_module_df) > 0L) unname(lapply(seq_len(nrow(py_module_df)), function(i) {
    row <- py_module_df[i, , drop = FALSE]
    list(
      from_file = row$from[[1]],
      to_module = row$to_module[[1]],
      kind = row$kind[[1]]
    )
  })) else list()
)

manifest_path <- file.path(out_dir, "r_manifest.json")
jsonlite::write_json(manifest, manifest_path, pretty = TRUE, auto_unbox = TRUE)

message("Wrote: ", r_graph_path)
message("Wrote: ", manifest_path)
