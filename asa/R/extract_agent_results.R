#' Extract Structured Data from Agent Traces
#'
#' Parses raw agent output to extract search snippets, Wikipedia content,
#' URLs, JSON data, and search tier information. This is the main function
#' for post-processing agent traces.
#'
#' @param raw_output Raw output string from agent invocation (the trace field
#'   from an asa_response object), or a structured JSON trace (asa_trace_v1)
#'   from \code{asa_result$trace_json}
#'
#' @return A list with components:
#' \itemize{
#'   \item search_snippets: Character vector of search result content
#'   \item search_urls: Character vector of URLs from search results
#'   \item wikipedia_snippets: Character vector of Wikipedia content
#'   \item json_data: Extracted JSON data (canonical by default; see option
#'     \code{asa.enable_trace_field_mining})
#'   \item json_data_canonical: Canonical terminal JSON extracted from trace
#'   \item json_data_inferred: Optional inferred JSON with trace-mining enabled
#'   \item search_tiers: Character vector of unique search tiers used
#'     (e.g., "primp", "selenium", "ddgs", "requests")
#' }
#'
#' @examples
#' \dontrun{
#' response <- run_agent("Who is the president of France?", agent)
#' extracted <- extract_agent_results(response$trace)
#' print(extracted$search_snippets)
#' print(extracted$search_tiers)  # Shows which search tier was used
#' }
#'
#' @export
extract_agent_results <- function(raw_output) {
  if (is.null(raw_output) || raw_output == "") {
    return(list(
      search_snippets = character(0),
      search_urls = character(0),
      wikipedia_snippets = character(0),
      json_data = NULL,
      json_data_canonical = NULL,
      json_data_inferred = NULL,
      search_tiers = character(0)
    ))
  }

  # Extract components
  json_data <- .extract_json_from_trace(raw_output)
  json_data_canonical <- .extract_json_from_trace_inner(raw_output)
  json_data_inferred <- .extract_json_from_trace(raw_output)

  list(
    search_snippets = extract_search_snippets(raw_output),
    search_urls = extract_urls(raw_output),
    wikipedia_snippets = extract_wikipedia_content(raw_output),
    json_data = json_data,
    json_data_canonical = json_data_canonical,
    json_data_inferred = json_data_inferred,
    search_tiers = extract_search_tiers(raw_output)
  )
}

