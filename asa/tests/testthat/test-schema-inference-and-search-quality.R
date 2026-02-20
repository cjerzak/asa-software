test_that("infer_required_json_schema recognizes schema: anchors", {
  utils <- asa_test_import_module("state_utils")

  prompt <- paste0(
    "Return STRICT JSON only with this schema:\n",
    "{\n",
    "  \"status\": \"complete\"|\"partial\",\n",
    "  \"steps\": [{\"step\": integer, \"fact\": string, \"checksum\": integer}],\n",
    "  \"missing\": [integer],\n",
    "  \"notes\": string\n",
    "}\n"
  )

  schema <- utils$infer_required_json_schema(prompt)
  expect_true(is.list(schema))
  expect_true(all(c("status", "steps", "missing", "notes") %in% names(schema)))
  expect_true(is.list(schema$steps))
  expect_true(length(schema$steps) >= 1L)
})

test_that("structured Search JSON is not flagged as empty low-signal output", {
  python_path <- asa_test_skip_if_no_python(
    required_files = "asa_backend/graph/_legacy_agent_graph.py",
    initialize = FALSE
  )
  asa_test_require_langgraph_stack(ASA_TEST_LANGGRAPH_MODULES)

  graph <- tryCatch(
    reticulate::import_from_path("asa_backend.graph._legacy_agent_graph", path = python_path),
    error = function(e) {
      skip(paste0("Failed to import graph module: ", conditionMessage(e)))
    }
  )

  classifier <- graph[["_classify_tool_message_quality"]]

  quality <- reticulate::py_to_r(classifier(reticulate::dict(
    type = "tool",
    name = "Search",
    content = "{\"step\":1,\"fact\":\"fact_1\",\"checksum\":11}"
  )))
  expect_false(isTRUE(quality$is_empty))
  expect_false(isTRUE(quality$is_off_target))

  empty_quality <- reticulate::py_to_r(classifier(reticulate::dict(
    type = "tool",
    name = "Search",
    content = ""
  )))
  expect_true(isTRUE(empty_quality$is_empty))
})
