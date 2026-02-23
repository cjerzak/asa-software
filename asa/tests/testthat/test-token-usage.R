# Tests for Python-side token usage extraction.

test_that("state_utils parses response_metadata token_usage with input/output keys", {
  python_path <- asa_test_skip_if_no_python(
    required_files = c("shared/state_graph_utils.py", "shared/message_text_utils.py"),
    initialize = TRUE
  )
  state_utils <- asa_test_import_from_path_or_skip("shared.state_graph_utils", python_path)

  reticulate::py_run_string(paste(
    "class _TokenMsg:",
    "    def __init__(self):",
    "        self.response_metadata = {'token_usage': {'input_tokens': 101, 'output_tokens': 19}}",
    "msg_token_usage = _TokenMsg()",
    sep = "\n"
  ))

  usage <- reticulate::py_to_r(
    state_utils$`_token_usage_dict_from_message`(reticulate::py$msg_token_usage)
  )

  expect_equal(usage$input_tokens, 101L)
  expect_equal(usage$output_tokens, 19L)
  expect_equal(usage$total_tokens, 120L)
})

test_that("state_utils parses camelCase usage metadata for code execution paths", {
  python_path <- asa_test_skip_if_no_python(
    required_files = c("shared/state_graph_utils.py", "shared/message_text_utils.py"),
    initialize = TRUE
  )
  state_utils <- asa_test_import_from_path_or_skip("shared.state_graph_utils", python_path)

  reticulate::py_run_string(paste(
    "class _CamelUsageMsg:",
    "    def __init__(self):",
    "        self.response_metadata = {",
    "            'usage_metadata': {",
    "                'inputTokenCount': 220,",
    "                'outputTokenCount': 40,",
    "                'totalTokenCount': 260",
    "            }",
    "        }",
    "msg_camel_usage = _CamelUsageMsg()",
    sep = "\n"
  ))

  usage <- reticulate::py_to_r(
    state_utils$`_token_usage_dict_from_message`(reticulate::py$msg_camel_usage)
  )

  expect_equal(usage$input_tokens, 220L)
  expect_equal(usage$output_tokens, 40L)
  expect_equal(usage$total_tokens, 260L)
})

test_that("state_utils falls back to additional_kwargs response metadata", {
  python_path <- asa_test_skip_if_no_python(
    required_files = c("shared/state_graph_utils.py", "shared/message_text_utils.py"),
    initialize = TRUE
  )
  state_utils <- asa_test_import_from_path_or_skip("shared.state_graph_utils", python_path)

  reticulate::py_run_string(paste(
    "class _AdditionalKwargsMsg:",
    "    def __init__(self):",
    "        self.additional_kwargs = {",
    "            'response_metadata': {",
    "                'token_usage': {'prompt_tokens': 14, 'completion_tokens': 6}",
    "            }",
    "        }",
    "msg_additional_kwargs = _AdditionalKwargsMsg()",
    sep = "\n"
  ))

  usage <- reticulate::py_to_r(
    state_utils$`_token_usage_dict_from_message`(reticulate::py$msg_additional_kwargs)
  )

  expect_equal(usage$input_tokens, 14L)
  expect_equal(usage$output_tokens, 6L)
  expect_equal(usage$total_tokens, 20L)
})
