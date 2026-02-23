# Tests for scratchpad persistence and prompt injection via save_finding tool.

test_that("save_finding tool_calls append to scratchpad and are injected into subsequent system prompts", {
  asa_test_skip_if_no_python(required_files = "asa_backend/api/agent_api.py")
  asa_test_skip_if_missing_python_modules(c(
    "langchain_core",
    "langgraph",
    "langgraph.prebuilt",
    "pydantic"
  ), method = "import")

  custom_ddg <- asa_test_import_langgraph_module("custom_ddg_production")
  msgs <- reticulate::import("langchain_core.messages", convert = TRUE)

  # Stub LLM:
  # 1) On first call, emits two save_finding tool calls (one with an invalid category).
  # 2) On second call, returns a final answer (no tool calls).
  # It also records system prompts so we can assert scratchpad prompt injection happened.
  reticulate::py_run_string(paste0(
    "from langchain_core.messages import AIMessage\n",
    "\n",
    "class _RecordingScratchpadLLM:\n",
    "    def __init__(self):\n",
    "        self.n = 0\n",
    "        self.system_prompts = []\n",
    "    def bind_tools(self, tools):\n",
    "        return self\n",
    "    def invoke(self, messages):\n",
    "        self.n += 1\n",
    "        try:\n",
    "            self.system_prompts.append(getattr(messages[0], 'content', None))\n",
    "        except Exception:\n",
    "            self.system_prompts.append(None)\n",
    "        if self.n == 1:\n",
    "            return AIMessage(\n",
    "                content='calling save_finding',\n",
    "                tool_calls=[\n",
    "                    {'name':'save_finding','args':{'finding':'FINDING_A','category':'insight'},'id':'call_1'},\n",
    "                    {'name':'save_finding','args':{'finding':'FINDING_B','category':'not-a-real-cat'},'id':'call_2'},\n",
    "                ],\n",
    "            )\n",
    "        return AIMessage(content='done')\n",
    "\n",
    "stub_llm = _RecordingScratchpadLLM()\n"
  ))

  agent <- custom_ddg$create_standard_agent(
    model = reticulate::py$stub_llm,
    tools = list(),
    checkpointer = NULL,
    debug = FALSE
  )

  initial <- msgs$HumanMessage(content = "hi")

  final_state <- agent$invoke(
    list(messages = list(initial)),
    config = list(
      recursion_limit = as.integer(20),
      configurable = list(thread_id = "test_scratchpad")
    )
  )

  # The tool node wrapper should have extracted tool_calls into state$scratchpad.
  expect_true(is.list(final_state$scratchpad))
  expect_true(length(final_state$scratchpad) >= 2L)
  expect_equal(final_state$scratchpad[[1]]$finding, "FINDING_A")
  expect_equal(final_state$scratchpad[[1]]$category, "insight")
  expect_equal(final_state$scratchpad[[2]]$finding, "FINDING_B")
  # Invalid categories should be sanitized to "fact".
  expect_equal(final_state$scratchpad[[2]]$category, "fact")

  # The next agent turn should include the scratchpad block in the system prompt.
  sys_prompts <- reticulate::py_to_r(reticulate::py$stub_llm$system_prompts)
  expect_true(length(sys_prompts) >= 2L)
  expect_false(grepl("=== YOUR SCRATCHPAD (saved findings) ===", sys_prompts[[1]], fixed = TRUE))
  expect_true(grepl("=== YOUR SCRATCHPAD (saved findings) ===", sys_prompts[[2]], fixed = TRUE))
  expect_true(grepl("[insight] FINDING_A", sys_prompts[[2]], fixed = TRUE))
  expect_true(grepl("[fact] FINDING_B", sys_prompts[[2]], fixed = TRUE))
})
