# Tests for invoke_graph_safely() (GraphRecursionError guard)

test_that("invoke_graph_safely returns last state on GraphRecursionError", {
  prod <- asa_test_import_langgraph_module(
    "custom_ddg_production",
    required_files = "custom_ddg_production.py",
    required_modules = ASA_TEST_LANGGRAPH_MODULES
  )

  # Build a minimal LangGraph that loops forever (no stop condition) so we can
  # deterministically trigger GraphRecursionError with a small recursion_limit.
  reticulate::py_run_string(paste0(
    "from typing import TypedDict\n",
    "from langgraph.managed import RemainingSteps\n",
    "from langgraph.graph import StateGraph\n",
    "\n",
    "class _LoopState(TypedDict):\n",
    "    x: int\n",
    "    remaining_steps: RemainingSteps\n",
    "\n",
    "def _inc(state: _LoopState):\n",
    "    return {'x': int(state.get('x', 0)) + 1}\n",
    "\n",
    "def _cont(state: _LoopState):\n",
    "    return 'loop'\n",
    "\n",
    "wf = StateGraph(_LoopState)\n",
    "wf.add_node('n', _inc)\n",
    "wf.set_entry_point('n')\n",
    "wf.add_conditional_edges('n', _cont, {'loop': 'n'})\n",
    "loop_graph = wf.compile()\n"
  ))

  out <- NULL
  expect_silent({
    out <- prod$invoke_graph_safely(
      reticulate::py$loop_graph,
      list(x = 0L),
      config = list(recursion_limit = as.integer(5))
    )
  })

  # Returned object should be dict-like and include recursion marker fields.
  out_r <- reticulate::py_to_r(out)
  expect_true(is.list(out_r))
  expect_equal(as.character(out_r$stop_reason), "recursion_limit")
  expect_true("error" %in% names(out_r))
  expect_true(nzchar(as.character(out_r$error)))
  expect_true(as.integer(out_r$x) >= 4L)
})

