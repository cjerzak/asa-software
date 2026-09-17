test_that("MCP search server runs parallel tool calls concurrently with a thread-safe deadline", {
  asa_test_skip_if_no_python(required_files = "asa_backend/free_code/mcp_search_server.py")
  python_path <- asa_test_python_path(required_files = "asa_backend/free_code/mcp_search_server.py")

  guard <- list(
    recursion_limit = 16L, search_budget_limit = 24L, repeated_timeout_limit = 2L,
    repeated_fetch_timeout_limit = 1L, total_timeout_limit = 6L,
    tool_deadline_seconds = 1, mcp_timeout_ms = 7000L
  )
  # os.environ is a snapshot inside the embedded interpreter: set the module's
  # inputs on the Python side before importing it.
  reticulate::py_run_string(sprintf("
import os, json
os.environ['ASA_FREE_CODE_LOOP_GUARD_JSON'] = json.dumps(%s)
os.environ['ASA_FREE_CODE_USE_BROWSER'] = 'false'
os.environ['ASA_FREE_CODE_ALLOW_READ_WEBPAGES'] = 'false'
os.environ['ASA_FREE_CODE_SEARCH_OPTIONS_JSON'] = '{}'
os.environ.pop('ASA_FREE_CODE_MCP_LOG_FILE', None)
", jsonlite::toJSON(guard, auto_unbox = TRUE)))

  server <- tryCatch(
    reticulate::import_from_path("asa_backend.free_code.mcp_search_server", path = python_path),
    error = function(e) testthat::skip(paste("MCP search server dependencies unavailable:", conditionMessage(e)))
  )
  main <- reticulate::import_main(convert = FALSE)
  main$server <- server

  reticulate::py_run_string("
import threading, time

class _SlowTool:
    def __init__(self, seconds):
        self.seconds = seconds
        self.calls = 0
        self.lock = threading.Lock()
    def invoke(self, query):
        with self.lock:
            self.calls += 1
        time.sleep(self.seconds)
        return 'result for ' + str(query)

# 1. The deadline fires on time even though the tool keeps sleeping.
t0 = time.monotonic()
try:
    server._run_with_deadline(lambda: time.sleep(5), 0.5)
    deadline_raised = False
except server._ToolDeadlineExpired:
    deadline_raised = True
deadline_elapsed = time.monotonic() - t0

# 2. Four parallel tool calls (as opencode dispatches them) run concurrently.
server.TOOLS['search'] = _SlowTool(0.8)
results = {}
def _call(i):
    results[i] = server._call_tool('web_search', {'query': 'query %d' % i, 'max_results': 3})
threads = [threading.Thread(target=_call, args=(i,)) for i in range(4)]
t1 = time.monotonic()
for t in threads: t.start()
for t in threads: t.join()
parallel_elapsed = time.monotonic() - t1
parallel_ok = all(not results[i].get('isError') for i in range(4))
parallel_calls = server.TOOLS['search'].calls
network_calls = server.TOOL_STATE['network_calls']

# 3. A tool that overruns the deadline is reported as tool_timeout promptly,
#    and the loop guard sees it.
server.TOOLS['search'] = _SlowTool(3.0)
t2 = time.monotonic()
timeout_result = server._call_tool('web_search', {'query': 'slow query', 'max_results': 3})
timeout_elapsed = time.monotonic() - t2
timeout_text = timeout_result['content'][0]['text']
timeouts_recorded = server.TOOL_STATE['timeouts']
")
  py <- reticulate::py

  expect_true(py$deadline_raised)
  expect_lt(py$deadline_elapsed, 2)

  expect_true(py$parallel_ok)
  expect_equal(py$parallel_calls, 4L)
  expect_equal(py$network_calls, 4L)
  expect_lt(py$parallel_elapsed, 2.5)  # 4 x 0.8 s would be 3.2 s if serialised

  expect_true(py$timeout_result$isError)
  expect_match(py$timeout_text, "tool_timeout")
  expect_lt(py$timeout_elapsed, 2.5)
  expect_equal(py$timeouts_recorded, 1L)
})
