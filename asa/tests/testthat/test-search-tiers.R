# test-search-tiers.R
# Tests for DuckDuckGo 4-tier browser search fallback markers

test_that(".extract_search_tier detects each tier from trace markers", {
  expect_equal(asa:::.extract_search_tier(NULL), "unknown")
  expect_equal(asa:::.extract_search_tier(NA_character_), "unknown")
  expect_equal(asa:::.extract_search_tier(""), "unknown")

  expect_equal(
    asa:::.extract_search_tier("ToolMessage(content=\"{'_tier': 'primp'}\")"),
    "primp"
  )
  expect_equal(
    asa:::.extract_search_tier("ToolMessage(content='{\"_tier\": \"selenium\"}')"),
    "selenium"
  )
  expect_equal(
    asa:::.extract_search_tier("ToolMessage(content=\"{'_tier': 'ddgs'}\")"),
    "ddgs"
  )
  expect_equal(
    asa:::.extract_search_tier("ToolMessage(content='{\"_tier\": \"requests\"}')"),
    "requests"
  )
})

test_that(".extract_search_tier prefers the highest tier regardless of order", {
  # primp is highest priority — test with primp appearing first (not last)
  trace_primp_first <- paste(
    "ToolMessage(content=\"{'_tier': 'primp'}\")",
    "ToolMessage(content=\"{'_tier': 'ddgs'}\")",
    "ToolMessage(content=\"{'_tier': 'requests'}\")"
  )
  expect_equal(asa:::.extract_search_tier(trace_primp_first), "primp")

  # primp in the middle
  trace_primp_middle <- paste(
    "ToolMessage(content=\"{'_tier': 'requests'}\")",
    "ToolMessage(content=\"{'_tier': 'primp'}\")",
    "ToolMessage(content=\"{'_tier': 'ddgs'}\")"
  )
  expect_equal(asa:::.extract_search_tier(trace_primp_middle), "primp")

  # No primp — selenium should win over ddgs and requests
  trace_no_primp <- paste(
    "ToolMessage(content=\"{'_tier': 'requests'}\")",
    "ToolMessage(content=\"{'_tier': 'ddgs'}\")",
    "ToolMessage(content=\"{'_tier': 'selenium'}\")"
  )
  expect_equal(asa:::.extract_search_tier(trace_no_primp), "selenium")

  # Only lower tiers — ddgs wins over requests
  trace_low <- paste(
    "ToolMessage(content=\"{'_tier': 'requests'}\")",
    "ToolMessage(content=\"{'_tier': 'ddgs'}\")"
  )
  expect_equal(asa:::.extract_search_tier(trace_low), "ddgs")
})

test_that("extract_search_tiers returns all unique tiers from raw traces", {
  trace <- paste(
    "ToolMessage(content=\"{'_tier': 'ddgs'}\")",
    "ToolMessage(content=\"{'_tier': 'requests'}\")",
    "ToolMessage(content='{\"_tier\": \"ddgs\"}')",
    "ToolMessage(content='{\"_tier\": \"selenium\"}')",
    "ToolMessage(content='{\"_tier\": \"primp\"}')"
  )
  tiers <- extract_search_tiers(trace)
  expect_setequal(tiers, c("primp", "selenium", "ddgs", "requests"))
})

test_that("extract_search_tiers reads tiers from asa_trace_v1 JSON traces", {
  trace_json <- jsonlite::toJSON(
    list(
      format = "asa_trace_v1",
      messages = list(
        list(
          message_type = "ToolMessage",
          name = "Search",
          content = "[{'_tier': 'primp'}, {'_tier': 'requests'}]"
        ),
        list(
          message_type = "toolmessage",
          name = "wikipedia",
          content = "not relevant"
        ),
        list(
          message_type = "tool",
          name = "search",
          content = "{\"_tier\": \"ddgs\"}"
        )
      )
    ),
    auto_unbox = TRUE
  )

  tiers <- extract_search_tiers(trace_json)
  expect_setequal(tiers, c("primp", "requests", "ddgs"))
})

test_that("extract_agent_results includes search_tiers", {
  trace <- "ToolMessage(content=\"{'_tier': 'selenium'}\")"
  extracted <- extract_agent_results(trace)
  expect_true(is.list(extracted))
  expect_setequal(extracted$search_tiers, "selenium")
})

