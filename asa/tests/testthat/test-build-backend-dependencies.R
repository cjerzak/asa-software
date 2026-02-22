# test-build-backend-dependencies.R

test_that("build_backend dependency groups stay unpinned and complete", {
  groups <- asa:::.build_backend_python_dependency_groups()

  expect_true(is.list(groups))
  expect_setequal(
    names(groups),
    c("core", "langchain", "search_scraping", "http_proxy", "utilities")
  )

  for (name in names(groups)) {
    pkgs <- groups[[name]]
    expect_type(pkgs, "character")
    expect_true(length(pkgs) >= 1L)
    expect_true(all(nzchar(pkgs)))
  }

  all_specs <- unlist(groups, use.names = FALSE)
  expect_equal(length(unique(all_specs)), length(all_specs))
  expect_false(any(grepl("(>=|<=|==|!=|~=|>|<)", all_specs, perl = TRUE)))

  must_include <- c(
    "langchain-google-genai",
    "langgraph",
    "ddgs",
    "langextract[openai]"
  )
  expect_true(all(must_include %in% all_specs))
})
