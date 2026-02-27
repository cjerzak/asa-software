test_that("search_options defaults selenium browser preference to firefox_first", {
  opt <- asa::search_options()
  expect_identical(opt$selenium_browser_preference, "firefox_first")
})

test_that("search_options accepts explicit selenium browser preference", {
  opt <- asa::search_options(selenium_browser_preference = "chrome_first")
  expect_identical(opt$selenium_browser_preference, "chrome_first")
})

test_that("search_options validates selenium browser preference", {
  expect_error(
    asa::search_options(selenium_browser_preference = "firefox_only"),
    "selenium_browser_preference"
  )
})

test_that("print.asa_search includes non-default selenium browser preference", {
  printed <- capture.output(print(asa::search_options(selenium_browser_preference = "chrome_first")))
  expect_true(any(grepl("selenium_browser_preference=chrome_first", printed, fixed = TRUE)))
})
