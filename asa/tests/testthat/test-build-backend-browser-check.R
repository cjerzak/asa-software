# test-build-backend-browser-check.R

test_that(".parse_major_version extracts major version", {
  expect_equal(asa:::.parse_major_version("Google Chrome 144.0.7559.110"), 144L)
  expect_equal(asa:::.parse_major_version("ChromeDriver 145.0.7632.26 (hash)"), 145L)
  expect_true(is.na(asa:::.parse_major_version("not a version string")))
  expect_true(is.na(asa:::.parse_major_version(NULL)))
  expect_true(is.na(asa:::.parse_major_version(NA_character_)))
})

