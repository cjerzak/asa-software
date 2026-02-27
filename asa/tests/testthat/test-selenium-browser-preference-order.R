test_that("Selenium browser engine order resolves by preference", {
  python_path <- asa_test_skip_if_no_python(required_files = "asa_backend/search/ddg_transport.py")
  asa_test_skip_if_missing_python_modules(c("selenium", "bs4"))

  ddg <- asa_test_import_from_path_or_skip("asa_backend.search.ddg_transport", python_path)

  firefox_first <- reticulate::py_to_r(
    ddg$`_resolve_browser_engine_order`("firefox_first", disable_uc = FALSE)
  )
  expect_identical(firefox_first, c("firefox", "uc_chrome", "chrome"))

  chrome_first <- reticulate::py_to_r(
    ddg$`_resolve_browser_engine_order`("chrome_first", disable_uc = FALSE)
  )
  expect_identical(chrome_first, c("uc_chrome", "chrome", "firefox"))

  firefox_no_uc <- reticulate::py_to_r(
    ddg$`_resolve_browser_engine_order`("firefox_first", disable_uc = TRUE)
  )
  expect_identical(firefox_no_uc, c("firefox", "chrome"))
})
