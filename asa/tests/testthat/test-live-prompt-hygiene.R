test_that("recursion-limit prompt stays answer-neutral", {
  prompt <- asa_test_recursion_limit_prompt()

  expect_false(grepl("Known seed data", prompt, fixed = TRUE))
  expect_false(grepl("Ada Lovelace", prompt, fixed = TRUE))
  expect_false(grepl("Alan Turing", prompt, fixed = TRUE))
  expect_false(grepl("Grace Hopper", prompt, fixed = TRUE))
  expect_false(grepl("MUST include these three people", prompt, fixed = TRUE))
  expect_true(grepl("only use facts supported by the Search output", prompt, fixed = TRUE))
})
