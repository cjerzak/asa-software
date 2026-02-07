# ---------------------------------------------------------------------------
# Tests for _default_leaf_value Unknown-preference behavior
#
# When JSON repair fills missing keys (e.g., after LLM failure), the fallback
# values should prefer "Unknown" sentinels over the first enum value to avoid
# producing falsely confident output.
# ---------------------------------------------------------------------------

test_that("_default_leaf_value returns Unknown for pure enum with Unknown option", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")
  utils <- reticulate::import_from_path("state_utils", path = python_path)

  # Education level enum with Unknown at the end
  result <- utils$`_default_leaf_value`("High School|Some College|Bachelor|Unknown")
  expect_equal(result, "Unknown")

  # Disability status with Unknown
  result <- utils$`_default_leaf_value`("No disability|Some disability|Unknown")
  expect_equal(result, "Unknown")

  # LGBTQ status with Unknown
  result <- utils$`_default_leaf_value`("Non-LGBTQ|Openly LGBTQ|Unknown")
  expect_equal(result, "Unknown")

  # Social class with Unknown
  result <- utils$`_default_leaf_value`("Working class|Middle class|Upper/elite|Unknown")
  expect_equal(result, "Unknown")
})

test_that("_default_leaf_value returns first enum when no Unknown present", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")
  utils <- reticulate::import_from_path("state_utils", path = python_path)

  result <- utils$`_default_leaf_value`("Low|Medium|High")
  expect_equal(result, "Low")

  result <- utils$`_default_leaf_value`("complete|partial")
  expect_equal(result, "partial")
})

test_that("_default_leaf_value returns Unknown for mixed type+Unknown descriptors", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")
  utils <- reticulate::import_from_path("state_utils", path = python_path)

  # string|Unknown should return "Unknown", not ""
  result <- utils$`_default_leaf_value`("string|Unknown")
  expect_equal(result, "Unknown")

  # integer|null|Unknown should return "Unknown", not None
  result <- utils$`_default_leaf_value`("integer|null|Unknown")
  expect_equal(result, "Unknown")
})

test_that("_default_leaf_value returns None for null descriptors without Unknown", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")
  utils <- reticulate::import_from_path("state_utils", path = python_path)

  result <- utils$`_default_leaf_value`("string|null")
  expect_null(result)

  result <- utils$`_default_leaf_value`("integer|null")
  expect_null(result)
})

test_that("_default_leaf_value preserves type-specific defaults", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")
  utils <- reticulate::import_from_path("state_utils", path = python_path)

  expect_equal(utils$`_default_leaf_value`("string"), "")
  expect_equal(utils$`_default_leaf_value`("array"), list())
  expect_equal(utils$`_default_leaf_value`("object"), reticulate::py_eval("{}"))
  expect_null(utils$`_default_leaf_value`("integer"))
  expect_null(utils$`_default_leaf_value`("number"))
})

test_that("_default_leaf_value preserves partial preference for status key", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")
  utils <- reticulate::import_from_path("state_utils", path = python_path)

  result <- utils$`_default_leaf_value`("complete|partial", key = "status")
  expect_equal(result, "partial")

  # status key with Unknown should still prefer partial
  result <- utils$`_default_leaf_value`("complete|partial|Unknown", key = "status")
  expect_equal(result, "partial")
})

test_that("repair_json_output_to_schema fills Unknown for enum fields with Unknown option", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")
  utils <- reticulate::import_from_path("state_utils", path = python_path)

  schema <- list(
    name = "string",
    education_level = "High School|Some College|Bachelor|Graduate|Unknown",
    disability_status = "No disability|Some disability|Unknown",
    lgbtq_status = "Non-LGBTQ|Openly LGBTQ|Unknown",
    status = "complete|partial"
  )

  out <- utils$repair_json_output_to_schema("{}", schema, fallback_on_failure = TRUE)
  parsed <- jsonlite::fromJSON(out, simplifyVector = FALSE)

  expect_equal(parsed$education_level, "Unknown")
  expect_equal(parsed$disability_status, "Unknown")
  expect_equal(parsed$lgbtq_status, "Unknown")
  expect_equal(parsed$name, "")
  # status should get "partial" (first matching partial in enum)
  expect_equal(parsed$status, "partial")
})

test_that("repair_json_output_to_schema preserves existing non-null values", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")
  utils <- reticulate::import_from_path("state_utils", path = python_path)

  schema <- list(
    name = "string",
    education_level = "High School|Some College|Bachelor|Unknown",
    status = "complete|partial"
  )

  input_json <- '{"name":"Ada Lovelace","education_level":"Bachelor"}'
  out <- utils$repair_json_output_to_schema(input_json, schema, fallback_on_failure = TRUE)
  parsed <- jsonlite::fromJSON(out, simplifyVector = FALSE)

  expect_equal(parsed$name, "Ada Lovelace")
  expect_equal(parsed$education_level, "Bachelor")
  expect_equal(parsed$status, "partial")
})

test_that("repair_json_output_to_schema is idempotent with Unknown defaults", {
  python_path <- asa_test_skip_if_no_python(required_files = "state_utils.py")
  asa_test_skip_if_missing_python_modules(c("pydantic"), method = "import")
  utils <- reticulate::import_from_path("state_utils", path = python_path)

  schema <- list(
    name = "string",
    education_level = "High School|Some College|Unknown",
    social_class = "Working class|Middle class|Unknown",
    status = "complete|partial"
  )

  out1 <- utils$repair_json_output_to_schema("{}", schema, fallback_on_failure = TRUE)
  out2 <- utils$repair_json_output_to_schema(out1, schema, fallback_on_failure = TRUE)
  expect_equal(out2, out1)
})
