test_that("select_proxy() works as expected", {

  # Non SSA region 1 survey (all PCE values present)
  df <- data.frame(survey_pce = 100, survey_gdp = 200)
  out <- select_proxy(df, region_code = "ECA", ref_pce = 105, ref_gdp = 205)
  expect_equal(out, list(value0 = 100, value1 = NULL, req_value = 105))

  # Non SSA region 2 surveys (all PCE values present)
  df <- data.frame(survey_pce = c(100, 110), survey_gdp = c(200, 210))
  out <- select_proxy(df, region_code = "ECA", ref_pce = 105, ref_gdp = 205)
  expect_equal(out, list(value0 = 100, value1 = 110, req_value = 105))

  # Non SSA region 1 survey (survey PCE value missing)
  df <- data.frame(survey_pce = NA, survey_gdp = 200)
  out <- select_proxy(df, region_code = "ECA", ref_pce = 105, ref_gdp = 205)
  expect_equal(out, list(value0 = 200, value1 = NULL, req_value = 205))

  # Non SSA region 1 survey (reference PCE value missing)
  df <- data.frame(survey_pce = 100, survey_gdp = 200)
  out <- select_proxy(df, region_code = "ECA", ref_pce = NA, ref_gdp = 205)
  expect_equal(out, list(value0 = 200, value1 = NULL, req_value = 205))

  # Non SSA region 2 surveys (one survey PCE value missing)
  df <- data.frame(survey_pce = c(100, NA), survey_gdp = c(200, 210))
  out <- select_proxy(df, region_code = "ECA", ref_pce = 105, ref_gdp = 205)
  expect_equal(out, list(value0 = 200, value1 = 210, req_value = 205))

  # Non SSA region 2 surveys (PCE reference value missing)
  df <- data.frame(survey_pce = c(100, 110), survey_gdp = c(200, 210))
  out <- select_proxy(df, region_code = "ECA", ref_pce = NA, ref_gdp = 205)
  expect_equal(out, list(value0 = 200, value1 = 210, req_value = 205))

  # SSA region 1 survey (all values present)
  df <- data.frame(survey_pce = 100, survey_gdp = 200)
  out <- select_proxy(df, region_code = "SSA", ref_pce = 105, ref_gdp = 205)
  expect_equal(out, list(value0 = 200, value1 = NULL, req_value = 205))

  # SSA region 2 surveys (all values present)
  df <- data.frame(survey_pce = c(100, 110), survey_gdp = c(200, 210))
  out <- select_proxy(df, region_code = "SSA", ref_pce = 105, ref_gdp = 205)
  expect_equal(out, list(value0 = 200, value1 = 210, req_value = 205))

  # SSA region 1 survey (survey GDP missing)
  df <- data.frame(survey_pce = 100, survey_gdp = NA)
  out <- select_proxy(df, region_code = "SSA", ref_pce = 105, ref_gdp = 205)
  expect_equal(out, list(value0 = NA, value1 = NULL, req_value = 205))

  # SSA region 2 surveys (one survey GDP value missing)
  df <- data.frame(survey_pce = c(100, 105), survey_gdp = c(200, NA))
  out <- select_proxy(df, region_code = "SSA", ref_pce = 105, ref_gdp = 205)
  expect_equal(out, list(value0 = 200, value1 = NA_integer_, req_value = 205))

  # SSA region 1 survey (reference GDP missing)
  df <- data.frame(survey_pce = 100, survey_gdp = 200)
  out <- select_proxy(df, region_code = "SSA", ref_pce = 105, ref_gdp = NA)
  expect_equal(out, list(value0 = 200, value1 = NULL, req_value = NA))

  # SSA region 2 surveys (reference GDP value missing)
  df <- data.frame(survey_pce = c(100, 105), survey_gdp = c(200, 205))
  out <- select_proxy(df, region_code = "SSA", ref_pce = 105, ref_gdp = NA)
  expect_equal(out, list(value0 = 200, value1 = 205, req_value = NA))
})

test_that("check_inputs_select_proxy() catches errors correctly", {
  # Too many rows
  df <- data.frame(survey_pce = c(100, 105, 110), survey_gdp = c(200, 205, 210))
  expect_error(check_inputs_select_proxy(df, "ECA"))
  # survey_gdp missing
  df <- data.frame(survey_pce = c(100, 105))
  expect_error(check_inputs_select_proxy(df, "ECA"))
  # survey_pce missing
  df <- data.frame(survey_gdp = c(100, 105))
  expect_error(check_inputs_select_proxy(df, "ECA"))
  # Incorrect region codes
  df <- data.frame(survey_pce = c(100, 105), survey_gdp = c(200, 205))
  expect_error(check_inputs_select_proxy(df, "SST"))
  expect_error(check_inputs_select_proxy(df, "Sub-Saharan Africa"))
})
