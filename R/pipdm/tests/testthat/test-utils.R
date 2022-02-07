# check_no_national_survey
test_that("check_no_national_survey() works as expected", {
  df <- data.frame(
    country_code = c("NOR", "CHN", "CHN"), survey_year = 2005,
    survey_coverage = c("national", "urban", "rural")
  )
  expect_identical(check_no_national_survey(df), "CHN")
})

# find_unique_coverage
test_that("find_unique_coverage() works as expected", {
  df <- data.frame(
    country_code = c("NOR", "ARG"), survey_year = 2005,
    survey_coverage = c("national", "urban")
  )
  expect_identical(
    find_unique_coverage("ARG", df),
    data.frame(country_code = "ARG", survey_coverage = "urban")
  )
})

# check_inputs_ref_years
test_that("check_inputs_ref_years() works as expected", {
  expect_silent(check_inputs_ref_years(2005))
  expect_silent(check_inputs_ref_years(2005L))
  expect_error(check_inputs_ref_years("2005"))
})

# check_inputs_pip_years
test_that("check_inputs_pip_years() works as expected", {
  expect_silent(check_inputs_pip_years(2005))
  expect_silent(check_inputs_pip_years(2005L))
  expect_error(check_inputs_pip_years("2005"))
})

# check_inputs_db_class
test_that("check_inputs_db_class() works as expected", {
  expect_silent(check_inputs_db_class(data.table::data.table(x = 10)))
  expect_error(check_inputs_db_class(data.frame(x = 10)))
})
