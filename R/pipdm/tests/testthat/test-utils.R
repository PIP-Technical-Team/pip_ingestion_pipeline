# create_line_up_check
test_that("create_line_up_check() works as expected", {
  # Create test data
  dt <- data.table(
    cache_id = c('ARG_1980_EPH_D2_INC_GROUP', 'ARG_2002_EPH_D2_INC_GPWG', 'ARG_2018_EPHC-S2_D2_INC_GPWG',
                 'IND_1977_NSS_D2_CON_GROUP', 'IND_1993_NSS-SCH1_D2_CON_GPWG', 'IND_2011_NSS-SCH1_D2_CON_GPWG',
                 'IND_1977_NSS_D2_CON_GROUP', 'IND_1993_NSS-SCH1_D2_CON_GPWG', 'IND_2011_NSS-SCH1_D2_CON_GPWG',
                 'URY_1981_ENH_D2_INC_GROUP', 'URY_1996_ECH_D2_INC_GPWG', 'URY_2006_ECH_D1_INC_GPWG',
                 'USA_1974_CPS-LIS_D1_INC_BIN', 'USA_2001_CPS-LIS_D1_INC_BIN', 'USA_2018_CPS-ASEC-LIS_D1_INC_BIN'),
    country_code = c(rep('ARG', 3), rep('IND', 6), rep('URY', 3), rep('USA', 3)),
    reporting_level = c(rep('urban', 3), rep(c('urban', 'rural'), each = 3), c('urban', 'urban', 'national'),
                        rep('national', 3))
  )

  res <- create_line_up_check(dt)
  # If all surveys for a specfic country are national they should be included
  expect_true(all(res[country_code == 'USA',]$is_used_for_line_up))
  # If all surveys for a specfic country are non-national they should be included
  expect_true(all(res[country_code == 'ARG',]$is_used_for_line_up))
  # If surveys are split by U/R (domain = 2) they should be included
  expect_true(all(res[country_code == 'IND',]$is_used_for_line_up))
  # If a country has both national and urban/rural surveys than the latter should be discarded
  expect_false(all(res[country_code == 'URY',]$is_used_for_line_up))
  expect_true(all(!res[country_code == 'URY' & reporting_level != 'national',]$is_used_for_line_up))
  expect_true(all(res[country_code == 'URY' & reporting_level == 'national',]$is_used_for_line_up))
})

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
