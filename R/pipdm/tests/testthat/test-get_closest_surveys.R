# Constants
df <- data.frame(country_code = "ABC", survey_year = c(2000, 2001.5, 2008.2, 2015))
df2 <- data.frame(country_code = NULL, survey_year = NULL)

test_that("select_closest_surveys() works as expected", {
  expect_identical(select_closest_surveys(df2, 2005), df2) # No survey
  expect_equal(select_closest_surveys(df, 1995)$survey_year, 2000) # Single sided below
  expect_equal(select_closest_surveys(df, 2018)$survey_year, 2015) # Single sided above
  expect_equal(select_closest_surveys(df, 2015)$survey_year, 2015) # Reference year = Survey year
  expect_equal(select_closest_surveys(df, 2005)$survey_year, c(2001.5, 2008.2)) # Both sides
  expect_equal(select_closest_surveys(df, 2008)$survey_year, c(2001.5, 2008.2)) # Both sides
  expect_equal(select_closest_surveys(df, 2010)$survey_year, c(2008.2, 2015)) # Both sides
})
