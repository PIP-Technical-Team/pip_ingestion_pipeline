test_that("select_lineup_survey() works as expected", {

  # Return as is if there is only a single survey to choose from
  df <- data.frame(survey_year = 2005, welfare_type = "consumption")
  out <- select_lineup_survey(df, ref_year = 2007)
  expect_equal(out, df)

  # Reference year equal to the survey year
  # Select consumption estimate if there are two surveys
  df <- data.frame(
    survey_year = c(2005, 2005),
    welfare_type = c("consumption", "income")
  )
  out <- select_lineup_survey(df, ref_year = 2005)
  expect_true(all(out$survey_year == 2005 & out$welfare_type == "consumption"))
  # Otherwise select the survey at the reference year
  df <- data.frame(
    survey_year = c(2005, 2010),
    welfare_type = c("consumption", "income")
  )
  out <- select_lineup_survey(df, ref_year = 2010)
  expect_true(all(out$survey_year == 2010 & out$welfare_type == "income"))
  # Return as is if there are two income surveys
  df <- data.frame(
    survey_year = c(2005, 2005),
    welfare_type = c("income", "income")
  )
  out <- select_lineup_survey(df, ref_year = 2005)

  # Cases with at least one instance of both-sides surveys
  df <- data.frame(
    survey_year = c(2005, 2010, 2010),
    welfare_type = c("consumption", "consumption", "income")
  )
  out <- select_lineup_survey(df, ref_year = 2007)
  expect_equal(out, df[df[["welfare_type"]] == "consumption", ])
  df <- data.frame(
    survey_year = c(2005, 2009, 2010, 2010),
    welfare_type = c("consumption", "income", "consumption", "income")
  )
  out <- select_lineup_survey(df, ref_year = 2007)
  expect_equal(out, df[df[["welfare_type"]] == "consumption", ])

  # Both sides - Income
  # Keep both
  df <- data.frame(survey_year = c(2005, 2010), welfare_type = c("income", "income"))
  out <- select_lineup_survey(df, ref_year = 2009)
  expect_equal(out, df)

  # Both sides - Consumption
  # Keep both
  df <- data.frame(survey_year = c(2005, 2010), welfare_type = c("consumption", "consumption"))
  out <- select_lineup_survey(df, ref_year = 2009)
  expect_equal(out, df)

  # Both sides - Income and Consumption
  # Use estimate closest to the line-up year
  df <- data.frame(survey_year = c(2005, 2010), welfare_type = c("income", "consumption"))
  out <- select_lineup_survey(df, ref_year = 2007)
  expect_true(all(out$survey_year == 2005, out$welfare_type == "income"))
  out <- select_lineup_survey(df, ref_year = 2008)
  expect_true(all(out$survey_year == 2010, out$welfare_type == "consumption"))

  # Both sides - Income and Consumption with equal distance
  # Use consumption estimate
  df <- data.frame(survey_year = c(2005.5, 2008.5), welfare_type = c("income", "consumption"))
  out <- select_lineup_survey(df, ref_year = 2007)
  expect_true(all(out$survey_year == 2008.5, out$welfare_type == "consumption"))
  df <- data.frame(survey_year = c(2005.5, 2008.5), welfare_type = c("consumption", "income"))
  out <- select_lineup_survey(df, ref_year = 2007)
  expect_true(all(out$survey_year == 2005.5, out$welfare_type == "consumption"))
})
