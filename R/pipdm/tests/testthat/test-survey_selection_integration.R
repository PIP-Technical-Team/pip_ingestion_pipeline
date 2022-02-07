# This test file is to test that the integration of get_closest_surveys and
# select_lineup_survey is working as expected
# unit tests based on selection tree described here:
# https://worldbank.github.io/PIP-Methodology/lineupestimates.html#choosing-between-consumption-and-income-estimates

df <- readRDS("../testdata/input_to_select_lineup_svy.rds")
# df <- readRDS("./tests/testdata/input_to_select_lineup_svy.rds")

test_that("selection works when there is an estimate at line-up year", {
  # Only consumption based estimate exists
  ref_year <- 1989
  df$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 1)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, 1989)
  expect_equal(length(out$svy_lineup_items[[1]]$welfare_type), 1)
  expect_equal(out$svy_lineup_items[[1]]$welfare_type, "consumption")

  # Only income based estimate exists
  ref_year <- 1992
  df$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 1)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, 1992)
  expect_equal(length(out$svy_lineup_items[[1]]$welfare_type), 1)
  expect_equal(out$svy_lineup_items[[1]]$welfare_type, "income")

  # Both consumption and income based estimates exist
  ref_year <- 2007
  df$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 1)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, 2007)
  expect_equal(length(out$svy_lineup_items[[1]]$welfare_type), 1)
  expect_equal(out$svy_lineup_items[[1]]$welfare_type, "consumption")
})

test_that("selection works when there are estimate only before / after line-up year", {
  # Consumption based estimate exists
  ref_year <- 1985
  df$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 1)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, 1989)
  expect_equal(length(out$svy_lineup_items[[1]]$welfare_type), 1)
  expect_equal(out$svy_lineup_items[[1]]$welfare_type, "consumption")

  # Income based estimate exists
  ref_year <- 2020
  df$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 1)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, 2018)
  expect_equal(length(out$svy_lineup_items[[1]]$welfare_type), 1)
  expect_equal(out$svy_lineup_items[[1]]$welfare_type, "income")
})

test_that("selection works when there are estimates before and after line-up year", {
  # Consumption based estimate exists before and after (with income estimate in between)
  # The income estimate is expected to be dropped in that case
  ref_year <- 1990
  df$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 2)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, c(1989, 1994))
  expect_equal(length(unique(out$svy_lineup_items[[1]]$welfare_type)), 1)
  expect_equal(unique(out$svy_lineup_items[[1]]$welfare_type), "consumption")

  # Income based estimate exists before and after (with consumption estimate in between)
  df2 <- df
  df2$data[[1]] <- df2$data[[1]][!df2$data[[1]]$survey_year %in% c(1989, 1994:2006), ] # Remove some consumption surveys to match assumptions
  ref_year <- 2000
  df2$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df2)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 2)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, c(1992, 2007))
  expect_equal(length(unique(out$svy_lineup_items[[1]]$welfare_type)), 1)
  expect_equal(unique(out$svy_lineup_items[[1]]$welfare_type), "income")

  # Estimates on both side, but not of different welfare types.
  # Closest to consumption estimate year.
  # Expectation: Select consumption estimate (closest)
  df2 <- df
  df2$data[[1]] <- df2$data[[1]][df2$data[[1]]$survey_year %in% c(1989, 1992), ] # Remove some surveys to match assumptions
  ref_year <- 1990
  df2$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df2)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 1)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, c(1989))
  expect_equal(length(out$svy_lineup_items[[1]]$welfare_type), 1)
  expect_equal(out$svy_lineup_items[[1]]$welfare_type, "consumption")

  # Estimates on both side, but not of different welfare types.
  # Closest to income estimate year.
  # Expectation: Select income estimate (closest)
  df2 <- df
  df2$data[[1]] <- df2$data[[1]][df2$data[[1]]$survey_year %in% c(1989, 1992), ] # Remove some surveys to match assumptions
  ref_year <- 1991
  df2$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df2)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 1)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, c(1992))
  expect_equal(length(out$svy_lineup_items[[1]]$welfare_type), 1)
  expect_equal(out$svy_lineup_items[[1]]$welfare_type, "income")

  # Estimates on both side, but not of different welfare types.
  # Distance from line-up to each estimate year is the same
  # Expectation: Select consumption estimate
  df2 <- df
  df2$data[[1]] <- df2$data[[1]][df2$data[[1]]$survey_year %in% c(1989, 1992), ] # Remove some surveys to match assumptions
  df2$data[[1]]$survey_year[df2$data[[1]]$survey_year == 1989] <- 1990 # Change survey_year value to match assumption
  ref_year <- 1991
  df2$reference_year_index <- ref_year
  out <- db_get_closest_surveys(df2)
  out <- db_select_lineup_surveys(out)

  expect_equal(length(out$svy_lineup_items[[1]]$survey_year), 1)
  expect_equal(out$svy_lineup_items[[1]]$survey_year, c(1990))
  expect_equal(length(out$svy_lineup_items[[1]]$welfare_type), 1)
  expect_equal(out$svy_lineup_items[[1]]$welfare_type, "consumption")
})
