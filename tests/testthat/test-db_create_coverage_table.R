library(testthat)
library(data.table)
library(collapse)

source(
  normalizePath(
    file.path("..", "..", "R", "db_create_coverage_table.R"),
    winslash = "/",
    mustWork = TRUE
  ),
  local = FALSE
)

make_coverage_inputs <- function() {
  ref_year_table <- data.table(
    region_code = c("SSA", "SSA", "SSA", "SSA", "SSA", "SSA"),
    country_code = c("AAA", "BBB", "CCC", "CCC", "DDD", "DDD"),
    reporting_year = c(2020, 2020, 2019, 2019, 2021, 2021),
    survey_year = c(2018.5, 2022.5, 2018.5, 2020.2, 2018.5, 2024.6),
    welfare_type = "consumption",
    pop_data_level = "national",
    reporting_level = "national"
  )

  pop_table <- data.table(
    country_code = c("AAA", "BBB", "CCC", "DDD"),
    pop_data_level = "national",
    year = c(2020, 2020, 2019, 2021),
    pop = c(100, 100, 100, 100),
    pop_domain = NA_character_
  )

  cl_table <- data.table(
    country_code = c("AAA", "BBB", "CCC", "DDD"),
    region_code = "SSA",
    africa_split_code = NA_character_
  )

  incgrp_table <- data.table(
    country_code = c("AAA", "BBB", "CCC", "DDD"),
    year_data = c(2020, 2020, 2019, 2021),
    incgroup_historical = c(
      "Low income",
      "Lower middle income",
      "Low income",
      "Lower middle income"
    )
  )

  list(
    ref_year_table = ref_year_table,
    pop_table = pop_table,
    cl_table = cl_table,
    incgrp_table = incgrp_table
  )
}

test_that("single survey years are normalized to the correct side", {
  out <- get_closest_coverage_surveys(
    reporting_year = c(2020, 2020, 2020, 2020),
    survey_year_1 = c(2022.5, 2018.5, 2020.0, 2024.6),
    survey_year_2 = c(NA_real_, NA_real_, NA_real_, 2018.5)
  )

  expect_equal(out$before, c(NA_real_, 2018.5, 2020.0, 2018.5))
  expect_equal(out$after, c(2022.5, NA_real_, 2020.0, 2024.6))
})

test_that("coverage rule enforces the COVID break on both sides", {
  expect_false(is_coverage_survey_valid(2020, 2018.5))
  expect_true(is_coverage_survey_valid(2020, 2022.5))
  expect_false(is_coverage_survey_valid(2019, 2020.2))
  expect_true(is_coverage_survey_valid(2019, 2018.5))
  expect_true(is_coverage_survey_valid(2019, 2019.5))
  expect_false(is_coverage_survey_valid(2020, 2019.5))
})

test_that("coverage rule reverts to the standard three-year window outside COVID years", {
  expect_true(is_coverage_survey_valid(2023, 2020.5))
  expect_false(is_coverage_survey_valid(2023, 2019.5))
  expect_true(is_coverage_survey_valid(2016, 2018.5))
})

test_that("db_create_coverage_table flags country-year coverage correctly", {
  inputs <- make_coverage_inputs()

  out <- db_create_coverage_table(
    ref_year_table = inputs$ref_year_table,
    pop_table = inputs$pop_table,
    cl_table = inputs$cl_table,
    incgrp_table = inputs$incgrp_table,
    ref_years = c(2019, 2020, 2021),
    gls = list()
  )

  coverage_dt <- out$country_year_coverage[
    order(country_code, reporting_year),
    .(
      country_code,
      reporting_year,
      survey_year_before,
      survey_year_after,
      coverage
    )
  ]

  expected <- data.table(
    country_code = c("AAA", "BBB", "CCC", "DDD"),
    reporting_year = c(2020, 2020, 2019, 2021),
    survey_year_before = c(2018.5, NA_real_, 2018.5, 2018.5),
    survey_year_after = c(NA_real_, 2022.5, 2020.2, 2024.6),
    coverage = c(FALSE, TRUE, TRUE, FALSE)
  )

  expect_equal(coverage_dt, expected)
})

test_that("db_create_coverage_table aggregates regional and income coverage", {
  inputs <- make_coverage_inputs()

  out <- db_create_coverage_table(
    ref_year_table = inputs$ref_year_table,
    pop_table = inputs$pop_table,
    cl_table = inputs$cl_table,
    incgrp_table = inputs$incgrp_table,
    ref_years = c(2019, 2020, 2021),
    gls = list()
  )

  region_coverage <- out$region[
    region_code == "SSA",
    .(reporting_year, coverage)
  ][order(reporting_year)]

  expect_equal(
    region_coverage,
    data.table(
      reporting_year = c(2019, 2020, 2021),
      coverage = c(100, 50, 0)
    )
  )

  incgrp_coverage <- out$incgrp[order(reporting_year)]

  expect_equal(
    incgrp_coverage,
    data.table(
      reporting_year = c(2019, 2020, 2021),
      incgroup_historical = "LIC/LMIC",
      coverage = c(100, 50, 0)
    )
  )
})
