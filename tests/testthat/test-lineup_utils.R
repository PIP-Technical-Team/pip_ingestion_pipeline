library(testthat)
library(data.table)

# ---- calc_cmd_quantiles (already tested in test-cmd_estimate_dist.R) ---------
# ---- Pure utility function tests for lineup_utils.R -------------------------

# ---- transform_input ---------------------------------------------------------

test_that("transform_input with scalar year broadcasts to all countries", {
  inp <- list(country_code = c("ZAF", "COL"), year = 2020)
  out <- transform_input(inp)
  expect_length(out, 2L)
  expect_equal(out[[1L]]$country_code, "ZAF")
  expect_equal(out[[1L]]$year,         2020)
  expect_equal(out[[2L]]$country_code, "COL")
})

test_that("transform_input with list of year vectors produces one element per country-year", {
  inp <- list(
    country_code = c("ZAF", "COL"),
    year         = list(c(2020L, 2021L), c(2015L))
  )
  out <- transform_input(inp)
  expect_length(out, 3L)   # 2 + 1
  codes <- vapply(out, `[[`, character(1L), "country_code")
  expect_equal(sort(codes), sort(c("ZAF", "ZAF", "COL")))
})

test_that("transform_input aborts when year list length mismatches country_code length", {
  inp <- list(country_code = c("ZAF", "COL"), year = list(2020L))
  expect_error(transform_input(inp), regexp = "Length of")
})

# ---- uniq_vars ---------------------------------------------------------------

test_that("uniq_vars identifies single-value columns", {
  dt <- data.table(a = 1L, b = 1:5, c = "x")
  expect_equal(sort(uniq_vars(dt)), c("a", "c"))
})

test_that("uniq_vars returns empty character for all-varying data", {
  dt <- data.table(a = 1:3, b = 4:6)
  expect_length(uniq_vars(dt), 0L)
})

# ---- vars_to_attr ------------------------------------------------------------

test_that("vars_to_attr removes column and stores it as attribute", {
  dt <- data.table(x = rep("A", 5), y = 1:5)
  out <- vars_to_attr(dt, "x")
  expect_false("x" %in% names(out))
  expect_equal(attr(out, "x"), "A")
})

# ---- get_refy_mult_factor ---------------------------------------------------

test_that("get_refy_mult_factor assigns extrapolation approach correctly", {
  df <- data.table(
    estimation_type = "extrapolation",
    monotonic = TRUE,
    same_direction = TRUE,
    nac = 110,
    nac_sy = 100,
    predicted_mean_ppp = 5,
    svy_mean = 5
  )
  out <- get_refy_mult_factor(df)
  expect_equal(out$lineup_approach, "extrapolation")
  expect_equal(out$mult_factor, 1.1)
})

test_that("get_refy_mult_factor assigns interpolation_same and uses predicted_mean ratio", {
  df <- data.table(
    estimation_type = "interpolation",
    monotonic = TRUE,
    same_direction = TRUE,
    nac = 110,
    nac_sy = 100,
    predicted_mean_ppp = 6,
    svy_mean = 3
  )
  out <- get_refy_mult_factor(df)
  expect_equal(out$lineup_approach, "interpolation_same")
  expect_equal(out$mult_factor, 2)
})

test_that("get_refy_mult_factor assigns survey approach and mult_factor=1 as default", {
  df <- data.table(
    estimation_type = "survey",
    monotonic = NA,
    same_direction = NA,
    nac = 100,
    nac_sy = 100,
    predicted_mean_ppp = 5,
    svy_mean = 5
  )
  out <- get_refy_mult_factor(df)
  expect_equal(out$lineup_approach, "survey")
  expect_equal(out$mult_factor, 1)
})
