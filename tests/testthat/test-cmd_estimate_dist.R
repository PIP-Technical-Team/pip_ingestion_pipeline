library(testthat)
library(data.table)

# ---- helpers -----------------------------------------------------------------

make_cf <- function(
  code = "ETH",
  year = 2021L,
  t1_comp1 = 1.5,
  t1_qf = 0.8,
  t2_comp1 = NA_real_,
  t2_qf = NA_real_,
  tier1_sme = 100,
  tier2_sme = NA_real_
) {
  data.table(
    code = code,
    year = year,
    t1_comp1 = t1_comp1,
    t1_qf = t1_qf,
    t2_comp1 = t2_comp1,
    t2_qf = t2_qf,
    tier1_sme = tier1_sme,
    tier2_sme = tier2_sme
  )
}

small_qs <- log(c(0.1, 0.5, 0.9) / (1 - c(0.1, 0.5, 0.9)))

# ---- calc_cmd_quantiles ------------------------------------------------------

test_that("calc_cmd_quantiles returns numeric vector of length n", {
  q <- calc_cmd_quantiles(100L)
  expect_length(q, 100L)
  expect_type(q, "double")
})

test_that("calc_cmd_quantiles default n=1000 gives 1000 values", {
  expect_length(calc_cmd_quantiles(), 1000L)
})

test_that("calc_cmd_quantiles values are finite real numbers", {
  q <- calc_cmd_quantiles(50L)
  expect_true(all(is.finite(q)))
})

# ---- get_cmd_welfare ---------------------------------------------------------

test_that("get_cmd_welfare uses tier-1 when t1_comp1 is not NA", {
  CF <- make_cf(t1_comp1 = 1.0, t1_qf = 0.0) # welfare = exp(1) everywhere
  w <- get_cmd_welfare("ETH", 2021L, CF, small_qs, py = 2021L)
  expect_equal(w, rep(exp(1), length(small_qs)), tolerance = 1e-10)
})

test_that("get_cmd_welfare uses tier-2 when t1_comp1 is NA", {
  CF <- make_cf(
    t1_comp1 = NA_real_,
    t1_qf = NA_real_,
    t2_comp1 = 1.0,
    t2_qf = 0.0
  )
  w <- get_cmd_welfare("ETH", 2021L, CF, small_qs, py = 2021L)
  expect_equal(w, rep(exp(1), length(small_qs)), tolerance = 1e-10)
})

test_that("get_cmd_welfare returns NULL and warns for missing country-year", {
  CF <- make_cf("ZAF", 2021L)
  expect_warning(
    result <- get_cmd_welfare("ETH", 2021L, CF, small_qs, py = 2021L),
    regexp = "No CMD coefficient"
  )
  expect_null(result)
})

test_that("get_cmd_welfare aborts for duplicate coefficient rows", {
  CF <- rbind(make_cf("ETH", 2021L), make_cf("ETH", 2021L))
  expect_error(
    get_cmd_welfare("ETH", 2021L, CF, small_qs, py = 2021L),
    regexp = "Multiple CMD coefficient"
  )
})

test_that("get_cmd_welfare applies bottom-censoring for py=2021", {
  # Use large negative coefficients so raw welfare < 0.28
  CF <- make_cf(t1_comp1 = -10, t1_qf = 0.0)
  w <- get_cmd_welfare("ETH", 2021L, CF, small_qs, py = 2021L)
  expect_true(all(w >= 0.28))
})

test_that("get_cmd_welfare applies different floor for py=2017", {
  CF <- make_cf(t1_comp1 = -10, t1_qf = 0.0)
  w <- get_cmd_welfare("ETH", 2021L, CF, small_qs, py = 2017L)
  expect_true(all(w >= 0.25))
})

# ---- get_csum_dist -----------------------------------------------------------

test_that("get_csum_dist returns expected columns", {
  qq <- data.table(
    reporting_level = c("national", "national", "national"),
    welfare = c(1.0, 2.0, 3.0),
    weight = c(100, 200, 300)
  )
  out <- get_csum_dist(qq)
  expected_cols <- c(
    "reporting_level",
    "welfare",
    "weight",
    "cw",
    "cwy",
    "cwy2",
    "cwylog",
    "index"
  )
  expect_true(all(expected_cols %in% names(out)))
})

test_that("get_csum_dist prepends a zero row per reporting_level", {
  qq <- data.table(
    reporting_level = c("national", "national"),
    welfare = c(1.0, 2.0),
    weight = c(100, 200)
  )
  out <- get_csum_dist(qq)
  # Original 2 rows + 1 prepended zero row = 3
  expect_equal(nrow(out), 3L)
  expect_equal(out$welfare[1L], 0)
})

test_that("get_csum_dist index starts at 0", {
  qq <- data.table(
    reporting_level = "national",
    welfare = c(1, 2),
    weight = c(50, 50)
  )
  out <- get_csum_dist(qq)
  expect_equal(min(out$index), 0L)
})

test_that("get_csum_dist cw column is non-decreasing within each level", {
  qq <- data.table(
    reporting_level = rep("national", 5),
    welfare = 1:5,
    weight = rep(100, 5)
  )
  out <- get_csum_dist(qq)
  cw <- out[reporting_level == "national", cw]
  expect_true(all(diff(cw) >= 0))
})
