# Constants
df <- data.frame(year = 2005:2010, gdp = 100:105)

# safe_weighted_mean
test_that("safe_weighted_mean() works as expected", {
  expect_identical(safe_weighted_mean(x = 1:10, w = 1:10), 7)
  expect_identical(safe_weighted_mean(x = 2, w = "1"), NA)
  expect_identical(safe_weighted_mean(x = 1:10, w = 1:9), NA)
})

# get_years
test_that("get_years() works as expected", {
  expect_identical(get_years(2005.5), c(2005, 2006))
  expect_identical(get_years(2008.2), c(2008, 2009))
  expect_identical(get_years(2008), 2008)
})

# get_weights
test_that("get_weights() works as expected", {
  expect_equal(get_weights(2005.5), c(.5, .5))
  expect_equal(get_weights(2008.2), c(.8, .2))
  expect_equal(get_weights(2008), 1)
})

# get_values
test_that("get_values() works as expected", {
  expect_equal(get_values(c(2005, 2006), df, "gdp"), c(100, 101))
  expect_equal(get_values(c(2008, 2009), df, "gdp"), c(103, 104))
  expect_equal(get_values(2008, df, "gdp"), 103)
})

# adjust_aux_values
test_that("adjust_aux_values() works as expected", {
  expect_equal(adjust_aux_values(2005.5, df, "gdp"), 100.5)
  expect_equal(adjust_aux_values(2008.2, df, "gdp"), 103.2)
  expect_equal(adjust_aux_values(2008, df, "gdp"), 103)
})
