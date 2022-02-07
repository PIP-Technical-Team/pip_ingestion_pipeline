skip_if(Sys.getenv("PIPDM_RUN_LOCAL_TESTS") != "TRUE")

library(data.table)

gdm <- pipload::pip_load_aux("gdm")
pop <- pipload::pip_load_aux("pop")
cch <- pipload::pip_load_cache_inventory()

get_mean <- function(gdm, survey_id) {
  dtm <- gdm[survey_id == get("survey_id", envir = -2)]

  mean <- dtm[, survey_mean_lcu]
  names(mean) <- dtm[, pop_data_level]
  return(mean)
}

dnames <- c("mean", "median", "gini", "polarization", "mld", "quantiles")


get_dist_stata <- function(cache_id) {
  dt <- pipload::pip_load_cache(cache_id = cache_id)

  cache_id <- dt[, unique(cache_id)]

  dtmean <- dt[, .(mean = collapse::fmean(welfare, w = weight)),
    by = reporting_level
  ]
  mean <- dtmean[, mean]
  names(mean) <- dtmean[, reporting_level]
  ld <- db_compute_dist_stats(dt, mean, pop, cache_id)
  return(list(ld = ld, ldnames = names(mean)))
}



test_that("national microdata", {
  ld <- get_dist_stata("PHL_2018_FIES_D1_CON_GPWG")
  ll <- ld$ld

  expect_equal(names(ll), "national")

  for (i in seq_along(ll)) {
    expect_equal(names(ll[[i]]), dnames)
  }
})

test_that("urban/rural microdata", {
  ld <- get_dist_stata("IDN_2000_SUSENAS_D2_CON_GPWG")
  ll <- ld$ld
  ln <- c("national", "rural", "urban")

  expect_equal(names(ll), ln)
  for (i in seq_along(ll)) {
    expect_equal(names(ll[[i]]), dnames)
  }


  ld <- get_dist_stata("IND_2004_NSS-SCH1_D2_CON_GPWG")
  ll <- ld$ld

  expect_equal(names(ll), ln)
  for (i in seq_along(ll)) {
    expect_equal(names(ll[[i]]), dnames)
  }
})

test_that("national Group Data", {
  ld <- get_dist_stata("ARE_2019_HIES_D1_INC_GROUP")
  ll <- ld$ld
  ln <- c("national")

  expect_equal(names(ll), ln)
  expect_equal(names(ll[[1]]), dnames)
})

test_that("Urban/Rural Group Data", {
  ld <- get_dist_stata("IDN_1990_SUSENAS_D2_CON_GROUP")
  ll <- ld$ld
  ln <- c("national", "rural", "urban")

  expect_equal(names(ll), ln)

  for (i in seq_along(ll)) {
    expect_equal(names(ll[[i]]), dnames)
  }
})

test_that("Imputed Data", {
  ld <- get_dist_stata("SOM_2017_SHFS-W2_D1_CON_GPWG")
  ll <- ld$ld
  ln <- c("national")

  expect_equal(names(ll), ln)
  expect_equal(names(ll[[1]]), dnames)
})
