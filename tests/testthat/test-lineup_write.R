library(testthat)
library(data.table)

# ── Helpers ───────────────────────────────────────────────────────────────────

# Build a minimal synthetic distribution data.table that satisfies the
# required columns for get_dist_stats(). Used to verify the column contract
# of process_country_lineup() without needing network access.
make_synth_dist <- function(
  country_code = "ZAF",
  reporting_year = 2020L,
  reporting_level = "national",
  n = 100L
) {
  set.seed(42L)
  data.table(
    country_code = country_code,
    reporting_year = reporting_year,
    reporting_level = reporting_level,
    welfare = abs(rnorm(n, mean = 5, sd = 2)),
    weight = rep(1 / n, n)
  )
}

# ── get_dist_stats() column contract ─────────────────────────────────────────
# process_country_lineup() accumulates env_acc$<key> = get_dist_stats()$dt_dist
# for every year, then row-binds them.  These tests confirm that the
# accumulated data.table has the expected column set.

test_that("get_dist_stats returns list with dist_stats and dt_dist elements", {
  df <- make_synth_dist()
  out <- get_dist_stats(df)
  expect_named(out, c("dist_stats", "dt_dist"), ignore.order = FALSE)
  expect_true(is.data.table(out$dt_dist))
})

test_that("get_dist_stats dt_dist has required scalar stat columns", {
  df <- make_synth_dist()
  out <- get_dist_stats(df)$dt_dist
  required <- c(
    "country_code",
    "reporting_year",
    "reporting_level",
    "min",
    "max",
    "mean",
    "median",
    "gini",
    "mld",
    "polarization"
  )
  expect_true(all(required %in% names(out)))
})

test_that("get_dist_stats dt_dist includes decile columns", {
  df <- make_synth_dist()
  out <- get_dist_stats(df)$dt_dist
  decile_cols <- paste0("decile", 1:10)
  expect_true(all(decile_cols %in% names(out)))
})

test_that("get_dist_stats dt_dist has one row per reporting_level", {
  df <- rbind(
    make_synth_dist(reporting_level = "national"),
    make_synth_dist(
      reporting_level = "urban",
      country_code = "ZAF",
      reporting_year = 2020L
    )
  )
  out <- get_dist_stats(df)$dt_dist
  expect_equal(nrow(out), 2L)
  expect_setequal(out$reporting_level, c("national", "urban"))
})

test_that("get_dist_stats aborts when required columns are missing", {
  df <- data.table(welfare = 1:5, weight = rep(0.2, 5))
  expect_error(get_dist_stats(df), regexp = "missing required columns")
})

# ── get_csum_dist() ───────────────────────────────────────────────────────────

test_that("get_csum_dist prepends zero row and adds cumulative columns", {
  qq <- data.table(
    reporting_level = rep("national", 5L),
    welfare = c(1, 2, 3, 4, 5),
    weight = rep(0.2, 5L)
  )
  out <- get_csum_dist(qq)
  # Prepended zero row means 6 rows total
  expect_equal(nrow(out), 6L)
  expect_true(all(c("cw", "cwy", "cwy2", "cwylog", "index") %in% names(out)))
  # First row per level should be welfare == 0
  expect_equal(out[index == 0L, welfare], 0)
})

test_that("get_csum_dist index starts at 0 per reporting_level", {
  qq <- data.table(
    reporting_level = c(rep("national", 3L), rep("urban", 3L)),
    welfare = c(1, 2, 3, 1.5, 2.5, 3.5),
    weight = rep(1 / 6, 6L)
  )
  out <- get_csum_dist(qq)
  expect_equal(min(out[reporting_level == "national", index]), 0L)
  expect_equal(min(out[reporting_level == "urban", index]), 0L)
})

# ── write_ind_csum() ──────────────────────────────────────────────────────────

test_that("write_ind_csum writes an .fst file with expected name", {
  tmp <- withr::local_tempdir()
  qq <- data.table(
    reporting_level = "national",
    welfare = c(0, 1, 2),
    weight = c(0, 0.5, 0.5),
    cw = c(0, 0.5, 1),
    cwy = c(0, 0.5, 2),
    cwy2 = c(0, 0.5, 4),
    cwylog = c(0, log(1) * 0.5, log(2) * 0.5),
    index = 0L:2L
  )
  write_ind_csum(qq, country_code = "ZAF", ref_year = 2020L, path = tmp)
  expected_path <- file.path(tmp, "ZAF_2020.fst")
  expect_true(file.exists(expected_path))
})

test_that("write_ind_csum round-trips data correctly", {
  tmp <- withr::local_tempdir()
  qq <- data.table(
    reporting_level = "national",
    welfare = c(0, 1, 2),
    weight = c(0, 0.5, 0.5),
    cw = c(0, 0.5, 1),
    cwy = c(0, 0.5, 2),
    cwy2 = c(0, 0.5, 4),
    cwylog = c(0, 0, log(2) * 0.5),
    index = 0L:2L
  )
  write_ind_csum(qq, country_code = "COL", ref_year = 2019L, path = tmp)
  back <- fst::read_fst(file.path(tmp, "COL_2019.fst"), as.data.table = TRUE)
  expect_equal(back$welfare, qq$welfare)
  expect_equal(back$weight, qq$weight)
})

# ── process_country_lineup() integration smoke test ──────────────────────────
# Requires a running PIP pipeline environment (PIP_ROOT_DIR and access to
# cache files on the network).  Skip on CI / developer machines without data.

test_that("process_country_lineup returns data.table with correct columns", {
  skip_if(
    !nzchar(Sys.getenv("PIP_ROOT_DIR")),
    "PIP_ROOT_DIR not set — skipping integration test"
  )
  skip_if_not_installed("pipload")
  skip_if_not_installed("pipfun")

  # Build a minimal 1-country pipeline environment
  py <- 2021L
  gls <- pipfun::pip_create_globals(
    root_dir = Sys.getenv("PIP_ROOT_DIR"),
    vintage = list(
      release = Sys.getenv("PIP_RELEASE", "20260324"),
      ppp_year = py,
      identity = "PROD"
    ),
    create_dir = FALSE
  )
  dl_aux <- read_aux_list(fs::path(gls$OUT_DIR_PC, gls$vintage_dir))

  # Load df_refy_mult from the targets store if available
  skip_if(
    !targets::tar_exist_objects("df_refy_mult"),
    "Target df_refy_mult not in store — skipping"
  )
  df_refy <- targets::tar_read(df_refy_mult)
  full_list <- get_full_list(
    lineup_years = gls$PIP_LINEUP_YEARS,
    df_refy = df_refy,
    only_country = "CHL"
  )
  expect_length(full_list, 1L)

  tmp <- withr::local_tempdir()
  result <- process_country_lineup(
    country_entry = full_list[[1L]],
    df_refy = df_refy,
    path = tmp,
    gls = gls,
    dl_aux = dl_aux,
    py = py
  )

  expect_true(is.data.table(result))
  expected_cols <- c(
    "country_code",
    "reporting_year",
    "reporting_level",
    "min",
    "max",
    "mean",
    "median",
    "gini",
    "mld",
    "polarization"
  )
  expect_true(all(expected_cols %in% names(result)))
  expect_true(all(paste0("decile", 1:10) %in% names(result)))

  # All rows should be for CHL
  expect_true(all(result$country_code == "CHL"))

  # .fst files written for each year
  fst_files <- list.files(tmp, pattern = "^CHL_.*\\.fst$")
  expect_equal(length(fst_files), nrow(result))
})
