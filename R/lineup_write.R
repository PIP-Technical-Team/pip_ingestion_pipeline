# Lineup distribution write functions
# Adapted from pip_lineups_pipeline/pipdata_pip_write_lineups.R

#' Write a single reference-year distribution as a `.qs` file
#'
#' Saves `df_refy` (including all R attributes) to
#' `{path}/{country_code}_{ref_year}.qs`.
#'
#' @param df_refy data.table. Output of [get_refy_distributions()].
#' @param path character / fs_path. Directory to write to.
#'
#' @return invisible TRUE.
#' @keywords internal
write_refy_dist <- function(df_refy, path) {
  cntry_code <- attributes(df_refy)$country_code
  ref_year <- attributes(df_refy)$reporting_year

  qs::qsave(
    x = df_refy,
    file = fs::path(path, paste0(cntry_code, "_", ref_year), ext = "qs"),
    preset = "fast",
    nthreads = getOption("Ncpus", parallel::detectCores(logical = FALSE))
  )

  invisible(TRUE)
}


#' Estimate and save all reference-year distributions as `.qs` files
#'
#' Iterates over every element of `cntry_refy`, calls
#' [get_refy_distributions()] → [get_refy_quantiles()] →
#' [add_aux_data_attr()] → [write_refy_dist()] for every country-year
#' combination.
#'
#' @param df_refy data.table. Full reference-year table.
#' @param cntry_refy list. Each element is `list(country_code, year)` where
#'   `year` is a vector of reference years for that country.
#' @param path character / fs_path. Directory to write `.qs` files.
#' @param gls list. Global settings from [pipfun::pip_create_globals()].
#' @param dl_aux list. Auxiliary data.
#' @param py integer. PPP base year (2011, 2017, or 2021). Must be supplied
#'   explicitly — do not rely on `gls$vintage_dir` parsing.
#'
#' @return invisible TRUE (side-effect: writes files).
#' @keywords internal
write_multiple_refy_dist <- function(
  df_refy,
  cntry_refy,
  path,
  gls,
  dl_aux,
  py
) {
  lapply(
    cli::cli_progress_along(cntry_refy, total = length(cntry_refy)),
    FUN = \(i) {
      x <- cntry_refy[[i]]
      lapply(x$year, FUN = \(year = x$year, country_code = x$country_code) {
        suppressMessages(
          get_refy_distributions(
            df_refy = df_refy,
            cntry_code = country_code,
            ref_year = year,
            gls = gls,
            py = py
          ) |>
            get_refy_quantiles(nobs = 2e4) |>
            add_aux_data_attr(dl_aux = dl_aux, df_refy = df_refy, py = py) |>
            write_refy_dist(path = path)
        )
      })
    }
  )

  invisible(TRUE)
}


#' Estimate and save cumulative-sum distributions as `.fst` files
#'
#' @description
#' `r lifecycle::badge("deprecated")`
#'
#' This monolithic function has been superseded by [process_country_lineup()],
#' which is called from a `{targets}` dynamic branch (`pattern = map(...)`)
#' for incremental per-country caching.  `write_csum_refy()` is kept for
#' backward compatibility with ad-hoc scripts in `pip_lineups_pipeline/` but
#' should not be used in `_targets.R`.
#'
#' For every country-year in `cntry_refy`, calls
#' [get_refy_distributions()] → [get_refy_quantiles()] →
#' [get_csum_dist()] → [write_ind_csum()].
#' Distributional statistics are accumulated in `env_acc` when provided.
#'
#' @param df_refy data.table. Full reference-year table.
#' @param cntry_refy list. Country-year list; see [write_multiple_refy_dist()].
#' @param path character / fs_path. Directory to write `.fst` files.
#' @param gls list. Global settings.
#' @param dl_aux list. Auxiliary data.
#' @param env_acc environment or NULL. Distributional stats accumulator.
#' @param py integer. PPP base year (2011, 2017, or 2021). Must be supplied
#'   explicitly — do not rely on `gls$vintage_dir` parsing.
#'
#' @return invisible TRUE.
#' @family lineup
#' @export
write_csum_refy <- function(
  df_refy,
  cntry_refy,
  path,
  gls,
  dl_aux,
  env_acc = NULL,
  py
) {
  lifecycle::deprecate_warn(
    when = "2026-03-11",
    what = "write_csum_refy()",
    with = "process_country_lineup()",
    details = paste0(
      "Use `process_country_lineup()` with `targets::tar_target(..., ",
      "pattern = map(lineup_full_list))` for incremental per-country caching."
    )
  )

  if (is.null(env_acc)) {
    env_acc <- new.env(parent = .GlobalEnv)
  }

  lapply(
    cli::cli_progress_along(cntry_refy, total = length(cntry_refy)),
    FUN = \(i) {
      x <- cntry_refy[[i]]
      lapply(x$year, FUN = \(year = x$year, country_code = x$country_code) {
        suppressMessages(
          get_refy_distributions(
            df_refy = df_refy,
            cntry_code = country_code,
            ref_year = year,
            gls = gls,
            py = py,
            dl_aux = dl_aux,
            env_acc = env_acc
          ) |>
            get_refy_quantiles(nobs = 2e4) |>
            get_csum_dist() |>
            write_ind_csum(
              path = path,
              country_code = country_code,
              ref_year = year
            )
        )
      })
    }
  )

  invisible(TRUE)
}


#' Write a single cumulative-sum distribution as an `.fst` file
#'
#' Saves `df_refy` to `{path}/{country_code}_{ref_year}.fst` with
#' zero compression for fast access.
#'
#' @param df_refy data.table. Output of [get_csum_dist()].
#' @param country_code character. ISO3c code.
#' @param ref_year integer. Reference year.
#' @param path character / fs_path. Target directory.
#'
#' @return invisible TRUE.
#' @keywords internal
write_ind_csum <- function(df_refy, country_code, ref_year, path) {
  fst::write_fst(
    x = df_refy,
    path = fs::path(path, paste0(country_code, "_", ref_year, ".fst")),
    compress = 0L
  )

  invisible(TRUE)
}


#' Estimate and save cumulative-sum distributions for a single country
#'
#' Processes all reference years for one country entry: calls
#' [get_refy_distributions()] → [get_refy_quantiles()] →
#' [get_csum_dist()] → [write_ind_csum()] for every year in
#' `country_entry$year`.
#'
#' This is the per-country worker function used by the `{targets}` dynamic
#' branch `lineup_dist_country` (`pattern = map(lineup_full_list)`).
#' Each branch processes one country and returns its dist-stats `data.table`;
#' a combining target `rowbind()`s the 172 results into the final
#' `lineup_dist_out` table.
#'
#' @param country_entry list. A single element of the list produced by
#'   [get_full_list()]: `list(country_code = "AGO", year = c(1981, 1982, ...))`.
#' @param df_refy data.table. Full reference-year table (passed through to
#'   [get_refy_distributions()]).
#' @param path character / fs_path. Directory to write `.fst` files.
#' @param gls list. Global settings from [pipfun::pip_create_globals()].
#' @param dl_aux list. Auxiliary data (output of [read_aux_list()]).
#' @param py integer. PPP base year (2011, 2017, or 2021). Must be supplied
#'   explicitly — do not derive from `gls`.
#'
#' @return data.table with distributional statistics for all reference years of
#'   this country (one row per year × reporting level). Columns are those
#'   produced by [get_dist_stats()]: `country_code`, `reporting_year`,
#'   `reporting_level`, `min`, `max`, `mean`, `median`, `gini`, `mld`,
#'   `polarization`, `decile1`–`decile10`.
#' @family lineup
#' @export
process_country_lineup <- function(
  country_entry,
  df_refy,
  path,
  gls,
  dl_aux,
  py
) {
  env_acc <- new.env(parent = emptyenv())
  country_code <- country_entry$country_code

  lapply(country_entry$year, FUN = \(year) {
    suppressMessages(
      get_refy_distributions(
        df_refy = df_refy,
        cntry_code = country_code,
        ref_year = year,
        gls = gls,
        py = py,
        dl_aux = dl_aux,
        env_acc = env_acc
      ) |>
        get_refy_quantiles(nobs = 2e4) |>
        get_csum_dist() |>
        write_ind_csum(
          path = path,
          country_code = country_code,
          ref_year = year
        )
    )
  })

  rowbind(as.list(env_acc))
}
