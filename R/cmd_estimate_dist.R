# CMD (Cross-country Missing Data) welfare distribution estimation
# Adapted from CMD_new_methodoly/R/utils.R

#' Estimate and write CMD distributions for all countries
#'
#' Loops over every country in `md`, estimates its CMD welfare distribution,
#' scales population weights, and writes the result as an `.fst` file.
#' The function is designed to run country-by-country to keep peak memory low
#' when `qs` is large (e.g. 100 000 quantiles).
#'
#' @param md data.table. Missing-data countries table with columns
#'   `country_code`, `year`, `reporting_pop`, `id`.
#' @param CF data.table. CMD coefficient table (output of [load_cmd_coeff()]).
#' @param qs numeric. Logit-transformed quantile vector (output of
#'   [calc_cmd_quantiles()]).
#' @param py integer. PPP base year (2011, 2017, or 2021).
#' @param dir fs_path / character. Vintage output directory
#'   (`gls$OUT_LINEUP_DIR_PC` is passed here after stripping the
#'   `lineup_data` suffix, i.e. pass `fs::path(gls$OUT_DIR_PC, gls$vintage_dir)`).
#' @param env_acc environment or NULL. When non-NULL, distributional statistics
#'   are accumulated here keyed as `"CC_YYYY"`.
#'
#' @return invisible TRUE (side-effect: writes files to `dir/lineup_data/`).
#' @family cmd
#' @export
estimate_and_write_full_cmd <- function(md, CF, qs, py, dir, env_acc = NULL) {
  aux_dir <- fs::path(dir, "_aux")
  lineup_dir <- fs::path(dir, "lineup_data")

  # Load population data for weight scaling
  pop <- get_pop_to_scale(aux_dir = aux_dir)

  countries <- funique(md$country_code)

  for (x in seq_along(countries)) {
    cn <- countries[x]
    md_cn <- md[country_code == cn]

    tryCatch(
      {
        l_cmd <- list_cmd_welfare(
          md_cn,
          CF,
          qs,
          py = py,
          env_acc = env_acc
        )

        l_cmd <- scale_weights(l_cmd = l_cmd, pop = pop)
        l_cmd <- select_and_order(l_cmd)

        write_cmd_dist(l_cmd, path = lineup_dir)
      },
      error = function(e) {
        na_cols <- names(md_cn)[vapply(md_cn, anyNA, logical(1L))]
        na_msg <- if (length(na_cols) > 0L) {
          paste0("NA found in: ", toString(na_cols))
        } else {
          "No NAs detected in md subset"
        }

        cli::cli_warn(c(
          "Skipping {.val {cn}} (iteration {x}/{length(countries)}).",
          "i" = na_msg,
          "x" = conditionMessage(e)
        ))
      }
    )
  }

  invisible(TRUE)
}


#' Select columns and compute cumulative-sum distribution for a CMD list
#'
#' Keeps only `welfare`, `weight`, and `reporting_level`; then calls
#' [get_csum_dist()] to add cumulative columns.
#'
#' @param l_cmd list of data.tables, each representing a country-year.
#' @return list of data.tables with cumulative columns added.
#' @family cmd
#' @export
select_and_order <- function(l_cmd) {
  lapply(l_cmd, function(dt) {
    dt <- dt[, .(welfare, weight, reporting_level)]
    get_csum_dist(dt)
  })
}


#' Calculate welfare distribution for a single country-year
#'
#' Uses CMD coefficients to estimate the welfare distribution for one
#' country-year combination.  Returns `NULL` when no valid coefficient
#' row is found.
#'
#' @param country_code character. ISO3c country code.
#' @param reporting_year integer. Survey year.
#' @param CF data.table. Coefficient table; must contain columns `code`,
#'   `year`, `t1_comp1`, `t1_qf`, `t2_comp1`, `t2_qf`, `tier1_sme`,
#'   `tier2_sme`.
#' @param qs numeric. Logit-transformed quantile vector.
#' @param py integer. PPP base year (2011, 2017, or 2021).
#'
#' @return numeric vector of welfare values, or NULL.
#' @family cmd
#' @export
#'
#' @examples
#' \dontrun{
#' get_cmd_welfare("ETH", 2021, CF, qs)
#' }
get_cmd_welfare <- function(country_code, reporting_year, CF, qs, py = 2021) {
  cf <- CF[code == country_code & year == reporting_year]

  if (nrow(cf) == 0L) {
    cli::cli_warn(
      "No CMD coefficient row found for {country_code} / {reporting_year}. Skipping."
    )
    return(NULL)
  }
  if (nrow(cf) > 1L) {
    cli::cli_abort(
      "Multiple CMD coefficient rows ({nrow(cf)}) found for {country_code} / {reporting_year}."
    )
  }

  welfare <- if (is.na(cf$t1_comp1)) {
    exp(cf$t2_comp1 + cf$t2_qf * qs)
  } else {
    exp(cf$t1_comp1 + cf$t1_qf * qs)
  }

  # Bottom censoring by PPP year
  bc <- fcase(
    py == 2021L , 0.28 ,
    py == 2017L , 0.25 ,
    py == 2011L , 0.22 ,
    default = 0
  )

  welfare[welfare <= bc] <- bc
  welfare
}


#' Calculate welfare distributions for all country-years in a data.table
#'
#' Iterates over every row of `md`, calls [get_cmd_welfare()], and returns
#' a named list of data.tables.
#'
#' @param md data.table. Must contain `country_code`, `year`,
#'   `reporting_pop`, `id`.
#' @param CF data.table. Coefficient table; see [get_cmd_welfare()].
#' @param qs numeric. Logit-transformed quantile vector.
#' @param py integer. PPP base year.
#' @param env_acc environment or NULL. Accumulator for distributional stats.
#'
#' @return Named list of data.tables (or NULLs) keyed by `md$id`.
#' @family cmd
#' @export
#'
#' @examples
#' \dontrun{
#' list_cmd_welfare(md, CF, qs, py = 2021)
#' }
list_cmd_welfare <- function(md, CF, qs, py, env_acc = NULL) {
  l_cmd <- vector("list", length = nrow(md))

  for (i in seq_len(nrow(md))) {
    country_code <- md$country_code[i]
    reporting_year <- md$year[i]
    weight <- md$reporting_pop[i] / length(qs)

    welfare <- get_cmd_welfare(country_code, reporting_year, CF, qs, py = py)

    if (is.null(welfare)) {
      l_cmd[[i]] <- NULL
      next
    }

    dt <- data.table(
      welfare = welfare,
      weight = weight,
      country_code = country_code,
      reporting_year = reporting_year,
      reporting_level = "national"
    )

    l_cmd[[i]] <- add_cmd_attributes(
      dt,
      country_code,
      reporting_year,
      env_acc = env_acc
    )
  }

  names(l_cmd) <- md[, id]
  l_cmd
}


#' Add distributional attributes to a CMD data.table
#'
#' Attaches `reporting_year`, `country_code`, `dist_stats`, and
#' `lineup_approach` as R attributes.  When `env_acc` is supplied the
#' flat `dt_dist` data.table is also stored there for later assembly.
#'
#' @param dt data.table. Single country-year CMD distribution.
#' @param country_code character. ISO3c code.
#' @param reporting_year integer. Reporting year.
#' @param env_acc environment or NULL. Stats accumulator.
#'
#' @return `dt` with attributes added.
#' @family cmd
#' @export
add_cmd_attributes <- function(
  dt,
  country_code,
  reporting_year,
  env_acc = NULL
) {
  attr(dt, "reporting_year") <- reporting_year
  attr(dt, "country_code") <- country_code

  dist <- get_dist_stats(dt)
  attr(dt, "dist_stats") <- dist

  if (!is.null(env_acc)) {
    key <- paste(country_code, reporting_year, sep = "_")
    rlang::env_poke(env = env_acc, nm = key, value = dist$dt_dist)
  }

  attr(dt, "lineup_approach") <- "missing_data"
  dt
}


#' Write CMD distributions to `.fst` files
#'
#' Writes each element of `l_cmd` as `{name}.fst` under `path`.
#'
#' @param l_cmd named list of data.tables.
#' @param path character / fs_path. Directory to write to.
#'
#' @return invisible TRUE.
#' @family cmd
#' @export
write_cmd_dist <- function(l_cmd, path) {
  country_years <- names(l_cmd)

  lapply(
    cli::cli_progress_along(country_years, total = length(country_years)),
    FUN = \(i) {
      fst::write_fst(
        x = l_cmd[[i]],
        path = fs::path(path, paste0(country_years[i], ".fst"))
      )
    }
  )

  invisible(TRUE)
}


#' Load CMD coefficients from GitHub
#'
#' Downloads `cmd_coeff.qs` from the `aux_missing_countries` repository on
#' PIP-Technical-Team and returns the parsed object.
#'
#' @param branch character. Branch name (default `"qs_file"`).
#'
#' @return list with two data.tables of coefficients.
#' @family cmd
#' @export
load_cmd_coeff <- function(branch = "qs_file") {
  base_url <- "https://raw.githubusercontent.com"
  coeff_url <- paste(
    base_url,
    "PIP-Technical-Team",
    "aux_missing_countries",
    branch,
    "04-outputdata/cmd_coeff.qs",
    sep = "/"
  )

  temp_file <- tempfile(fileext = ".qs")
  req <- httr::GET(coeff_url, httr::write_disk(path = temp_file))

  if (httr::http_error(req)) {
    cli::cli_abort(
      "Failed to download CMD coefficients from {.url {coeff_url}} (HTTP {req$status_code})."
    )
  }

  qs::qread(temp_file)
}


#' Compute logit-transformed quantile vector
#'
#' @param n integer. Number of quantiles (default 1 000).
#'
#' @return numeric vector of length `n`.
#' @family cmd
#' @export
calc_cmd_quantiles <- function(n = 1000L) {
  quantiles <- seq(1, n, 1) / n - 5 / (n * 10)
  log(quantiles / (1 - quantiles))
}


#' Compute cumulative-sum distribution
#'
#' Prepends a zero row per `reporting_level`, sorts by welfare, then adds
#' cumulative columns `cw`, `cwy`, `cwy2`, `cwylog`, and `index`.
#'
#' @param qq data.table with columns `reporting_level`, `welfare`, `weight`.
#'
#' @return data.table with cumulative columns and `index`.
#' @family cmd
#' @family lineup
#' @export
get_csum_dist <- function(qq) {
  first_rows <- data.table(reporting_level = funique(qq$reporting_level)) |>
    fmutate(welfare = 0, weight = 0)

  qq <- rowbind(qq, first_rows) |>
    setorder(reporting_level, welfare)

  qq[, `:=`(
    cw = weight,
    cwy = weight * welfare,
    cwy2 = weight * welfare * welfare,
    cwylog = log(pmax(welfare, 1e-10)) * weight
  )]

  g <- GRP(qq, ~reporting_level, sort = FALSE)

  csum <- add_vars(
    get_vars(qq, c("reporting_level", "welfare", "weight")),
    get_vars(qq, c("cw", "cwy", "cwy2", "cwylog")) |> fcumsum(g)
  )

  csum[, index := as.integer(rowid(reporting_level) - 1L)]
  csum
}
