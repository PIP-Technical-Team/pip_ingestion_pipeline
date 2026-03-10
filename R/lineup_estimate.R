# Lineup distribution estimation
# Adapted from pip_lineups_pipeline/pipdata_pip_estimate_lineups.R

#' Estimate reference-year distribution for a single country-year
#'
#' Loads cached survey data for the surveys bracketing `ref_year`, scales
#' their weights to reference-year population, applies a multiplication
#' factor (from extrapolation or interpolation), applies bottom-censoring,
#' and attaches distributional statistics as attributes.
#'
#' @param df_refy data.table. Reference-year table filtered to one
#'   country-year; must contain `country_code`, `reporting_level`,
#'   `welfare_type`, `survey_year`, `reporting_year`,
#'   `relative_distance`, `reporting_pop`, `survey_id`, `cache_id`,
#'   `survey_acronym`, `distribution_type`, `is_interpolated`,
#'   `lineup_approach`, `mult_factor`.
#' @param cntry_code character. ISO3c country code.
#' @param ref_year integer. Reference year.
#' @param gls list. Global settings list from [pipfun::pip_create_globals()].
#' @param py integer. PPP base year (2011, 2017, or 2021). Default 2021.
#' @param dl_aux list. Auxiliary data (output of [read_aux_list()]).
#' @param env_acc environment or NULL. Accumulator for `dt_dist` data.tables.
#'
#' @return data.table with welfare and weight columns, plus distributional
#'   statistics attached as R attributes.
#' @export
get_refy_distributions <- function(
  df_refy,
  cntry_code,
  ref_year,
  gls,
  py = 2021,
  dl_aux,
  env_acc = NULL
) {
  # Drop factors
  df_refy <- lapply(df_refy, function(x) {
    if (is.factor(x)) as.character(x) else x
  }) |>
    qDT()

  df_refy <- df_refy |>
    fsubset(country_code == cntry_code & reporting_year == ref_year) |>
    fselect(
      "country_code",
      "reporting_level",
      "welfare_type",
      "survey_year",
      "reporting_year",
      "relative_distance",
      "reporting_pop",
      "survey_id",
      "cache_id",
      "survey_acronym",
      "distribution_type",
      "is_interpolated",
      "lineup_approach",
      "mult_factor"
    )

  # Move non-join metadata to attributes
  df_refy <- df_refy |>
    vars_to_attr(
      vars = c(
        "survey_id",
        "survey_acronym",
        "distribution_type",
        "is_interpolated",
        "lineup_approach"
      )
    )

  # Load cached survey data
  cache_id <- funique(df_refy$cache_id)
  gv(df_refy, "cache_id") <- NULL

  df_svy <- collapse::rowbind(lapply(as.list(cache_id), function(x) {
    pipload::pip_load_cache(cache_id = x, version = gls$vintage_dir) |>
      fselect(
        country_code,
        surveyid_year,
        survey_acronym,
        survey_year,
        welfare_ppp,
        weight,
        reporting_level,
        welfare_type,
        imputation_id
      )
  }))

  # Join and scale weights
  df <- joyn::joyn(
    x = df_refy,
    y = df_svy,
    by = c("country_code", "survey_year", "reporting_level", "welfare_type"),
    keep = "left",
    match_type = "1:m",
    verbose = FALSE,
    sort = FALSE,
    reportvar = FALSE
  ) |>
    fgroup_by(survey_year, reporting_level) |>
    fmutate(
      n_imp = data.table::uniqueN(imputation_id),
      svy_pop = fsum(weight),
      weight = weight * (reporting_pop / svy_pop) * relative_distance
    ) |>
    fungroup() |>
    fmutate(welfare = welfare_ppp * mult_factor)

  # ARG: re-scale to national population
  if (cntry_code == "ARG") {
    yr_col <- as.character(ref_year)
    pop_tab <- dl_aux$pop[country_code == cntry_code & data_level == "national"]
    if (!yr_col %in% names(pop_tab)) {
      cli::cli_abort("Year {yr_col} not found in dl_aux$pop for ARG.")
    }
    pop_national <- as.numeric(pop_tab[[yr_col]])
    df <- df |>
      fmutate(
        .w_sum = fsum(weight),
        weight = weight * (pop_national / .w_sum)
      ) |>
      fselect(-.w_sum)
  }

  # Bottom censoring
  bc <- fcase(
    py == 2021L , 0.28 ,
    py == 2017L , 0.25 ,
    py == 2011L , 0.22 ,
    default = 0
  )
  df[welfare <= bc, welfare := bc]

  setkey(df, NULL)

  # Distributional statistics
  dist_stats <- get_dist_stats(df = df)
  attr(df, "dist_stats") <- dist_stats$dist_stats
  attr(df, "dt_dist_stats") <- dist_stats$dt_dist

  if (!is.null(env_acc)) {
    key <- paste(cntry_code, ref_year, sep = "_")
    rlang::env_poke(env = env_acc, nm = key, value = dist_stats$dt_dist)
  }

  # Carry over df_refy attributes (avoid overwriting existing ones)
  existing_attr <- names(attributes(df))
  for (nm in names(attributes(df_refy))) {
    if (
      !nm %in%
        c(
          "dim",
          "row.names",
          "names",
          "class",
          ".internal.selfref",
          existing_attr
        )
    ) {
      attr(df, nm) <- attr(df_refy, nm)
    }
  }

  # Promote selected columns to attributes; drop non-welfare columns
  df <- vars_to_attr(df, "n_imp")
  df <- vars_to_attr(
    df,
    c(
      "country_code",
      "survey_acronym",
      "survey_year",
      "reporting_year",
      "welfare_type"
    )
  )

  gv(
    df,
    c(
      "svy_pop",
      "relative_distance",
      "reporting_pop",
      "surveyid_year",
      "mult_factor",
      "welfare_ppp",
      "imputation_id"
    )
  ) <- NULL

  df
}


#' Quantise a reference-year distribution to `nobs` equally-weighted bins
#'
#' @param df data.table. Output of [get_refy_distributions()].
#' @param nobs integer. Number of quantile bins (default 20 000).
#'
#' @return data.table with the same columns as `df` but `nobs` rows per
#'   `reporting_level`, with all original attributes preserved.
#' @export
get_refy_quantiles <- function(df, nobs = 2e4) {
  setorder(df, reporting_level, welfare)
  df_attr <- attributes(df)

  rls <- funique(df$reporting_level)
  probs <- seq(1, nobs, 1) / nobs - 5 / (nobs * 10)

  qx <- lapply(rls, \(rl) {
    x <- df[reporting_level == rl]
    xpop <- fsum(x$weight)
    Qx <- fquantile(x$welfare, w = x$weight, probs = probs, names = FALSE)
    data.table(welfare = Qx, weight = xpop / nobs, reporting_level = rl) |>
      fselect(df_attr$names)
  }) |>
    rowbind()

  attributes(qx) <- df_attr
  qx
}


#' Compute multiplication factor for lineup mean adjustment
#'
#' Adds a `lineup_approach` and `mult_factor` column to `df_refy` based on
#' estimation type, monotonicity, and direction checks.
#'
#' @param df_refy data.table. Reference-year table with columns
#'   `estimation_type`, `monotonic`, `same_direction`, `nac`, `nac_sy`,
#'   `predicted_mean_ppp`, `svy_mean`.
#'
#' @return `df_refy` with two new columns.
#' @export
get_refy_mult_factor <- function(df_refy) {
  df_refy |>
    fmutate(
      lineup_approach = fcase(
        estimation_type == "extrapolation"                                                 ,
        "extrapolation"                                                                    ,
        estimation_type == "interpolation" & monotonic == TRUE & same_direction == TRUE    ,
        "interpolation_same"                                                               ,
        estimation_type == "interpolation" & !(monotonic == TRUE & same_direction == TRUE) ,
        "interpolation_diverge"                                                            ,
        default = "survey"
      ),
      mult_factor = fcase(
        lineup_approach %in% c("extrapolation", "interpolation_diverge") ,
        nac / nac_sy                                                     ,
        lineup_approach == "interpolation_same"                          ,
        predicted_mean_ppp / svy_mean                                    ,
        default = 1
      )
    )
}


#' Compute distributional statistics for a lineup distribution
#'
#' @param df data.table. Must contain `reporting_level`, `welfare`, `weight`,
#'   `country_code`, `reporting_year`.
#'
#' @return List with:
#'   - `dist_stats`: nested list of statistics per reporting level.
#'   - `dt_dist`: flat data.table with one row per reporting level.
#' @export
get_dist_stats <- function(df) {
  required <- c(
    "reporting_level",
    "welfare",
    "weight",
    "country_code",
    "reporting_year"
  )
  missing <- setdiff(required, names(df))
  if (length(missing)) {
    cli::cli_abort("df is missing required columns: {.val {missing}}")
  }

  levels <- funique(df$reporting_level)

  min_v <- as.list(fmin(df$welfare, g = df$reporting_level))
  max_v <- as.list(fmax(df$welfare, g = df$reporting_level))
  mean_v <- as.list(fmean(df$welfare, w = df$weight, g = df$reporting_level))
  median_v <- as.list(fmedian(
    df$welfare,
    w = df$weight,
    g = df$reporting_level
  ))

  gini_v <- sapply(levels, \(x) {
    wbpip::md_compute_gini(
      welfare = df$welfare[df$reporting_level == x],
      weight = df$weight[df$reporting_level == x]
    )
  }) |>
    as.list()

  mld_v <- sapply(levels, \(x) {
    wbpip::md_compute_mld(
      welfare = df$welfare[df$reporting_level == x],
      weight = df$weight[df$reporting_level == x],
      mean = mean_v[[x]]
    )
  }) |>
    as.list()

  pol_v <- sapply(levels, \(x) {
    wbpip::md_compute_polarization(
      welfare = df$welfare[df$reporting_level == x],
      weight = df$weight[df$reporting_level == x],
      gini = gini_v[[x]],
      mean = mean_v[[x]],
      median = median_v[[x]]
    )
  }) |>
    as.list()

  deciles <- lapply(levels, \(x) {
    d <- wbpip:::md_compute_quantiles_share(
      welfare = df$welfare[df$reporting_level == x],
      weight = df$weight[df$reporting_level == x]
    )
    names(d) <- paste0("decile", seq_along(d))
    qDT(list2DF(as.list(d)))
  })
  names(deciles) <- levels
  deciles_dt <- rowbind(deciles)
  deciles_dt <- data.table(reporting_level = levels, deciles_dt)

  dist_stats <- list(
    min = min_v,
    max = max_v,
    mean = mean_v,
    median = median_v,
    gini = gini_v,
    mld = mld_v,
    polarization = pol_v,
    deciles = deciles
  )

  country_code <- funique(df$country_code)
  reporting_year <- funique(df$reporting_year)

  dt_dist <- data.table(
    country_code = country_code,
    reporting_year = reporting_year,
    reporting_level = names(mean_v),
    min = unlist(min_v),
    max = unlist(max_v),
    mean = unlist(mean_v),
    median = unlist(median_v),
    gini = unlist(gini_v),
    mld = unlist(mld_v),
    polarization = unlist(pol_v)
  ) |>
    joyn::left_join(
      y = deciles_dt,
      by = "reporting_level",
      reportvar = FALSE,
      verbose = FALSE
    )

  list(dist_stats = dist_stats, dt_dist = dt_dist)
}


#' Attach auxiliary data as an attribute to a lineup data.table
#'
#' @param df data.table. Output of [get_refy_distributions()].
#' @param dl_aux list. Auxiliary data.
#' @param df_refy data.table. Reference-year table.
#' @param py integer. PPP base year.
#'
#' @return `df` with an `aux_data` attribute.
#' @export
add_aux_data_attr <- function(df, dl_aux, df_refy, py = 2021) {
  code <- attr(df, "country_code")
  year <- attr(df, "reporting_year")
  reporting_level <- attr(df, "reporting_level_rows")$reporting_level |>
    funique()

  aux_data_list <- aux_data(
    cde = code,
    yr = year,
    reporting_level = reporting_level,
    dl_aux = dl_aux,
    df_refy = df_refy,
    py = py
  )

  attr(df, "aux_data") <- aux_data_list
  df
}


#' Assemble auxiliary data for one country-reference-year combination
#'
#' @param cde character. Country code.
#' @param yr integer. Reference year.
#' @param reporting_level character. Reporting level.
#' @param dl_aux list. Auxiliary data tables.
#' @param df_refy data.table. Reference-year table.
#' @param py integer. PPP base year.
#'
#' @return Named list with elements `pce`, `pop`, `gdp`, `ppp`, `cpi`.
#' @export
aux_data <- function(cde, yr, reporting_level, dl_aux, df_refy, py = 2021) {
  if (length(yr) > 1L) {
    cli::cli_alert_warning("reporting_year is non-unique.")
  }
  if (length(cde) > 1L) {
    cli::cli_alert_warning("country_code is non-unique.")
  }

  stopifnot(py %in% c(2011L, 2017L, 2021L))
  output <- list()
  yr <- as.character(yr)

  # PCE
  output[["pce"]] <- if (yr %in% names(dl_aux$pce)) {
    dl_aux$pce[country_code == cde, ..yr] |> unlist() |> unname() |> funique()
  } else {
    NA_real_
  }

  # POP
  tempvs <- c("data_level", yr)
  temp <- dl_aux$pop[country_code == cde, ..tempvs]
  output[["pop"]] <- setNames(as.list(temp[[yr]]), temp$data_level)

  # GDP
  output[["gdp"]] <- dl_aux$gdp[
    country_code == cde & data_level %in% reporting_level,
    ..yr
  ] |>
    funique() |>
    as.numeric()

  # PPP
  tempvs <- c("data_level", as.character(py))
  temp <- dl_aux$ppp[country_code == cde, ..tempvs]
  output[["ppp"]] <- setNames(as.list(temp[[tempvs[2L]]]), temp$data_level)

  # CPI
  output[["cpi"]] <- if (yr %in% names(dl_aux$cpi)) {
    tempvs <- c("data_level", yr)
    temp <- dl_aux$cpi[country_code == cde, ..tempvs]
    setNames(as.list(temp[[yr]]), temp$data_level)
  } else {
    NA_real_
  }

  output
}
