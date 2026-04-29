#' Create coverage tables
#'
#' Create a list of tables with coverage estimates at 1) regional, WLD, TOT and
#' 2) income group levels.
#'
#' @param ref_year_table data.table: Full interpolated means table. Output of
#' `db_create_ref_year_table()`.
#' @param cl_table data.table: Country list table with all WDI countries.
#' @param incgrp_table data.table: Table with historical income groups for all
#' WDI countries.
#' @inheritParams db_create_ref_year_table
#' @param digits numeric: The number of digits the returned coverage numbers are
#' rounded by.
#' @param urban_rural_countries character: A string with 3-letter country codes.
#' Countries where the coverage calculation is based on urban or rural
#' population numbers.
#' @param gls list from `pipfun::pip_create_globals()`
#'
#' @return list
#' @export
db_create_coverage_table <- function(
  ref_year_table,
  pop_table,
  cl_table,
  incgrp_table,
  ref_years,
  digits = 2,
  urban_rural_countries = c("ARG", "CHN", "IDN", "IND", "SUR"),
  gls
) {
  # ---- Prepare Reference year table ----

  # Select relevant columns
  dt <- ref_year_table[,
    c(
      "region_code", # "wb_region_code",
      "country_code",
      "reporting_year",
      "survey_year",
      "welfare_type",
      "pop_data_level",
      "reporting_level"
    )
  ]

  # Transform table to one row per country-year-reporting_level
  dt <- dt[,
    .(survey_year = toString(survey_year)),
    by = list(
      country_code,
      reporting_year,
      pop_data_level,
      welfare_type,
      region_code,
      reporting_level
    )
  ]

  dt[
    grepl(",", survey_year),
    survey_year_after := sub(".*,\\s*", "", survey_year) |>
      as.numeric()
  ]
  dt[,
    survey_year_before := sub(", .*", "", survey_year) |>
      as.numeric()
  ]

  # ---- Prepare Population table ----

  # Select national population estimates except for selected countries
  pop_table <- pop_table[
    (pop_data_level == "national" |
      country_code %in% urban_rural_countries),
  ]

  # Remove national population estimates for selected countries
  pop_table <- pop_table[
    !(pop_data_level == "national" &
      country_code %in% urban_rural_countries),
  ]

  # Select population estimates for selected reference years
  pop_table <- pop_table[year %in% ref_years, ]

  # Remove domain column
  pop_table$pop_domain <- NULL

  # Merge with cl (to get *_region_code for all countries)
  pop_table <-
    merge(
      pop_table,
      cl_table[, c('country_code', 'region_code', 'africa_split_code')],
      by = 'country_code',
      all.x = TRUE
    )

  # Merge with historical income group table
  pop_table <- joyn::joyn(
    pop_table,
    incgrp_table[, c('country_code', 'year_data', 'incgroup_historical')],
    by = c('country_code', 'year=year_data'),
    match_type = "m:1",
    keep = "left",
    reportvar = FALSE
  )

  # last observation carried forward and first observation carried backward to fill in missing income group values
  pop_table[,
    incgroup_historical := na_locf(incgroup_historical),
    by = country_code
  ][, incgroup_historical := na_focb(incgroup_historical), by = country_code]

  setnames(
    pop_table,
    c("year", "pop_data_level"),
    c("reporting_year", "reporting_level")
  )
  # ---- Merge datasets ----

  # Merge dt with pop_table (full outer join)
  dt <- joyn::joyn(
    dt,
    pop_table,
    by = c("country_code", "reporting_year", "reporting_level", "region_code"),
    match_type = "1:1",
    keep = "full",
    reportvar = FALSE
  )

  # ---- Create coverage column ----

  # Remove rows with missing Population data
  dt <- dt[!is.na(pop), ]

  # set limits for rules
  year_threshold <- 3
  year_break <- 2019.5

  # Step 1: Normalize the parsed survey years so that a single survey year is
  # correctly assigned even when it falls after the reporting year.
  normalized_surveys <- get_closest_coverage_surveys(
    reporting_year = dt$reporting_year,
    survey_year_1 = dt$survey_year_before,
    survey_year_2 = dt$survey_year_after
  )

  dt[, `:=`(
    survey_year_before = normalized_surveys$before,
    survey_year_after = normalized_surveys$after
  )]

  # Step 2: Apply the three-year rule with the COVID coverage break.
  dt[,
    coverage := is_coverage_survey_valid(
      reporting_year = reporting_year,
      survey_year = survey_year_before,
      year_threshold = year_threshold,
      year_break = year_break
    ) |
      is_coverage_survey_valid(
        reporting_year = reporting_year,
        survey_year = survey_year_after,
        year_threshold = year_threshold,
        year_break = year_break
      )
  ]

  # ---- Calculate world and regional coverage ----

  # PCN Regional coverage
  out_region <- dt |>
    fgroup_by(reporting_year, region_code) |>
    fsummarise(coverage = fmean(coverage, pop)) |>
    fungroup()

  # World coverage
  out_wld <- dt |>
    fgroup_by(reporting_year) |>
    fsummarise(coverage = fmean(coverage, pop)) |>
    fungroup() |>
    ftransform(region_code = "WLD")

  # Total coverage (World less Other High Income)
  out_tot <- dt |>
    fsubset(region_code != "OHI") |>
    fgroup_by(reporting_year) |>
    fsummarise(coverage = stats::weighted.mean(coverage, pop)) |>
    fungroup() |>
    ftransform(region_code = "TOT")

  # Income group coverage
  out_inc <- dt |>
    fsubset(incgroup_historical %in% c("Low income", "Lower middle income")) |>
    fgroup_by(reporting_year) |>
    fsummarise(coverage = fmean(coverage, pop)) |>
    fungroup() |>
    ftransform(incgroup_historical = "LIC/LMIC") |>
    fselect(c('reporting_year', 'incgroup_historical', 'coverage'))

  if (dt[!is.na(africa_split_code), .N] == 0L) {
    out_ssa <- data.table(
      reporting_year = integer(),
      region_code = character(),
      coverage = numeric()
    )
  } else {
    out_ssa <- dt |>
      fsubset(!is.na(africa_split_code)) |>
      fgroup_by(reporting_year, africa_split_code) |>
      fselect(coverage, pop) |>
      fmean(w = pop, keep.w = FALSE) |>
      frename(region_code = africa_split_code) |>
      qDT()
  }

  # Create output list
  out <- list(
    region = rowbind(out_region, out_wld, out_tot, out_ssa, fill = TRUE) |>
      setorder(region_code, reporting_year),
    incgrp = out_inc,
    country_year_coverage = dt
  )

  # Adjust digits
  out$region$coverage <- round(out$region$coverage * 100, digits)
  out$incgrp$coverage <- round(out$incgrp$coverage * 100, digits)

  return(out)
}

#' Set missing to first available value
#' @noRd
impute_missing <- function(x) {
  x[is.na(x)] <- x[!is.na(x)][1]
  return(x)
}

#' Normalize coverage survey years
#' @noRd
get_closest_coverage_surveys <- function(
  reporting_year,
  survey_year_1,
  survey_year_2
) {
  survey_year_before <- data.table::fcase(
    !is.na(survey_year_1) & survey_year_1 <= reporting_year & !is.na(survey_year_2) & survey_year_2 <= reporting_year ,
    pmax(survey_year_1, survey_year_2)                                                                                ,
    !is.na(survey_year_1) & survey_year_1 <= reporting_year                                                           ,
    survey_year_1                                                                                                     ,
    !is.na(survey_year_2) & survey_year_2 <= reporting_year                                                           ,
    survey_year_2                                                                                                     ,
    default = NA_real_
  )

  survey_year_after <- data.table::fcase(
    !is.na(survey_year_1) & survey_year_1 >= reporting_year & !is.na(survey_year_2) & survey_year_2 >= reporting_year ,
    pmin(survey_year_1, survey_year_2)                                                                                ,
    !is.na(survey_year_1) & survey_year_1 >= reporting_year                                                           ,
    survey_year_1                                                                                                     ,
    !is.na(survey_year_2) & survey_year_2 >= reporting_year                                                           ,
    survey_year_2                                                                                                     ,
    default = NA_real_
  )

  return(list(before = survey_year_before, after = survey_year_after))
}

#' Check whether a survey year counts for coverage
#' @noRd
is_coverage_survey_valid <- function(
  reporting_year,
  survey_year,
  year_threshold = 3,
  year_break = 2019.5
) {
  threshold_low <- year_break - year_threshold
  threshold_high <- year_break + year_threshold

  in_covid_window <- reporting_year >= threshold_low &
    reporting_year <= threshold_high

  same_covid_side <- !in_covid_window |
    (reporting_year < year_break & survey_year <= year_break) |
    (reporting_year > year_break & survey_year > year_break)

  valid_survey <- !is.na(survey_year) &
    abs(reporting_year - survey_year) <= year_threshold &
    same_covid_side

  valid_survey[is.na(valid_survey)] <- FALSE

  return(valid_survey)
}
