# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= "2.15.1") {
  utils::globalVariables(
    c("req_items")
  )
}

#' Finalize reference year table
#'
#' Finalize the reference year table by subsetting the correct rows and
#' columns.
#'
#' @param dt data.table: Output of [db_compute_predicted_means()].
#' @inheritParams db_create_ref_year_table
#'
#' @return data.table
#' @keywords internal
db_finalize_ref_year_table <- function(dt, pop_table) {

  # CHECK inputs
  check_inputs_db_class(dt)

  # nrows in 'req_items'
  n <- purrr::map_int(dt$req_items, nrow)

  # Add estimation type column
  dt$estimation_type <-
    data.table::fifelse(
      n == 1,
      "extrapolation",
      "interpolation"
    )

  # Unnest 'req_items'
  dt <- tidyfast::dt_unnest(dt, req_items)

  # Merge with population table
  dt <- merge(dt, pop_table, all.x = TRUE,
              by.x = c('country_code', 'pop_data_level',
                       'reference_year'),
              by.y = c('country_code', 'pop_data_level',
                       'year'))

  # Add survey as category to estimation_type
  dt$estimation_type <-
    data.table::fifelse(
      dt$survey_year == dt$reference_year,
      "survey",
      dt$estimation_type
    )

  # Remove rows with national survey coverage and missing reference year mean
  na_check <- dt$survey_coverage == "national" & is.na(dt$pred_mean_ppp)
  if (any(na_check)) {
    cc <- dt[na_check]$country_code %>% unique()
    dt <- dt[!na_check]
    msg <- sprintf(
      paste(
        "Info: %s country-year(s) with national survey coverage have",
        "missing values for the reference year mean. Removing such rows for '%s'."
      ),
      sum(na_check), paste(cc, collapse = "', '")
    )
    rlang::inform(msg)
  }

  # Add PCE and GDP datalevel columns
  dt$gdp_data_level <- dt$nac_data_level
  dt$pce_data_level <- dt$nac_data_level
  dt$nac_data_level <- NULL

  # Add is_interpolated column
  dt$is_interpolated <- data.table::fifelse(
    dt$survey_year == dt$reference_year, FALSE, TRUE
  )

  # Set predicted_mean_ppp to NA for
  # interpolated rows that are based on
  # aggregated numbers
  dt$predicted_mean_ppp <- data.table::fifelse(
    dt$is_interpolated &
      dt$reporting_level == "national" &
      dt$is_used_for_aggregation,
    NA_real_,
    dt$predicted_mean_ppp
  )

  # Select final columns
  cols <- c(
    "survey_id", "cache_id", "wb_region_code",
    "pcn_region_code", "country_code", "reference_year",
    "surveyid_year", "survey_year", "survey_acronym",
    "survey_coverage", "survey_comparability",
    "comparable_spell", "welfare_type", "survey_mean_lcu",
    "survey_mean_ppp", "predicted_mean_ppp",
    "ppp", "cpi", "pop", "gdp", "pce",
    "pop_data_level", "gdp_data_level",
    "pce_data_level", "cpi_data_level",
    "ppp_data_level", "reporting_level", "distribution_type",
    "gd_type", "is_interpolated",
    "is_used_for_line_up", "is_used_for_aggregation",
    "estimation_type",
    "display_cp"
  )
  dt <- dt[, .SD, .SDcols = cols]

  # Rename variables
  dt <- dt %>% dplyr::rename(
    reporting_year = reference_year,
    reporting_pop = pop,
    reporting_pce = pce,
    reporting_gdp = gdp
  )

  # Recode class
  dt$reporting_year <- as.numeric(dt$reporting_year)

  # Add interpolation id
  dt <- dt %>%
    base::transform(
      interpolation_id = paste(country_code, reporting_year, pop_data_level, sep = "_")
    )

  # change factors to characters
  # nn <- names(dt[, .SD, .SDcols = is.factor])
  # dt[, (nn) := lapply(.SD, as.character),
  #        .SDcols = nn]

  # Sort rows
  data.table::setorder(dt, country_code, reporting_year, welfare_type, reporting_level)

  return(dt)
}
