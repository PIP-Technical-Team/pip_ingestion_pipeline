#' Create survey estimation table
#'
#' Merge the survey mean and distributional statistics tables.
#'
#' @inheritParams db_create_ref_year_table
#' @param dist_table data.table: Output of [db_create_dist_table()].
#' @return data.table
#' @export
db_create_svy_estimation_table <- function(dsm_table, dist_table, gdp_table, pce_table) {

  # TEMP FIX: TO BE REMOVED
  dist_table$survey_id <- toupper(dist_table$survey_id)

  # Remove cols from dist_table
  dist_table$reporting_year <- NULL
  dist_table$problem <- NULL

  # Merge DSM table w/ dist stat table (full join)
  dt <- joyn::merge(dsm_table, dist_table, match_type = "1:1")
  dt$report <- NULL

  # Merge with GDP
  gdp_table$gdp_domain <- NULL
  dt <- data.table::merge.data.table(
    dt, gdp_table,
    all.x = TRUE,
    by.x = c("country_code", "reporting_year", "gdp_data_level"),
    by.y = c("country_code", "year", "gdp_data_level")
  )

  # Merge with PCE
  pce_table$pce_domain <- NULL
  dt <- data.table::merge.data.table(
    dt, pce_table,
    all.x = TRUE,
    by.x = c("country_code", "reporting_year", "pce_data_level"),
    by.y = c("country_code", "year", "pce_data_level")
  )

  # Remove rows with missing survey_mean_ppp
  # This shouldn't be the case
  # A problem with PHL 2009
  if (anyNA(dt$survey_mean_ppp)) {
    rlang::warn(c(
      sprintf(
        "Removing %s rows with missing `survey_mean_ppp`: ",
        sum(is.na(dt$survey_mean_ppp))
      ),
      unique(dt[is.na(survey_mean_ppp)]$cache_id)
    ))
    dt <- dt[!is.na(survey_mean_ppp), ]
  }

  # Remove rows with missing ppp
  # CHN, IDN, why?
  if (anyNA(dt$ppp)) {
    rlang::warn(c(
      sprintf(
        "Removing %s rows with missing `ppp`:",
        sum(is.na(dt$ppp))
      ),
      unique(dt[is.na(ppp)]$cache_id)
    ))
    dt <- dt[!is.na(ppp), ]
  }

  # Fix and add columns
  # dt$reporting_year <- as.integer(dt$reporting_year)
  dt$estimation_type <- "survey"
  dt$predicted_mean_ppp <- numeric(0)
  # dt$median <- dt$survey_median_ppp
  dt <- dt %>%
    data.table::setnames(
      c("gdp", "pce", "pcn_region_code"),
      c("reporting_gdp", "reporting_pce", "region_code")
    )

  # Order final columns
  cols <- c(
    "survey_id", "cache_id", "region_code", "wb_region_code",
    "country_code", "reporting_year", "surveyid_year",
    "survey_year", "survey_acronym", "survey_coverage",
    "survey_comparability", "comparable_spell", "welfare_type",
    "reporting_level",
    # "mean", "median", "mld", "gini",
    # "polarization", sprintf("decile%s", 1:10),
    "survey_mean_lcu", "survey_mean_ppp",
    "survey_median_ppp", "survey_median_lcu",
    "predicted_mean_ppp", "ppp", "cpi",
    "reporting_pop", "reporting_gdp",
    "reporting_pce", "pop_data_level",
    "gdp_data_level", "pce_data_level",
    "cpi_data_level", "ppp_data_level",
    "distribution_type", "gd_type",
    "is_interpolated",
    "is_used_for_aggregation",
    "estimation_type",
    "display_cp"
  )
  dt <- dt[, .SD, .SDcols = cols]

  return(dt)
}
