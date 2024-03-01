#' Create reference estimation table
#'
#' Merge the interpolated means and distributional statistics tables.
#'
#' @param ref_year_table data.table: Output of [db_create_ref_year_table()].
#' @param dist_table data.table: Output of [db_create_dist_table()].
#'
#' @return data.table
#' @export
db_create_ref_estimation_table <- function(ref_year_table, dist_table) {


  # TEMP FIX: TO BE REMOVED
  dist_table$survey_id <- toupper(dist_table$survey_id)

  # Merge refyear table w/ dist stat table (left join)
  dist_table$reporting_year <- NULL
  dt <- joyn::joyn(ref_year_table, dist_table,
                    by = c(
                      "survey_id", "cache_id", "wb_region_code",
                      "pcn_region_code", "country_code", "survey_acronym",
                      "surveyid_year", "survey_year",
                      "welfare_type", "pop_data_level"
                    ),
                    match_type = "m:1", 
                   keep = "left", 
                   reportvar = FALSE
  )
  
  # Set dist stat columns to NA for interpolated surveys
  dist_cols <- c(
    "survey_median_lcu", "survey_median_ppp",
    "gini", "mld", "polarization", "decile1",
    "decile2", "decile3", "decile4", "decile5",
    "decile6", "decile7", "decile8", "decile9",
    "decile10"
  )
  dt <- set_int_cols_to_na(dt, dist_cols)

  # Remove rows with missing predicted_mean_ppp
  if (anyNA(dt$predicted_mean_ppp)) {
    rlang::warn(c(
      sprintf(
        "Removing %s rows with missing `predicted_mean_ppp`:",
        sum(is.na(dt$predicted_mean_ppp))
      ),
      unique(dt[is.na(predicted_mean_ppp)]$cache_id)
    ))
    dt <- dt[!is.na(predicted_mean_ppp), ]
  }

  # Remove rows with missing ppp
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

  # dt$median <- dt$survey_median_ppp
  dt <- dt %>%
    data.table::setnames(
      "pcn_region_code", "region_code"
    )

  # Order final columns
  cols <- c(
    "survey_id", "cache_id", "region_code", "wb_region_code",
    "country_code", "reporting_year", "surveyid_year",
    "survey_year", "survey_time", "survey_acronym", "survey_coverage",
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
    "is_used_for_line_up", "is_used_for_aggregation",
    "estimation_type",
    "interpolation_id",
    "display_cp"
  )
  dt <- dt[, .SD, .SDcols = cols]

  return(dt)
}

#' set_int_cols_to_na
#' @noRd
set_int_cols_to_na <- function(dt, cols) {
  for (i in seq_along(cols)) {
    dt[[cols[i]]] <- data.table::fifelse(
      dt$estimation_type == "interpolation",
      NA_real_, dt[[cols[i]]]
    )
  }

  return(dt)
}
