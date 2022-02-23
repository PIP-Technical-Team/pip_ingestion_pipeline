#' Create metadata table
#'
#' @param lcu_table data.table: A table with newly estimated LCU survey means.
#'   Output of `db_create_lcu_table()`.
#' @param pfw_table data.table: A table with the price framework file.
#'
#' @export
db_create_metadata_table <- function(pfw_table, lcu_table) {

  # PFW columns
  pfw_table <-
    pfw_table[, c(
      "wb_region_code", "pcn_region_code", "country_code",
      "survey_coverage", "survey_acronym", "surveyid_year",
      "reporting_year", "survey_year", "welfare_type",
      "survey_comparability", "comp_note"
    )]
  pfw_table <- dplyr::rename(pfw_table, comparability_note = comp_note)
  pfw_table$surveyid_year <- as.integer(pfw_table$surveyid_year)

  # LCU columns
  lcu_table <-
    lcu_table[, c(
      "survey_id",
      "country_code",
      "surveyid_year",
      "survey_acronym",
      "survey_year",
      "welfare_type",
      "distribution_type",
      "gd_type",
      "cpi_data_level",
      "ppp_data_level",
      "gdp_data_level",
      "pce_data_level",
      "reporting_level"
    )]

  # Merge datasets
  dt <- data.table::merge.data.table(
    pfw_table, lcu_table,
    by = c(
      "country_code", "surveyid_year", "survey_acronym",
      "survey_year", "welfare_type"
    )
  )

  # Order columns
  dt %>% data.table::setcolorder(
    c(
      "survey_id", "cache_id", "wb_region_code", "pcn_region_code",
      "country_code", "survey_acronym", "survey_coverage",
      "surveyid_year", "reporting_year", "survey_year",
      "welfare_type"
    )
  )

  return(dt)
}
