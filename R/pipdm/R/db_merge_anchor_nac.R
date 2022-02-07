# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= "2.15.1") {
  utils::globalVariables(
    c(
      "country_code", "gdp_data_level", "pce_data_level",
      "gdp_domain", "pce_domain"
    )
  )
}

#' Merge survey anchor with national accounts data
#'
#' Create a combined table of survey metadata information and national accounts
#' data.
#'
#' For surveys that span two calender years the returned GDP and PCE values are
#' adjusted by the weighted average of the years in question.
#'
#' @inheritParams db_create_ref_year_table
#' @param nac_table data.table: Output of [db_create_nac_table()].
#'
#' @seealso [adjust_aux_values()]
#' @return data.table
#' @keywords internal
db_merge_dsm_nac <- function(dsm_table, nac_table) {

  # Create nested NAC table
  nac_nested <- nac_table %>%
    tidyfast::dt_nest(country_code, nac_data_level, .key = "data")

  # Merge dsm_table with nac_nested (left join)
  dt <- joyn::merge(dsm_table, nac_nested,
    by = c("country_code", "nac_data_level"),
    match_type = "m:1",
    keep = "left",
    reportvar = FALSE
  )

  # Adjust GDP and PCE values for surveys spanning two calender years
  dt$survey_gdp <- purrr::map2_dbl(dt$survey_year, dt$data,
    adjust_aux_values,
    value_var = "gdp"
  )
  dt$survey_pce <- purrr::map2_dbl(dt$survey_year, dt$data,
    adjust_aux_values,
    value_var = "pce"
  )

  # Remove nested data column
  dt$data <- NULL

  return(dt)
}
