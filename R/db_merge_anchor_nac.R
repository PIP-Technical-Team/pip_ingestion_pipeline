# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= '2.15.1')
  utils::globalVariables(
    c('country_code', 'gdp_data_level', 'pce_data_level',
      'gdp_domain', 'pce_domain')
  )

#' Merge survey anchor with national accounts data
#'
#' Create a combined table of survey metadata information and national accounts
#' data.
#'
#' For surveys that span two calender years the returned GDP and PCE values are
#' adjusted by the weighted average of the years in question.
#'
#' @param svy_anchor data.table: A table with survey metadata information.
#' @param nac_table data.table: Output of [db_create_nac_table()].
#'
#' @seealso [adjust_aux_values()]
#' @return data.table
#' @keywords internal
db_merge_anchor_nac <- function(svy_anchor, nac_table){

  # Create nested NAC table
  nac_nested <- nac_table %>%
    tidyfast::dt_nest(country_code, nac_data_level, .key = 'data')

  # Merge svy_anchor with nac_nested (left join)
  dt <- data.table::merge.data.table(
    svy_anchor, nac_nested, all.x = TRUE,
    by = c('country_code', 'nac_data_level'))

  # Adjust GDP and PCE values for surveys spanning two calender years
  dt$svy_gdp <- purrr::map2_dbl(dt$survey_year, dt$data,
                                adjust_aux_values, value_var = 'gdp')
  dt$svy_pce <- purrr::map2_dbl(dt$survey_year, dt$data,
                                adjust_aux_values, value_var = 'pce')

  # Remove nested data column
  dt$data <- NULL

  return(dt)
}
