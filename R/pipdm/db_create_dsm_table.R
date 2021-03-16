#' @import data.table
NULL

#' Create deflated survey mean table
#'
#' Create a table with deflated welfare means for each country and surveyid
#' year.
#'
#' @param lcu_table data.table: A table with newly estimated LCU survey means.
#'   Output of `db_create_lcu_table()`.
#' @param cpi_table data.table: A table with CPI data.
#' @param ppp_table data.table: A table with PPP data.
#' @return data.table
#' @export
db_create_dsm_table <- function(lcu_table,
                                cpi_table,
                                ppp_table) {


  #--------- Merge with CPI ---------

  # Select CPI columns
  cpi_table <-
    cpi_table[, .SD, .SDcols =
                c('country_code', 'survey_year', 'survey_acronym',
                  'cpi_data_level', 'cpi')]

  # Merge survey table with CPI (left join)
  dt <- data.table::merge.data.table(
    lcu_table, cpi_table, all.x = TRUE,
    by = c('country_code', 'survey_year', 'survey_acronym', 'cpi_data_level')
  )

  #--------- Merge with PPP ---------

  # Select default PPP values
  ppp_table <- ppp_table[ppp_default == TRUE]

  # Select PPP columns
  ppp_table <-
    ppp_table[, .SD, .SDcols =
                c('country_code', 'ppp_data_level', 'ppp')]

  # Merge survey table with PPP (left join)
  dt <- data.table::merge.data.table(
    dt, ppp_table, all.x = TRUE,
    by = c('country_code', 'ppp_data_level')
  )

  #--------- Deflate welfare mean ---------

  # svy_mean_ppp = survey_mean_lcu / cpi / ppp
  dt$survey_mean_ppp <-
    wbpip::deflate_welfare_mean(
      welfare_mean = dt$survey_mean_lcu, ppp = dt$ppp, cpi = dt$cpi)

  #--------- Finalize table ---------

  # Add is_interpolated column
  dt$is_interpolated <- FALSE

  # Add is_used_for_aggregation column
  # Temporary quick fix for is_used_for_aggregation column,
  # see issue PIP-Technical-Team/TMP_pipeline#14
  dt$is_used_for_aggregation <-
    ifelse(dt$pop_data_level != 'national',
           TRUE, FALSE)

  # Select and order columns
  dt <- dt[, .SD, .SDcols =
             c('survey_id', 'wb_region_code', 'pcn_region_code', 'country_code',
               'survey_acronym', 'survey_coverage', 'survey_comparability',
               'surveyid_year', 'reporting_year', 'survey_year', 'welfare_type',
               'survey_mean_lcu', 'survey_mean_ppp', #'survey_pop',
               'reporting_pop', 'ppp', 'cpi', 'pop_data_level',
               'gdp_data_level', 'pce_data_level',
               'cpi_data_level', 'ppp_data_level',
               'distribution_type', 'gd_type',
               'is_interpolated', 'is_used_for_aggregation')]

  # Sort rows
  data.table::setorder(dt, survey_id)

  return(dt)
}
