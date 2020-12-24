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


  # Early returns ------
  if (is.null(lcu_table)) {
    return(NULL)
  }

  #--------- Merge with CPI ---------

  # Select CPI columns
  cpi_table <-
    cpi_table[, .SD, .SDcols =
                c('country_code', 'surveyid_year', 'survey_acronym',
                  'cpi_data_level', 'cpi')]

  # Merge survey table with CPI (left join)
  dt <- data.table::merge.data.table(
    lcu_table, cpi_table, all.x = TRUE,
    by = c('country_code', 'surveyid_year', 'survey_acronym', 'cpi_data_level')
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

  # svy_mean_ppp = svy_mean_lcu / cpi / ppp
  dt$svy_mean_ppp <-
    wbpip::deflate_welfare_mean(
      welfare_mean = dt$svy_mean_lcu, ppp = dt$ppp, cpi = dt$cpi)

  #--------- Finalize table ---------

  # Select columns
  dt <- dt[, .SD, .SDcols =
             c('survey_id', 'country_code', 'surveyid_year', 'survey_acronym',
               'survey_year', 'welfare_type', 'svy_mean_lcu', 'svy_mean_ppp',
               'svy_pop', 'pop_data_level', 'gdp_data_level', 'pce_data_level',
               'cpi_data_level', 'ppp_data_level')]

  return(dt)
}
