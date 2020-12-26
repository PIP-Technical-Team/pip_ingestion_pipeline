# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= '2.15.1')
  utils::globalVariables(
    c('gdp', 'pce')
  )

#' Create reference year table
#'
#' Create a table with adjusted welfare means for each reference year.
#'
#' `db_create_ref_year_table()` creates a table for with interpolated or
#' extrapolated welfare means for all specified reference years based on the
#' provided survey, GDP and PCE data.
#'
#' @param gdp_table data.table: A table with GDP data.
#' @param pce_table data.table: A table with PCE data.
#' @param pop_table data.table: A table with population data.
#' @param pfw_table data.table: A table with the price framework file.
#' @param dsm_table data.table: A table with deflated survey means.
#' @param ref_years numeric: A vector with reference years.
#' @param pip_years numeric: A vector with calender years used in PIP.
#'
#' @return data.table
#' @export
db_create_ref_year_table <- function(gdp_table,
                                     pce_table,
                                     pop_table,
                                     pfw_table,
                                     dsm_table,
                                     ref_years,
                                     pip_years) {

  # CHECKS
  check_inputs_ref_years(ref_years)
  check_inputs_pip_years(pip_years)

  # Create Survey Anchor table
  svy_anchor <- db_create_svy_anchor(
    dsm_table = dsm_table, pfw_table = pfw_table)

  # Create National Accounts table
  dt_nac <- db_create_nac_table(
    gdp_table = gdp_table, pce_table = pce_table,
    pip_years = pip_years)

  # Create a table with survey metadata information and adjusted values
  # for GDP and PCE for surveys that span multiple years
  dt_svy <- db_merge_anchor_nac(
    nac_table = dt_nac, svy_anchor = svy_anchor)

  # Create reference year table
  dt_ref <-
    db_create_lkup_table(dt_svy, dt_nac, pop_table, ref_years) %>% #
    db_get_closest_surveys() %>% # Select closets surveys
    db_select_lineup_surveys() %>% # Select lineup surveys
    db_compute_predicted_means() %>% # Calculate predicted means
    db_finalize_ref_year_table(pfw_table) # Finalize table (select rows and columns)

  return(dt_ref)

}
