# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= "2.15.1") {
  utils::globalVariables(
    c("gdp", "pce")
  )
}

#' Create reference year table
#'
#' Create a table with adjusted welfare means for each reference year.
#'
#' `db_create_ref_year_table()` creates a table with interpolated or
#' extrapolated welfare means for all specified reference years based on the
#' provided survey, GDP and PCE data.
#'
#' @param dsm_table data.table: A table with deflated survey means.
#' @param gdp_table data.table: A table with GDP data.
#' @param pce_table data.table: A table with PCE data.
#' @param pop_table data.table: A table with population data.
#' @param ref_years numeric: A vector with reference years.
#' @param pip_years numeric: A vector with calender years used in PIP.
#' @param region_code character: A value with the region code column to use.
#'
#' @return data.table
#' @export
db_create_ref_year_table <- function(dsm_table,
                                     gdp_table,
                                     pce_table,
                                     pop_table,
                                     ref_years,
                                     pip_years,
                                     region_code = "region_code"
                                     ) {

  # CHECKS
  region_code <- match.arg(region_code)
  check_inputs_ref_years(ref_years)
  check_inputs_pip_years(pip_years)

  if (identical(dsm_table$gdp_data_level, dsm_table$pce_data_level)) {
    dsm_table$nac_data_level <- dsm_table$gdp_data_level
    dsm_table$gdp_data_level <- NULL
    dsm_table$pce_data_level <- NULL
  } else {
    rlang::abort("`gdp_data_level` and `pce_data_level` are not identical.")
  }

  # Create National Accounts table
  dt_nac <- db_create_nac_table(
    gdp_table = gdp_table, pce_table = pce_table,
    pip_years = pip_years
  )

  # Create a table with survey metadata information and adjusted values
  # for GDP and PCE for surveys that span multiple years
  dt_svy <- db_merge_dsm_nac(
    nac_table = dt_nac, dsm_table = dsm_table
  )

  # Create reference year table
  dt_ref <- db_create_lkup_table(dt_svy, dt_nac, ref_years, region_code)
  dt_ref <- db_get_closest_surveys(dt_ref) # Select closets surveys
  dt_ref <- db_select_lineup_surveys(dt_ref) # Select lineup surveys
  dt_ref <- db_compute_predicted_means(dt_ref) # Calculate predicted means
  dt_ref <- db_finalize_ref_year_table(dt_ref, pop_table) # Finalize table (select rows and columns)

  return(dt_ref)
}
