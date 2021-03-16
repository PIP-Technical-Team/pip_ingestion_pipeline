# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= '2.15.1')
  utils::globalVariables(
    c('req_items')
  )

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
db_finalize_ref_year_table <- function(dt, pfw_table) {

  # CHECK inputs
  check_inputs_db_class(dt)

  # Unnest 'req_items'
  dt <- tidyfast::dt_unnest(dt, req_items)

  # Check for countries without any national surveys
  cc <- check_no_national_survey(pfw_table)

  # Recode survey coverage for countries without any national surveys
  # and only one coverage level (e.g. ARG)
  if (!purrr::is_empty(cc)) {
    tmp <- purrr::map_df(cc, find_unique_coverage, pfw_table)
    dt$survey_coverage <- ifelse(dt$country_code %in% tmp$country_code,
                                 tmp$survey_coverage, dt$survey_coverage)
    msg <- sprintf('Info: Recoding survey coverage for \'%s\'.',
                   paste(tmp$country_code, collapse = '\', \''))
    rlang::inform(msg)
  }

  # Remove rows with national survey coverage and missing reference year mean
  na_check <- dt$survey_coverage == 'national' & is.na(dt$pred_mean_ppp)
  if (any(na_check)) {
    cc <- dt[na_check]$country_code %>% unique()
    dt <- dt[!na_check]
    msg <- sprintf(
      paste('Info: %s country-year(s) with national survey coverage have',
            'missing values for the reference year mean. Removing such rows for \'%s\'.'),
            sum(na_check), paste(cc, collapse = '\', \''))
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

  # Add is_used_for_aggregation column
  # Temporary quick fix for is_used_for_aggregation column,
  # see issue PIP-Technical-Team/TMP_pipeline#14
  dt$is_used_for_aggregation <-
    ifelse(dt$pop_data_level != 'national',
           TRUE, FALSE)

  # Select final columns
  # dt$reporting_year <- NULL
  cols <- c('survey_id', 'wb_region_code', 'pcn_region_code', 'country_code',
            'reference_year',  'surveyid_year', 'survey_year',
            'survey_acronym', 'survey_coverage', 'survey_comparability',
            'welfare_type', 'survey_mean_ppp', 'predicted_mean_ppp',
            'ppp', 'pop', 'gdp', 'pce', 'pop_data_level', 'gdp_data_level',
            'pce_data_level', 'cpi_data_level', 'ppp_data_level',
            'distribution_type', 'gd_type', 'is_interpolated',
            'is_used_for_aggregation')
  dt <- dt[, .SD, .SDcols = cols]

  # Rename variables
  dt <- dt %>% dplyr::rename(
    reporting_year = reference_year,
    reporting_pop = pop,
    reporting_pce = pce,
    reporting_gdp = gdp)

  # Sort rows
  data.table::setorder(dt, survey_id, pop_data_level)

  return(dt)

}
