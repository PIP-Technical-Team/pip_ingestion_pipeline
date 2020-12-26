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

  # Select final columns
  cols <- c('region_code', 'country_code', 'reference_year',
            'survey_year', 'survey_acronym', 'survey_coverage',
            'welfare_type', 'svy_mean_ppp', 'pred_mean_ppp',
            'pop', 'pop_data_level', 'nac_data_level',
            'cpi_data_level', 'ppp_data_level')
  dt <- dt[, .SD, .SDcols = cols]

  # Sort rows
  data.table::setorder(dt, country_code, reference_year, survey_acronym)

  return(dt)

}
