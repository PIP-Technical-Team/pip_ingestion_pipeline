#' Create a survey anchor table
#'
#' Create a survey anchor table by merging the output of `db_create_dsm_table()`
#' with the Price Framework file. See details.
#'
#' This function:
#'
#' * Adds the columns `region_code` and `survey_coverage` to the output of
#' `db_create_dsm_table()`.
#' * Adds national survey coverage rows for countries
#' without any national survey coverage (e.g. ARG).
#' * Creates a `nac_data_level` column if gdp_data_level and pce_data_level
#' are identical (as they should be).
#'
#' @inheritParams db_create_ref_year_table
#' @keywords internal
db_create_svy_anchor <- function(dsm_table, pfw_table) {

  # Check for countries without any national surveys
  cc <- check_no_national_survey(pfw_table)

  # Add national coverage rows for countries without national surveys
  if (!purrr::is_empty(cc)) {
    msg <- sprintf('Info: National coverage rows have been added for \'%s\'.',
                   paste(cc, collapse = '\', \''))
    rlang::inform(msg)
    rows_to_add <-
      pfw_table[pfw_table$country_code %in% cc,] %>%
      transform(survey_coverage = 'national')
    pfw_table <- rbind(pfw_table, rows_to_add)
  }

  # Select columns
  pfw_table <- pfw_table[, c('region_code', 'country_code', 'survey_coverage',
                             'surveyid_year', 'survey_acronym')]

  # Merge inventory with PFW (left join)
  dt <- data.table::merge.data.table(
    dsm_table, pfw_table, all.x = TRUE,
    by = c('country_code', 'surveyid_year', 'survey_acronym'))

  if (identical(dt$gdp_data_level, dt$pce_data_level)) {
    dt$nac_data_level <- dt$gdp_data_level
    dt$gdp_data_level <- NULL
    dt$pce_data_level <- NULL
  } else {
    rlang::abort('`gdp_data_level` and `pce_data_level` are not identical.')
  }

  # Order columns
  data.table::setcolorder(
    dt, c('survey_id', 'region_code', 'country_code', 'surveyid_year',
          'survey_acronym', 'survey_coverage', 'survey_year',
          'welfare_type', 'svy_mean_ppp', 'svy_pop'))

  return(dt)

}


