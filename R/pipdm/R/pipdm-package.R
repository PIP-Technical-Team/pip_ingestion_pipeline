#' @keywords internal
"_PACKAGE"

## usethis namespace: start
##
#' @rawNamespace import(collapse, except = fdroplevels)
#' @rawNamespace import(data.table, except = fdroplevels)
#' @importFrom tidyr expand_grid
#' @importFrom magrittr %>%
## usethis namespace: end
# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= "2.15.1") {
  utils::globalVariables(
    c(c(
      "survey_id",
      "welfare",
      "weight",
      "cpi_data_level",
      "ppp_data_level",
      "gdp_data_level",
      "pce_data_level",
      "pop_data_level",
      "reporting_level",
      "svy_mean_lcu",
      "surveyid_year",
      "survey_acronym",
      "gd_type",
      "welfare_type"
    ),
    c(
      "region_code",
      "country_code",
      "survey_coverage",
      "nac_data_level",
      "ppp_data_level",
      "cpi_data_level",
      "reporting_level",
      "nac_domain",
      "ppp_domain",
      "cpi_domain",
      "cpi_data_level_index",
      "cpi_domain_index",
      "reporting_level_index",
      "nac_data_level_index",
      "nac_domain_index",
      "ppp_data_level_index",
      "ppp_domain_index",
      "reference_year",
      "region_code_index",
      "country_code_index",
      "data_level_index",
      "domain_index",
      "survey_coverage_index",
      "welfare_type_index",
      "reference_year_index",
      "gdp",
      "pce"
    ),
    c(
      "survey_year", "is_ref_year", "n_per_welfare_type", "both_sides",
      "dist_from_ref_year", "welfare_type"
    ))
  )
}
# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= "2.15.1") {
  utils::globalVariables(
    c(
      "region_code",
      "country_code",
      "survey_coverage",
      "nac_data_level",
      "ppp_data_level",
      "cpi_data_level",
      "reporting_level",
      "nac_domain",
      "ppp_domain",
      "cpi_domain",
      "cpi_data_level_index",
      "cpi_domain_index",
      "reporting_level_index",
      "nac_data_level_index",
      "nac_domain_index",
      "ppp_data_level_index",
      "ppp_domain_index",
      "reference_year",
      "region_code_index",
      "country_code_index",
      "data_level_index",
      "domain_index",
      "survey_coverage_index",
      "welfare_type_index",
      "reference_year_index",
      "gdp",
      "pce"
    )
  )
}






NULL
