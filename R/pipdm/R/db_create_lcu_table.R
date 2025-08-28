#' @import data.table
NULL

# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= "2.15.1") {
  utils::globalVariables(
    c(
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
    )
  )
}

#' Create survey mean table (LCU)
#'
#' Create a table with welfare means in Local Currency Units (LCU) for each
#' survey.
#'
#' @param dl list: A list with survey mean datasets.
#' @param pop_table data.table: A table with population data.
#' @param pfw_table data.table: A table with the Price Framework.
#'
#' @return data.table
#' @export
db_create_lcu_table <- function(dl, pop_table, pfw_table) {

  # Convert list to data.table
  dt <- data.table::rbindlist(dl, use.names = TRUE)

  # ---- Merge with PFW ----

  # Select columns
  pfw_table <-
    pfw_table[, c(
      "wb_region_code", "region_code",
      "country_code", "survey_coverage",
      "surveyid_year", "survey_acronym",
      "reporting_year", "survey_comparability",
      "display_cp", "survey_time", "oth_welfare1_type"
    )]

  # Merge LCU table with PFW (left join)
  dt <- joyn::joyn(dt, pfw_table,
    by = c(
      "country_code",
      "surveyid_year",
      "survey_acronym"
    ),
    match_type = "m:1"
  )

  if (nrow(dt[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure PFW table is up to date"
    rlang::abort(c(
      msg,
      i = hint,
      i = "Make sure .dta data is up to date by running pipdp"
    ),
    class = "pipdm_error"
    )
  }

  dt <- dt[
    .joyn != "y" # THis requires explanation
  ][, .joyn := NULL]
  # NOTE AE: We have 21 obs in pfw that do not have surveyid. should we remove
  # them from the table? Also, why is this m:1? because of the welfare type in dt?
  #
  # Note AC: There are some surveys in PFW that are not in the PIP-folder because
  # they haven't been dowloaded. I know that some US surveys haven't been
  # downloaded because I don't have access to the files in DLW. Additionally
  # there were some cases of surveys that were added to PFW, but are not relevant
  # for this update (i.e they are not in DLW yet).
  #
  # Regarding welfare_type: If I remember correctly I decided to just merge on
  # Country, Acronym and Year because of cases like PHL. This should be sufficent
  # since other cases that have mulitiple welfare types for the same year aslo
  # have different survey acronyms (e.g POL). But the whole handling of multiple
  # welfare types in the same survey is probably something we could discuss.
  
  #-------- Fix display in country profiles ------------
  #This needs to be done when there is more than one welfare type in the same
  #survey.
  
  dt[(grepl("^i", oth_welfare1_type) & welfare_type == "income") |
       (grepl("^c", oth_welfare1_type) & welfare_type == "consumption") ,
     display_cp := 0
     ][(grepl("^i", oth_welfare1_type) & welfare_type == "consumption") |
         (grepl("^c", oth_welfare1_type) & welfare_type == "income") ,
       display_cp := 1]
  
  dt[, oth_welfare1_type := NULL]

  #--------- Merge with POP ---------

  # Create nested POP table
  pop_table$pop_domain <- NULL
  pop_nested <- pop_table %>%
    tidyfast::dt_nest(country_code, pop_data_level, .key = "data")

  # Merge dt with pop_nested (add survey_pop)
  dt <- joyn::joyn(dt, pop_nested,
    by = c("country_code", "pop_data_level"),
    match_type = "m:1"
  )

  if (nrow(dt[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure POP data includes all the countries and pop data levels"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }
  dt <- dt[
    .joyn != "y" # THis requires explanation
  ][, .joyn := NULL]
  # NOTE AE: We have 470 obs in pop with no info. Is this ok? Also, why is this
  # m:1? because of the welfare type in dt?
  #
  # NOTE AC: If we are missing population information in the final output data,
  # then yes that is a problem. But there will probably be more datapoints in
  # the population table then in the survey data.

  dt[
    ,
    survey_year := as.numeric(survey_year)
  ]

  # Adjust population values for surveys spanning two calender years
  dt$survey_pop <-
    purrr::map2_dbl(dt$survey_year, dt$data,
      adjust_aux_values,
      value_var = "pop"
    )

  # Remove nested data column
  dt$data <- NULL

  # Merge with pop_table (add reporting_pop)
  dt <- joyn::joyn(dt, pop_table,
    by = c(
      "country_code",
      "reporting_year = year",
      "pop_data_level"
    ),
    match_type = "m:1"
  )

  if (nrow(dt[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure POP data includes all the countries and pop data levels"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }
  dt <- dt[
    .joyn != "y" # All country/years for which we don't have data... its ox.
  ][, .joyn := NULL]


  data.table::setnames(dt, "pop", "reporting_pop")


  # ---- Finalize table ----

  # Sort rows
  data.table::setorder(dt, country_code, surveyid_year, survey_acronym)

  # Order columns
  data.table::setcolorder(
    dt, c(
      "survey_id", "cache_id", "country_code", "surveyid_year", "survey_acronym",
      "survey_year", "welfare_type", "survey_mean_lcu", "survey_pop",
      "reporting_pop"
    )
  )

  return(dt)
}
