#' @import data.table
NULL

# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= '2.15.1')
  utils::globalVariables(
    c('survey_id', 'welfare', 'weight', 'cpi_data_level', 'ppp_data_level',
      'gdp_data_level', 'pce_data_level', 'pop_data_level', 'svy_mean_lcu',
      'surveyid_year', 'survey_acronym', 'gd_type', 'welfare_type')
  )

#' Create survey mean table (LCU)
#'
#' Create a table with welfare means in Local Currency Units (LCU) for each
#' survey.
#'
#' @param dlc dataframe with welfare data loaded and clened from
#' `db_load_and_clean`.
#' @param pop data.table: A table with population data.
#'
#' @return data.table
#' @export
db_create_lcu_table <- function(dlc,
                                pop,
                                maindir) {

  # Early returns ------
  if (is.null(dlc)) {
    return(NULL)
  }

  data.table::setDT(dlc)
  #--------- calculate weighted mean ---------
  dl_vars <- grep(".*data_level", names(dlc), value = TRUE)

  dlc <-
    dlc[,
       .(svy_mean_lcu  = weighted.mean(welfare, weight, na.rm = TRUE)),
       by = c("survey_id", dl_vars, "survey_year", "welfare_type")
    ]

  #--------- create components of survey ID ---------

  cnames <-
    c(
      "country_code",
      "surveyid_year",
      "survey_acronym",
      "vermast",
      "M",
      "veralt",
      "A",
      "collection",
      "module"
    )

  dlc[,

     # Name sections of filename into variables
     (cnames) := tstrsplit(survey_id, "_", fixed=TRUE)
  ][,
    # create tool and source
    c("tool", "source") := tstrsplit(module, "-", fixed = TRUE)
  ][,
    # change to lower case
    c("vermast", "veralt") := lapply(.SD, tolower),
    .SDcols = c("vermast", "veralt")
  ][
    ,
    # Remove unnecessary variables
    c("M", "A", "collection") := NULL
  ][
    # Remove unnecessary rows
    !(is.na(survey_id))
  ]

  setorder(dlc, country_code, surveyid_year, module, vermast, veralt)


  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   Merge with population   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  pop$pop_domain <- NULL
  pop_nested <- pop %>%
    tidyfast::dt_nest(country_code, pop_data_level, .key = 'data')


  dt <- data.table::merge.data.table(
    dlc, pop_nested, all.x = TRUE,
    by = c('country_code', 'pop_data_level'))

  # Adjust population values for surveys spanning two calender years
  dt$svy_pop <-
    purrr::map2_dbl(dt$survey_year, dt$data,
                    adjust_aux_values, value_var = 'pop')

  # Remove nested data column
  dt$data <- NULL

  # Sort rows
  data.table::setorder(dt, country_code, surveyid_year, survey_acronym)

  # Order columns
  data.table::setcolorder(
    dt, c('survey_id', 'country_code', 'surveyid_year',
          'survey_year', 'welfare_type', 'svy_mean_lcu', 'svy_pop'))


  return(dt)

}

