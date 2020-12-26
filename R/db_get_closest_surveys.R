#' Get closest surveys for all reference years
#'
#' Retrieve the closest surveys for each reference year.
#'
#' @param dt data.table: Output of [db_create_lkup_table()].
#'
#' @seealso [get_closest_surveys()]
#' @return data.table
#' @keywords internal
db_get_closest_surveys <- function(dt){

  # CHECK inputs
  check_inputs_db_class(dt)

  # Create a nested list of survey line-up tables
  dt$svy_items <- purrr::map2(dt$data, dt$reference_year_index, get_closest_surveys)

  # Remove nested 'data' column
  dt$data <- NULL

  return(dt)
}

#' Get closest surveys to a reference year
#'
#' For any reference year, retrieves information about the closest surveys.
#'
#' @param svy_lkup data.frame: Look-up table for survey data.
#' @param ref_year integer: The selected reference year.
#'
#' @return data.frame
#' @keywords internal
get_closest_surveys <- function(svy_lkup, ref_year) {

  n <- nrow(svy_lkup)

  # Case 1: No survey found
  if (n == 0) {
    out <- svy_lkup
    return(out)
  }

  # Order table by survey year
  out <- svy_lkup[order(svy_lkup$survey_year), ]

  # Find interval
  i <- findInterval(ref_year, out[['survey_year']])

  # Case 2: reference year == survey year
  if (ref_year %in% out[['survey_year']]) {
    return(out[out[['survey_year']] == ref_year, ])
  }

  # Case 3: One-side - ref_year < min(survey_year)
  if (i == 0) {
    return(out[i + 1,])

    # Case 4: One-side - ref_year < max(survey_year)
  } else if (i == n) {
    return(out[i,])

    # Case 5: Both-side - min(survey_year) < ref_year < max(survey_year)
  } else {
    return(out[c(i, i + 1),])
  }
}
