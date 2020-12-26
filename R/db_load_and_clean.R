#' Load and clean welfare data from vector of survey ID
#'
#' @param survey_id character: vector of survey ID from
#' `pipload::pip_find_data(filter_to_pc = TRUE)`
#' @param maindir character: Main directory path
#'
#' @return
#' @export
#'
#' @examples
db_load_and_clean <- function(survey_id, maindir) {

  # Early returns ------
  if (is.null(survey_id)) {
    return(NULL)
  }

  #--------- Load data --------
  sm <- purrr:::map2(.x = survey_id,
                     .y = maindir,
                     sf_load_clean)

  sm <- data.table::rbindlist(sm,
                              use.names = TRUE,
                              fill = TRUE)
  
  if (nrow(sm) == 0) {
    sm <- NULL
  }
  
  return(sm)
}

#' Load one data at a time and clean it
#'
#' @param survey_id
#' @param maindir
#'
#' @return
#'
#' @examples
load_clean <- function(survey_id, maindir) {

  sm <- pipload::pip_load_data(survey_id = survey_id,
                               noisy     = FALSE,
                               maindir   = maindir)

  if (sm[, unique(gd_type)] %in% c("T01", "T02")) {
    # Transform to T05
  }

  if (sm[, unique(source)] != "GROUP") {
    sm <- suppressMessages(
      wbpip:::md_clean_data(sm,
                            welfare = welfare,
                            weight  = weight
      )$data
    )
  }
  return(sm)
}

sf_load_clean <- purrr:::possibly(.f = load_clean,
                                  otherwise = NULL)
