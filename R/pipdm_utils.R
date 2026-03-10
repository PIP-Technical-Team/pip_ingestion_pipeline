#' Create boolean column for surveys to include in the reference year table creation
#'
#' @param dt data.table A table with survey metadata information.
#' @return data.table
#' @seealso `db_create_dsm_table()`
#' @noRd
create_line_up_check <- function(dt){

  assertthat::assert_that(all(c("cache_id", "country_code", "reporting_level") %in% names(dt)))

  # Countries wo/ any national (reporting level) surveys
  cc <- check_no_national_survey(dt)

  # Create number of rows per cache_id
  dt[, n_rl := .N, by = cache_id]

  # Create check
  check <- (dt$reporting_level == "national") |  # Surveys w/ national reporting level and no split by U/R domain (e.g USA)
    (dt$reporting_level %in% c("urban", "rural") & dt$n_rl == 2) | # Surveys split by U/R domain (e.g. CHN, IND)
    dt$country_code %in% cc  # Countries wo/ any national surveys (e.g. ARG, SUR)

  # Add is_used_for_line_up
  dt$is_used_for_line_up <- ifelse(check, TRUE, FALSE)
  dt$n_rl <- NULL

  return(dt)
}

#' Find countries without national coverage
#'
#' @param df data.frame: A table with survey metadata information.
#'
#' @return character
#' @seealso `db_create_lkup_table()`
#' @noRd
check_no_national_survey <- function(df) {
  tmp <- table(df$country_code, df$reporting_level)
  tmp <- as.data.frame(tmp, stringsAsFactors = FALSE)
  names(tmp) <- c("country_code", "reporting_level", "freq")
  cc <- tmp[(tmp$reporting_level == "national" & tmp$freq == 0), ][["country_code"]]
  if (is.null(cc)) cc <- ""
  return(cc)
}

#' check_inputs_ref_years
#' @noRd
check_inputs_ref_years <- function(x) {
  if (!is.numeric(x)) {
    rlang::abort(c(
      "`ref_years` must be a numeric or integer vector: ",
      sprintf("You've supplied a %s vector.", class(x))
    ))
  }
}

#' check_inputs_pip_years
#' @noRd
check_inputs_pip_years <- function(x) {
  if (!is.numeric(x)) {
    rlang::abort(c(
      "`pip_years` must be a numeric or integer vector: ",
      sprintf("You've supplied a %s vector.", class(x))
    ))
  }
}

#' check_inputs_db_class
#' @noRd
check_inputs_db_class <- function(dt) {
  if (!any(class(dt) %in% "data.table")) {
    rlang::abort("`dt` must be of class `data.table`.")
  }
}

#' convert variagles with unique values along the data set to attrbitus and then
#' remove those unique variables
#'
#' @param x data frame.
#'
#' @return
#' @export
uniq_vars_to_attr <- function(x) {

  if (!data.table::is.data.table(x)) {
    x <- as.data.table(x)
  }

  N_vars   <- x[, lapply(.SD, uniqueN)]
  uni_vars <- names(N_vars)[N_vars == 1]
  mul_vars <- names(N_vars)[N_vars != 1]

  for (i in seq_along(uni_vars)) {

    var <- uni_vars[i]
    value <- x[, unique(get(var))]
    attr(x, var) <- value

  }

  x <- x[, ..mul_vars]

  return(x)

}
