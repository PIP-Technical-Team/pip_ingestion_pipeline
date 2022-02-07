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