#' Find countries without national coverage
#'
#' Check the PFW table for countries without any national survey
#' coverage. If any such countries exists the functions returns a character
#' vector with country codes for those countries.
#'
#' @param df data.frame: A table with survey metadata information.
#'
#' @return character
#' @seealso `db_merge_anchor_nac()` `db_finalize_ref_year_table()`
#' @noRd
check_no_national_survey <- function(df){
  tmp <- table(df$country_code, df$survey_coverage)
  tmp <- as.data.frame(tmp, stringsAsFactors = FALSE)
  names(tmp) <- c('country_code', 'survey_coverage', 'freq')
  cc <- tmp[(tmp$survey_coverage == 'national' & tmp$freq == 0), ][['country_code']]
  return(cc)
}

#' Find countries with only one coverage level
#'
#' Check the PFW table for countries that only have unique coverage
#' level. This is typically used together with the output from
#' `check_no_national_survey()`. Returns a data frame with the country code
#' and coverage level.
#'
#' @param cc character: A vector with country codes. Output of
#'   `check_no_national_survey()`.
#' @inheritParams check_no_national_survey
#'
#' @return data.frame
#' @seealso `db_finalize_ref_year_table()`
#' @noRd
find_unique_coverage <- function(cc, df){
  x <- unique(df[df$country_code == cc,]$survey_coverage)
  if (length(x) == 1) {
    out <- data.frame(country_code = cc, survey_coverage = x)
    return(out)
  }
}

#' check_inputs_ref_years
#' @noRd
check_inputs_ref_years <- function(x){
  if (!is.numeric(x))
    rlang::abort(c('`ref_years` must be a numeric or integer vector: ',
                   sprintf('You\'ve supplied a %s vector.', class(x))))
}

#' check_inputs_pip_years
#' @noRd
check_inputs_pip_years <- function(x){
  if (!is.numeric(x))
    rlang::abort(c('`pip_years` must be a numeric or integer vector: ',
                   sprintf('You\'ve supplied a %s vector.', class(x))))
}

#' check_inputs_db_class
#' @noRd
check_inputs_db_class <- function(dt){
  if (!any(class(dt) %in% 'data.table'))
    rlang::abort('`dt` must be of class `data.table`.')
}
