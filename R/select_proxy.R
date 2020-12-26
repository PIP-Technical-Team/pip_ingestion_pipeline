#' Select proxy values
#'
#' Select GDP or PCE as proxy value based on pre-specified rules.
#'
#' PCE is the default for all countries, except in Sub-Saharan Africa where GDP
#' is always used. If any PCE value is missing for the specific reference year,
#' GDP is used.
#'
#' @param svy_table data.frame: A table with information on surveys and national
#'   accounts data.
#' @param region_code character: World Bank three letter region code.
#' @param ref_gdp numeric: GDP value for the reference year.
#' @param ref_pce numeric: PCE value for the reference year.
#'
#' @return list
#' @references
#' Prydz, E.B., D. Jolliffe, C. Lakner, D.G. Mahler, P. Sangraula. 2019.
#' "[National Accounts Data used in Global Poverty Measurement](http://documents1.worldbank.org/curated/en/664751553100573765/pdf/135460-WP-PUBLIC-Disclosed-3-21-2019.pdf)".
#' Global Poverty Monitoring Technical Note 8.
#' World Bank, Washington, DC.
#' @keywords internal
select_proxy <- function(svy_table, region_code, ref_gdp, ref_pce) {

  # CHECKs
  check_inputs_select_proxy(svy_table, region_code)

  # If Sub-Saharan Africa, use GDP
  if (region_code == 'SSA') {
    if (nrow(svy_table) ==  1) {
      proxy <- list(value0 = svy_table$svy_gdp,
                    value1 = NULL,
                    req_value = ref_gdp)
    } else if (nrow(svy_table) == 2) {
      proxy <- list(value0 = svy_table$svy_gdp[1],
                    value1 = svy_table$svy_gdp[2],
                    req_value = ref_gdp)
    }
  } else {
    # For other countries, use PCE if available
    if (!anyNA(c(svy_table$svy_pce, ref_pce))) {
      if (nrow(svy_table) ==  1) {
        proxy <- list(value0 = svy_table$svy_pce,
                      value1 = NULL,
                      req_value = ref_pce)
      } else if (nrow(svy_table) == 2) {
        proxy <- list(value0 = svy_table$svy_pce[1],
                      value1 = svy_table$svy_pce[2],
                      req_value = ref_pce)
      }
    } else {
      # If PCE not available, use GDP
      if (nrow(svy_table) ==  1) {
        proxy <- list(value0 = svy_table$svy_gdp,
                      value1 = NULL,
                      req_value = ref_gdp)
      } else if (nrow(svy_table) == 2) {
        proxy <- list(value0 = svy_table$svy_gdp[1],
                      value1 = svy_table$svy_gdp[2],
                      req_value = ref_gdp)
      }
    }
  }
  return(proxy)
}

#' check_inputs_select_proxy
#' @inheritParams select_proxy
#' @noRd
check_inputs_select_proxy <- function(svy_table, region_code) {

  # Check svy_table
  if (nrow(svy_table) > 2) {
    rlang::abort('`svy_table` can\'t have more than two rows.')
  }
  if (!'svy_pce' %in% names(svy_table)) {
    rlang::abort('Column `svy_pce` not found in `svy_table`.')
  }
  if (!'svy_gdp' %in% names(svy_table)) {
    rlang::abort('Column `svy_gdp` not found in `svy_table`.')
  }

  # Check region input
  regions <- c('LAC', 'NAC', 'ECA', 'SSA', 'EAP', 'SAR', 'MNA')
  if (!region_code %in% regions) {
    msg <- sprintf('`region_code` must be one of \'%s\'.',
                   paste(regions, collapse = "', '"))
    msg2 <- sprintf('You\'ve supplied the code \'%s\'.', region_code)
    rlang::abort(c(msg, x = msg2))
  }

}
