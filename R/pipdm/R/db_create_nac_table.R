#' Merge GDP and PCE data
#'
#' Create a table with national accounts (NAC) data by merging separate tables
#' for GDP and PCE.
#'
#' Both input tables should contain ISO-3 country codes, calender years and
#' information indicating the data level and domain (e.g national, urban/rural
#' or sub-national) of the variable in question. Note that data levels must be
#' comparable across datasets.
#' @inheritParams db_create_ref_year_table
#' @return data.table
#' @keywords internal
db_create_nac_table <- function(gdp_table, pce_table, pip_years) {

  # Standardize ^_data_level ^_domain column names
  names(pce_table) <- sub("^pce[_]", "nac_", names(pce_table))
  names(gdp_table) <- sub("^gdp[_]", "nac_", names(gdp_table))

  # Merge GDP and PCE by country, year, data_level and domain (full join)
  dt <- joyn::merge(gdp_table, pce_table,
    by = c(
      "country_code", "year", "nac_data_level",
      "nac_domain"
    ),
    match_type = "1:1",
    reportvar = FALSE
  )

  # Remove domain column
  dt$nac_domain <- NULL

  # Subset to only include years used by PIP
  dt <- dt[year %in% pip_years]

  return(dt)
}
