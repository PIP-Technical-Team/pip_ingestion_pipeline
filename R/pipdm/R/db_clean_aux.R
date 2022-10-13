#' Clean auxiliary data
#'
#' Clean auxiliary data for PIP webpage.
#'
#' @param dt data.table: An auxiliary dataset.
#' @param name character: Name, e.g. 'ppp', 'gdp' etc.
#' @inheritParams db_create_ref_year_table
#' @return data.table
#' @export
db_clean_aux <- function(dt, name, pip_years) {
  if (name == "cpi") {

    # Subset columns
    dt <- dt[, c(
      "country_code", "survey_year",
      "cpi", "cpi_data_level"
    )]

    # Take floor value of survey year
    dt$year <- floor(dt$survey_year)

    # Remove duplicates
    dt <- unique(dt,  by = c("country_code", "cpi_data_level", "year"))
  }

  if (name == "ppp") {
    dt <- dt[ppp_default_by_year == TRUE]
  }

  # Rename columns
  names(dt) <- sub(paste0(name, "_"), "", names(dt))

  # Subset to only include years used by PIP
  dt <- dt[dt$year %in% pip_years, ]

  # Reshape to wide format
  dt <- dcast(data =  dt,
              formula =  country_code + data_level ~ year,
              value.var = name, 
              fill = NA)

  return(dt)
}
