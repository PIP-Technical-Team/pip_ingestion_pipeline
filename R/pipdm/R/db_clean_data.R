#' Clean survey data
#'
#' Clean a survey datasets by applying methods from `wbpip:::md_clean_data()`
#' and `wbpip:::gd_clean_data()`.
#'
#' @param dt data.table: A survey dataset.
#' @return data.table
#' @export
db_clean_data <- function(dt) {
  tryCatch(
    expr = {

      # Clean dataset
      dt <- clean_data(dt)

      # Order by population data level
      data.table::setorder(dt, pop_data_level)

      # Remove labels from each column
      dt[] <- lapply(dt, c)

      return(dt)
    }, # end of expr section

    error = function(e) {
      rlang::warn("Data cleaning failed. Returning NULL.")

      return(NULL)
    } # end of error
  ) # End of trycatch
}

#' clean_data
#' @inheritParams db_clean_data
#' @noRd
clean_data <- function(dt) {

  # Get distribution type
  dist_type <- unique(dt$distribution_type)

  # Get GD type (1, 2, or 5)
  gd_type <- as.numeric(sub("T0", "", unique(dt$gd_type)))

  # Calculate distributional statistics
  if (dist_type == "micro") {
    # Clean data (remove negative values etc.)
    df <- wbpip:::md_clean_data(
      dt,
      welfare = "welfare",
      weight = "weight",
      quiet = TRUE
    )$data

  } else if (dist_type == "group") {
    # Standardize to type 1
    df <- wbpip:::gd_clean_data(
      dt,
      welfare = "welfare",
      population = "weight",
      gd_type = gd_type,
      quiet = TRUE
    )

  } else if (dist_type == "aggregate") {
    # Split by area
    dt_rural <- dt[dt$area == "rural"]
    dt_urban <- dt[dt$area == "urban"]

    # Standardize rural
    if (nrow(dt_rural) > 0) {

      dt_rural <- wbpip:::gd_clean_data(
        dt_rural,
        welfare = "welfare",
        population = "weight",
        gd_type = gd_type,
        quiet = TRUE
      )
      ra <- TRUE
    } else {
      ra <- FALSE
    }

    # Standardize urban
    if (nrow(dt_urban) > 0) {

      dt_urban <- wbpip:::gd_clean_data(
        dt_urban,
        welfare = "welfare",
        population = "weight",
        gd_type = gd_type,
        quiet = TRUE
      )
      ua <- TRUE

    } else {
      ua <- FALSE
    }
    # Bind back together
    if (all(ua, ra)) {

      df <- rbind(dt_rural, dt_urban)

    } else if (ua) {

      df <- dt_urban

    } else if (ra) {

      df <- dt_rural

    } else {
      cli::cli_abort("there is neither urban nor rural observations in this
                     aggregate data",
                     wrap = TRUE)
    }

  } else if (dist_type == "imputed") {
    # Clean data (remove negative values etc.)
    df <- wbpip:::md_clean_data(
      dt,
      welfare = "welfare",
      weight = "weight",
      quiet = TRUE
    )$data

  } else {
    stop("`dist_type` not valid")
  }

  return(df)
}
