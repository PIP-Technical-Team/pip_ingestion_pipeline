#' Clean survey data
#'
#' Clean a survey datasets by applying methods from `wbpip:::md_clean_data()`
#' and `wbpip:::gd_clean_data()`.
#'
#' @param dt data.table: A survey dataset.
#' @param gc logical: If TRUE garbage collection is forced.
#' @return data.table
#' @export
db_clean_data <- function(dt, gc = FALSE) {

  tryCatch(
    expr = {

      # Clean dataset
      dt <- clean_data(dt)

      # Order by population data level
      data.table::setorder(dt, pop_data_level)

      # Remove labels from each column
      dt[] <- lapply(dt, c)

      # Garbage collection
      if (gc) gc(verbose = FALSE)

      return(dt)

    }, # end of expr section

    error = function(e) {

      rlang::warn('Data cleaning failed. Returning NULL.')

      # Garbage collection
      if (gc) gc(verbose = FALSE)

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
  gd_type <- as.numeric(sub('T0', '', unique(dt$gd_type)))

  # Calculate distributional statistics
  if (dist_type == 'micro') {
    # Clean data (remove negative values etc.)
    dt <- md_clean_data(
      dt, welfare = 'welfare', weight = 'weight',
      quiet = TRUE)$data
  }
  if (dist_type == 'group') {
    # Standardize to type 1
    dt <- gd_clean_data(
      dt,
      welfare = 'welfare',
      population = 'weight',
      gd_type = gd_type,
      quiet = TRUE)
  }
  if (dist_type == 'aggregate') {
    # Split by area
    dt_rural <- dt[dt$area == 'rural']
    dt_urban <- dt[dt$area == 'urban']
    # Standardize rural
    dt_rural <- gd_clean_data(
      dt_rural,
      welfare = 'welfare',
      population = 'weight',
      gd_type = gd_type,
      quiet = TRUE)
    # Standardize urban
    dt_urban <- gd_clean_data(
      dt_urban,
      welfare = 'welfare',
      population = 'weight',
      gd_type = gd_type,
      quiet = TRUE)
    # Bind back together
    dt <- rbind(dt_rural, dt_urban)
  }
  if (dist_type == 'imputed') {
    # Clean data (remove negative values etc.)
    dt <- md_clean_data(
      dt, welfare = 'welfare', weight = 'weight',
      quiet = TRUE)$data
  }

  return(dt)

}
