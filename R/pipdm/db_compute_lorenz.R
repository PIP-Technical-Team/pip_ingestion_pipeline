#' Compute Lorenz
#'
#' Calculate points on the Lorenz curve for a single survey dataset.
#'
#' Only available for microdata.
#'
#' @inheritParams db_clean_data
#' @export
db_compute_lorenz <- function(dt, gc = FALSE) {

  tryCatch(

    expr = {

      dist_type <- unique(dt$distribution_type)
      if (dist_type == 'micro') {
        res <- md_compute_lorenz(
          welfare = dt$welfare, weight = dt$weight
        )
      } else {
        rlang::info('Pre-calculation of Lorenz curves only implemented for microdata. Returning NULL.')
        res <- NULL
      }

      return(res)

    }, # end of expr section

    error = function(e) {

      rlang::warn('Lorenz curve calculation failed. Returning NULL.')

      return(NULL)

    } # end of error
  ) # End of trycatch

  # Garbage collection
  if (gc) gc(verbose = FALSE)

}
