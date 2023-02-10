#' Estimate Societal Poverty Line
#'
#' @param dt data frame with a variable called median
#' @param ppp_year numeric: PPP year
#'
#' @return
#' @export
#'
#' @examples
db_compute_spl <- function(dt, 
                           ppp_year, 
                           median_var = "median") {

#   ____________________________________________________
#   on.exit                                         ####
  on.exit({
    
  })

#   ____________________________________________________
#   Defenses                                        ####
  stopifnot( exprs = {
    is.data.frame(dt)
    median_var %in% names(dt)
    }
  )

#   ____________________________________________________
#   Early returns                                   ####
  if (FALSE) {
    return()
  }

#   ____________________________________________________
#   Computations                                     ####
  if (data.table::is.data.table(dt)) {
    df <- data.table::copy(dt)
  } else {
    df <- data.table::as.data.table(dt)
  }
  
  
  df[, spl := wbpip:::compute_spl(get(median_var), 
                                  ppp_year = ppp_year)]
  

#   ____________________________________________________
#   Return                                           ####
  return(df)

}