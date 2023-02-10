#' Compute lineup median for SPL calculations
#'
#' @param ref_lkup dataframe with estimations for reference year. In the
#'   original pipeline it would be the target `dt_prod_ref_estimation` from the
#'   function `db_create_ref_estimation_table()`
#' @param cache list of cacha files
#' @param ppp_year numeric: PPP year
#'
#' @return
#' @export
#'
#' @examples
#' \dontrun{
#' tar_load(dt_prod_ref_estimation)
#' db_compute_lineup_median(ref_lkup = dt_prod_ref_estimation, cache = cache) # you must load the cache first
#' }
db_compute_lineup_median <- function(ref_lkup, 
                                     cache) {

#   ____________________________________________________________________________
#   on.exit                                                                 ####
  on.exit({
    
  })

#   ____________________________________________________________________________
#   Defenses                                                                ####
  stopifnot( exprs = {
      is.list(cache)
      is.data.frame(ref_lkup)
    }
  )

#   ____________________________________________________________________________
#   Early returns                                                           ####
  if (FALSE) {
    return()
  }

#   ____________________________________________________________________________
#   Computations                                                            ####
  ref_lkup <-
    ref_lkup[,
             data_interpolation_id := paste(cache_id,
                                            reporting_level,
                                            sep = "_")
    ][,
      data_interpolation_id := paste(unique(data_interpolation_id),
                                     collapse = "|"),
      by = .(interpolation_id)]
  
  # Create interpolation list.
  # This is to facilitate interpolation computations
  
  # unique entries
  unique_vars <- c("country_code",
                   "reporting_year",
                   "welfare_type",
                   "reporting_level")
  
  ue <-
    unique(ref_lkup[,
                    ..unique_vars])
  
  # dt <- ref_lkup[country_code == "IND" & reporting_year  == 2016]
  
  # ue <- ue[1:3] 
  lue <- split(ue, 1:nrow(ue))
  # lue <- split(ue, 1:3)
  
  poss_get_lineup_median = purrr::possibly(.f = get_lineup_median, 
                                           otherwise = NA)
  
  medians <-
    purrr::map_dbl(cli::cli_progress_along(lue),
                   poss_get_lineup_median,
                   lue         = lue,
                   cache       = cache, 
                   ref_lkup    = ref_lkup, 
                   unique_vars = unique_vars)
  
  ue[, median := medians]
  
#   ____________________________________________________________________________
#   Return                                                                  ####
  return(ue)

}


#' Get the line up median
#'
#' @param i index or names of list lue
#' @param lue  list of one-row value from data.table (used `split()` to create )
#' @param unique_vars character: unique vars used in `db_compute_lineup_median`
#' @inheritParams db_compute_lineup_median
#'
#' @return
#' @export
#'
#' @examples
get_lineup_median <- function(i, 
                              lue,
                              ref_lkup, 
                              cache, 
                              unique_vars) {

#   ____________________________________________________________________________
#   on.exit                                                                 ####
  on.exit({
    
  })

#   ____________________________________________________________________________
#   Defenses                                                                ####
  stopifnot( exprs = {
      
    }
  )

#   ____________________________________________________________________________
#   Early returns                                                           ####
  if (FALSE) {
    return()
  }

#   ____________________________________________________________________________
#   Computations                                                            ###
  ue <- lue[[i]]
  
  
  
  dt <- ref_lkup[ue,
                 on = unique_vars]
  
  dt_vars <- names(dt)
  ul <- purrr::map(dt_vars,
            ~ {
              dt[, unique(get(.x))]
            })
  
  names(ul) <- dt_vars
  
  svy_data <- purrr::map(ul$cache_id,
                         ~ {
                           d <- cache[[.x]]
                           d <-
                             d[reporting_level == ul$reporting_level]
                           d
                         })
  
  names_out <- sprintf("df%s",
                       seq_along(ul$cache_id) - 1)
  names(svy_data) <- names_out
 
  
  tmp_stats <- wbpip:::prod_fg_compute_pip_stats(
    request_year           = ul[["reporting_year"]],
    data                   = svy_data,
    predicted_request_mean = dt[["predicted_mean_ppp"]],
    svy_mean_lcu           = dt[["survey_mean_lcu"]],
    svy_median_lcu         = dt[["survey_median_lcu"]],
    svy_median_ppp         = dt[["survey_median_ppp"]],
    survey_year            = dt[["survey_year"]],
    default_ppp            = ul[["ppp"]],
    distribution_type      = dt[["distribution_type"]],
    poverty_line           = 2.15,
    popshare               = .5,
    ppp                    = NULL
  )
  
  median <- tmp_stats$poverty_line
  
  

#   ____________________________________________________________________________
#   Return                                                                  ####
  return(median)

}


# 
