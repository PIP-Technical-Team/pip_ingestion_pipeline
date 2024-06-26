#' Calculate distributional statistics
#'
#' Calculate distributional statistics  for a single survey dataset.
#'
#' @inheritParams db_clean_data
#' @param pop_table data.table: A table with population data.
#' @param mean_table data frame with deflated means in PPP.
#' @param cache_id character: cache id to identify the right process
#' @param ppp_year numeric: Round year of PPP  values
#' @return list
#' @export
db_compute_dist_stats <- function(dt, 
                                  mean_table, 
                                  pop_table, 
                                  cache_id, 
                                  ppp_year) {
  tryCatch(
    expr = {

      # Compute dist stats
      res <- compute_dist_stats(dt, 
                                mean_table, 
                                pop_table, 
                                cache_id, 
                                ppp_year)

      return(res)
    }, # end of expr section

    error = function(e) {
      rlang::warn("Distributional statistics caluclation failed. Returning NULL.")

      return(NULL)
    } # end of error
  ) # End of trycatch
}

#' compute_dist_stats
#' @inheritParams db_compute_dist_stats
#' @return list
#' @noRd
compute_dist_stats <- function(dt, 
                               mean_table, 
                               pop_table, 
                               cache_id, 
                               ppp_year) {

  # identify procedure
  source     <- gsub("(.*_)([A-Z]+$)", "\\2", cache_id)
  data_level <- gsub("(.*_)(D[123])(.+$)", "\\2", cache_id)

  # Extract PPP means
  ci    <- cache_id
  mean  <- mean_table[cache_id == ci,
                     survey_mean_ppp ]

  names(mean) <- mean_table[cache_id == ci,
                            reporting_level  ]


  # NOTE: we should variable pop_data_level to something more general. We could
  # use something similar to vartiable reporting_level in the function db_filter_inventory

  # Order by population data level
  data.table::setorder(dt, pop_data_level, welfare_ppp)
  pop_level <- unique(as.character(dt$pop_data_level))

  # get estimates by level
  res <- purrr::map(
    .x = pop_level,
    .f = ~ get_dist_stats_by_level(dt, 
                                   mean, 
                                   source, 
                                   level = .x, 
                                   ppp_year = ppp_year)
  )

  names(res) <- pop_level

  if (data_level != "D1" && length(pop_level) > 1) { # Urban/rural or subnat level

    if (source == "GROUP") { # Group data

      # create synthetic vector
      wf <- purrr::map(
        .x = pop_level,
        .f = ~ get_synth_vector(dt, pop_table, mean, level = .x)
      ) |> 
        rbindlist()
      # data.table::setDT(wf)
      wf[,
         welfare_ppp := welfare
         ][, 
           imputation_id := ""]

    } else { # microdata

      wf <- data.table::copy(dt)
    }
    
    n_imid <- collapse::fnunique(wf$imputation_id) # Number of imputations id
    
    data.table::setorder(wf, imputation_id, welfare_ppp) # Data must be sorted
    if (n_imid == 1) {
      # national mean
      res_national <- md_dist_stats(wf, ppp_year = ppp_year)
    } else {
      # national mean
      res_national <- id_dist_stats(wf, ppp_year = ppp_year)
    }
    

    res <- append(list(res_national), res)
    names(res) <- c("national", pop_level)
  }

  return(res)
}

#' md_dist_stats
#' @inheritParams db_compute_dist_stats
#' @return list
#' @noRd
md_dist_stats <- function(dt, mean = NULL, ppp_year) {
  # Calculate dist stats
  res <- wbpip:::md_compute_dist_stats(
    welfare  = dt$welfare_ppp,
    weight   = dt$weight,
    mean     = mean
  )
  return(res)
}

#' gd_dist_stats
#' @inheritParams db_compute_dist_stats
#' @return list
#' @noRd
gd_dist_stats <- function(dt, mean, ppp_year) {
  # Calculate dist stats
  res <- wbpip:::gd_compute_dist_stats(
    welfare    = dt$welfare,  # cummulative distribution. Not actual welfare
    population = dt$weight,
    mean       = mean
  )

  # Rename deciles to quantiles (for comparability with md_compute_dist_stats)
  names(res)[which(names(res) == "deciles")] <- "quantiles"
  return(res)
}

#' id_dist_stats
#' @inheritParams db_compute_dist_stats
#' @return list
#' @noRd
id_dist_stats <- function(dt, ppp_year) {

  # Slit by imputation id   -----
  dl <- split(dt, f = list(dt$imputation_id))

  # Compute stats by imputation ID   ----
  dl_stats <- purrr::map(dl, md_dist_stats)

  # get mean of imputations----
  
  stats_names <- names(dl_stats[[1]])
  res <- vector("list", length(stats_names))
  names(res) <- stats_names
  
  for (x in stats_names) {
    res[[x]] <- mean_over_id(dl_stats, x)
  }
  
  # Return ---------
  return(res)
}


#' extract x from list and get the mean of all imputed obs
#'
#' @param l list with dist stats
#' @param x name of the element in l. e.g., mean, median, quantiles... 
#'
#' @return list
mean_over_id <- function(l, x) {
  map(l, x) |>  
    unlist2d(idcols = FALSE) |>
    fmean() |> 
    unname()
}


#' Get dist stats based on data level for md or gd
#'
#' @inheritParams db_compute_dist_stats
#' @inheritParams get_synth_vector
#'
#' @return list
#' @noRd
get_dist_stats_by_level <- function(dt, 
                                    mean, 
                                    source, 
                                    level, 
                                    ppp_year) {
  df <- dt[reporting_level == level]

  if (source == "GROUP") {

    res <- gd_dist_stats(df, mean[level], ppp_year)

  } else {

    is_imputed <- length(unique(df$imputation_id)) > 1

    if (is_imputed) {

        res <- id_dist_stats(df, ppp_year)

    } else {

      res <- md_dist_stats(df, mean[level], ppp_year)

    }
  }
  return(res)
}

#' Get synthetic vector based on data level
#'
#' @param dt data.table: A table with grouped data.
#' @param pop_table data.table: A table with population data.
#' @param mean numeric: A named vector of means. The name must correspond to
#'   the data level of the value,
#' @param level character: data level. it could  be national, urban, rural, or
#'   any other subnational division
#'
#' @return data.frame
#' @noRd
get_synth_vector <- function(dt, pop_table, mean, level) {
  df        <- dt[reporting_level == level]
  ccode     <- dt[, unique(country_code)]
  svid_year <- dt[, unique(surveyid_year)]

  popf <- pop_table[
    country_code     == ccode
    & year           == svid_year
    & pop_data_level == level,
    pop
  ]

  wf <- wbpip:::sd_create_synth_vector(
    welfare    = df$welfare,
    population = df$weight,
    mean       = mean[level],
    pop        = popf
  )
  return(wf)
}
