#' Create censoring table
#'
#' Create censoring table based on manual inputs recorded in the AUX data, as well as the regional coverage.
#'
#' @param censored list: List with manual provided countries or regions to censor.
#' @param coverage_list list: Coverage list. Output of `db_create_coverage_table()`.
#' @param coverage_threshold numeric: Value with the threshold to censor regions based on their coverage (e.g 50 %).
#'
#' @export
db_create_censoring_table <- function(censored, coverage_list, coverage_threshold) {

  
  # create separete data.tables
  ig <- copy(coverage_list$incgrp)
  re <- copy(coverage_list$region)
  
  
  # Censor WLD if LIC/LMIC coverage is less than threshold
  data.table::setnames(ig, 'coverage', 'coverage_incgrp')
  
  # world coverage
  wld <- data.table::merge.data.table(
    x = re[pcn_region_code == "WLD"],
    y = ig, 
    by = 'reporting_year')
  
  wld[, 
      coverage := fifelse(coverage_incgrp < coverage_threshold, 0, coverage)
      ]
  
  wld <- wld[, c('reporting_year', 'pcn_region_code', 'coverage')]
  
  # update regional table with world data
  re[pcn_region_code == "WLD"] <- wld

  # Filter by threshold
  dt <- re[coverage < coverage_threshold]
  
  data.table::setnames(dt, 'pcn_region_code', 'region_code')
  
  dt[, statistic := 'all']
  dt[, id := paste(region_code, reporting_year, sep = "_")]
  
  dt <- dt[, c('region_code', 'reporting_year', 'statistic', 'id')]

  # Add region-years to censor based on coverage
  censored$regions <- rbind(censored$regions, dt)

  return(censored)
}
