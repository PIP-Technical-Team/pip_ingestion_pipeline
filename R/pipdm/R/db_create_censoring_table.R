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

  # Censor WLD if LIC/LMIC coverage is less than threshold
  coverage_list$incgrp %>% data.table::setnames('coverage', 'coverage_incgrp')
  wld <- merge(coverage_list$region[pcn_region_code == "WLD"],
               coverage_list$incgrp, by = 'reporting_year')
  wld$coverage <- ifelse(wld$coverage_incgrp < coverage_threshold, 0, wld$coverage)
  wld <- wld[, c('reporting_year', 'pcn_region_code', 'coverage')]
  coverage_list$region[pcn_region_code == "WLD"] <- wld

  # Filter by threshold
  dt <- coverage_list$region[coverage < coverage_threshold]
  dt %>% data.table::setnames('pcn_region_code', 'region_code')
  dt$statistic <- 'all'
  dt$id <- with(dt, sprintf('%s_%s', region_code, reporting_year))
  dt <- dt[, c('region_code', 'reporting_year', 'statistic', 'id')]

  # Add region-years to censor based on coverage
  censored$regions <- rbind(censored$regions, dt)

  return(censored)
}
