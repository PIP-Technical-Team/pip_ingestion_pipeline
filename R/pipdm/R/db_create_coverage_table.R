#' Create coverage tables
#'
#' Create a list of tables with coverage estimates at 1) regional, WLD, TOT and
#' 2) income group levels.
#'
#' @param ref_year_table data.table: Full interpolated means table. Output of
#'   `db_create_ref_year_table()`.
#' @param cl_table data.table: Country list table with all WDI countries.
#' @param incgrp_table data.table: Table with historical income groups for all
#'   WDI countries.
#' @inheritParams db_create_ref_year_table
#' @param digits numeric: The number of digits the returned coverage numbers are
#'   rounded by.
#' @param urban_rural_countries character: A string with 3-letter country codes.
#'   Countries where the coverage calculation is based on urban or rural
#'   population numbers.
#' @param gls list from `pipfun::pip_create_globals()`
#'
#' @return list
#' @export
db_create_coverage_table <- function(ref_year_table,
                                     pop_table,
                                     cl_table,
                                     incgrp_table,
                                     ref_years,
                                     digits = 2,
                                     urban_rural_countries =
                                       c("ARG", "CHN", "IDN", "IND", "SUR"), 
                                     gls) {

  # ---- Prepare Reference year table ----

  # Select relevant columns
  dt <- ref_year_table[
    ,
    c(
      "pcn_region_code", # "wb_region_code",
      "country_code", "reporting_year",
      "survey_year", "welfare_type",
      "pop_data_level", "reporting_level"
    )
  ]

  # Transform table to one row per country-year-reporting_level
  dt <- dt[, .(survey_year = toString(survey_year)),
           by = list(
             country_code, reporting_year,
             pop_data_level, welfare_type,
             pcn_region_code, # wb_region_code,
             reporting_level
           )
  ]
  dt$survey_year_after <- dt$survey_year %>%
    regmatches(., gregexpr(", .*", .)) %>%
    gsub(", ", "", .) %>%
    ifelse(. == "character(0)", NA, .) %>%
    as.numeric()
  dt$survey_year <-
    gsub(", .*", "", dt$survey_year) %>%
    as.character() %>%
    as.numeric()
  dt <- data.table::setnames(dt, 'survey_year', 'survey_year_before')

  # ---- Prepare Population table ----

  # Select national population estimates except for selected countries
  pop_table <- pop_table[(pop_data_level == "national" |
    country_code %in% urban_rural_countries), ]

  # Remove national population estimates for selected countries
  pop_table <- pop_table[!(pop_data_level == "national" &
    country_code %in% urban_rural_countries), ]

  # Select population estimates for selected reference years
  pop_table <- pop_table[year %in% ref_years, ]

  # Remove domain column
  pop_table$pop_domain <- NULL

  # Merge with cl (to get *_region_code for all countries)
  pop_table <-
    merge(pop_table,
          cl_table[, c('country_code', 'pcn_region_code')],
          by = 'country_code',
          all.x = TRUE)

  # Merge with historical income group table
  pop_table <-
    merge(pop_table,
          incgrp_table[, c('country_code', 'year_data', 'incgroup_historical')],
          by.x = c('country_code', 'year'),
          by.y = c('country_code', 'year_data'),
          all.x = TRUE
    )
  # Impute incgroup_historical 1981-86 based on 1987 value
  pop_table <- pop_table[order(country_code, year)]
  pop_table[, incgroup_historical := impute_missing(incgroup_historical), by = country_code]

  # ---- Merge datasets ----

  # Merge dt with pop_table (full outer join)
  dt <- merge(dt, pop_table,
    by.x = c("country_code", "reporting_year", "pop_data_level", "pcn_region_code"),
    by.y = c("country_code", "year", "pop_data_level", "pcn_region_code"),
    all = TRUE
  )

  # ---- Create coverage column ----

  # Remove rows with missing Population data
  dt <- dt[!is.na(pop), ]

  # Create coverage column (current method)
  dt$coverage <- (abs(dt$reporting_year - dt$survey_year_before) <= 3 |
    abs(dt$reporting_year - dt$survey_year_after) <= 3)
  # dt$coverage <- data.table::fifelse(dt$coverage, 1, 0)
  dt[is.na(coverage), ]$coverage <- 0

  # ---- Calculate world and regional coverage ----

  # PCN Regional coverage
  out_region <- dt %>%
    dplyr::group_by(reporting_year, pcn_region_code) %>%
    dplyr::summarise(coverage = stats::weighted.mean(coverage, pop)) %>%
    data.table::as.data.table()
  
  # bypass coverage rule for nowcasted years
  lnp_year <- fmax(gls$PIP_LINEUP_YEARS)
  out_region[reporting_year > lnp_year & coverage < .5, 
             coverag := .51]

  # World coverage
  out_wld <- dt %>%
    dplyr::group_by(reporting_year) %>%
    dplyr::summarise(coverage = stats::weighted.mean(coverage, pop)) %>%
    base::transform(pcn_region_code = "WLD") %>%
    data.table::as.data.table()

  # Total coverage (World less Other High Income)
  out_tot <- dt %>%
    dplyr::filter(pcn_region_code != "OHI") %>%
    dplyr::group_by(reporting_year) %>%
    dplyr::summarise(coverage = stats::weighted.mean(coverage, pop)) %>%
    base::transform(pcn_region_code = "TOT") %>%
    data.table::as.data.table()

  # Income group coverage
  out_inc <- dt %>%
    dplyr::filter(incgroup_historical %in% c("Low income", "Lower middle income")) %>%
    dplyr::group_by(reporting_year) %>%
    dplyr::summarise(coverage = stats::weighted.mean(coverage, pop)) %>%
    dplyr::mutate(incgroup_historical = "LIC/LMIC") %>%
    data.table::as.data.table()
  out_inc <-  out_inc[, c('reporting_year', 'incgroup_historical', 'coverage')]

  # Create output list
  out <- list(region = rbind(out_region, out_wld, out_tot),
              incgrp = out_inc,
              country_year_coverage = dt)

  # Adjust digits
  out$region$coverage <- round(out$region$coverage * 100, digits)
  out$incgrp$coverage <- round(out$incgrp$coverage * 100, digits)

  return(out)
}

#' Set missing to first available value
#' @noRd
impute_missing <- function(x) {
  x[is.na(x)] <- x[!is.na(x)][1]
  return(x)
}


