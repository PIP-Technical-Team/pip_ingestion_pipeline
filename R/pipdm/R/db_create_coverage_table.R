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
  dt$survey_year_after <- dt$survey_year |>
    regmatches(., gregexpr(", .*", .)) |>
    gsub(", ", "", .) |>
    ifelse(. == "character(0)", NA, .) |>
    as.numeric()
  dt$survey_year <-
    gsub(", .*", "", dt$survey_year) |>
    as.character() |>
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
          cl_table[, c('country_code', 'pcn_region_code', 'africa_split_code')],
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
  pop_table[, incgroup_historical := impute_missing(incgroup_historical), 
            by = country_code]
  
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
  
  
  # set limits for rules
  year_threshold <- 3
  year_break     <- 2019.5
  
  # find differences for surveys before and after reporting year
  dt[, `:=`(
    lower_limit = year_threshold,
    upper_limit = survey_year_after - year_break
  )]
  
  # the upper limit only applies to those year that are within the 
  # year threshold and the difference between the year of the survey
  # afeter and the COVID break. All the other years have the same 
  # year threshold
  dt[upper_limit > year_threshold | upper_limit < 0, 
     upper_limit := year_threshold]
  
  # first we find coverage for the upper limit. 
  dt[!is.na(upper_limit), 
     coverage := (survey_year_after - reporting_year) <= upper_limit]
  
  # if the condition of lower limits meets, 
  # it has prevalence over the one of upper limit. That's why it 
  # is executed after. Yet, if the lower limit does not meet, but 
  # after limit does, then the coverage will TRUE. 
  
  dt[!is.na(lower_limit), 
     coverage := (reporting_year - survey_year_before) <= lower_limit]
  
  dt[is.na(coverage), 
     coverage := FALSE]
  
  # ---- Calculate world and regional coverage ----
  
  # PCN Regional coverage
  out_region <- dt  |> 
    fgroup_by(reporting_year, pcn_region_code) |> 
    fsummarise(coverage = fmean(coverage, pop)) |> 
    fungroup()
  
  
  # World coverage
  out_wld <- dt |>
    fgroup_by(reporting_year) |>
    fsummarise(coverage = fmean(coverage, pop)) |>
    fungroup() |> 
    ftransform(pcn_region_code = "WLD")
  
  # Total coverage (World less Other High Income)
  out_tot <- dt |>
    fsubset(pcn_region_code != "OHI") |>
    fgroup_by(reporting_year) |>
    fsummarise(coverage = stats::weighted.mean(coverage, pop)) |>
    fungroup() |> 
    ftransform(pcn_region_code = "TOT")
  
  # Income group coverage
  out_inc <- dt |>
    fsubset(incgroup_historical %in% c("Low income", "Lower middle income")) |>
    fgroup_by(reporting_year) |>
    fsummarise(coverage = stats::weighted.mean(coverage, pop)) |>
    fungroup() |> 
    ftransform(incgroup_historical = "LIC/LMIC") |> 
    fselect(c('reporting_year', 'incgroup_historical', 'coverage'))
  
  out_ssa <- dt |> 
    fsubset(!is.na(africa_split_code)) |> 
    fgroup_by(reporting_year, africa_split_code) |> 
    fselect(coverage, pop) |> 
    fmean(w = pop, keep.w = FALSE) |> 
    frename(pcn_region_code   = africa_split_code) |> 
    qDT()
  
  
  # Create output list
  out <- list(region = rowbind(out_region, out_wld, out_tot, out_ssa, 
                               fill = TRUE) |> 
                setorder(pcn_region_code, reporting_year ),
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


