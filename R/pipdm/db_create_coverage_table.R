#' Create coverage table
#'
#' Create a table with coverage estimates at regional, WLD and TOT levels.
#'
#' @param ref_year_table data.table: Full interpolated means table. Output of
#'   `db_create_ref_year_table()`.
#' @inheritParams db_create_ref_year_table
#' @param digits numeric: The number of digits the returned coverage numbers are
#'   rounded by.
#' @param special_countries character: A string with 3-letter country codes.
#'
#' @export
db_create_coverage_table <- function(ref_year_table,
                                     pop_table,
                                     ref_years,
                                     digits = 2,
                                     special_countries =
                                       c('ARG','CHN','IDN', 'IND')) {

  # ---- Prepare Reference year table ----

  # Select relevant columns
  dt <- ref_year_table[,
          c('wb_region_code', 'pcn_region_code',
            'country_code', 'reporting_year',
            'survey_year', 'welfare_type',
            'pop_data_level')]

  # Remove duplicated rows
  dt <- dt[!duplicated(dt), ]

  # Transform table to one row per country-year-coverage
  dt <- dt[, .(survey_year = toString(survey_year)),
           by = list(country_code, reporting_year,
                     pop_data_level, welfare_type,
                     pcn_region_code, wb_region_code)]
  dt$survey_year_2 <- dt$survey_year %>%
    regmatches(. , gregexpr(', .*', .)) %>%
    gsub(', ', '', .) %>%
    ifelse(. == 'character(0)', NA, .) %>%
  as.numeric()
  dt$survey_year <-
    gsub(', .*', '', dt$survey_year) %>%
    as.character() %>%
    as.numeric()

  # ---- Prepare Population table ----

  # Select national population estimates except for special countries
  pop_table <- pop_table[(pop_data_level == 'national' |
                            country_code %in% special_countries), ]

  # Remove national population estimates for selected countries
  pop_table <- pop_table[!(pop_data_level == 'national' &
                           country_code %in% special_countries), ]

  # Select population estimates for selected reference years
  pop_table <- pop_table[year %in% ref_years, ]

  # Remove domain column
  pop_table$pop_domain <- NULL


  # ---- Merge datasets ----

    # Merge dt with pop_table (full outer join)
  dt <- merge(dt, pop_table,
              by.x = c('country_code', 'reporting_year', 'pop_data_level'),
              by.y = c('country_code', 'year', 'pop_data_level'),
              all = TRUE)

  # ---- Create coverage column ----

  # Remove rows with missing Population data
  dt <- dt[!is.na(pop), ]

  # Create coverage column (current method)
  dt$coverage <- (abs(dt$reporting_year - dt$survey_year) < 3 |
                    abs(dt$reporting_year - dt$survey_year_2) < 3)
  dt$coverage <- data.table::fifelse(dt$coverage, 100, 0)
  dt[is.na(coverage),]$coverage <- 0

  # ---- Calculate world and regional coverage ----

  # PCN Regional coverage
  out_region <- dt %>%
    dplyr::group_by(reporting_year, pcn_region_code) %>%
    dplyr::summarise(coverage = stats::weighted.mean(coverage, pop)) %>%
    data.table::as.data.table()

  # World coverage
  out_wld <- dt %>%
    dplyr::group_by(reporting_year) %>%
    dplyr::summarise(coverage = stats::weighted.mean(coverage, pop)) %>%
    base::transform(pcn_region_code = 'WLD') %>%
    data.table::as.data.table()

  # Total coverage (World less Other High Income)
  out_tot <- dt %>%
    dplyr::filter(pcn_region_code != 'OHI') %>%
    dplyr::group_by(reporting_year) %>%
    dplyr::summarise(coverage = stats::weighted.mean(coverage, pop)) %>%
    base::transform(pcn_region_code = 'TOT') %>%
    data.table::as.data.table()

  # Combine to a single table
  out <- rbind(out_region, out_wld, out_tot)
  out$coverage <- round(out$coverage, digits)

  return(out)

}
