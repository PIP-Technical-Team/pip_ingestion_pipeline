#' Create table with grouped data survey means
#'
#' Create table with grouped data survey means from the PCN Masterfile.
#'
#' Survey means cannot be automatically calculated for grouped data, so at some
#' stage the mean needs to be entered manually. This function reads from the PCN
#' masterfile to ensure that PCN and PIP uses the same data means.
#'
#' This source **should** be changed in the future.
#'
#' @param pcn_master_path character: Path to PCN masterfile.
#' @param pfw_table data.table: A table with the price framework file.
#' @param inventory data.table: A table with the PIP inventory.
#'
#' @export
db_create_gd_svy_mean_table <- function(pcn_master_path, pfw_table, inventory) {

  # ---- Read from master file ----

  # Read SurveyMean sheet from Master file
  df <- readxl::read_xlsx(pcn_master_path, sheet = 'SurveyMean')

  # Select for grouped data surveys
  df <- df[grepl('[.]T0[1,2,5]$', df$DistributionFileName), ]

  # Select and rename columns
  df <- df[c('CountryCode', 'SurveyTime', 'DataType', 'Coverage',
             'SurveyMean_LCU', 'DistributionFileName', 'SurveyID')]
  names(df) <- c('country_code', 'survey_year',  'welfare_type',
                 'survey_coverage', 'survey_mean_lcu',
                 'pcn_source_file', 'pcn_survey_id')

  # Recode columns
  df$survey_coverage <- tolower(df$survey_coverage)
  df$welfare_type <- tolower(df$welfare_type)
  df$welfare_type <- ifelse(df$welfare_type == 'x', 'consumption', 'income')

  # Add pop_data_level column
  df$pop_data_level <-
    ifelse(!df$country_code %in% c('CHN', 'IDN', 'IND'),
           'national', df$survey_coverage)

  # Add dist and gd type columns
  df$distribution_type <-
    ifelse(df$pop_data_level == 'national',
           'group', 'aggregate')
  df$gd_type <- sub('.*[.]', '', df$pcn_source_file)

  # ---- Merge with PFW ----

  # Subset columns
  pfw_table <-
    pfw_table[, c('region_code', 'country_code', 'welfare_type', 'surveyid_year',
                  'survey_year', 'survey_acronym', 'inpovcal')]

  # Merge to add surveyid_year
  tmp <- pfw_table[, c('country_code', 'surveyid_year', 'survey_year')]
  df <- merge(df, tmp, by = c('country_code', 'survey_year'), all.x = TRUE)

  # Merge to add survey_acronym and inpovcal
  df <- merge(df, pfw_table, all.x = TRUE,
              by = c('country_code', 'surveyid_year',
                     'survey_year', 'welfare_type'))

  # Filter to select surveys in PovcalNet
  df <- df[df$inpovcal == 1, ]
  df <- df[!is.na(df$inpovcal), ]

  # ---- Merge with inventory ----

  # Create survey_id column
  inventory$survey_id <- sub('[.]dta', '', inventory$filename)

  # Subset GD rows
  inventory <- inventory[inventory$module == 'PC-GROUP',]

  # Subset columns
  inventory <- inventory[, c('country_code', 'surveyid_year',
                             'survey_acronym', 'survey_id')]
  # Merge to add PIP survey_id
  df <-  merge(df, inventory, all.x = TRUE,
               by = c('country_code', 'surveyid_year',
                      'survey_acronym'), )

  # ---- Finalize table ----

  # Select columns
  df <- df[c('country_code', 'surveyid_year', 'survey_year', 'welfare_type',
             'survey_mean_lcu', 'distribution_type', 'gd_type',
             'pop_data_level', 'pcn_source_file',
             'pcn_survey_id', 'survey_id')]
  df$survey_id <- toupper(df$survey_id)

  # Convert LCU means to daily values
  #df$survey_mean_lcu <- df$survey_mean_lcu * (12/365)

  # Convert to data.table
  dt <- data.table::as.data.table(df)

  # Sort rows
  data.table::setorder(dt, country_code, surveyid_year, pop_data_level)

  # Sort columns
  data.table::setcolorder(dt, 'survey_id')

  return(dt)
}
