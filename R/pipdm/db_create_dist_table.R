#' Create distributional statistics table
#'
#' Create a table with distributional statistics, including a deflated median.
#'
#' @param dl list: A list with distributional statistics datasets.
#' @param survey_id character: A vector with survey ids.
#' @inheritParams db_create_ref_year_table
#'
#' @return data.table
#' @export
db_create_dist_table <- function(dl, survey_id, dsm_table) {

  # Checks
  assertthat::assert_that(
    length(dl) == length(survey_id),
    msg = '`dl` and `survey_id` must be of equal lengths.')

  # ---- Covert from list to data table ----

  # Create data frame
  dl <- purrr::map(dl, .f = function(x) do.call('rbind', x))
  dl <- purrr::map2(dl, survey_id, function(x, y ) cbind(x, survey_id = y))
  df <- do.call('rbind', dl) %>% as.data.frame()
  df$pop_data_level <- sub('[.].*', '', row.names(df))
  df$survey_id <- unname(unlist(df$survey_id))
  df[1:5] <- purrr::map_df(
    df[1:5], function(x) unname(unlist(x)))
  df$quantiles <- df$quantiles %>% unname()
  row.names(df) <- NULL

  # Convert quantiles vectors to matrix
  deciles <-
    data.frame(
      matrix(unlist(df$quantiles),
             nrow = nrow(df),
             ncol = 10))
  names(deciles) <- paste0('decile', 1:10)
  df <- cbind(df, deciles)
  df$quantiles <- NULL

  # Remove mean column
  df$mean <- NULL

  # Convert to data table
  dt <- df %>%
    data.table::as.data.table()

  # ---- Merge with DSM ----

  # Select DSM columns
  dsm_table <-
    dsm_table[, .SD, .SDcols =
                c('survey_id', 'wb_region_code', 'pcn_region_code',
                  'country_code', 'surveyid_year', 'survey_year',
                  'reporting_year', 'survey_acronym', 'welfare_type',
                  'cpi', 'ppp')]

  # Merge survey table with CPI (left join)
  dt <- data.table::merge.data.table(
    dt, dsm_table, all.x = TRUE,
    by = 'survey_id')

  # ---- Deflate median ----

  # survey_median_ppp = median / cpi / ppp
  dt$survey_median_lcu <- dt$median
  dt$median <- NULL
  dt$survey_median_ppp <-
    wbpip::deflate_welfare_mean(
      welfare_mean = dt$survey_median_lcu, ppp = dt$ppp, cpi = dt$cpi)
  dt$cpi <- NULL
  dt$ppp <- NULL

  # --- Finalize table ----

  # Order columns
  data.table::setcolorder(
    dt,
    c('survey_id', 'wb_region_code', 'pcn_region_code',
      'country_code', 'survey_acronym', 'surveyid_year',
      'survey_year', 'reporting_year', 'welfare_type',
      'pop_data_level', 'survey_median_lcu',
      'survey_median_ppp'))

  # Sort rows
  data.table::setorder(dt, survey_id)

  return(dt)
}
