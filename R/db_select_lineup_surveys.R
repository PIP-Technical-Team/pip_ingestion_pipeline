#' @importFrom magrittr %>%
NULL

# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= "2.15.1") {
  utils::globalVariables(
    c(
      "survey_year", "is_ref_year", "n_per_welfare_type", "both_sides",
      "dist_from_ref_year", "welfare_type"
    )
  )
}

#' Select correct line-up surveys
#'
#' Select a single line-up survey when there are multiple line-up surveys
#' available.
#'
#' @param dt data.table: Output of [db_get_closest_surveys()].
#'
#' @seealso [select_lineup_survey()]
#' @return data.table
#' @keywords internal
db_select_lineup_surveys <- function(dt) {

  # CHECK inputs
  check_inputs_db_class(dt)

  # Fetch surveys to be used for line-up
  dt$svy_lineup_items <-
    purrr::map2(
      dt$svy_items,
      dt$reference_year_index,
      select_lineup_survey
    )

  # Remove nested 'svy_items' column
  dt$svy_items <- NULL

  return(dt)
}

#' Select a single line-up survey
#'
#' Prioritize surveys to be used for the line-up. This is needed when multiple
#' choices of surveys are available. For instance, if two surveys are available
#' for the same year, one using consumption, the other one income.
#'
#' @param df data.frame: Subset of reference_year table.
#' @param ref_year numeric: Reference year.
#'
#' @return data.frame
#'
#' @keywords internal
select_lineup_survey <- function(df, ref_year) {

  # Return as is if there is only a single survey to choose from
  n_svy <- nrow(df)
  if (n_svy == 1) {
    return(as.data.frame(df))
  }

  # Deal with cases where survey_year == ref_year
  tmp <- df %>%
    dplyr::mutate(
      is_ref_year = ref_year == survey_year
    ) %>%
    dplyr::filter(is_ref_year == TRUE) %>%
    dplyr::select(-is_ref_year) %>%
    as.data.frame()

  # Note: Modifications made by AE 11/19/2020
  # Previous code chunk returned an empty data frame when
  # the there were two income surveys for a survey year
  # equal to the reference year.
  if (nrow(tmp) == 1) {
    return(tmp)
  } else if (any(grepl("consumption", tmp$welfare_type))) {
    return(tmp[tmp[["welfare_type"]] == "consumption", ])
  } else if (nrow(tmp) == 2) {
    return(tmp)
  }

  # Deal with cases with at least one instance of both-sides surveys
  tmp <- df %>%
    dplyr::group_by(welfare_type) %>%
    dplyr::mutate(
      n_per_welfare_type = dplyr::n(),
      both_sides = length(n_per_welfare_type) == 2
    ) %>%
    dplyr::filter(both_sides == TRUE) %>%
    dplyr::select(-n_per_welfare_type, -both_sides) %>%
    as.data.frame()

  if (length(unique(tmp[["welfare_type"]])) > 1) {
    return(tmp[tmp[["welfare_type"]] == "consumption", ])
  }
  if (length(unique(tmp[["welfare_type"]])) == 1) {
    return(tmp)
  }

  # Deal with cases with only one survey of each welfare type
  # Choose survey closest to the ref_year
  if (nrow(df) == 2 & length(unique(df[["welfare_type"]])) > 1) {
    tmp <- df %>%
      dplyr::group_by(welfare_type) %>%
      dplyr::mutate(
        dist_from_ref_year = abs(ref_year - survey_year)
      ) %>%
      dplyr::ungroup() %>%
      dplyr::filter(dist_from_ref_year == min(dist_from_ref_year, na.rm = TRUE)) %>%
      dplyr::select(-dist_from_ref_year) %>%
      as.data.frame()
    if (nrow(tmp) == 1) {
      return(tmp)
    } else {
      return(tmp[tmp[["welfare_type"]] == "consumption", ])
    }
  }
}
