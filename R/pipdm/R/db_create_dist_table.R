#' Create distributional statistics table
#'
#' Create a table with distributional statistics, including a deflated median.
#'
#' @param dl list: A list with distributional statistics datasets.
#' @param crr_inv data frame with correspondence inventory
#' @inheritParams db_create_ref_year_table
#' @inheritParams pip_update_cache_inventory
#'
#' @return data.table
#' @export
db_create_dist_table <- function(dl,
                                 dsm_table,
                                 crr_inv,
                                 verbose = FALSE) {


  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #                   Prepare data   ---------
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  # Checks
  cache_id <- crr_inv$cache_id

  if(length(dl) != length(cache_id))  {
    cli::cli_abort(c("{.field dl} and {.field crr_inv$cache_id} must be of
                     equal lengths.",
                     i = "If you are on testing mode or using {.val pipcheck},
                     try filtering {.field cache_inventory} using
                     {.field cache_id}"),
                   wrap = TRUE)
  }

  names(dl) <- cache_id

  if (data.table::is.data.table(crr_inv)) {
    crr_inv <- data.table::copy(crr_inv)
  } else {
    crr_inv <- data.table::as.data.table(crr_inv)
  }

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #                   Treatment of quantiles   ---------
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  quantiles <-
    # create single lists, national, urban, and rural.
    unlist(dl,
      recursive = FALSE,
      use.names = TRUE
    ) %>%
    # Extract quantiles list
    purrr::map(`[[`, "quantiles") %>%
    # create a list with several tibbles for the deciles
    purrr::map(~ tibble::tibble(
      value = .x,
      decile = c(1:length(.x))
    ))


  qt <-
    # Put everything in a tibble
    tibble::enframe(quantiles,
      value = "quantiles",
      name  = "cache_id"
    ) %>%
    # Create column for reporting_level and fix survey name
    dplyr::mutate(
      reporting_level = gsub("(.+)(\\.)([a-z]+)", "\\3", cache_id),
      cache_id = gsub("(.+)(\\.)([a-z]+)", "\\1", cache_id)
    ) %>%
    # unnest the quantile tibble so that I have only a tibble wit no lists
    tidyr::unnest(quantiles) %>%
    # convert to wide to accommodate to output
    tidyr::pivot_wider(
      values_from = value,
      names_from = decile,
      names_prefix = "decile"
    )

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #            Treatment of other dist stats   ---------
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  dsnames <- names(dl[[1]][[1]])
  dsnames <- dsnames[!(dsnames %in% "quantiles")]

  diststats <-
    # create single lists, national, urban, and rural.
    unlist(dl,
      recursive = FALSE,
      use.names = TRUE
    ) %>%
    # Extract quantiles list
    purrr::map(`[`, dsnames) %>%
    # create a list with several tibbles for the deciles
    purrr::map(tibble::as_tibble)


  ds <-
    # Put everything in a tibble
    tibble::enframe(diststats,
      value = "diststats",
      name  = "cache_id"
    ) %>%
    # Create column for reporting_level and fix survey name
    dplyr::mutate(
      reporting_level = gsub("(.+)(\\.)([a-z]+)", "\\3", cache_id),
      cache_id = gsub("(.+)(\\.)([a-z]+)", "\\1", cache_id)
    ) %>%
    # unnest the quantile tible so that I have only a tiblle wit no lists
    tidyr::unnest(diststats)


  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #           Merges and clean data   ---------
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## merge quantiles with other dist stats --------
  df <- joyn::joyn(qt, ds,
    by = c("cache_id", "reporting_level"),
    match_type = "1:1"
  )

  if (nrow(df[.joyn != "x & y"]) > 0) {
    msg <- "quantiles and othes-dist-stats tables should have the
    very same observations"
    hint <- "There is something wrong with the code above"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }

  df[, .joyn := NULL]


  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ## Merge with correspondence inventory to get survey_id --------

  df[crr_inv,
    on = "cache_id",
    survey_id := i.survey_id
  ]

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  ##           Merge with DSM --------

  dsm_table <-
    dsm_table[, .SD,
      .SDcols =
        c(
          "survey_id", "cache_id", "wb_region_code", "pcn_region_code",
          "country_code", "surveyid_year", "survey_year",
          "reporting_year", "survey_acronym", "welfare_type",
          "cpi", "ppp", "pop_data_level", "reporting_level"
        )
    ]

  # Merge dist stats with DSM (left join)
  dt <- joyn::joyn(df, dsm_table,
    by = c("cache_id", "reporting_level"),
    match_type = "1:1"
  )
  dt_p <- data.table::copy(dt)
  dt_p <- dt_p[,
               problem := paste(cache_id, reporting_level, sep = "-")
             ][,
               .(.joyn, problem)]

  dy <- dt_p[.joyn == "y", problem]
  dx <- dt_p[.joyn == "x", problem]

  if (length(dy) > 0) {
    # NOTE AE: should this be an error to abort or just to notify? I made it to
    # notify
    # NOTE AC: Sorry I don't understand what this code catches.

    if (verbose) {
      cli::cli_rule(left = "The following obs from deflated means table do not have
                    correspondence with dist stats table")
      cli::cli_ul(dy)
      cli::cli_rule(right = "end")
    }
  }

  if (length(dx) > 0) {
    # NOTE AE: should this be an error to abort or just to notify? I made it
    # to abort
    # NOTE AC: The column survey_mean_ppp should not a part of the dist stat table,
    # so that's not really an issue. But if you mean survey_median_lcu, then yes you are
    # right. All observations should have a non-missing median. In that case the problem
    # would lie in db_compute_dist_stats() or in the underlying wbpip functions.

    if (verbose) {
      cli::cli_rule(left = "error on deflated means in the following")
      cli::cli_ul(dx)
      cli::cli_rule(right = "end")
    }

    msg <- "We should no have surveys without deflated means"
    hint <- "Check calculation of means in `db_create_dsm_table` or
    `db_create_lcu_table`"

    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }

  dt <- dt[.joyn == "x & y"][, .joyn := NULL]

  ## ---- Deflate median ----

  data.table::setnames(dt, "median", "survey_median_ppp")

  dt[
    ,
    survey_median_lcu := survey_median_ppp*ppp*cpi,
  ][
    ,
    c("cpi", "ppp") := NULL
  ]
  
  #--- Finalize table ----

  # TEMP FIX: TO BE REMOVED
  # Make sure survey_id is uppercase
  dt$survey_id <- toupper(dt$survey_id)

  # Order columns
  data.table::setcolorder(
    dt,
    c(
      "survey_id", "cache_id", "wb_region_code", "pcn_region_code",
      "country_code", "survey_acronym", "surveyid_year",
      "survey_year", "reporting_year", "welfare_type",
      "reporting_level", "survey_median_lcu",
      "survey_median_ppp"
    )
  )

  # Sort rows
  data.table::setorder(dt, cache_id)

  # change factors to characters
  nn <- names(dt[, .SD, .SDcols = is.factor])
  if (length(nn)) {
    dt[, (nn) := lapply(.SD, as.character),
       .SDcols = nn]
  }

  return(dt)
}
