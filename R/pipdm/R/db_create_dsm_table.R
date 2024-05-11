#' @import data.table
NULL

#' Create deflated survey mean table
#'
#' Create a table with deflated welfare means for each country and surveyid
#' year.
#'
#' @param lcu_table data.table: A table with newly estimated LCU survey means.
#'   Output of `db_create_lcu_table()`.
#' @param cpi_table data.table: A table with CPI data.
#' @param ppp_table data.table: A table with PPP data.
#' @return data.table
#' @export
db_create_dsm_table <- function(lcu_table,
                                cpi_table,
                                ppp_table) {


  #--------- Merge with CPI ---------

  # Select CPI columns
  cpi_table <-
    cpi_table[, .SD,
      .SDcols =
        c(
          "country_code", "survey_year", "survey_acronym",
          "cpi_data_level", "cpi"
        )
    ]

  # Merge survey table with CPI (left join)
  dt <- joyn::joyn(lcu_table, cpi_table,
    by = c(
      "country_code", "survey_year",
      "survey_acronym", "cpi_data_level"
    ),
    match_type = "m:1"
  )

  if (nrow(dt[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure CPI table is up to date"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }

  dt <- dt[
    .joyn != "y" # This is unnecessary data in cpi table... should we have it?
  ][, .joyn := NULL]

  #--------- Merge with PPP ---------

  # Select default PPP values
  ppp_table <- ppp_table[ppp_default == TRUE]

  # Select PPP columns
  ppp_table <-
    ppp_table[, .SD,
      .SDcols =
        c("country_code", "ppp_data_level", "ppp")
    ]

  # Merge survey table with PPP (left join)
  jn <- joyn::joyn(dt, ppp_table,
    by = c("country_code", "ppp_data_level"),
    match_type = "m:1"
  )

  if (nrow(jn[.joyn == "x"]) > 0) {
    msg <- "We should not have NOT-matching observations from survey-mean tables"
    hint <- "Make sure PPP table is up to date"
    rlang::abort(c(
      msg,
      i = hint
    ),
    class = "pipdm_error"
    )
  }


  cdt <- dt[, unique(country_code)]
  cppp <- jn[.joyn == "y", unique(country_code)]

  dt <- jn[
    .joyn != "y" # Countries in PPP table for which we don't have data
  ][, .joyn := NULL]

  #--------- Deflate welfare mean ---------

  # svy_mean_ppp = survey_mean_lcu / cpi / ppp
  dt$survey_mean_ppp <-
    wbpip::deflate_welfare_mean(
      welfare_mean = dt$survey_mean_lcu, ppp = dt$ppp, cpi = dt$cpi
    )


  #--------- Add comparable spell ---------

  dl <- split(dt, list(dt$country_code, dt$survey_comparability))
  dl <- lapply(dl, function(x) {
    if (nrow(x) == 1) {
      x$comparable_spell <- x$reporting_year
    } else {
      x$comparable_spell <-
        sprintf(
          "%s - %s",
          x$reporting_year[1],
          x$reporting_year[length(x$reporting_year)]
        )
    }
    return(x)
  })
  dt <- data.table::rbindlist(dl)

  #--------- Finalize table ---------

  # Add is_interpolated column
  dt$is_interpolated <- FALSE

  # Add is_used_for_line_up column
  dt <- create_line_up_check(dt)

  # Add is_used_for_aggregation column
  dt[, n_rl := .N, by = cache_id]
  dt[, is_used_for_aggregation := 
       ifelse((dt$reporting_level %in% c("urban", "rural") & dt$n_rl == 2), 
              TRUE, FALSE)]
  dt$n_rl <- NULL
  
  ## replace reporting_pop with survey_pop  ----------
  
  # In principle, for all survey year calculations,
  # we should use survey_pop information because it reflects the population in
  # decimal years. reporting_pop should the same are survey_pop for integer
  # years. The following code should show you that. You must uncomment and be in
  # debugging mode
  #  dt[ reporting_pop != survey_pop, .(country_code, reporting_year, survey_year)]
  
  dt[, reporting_pop := survey_pop]
  

  ## Select and order columns -----------
  dt <- dt[, .SD,
    .SDcols =
      c(
        "survey_id", "cache_id", "wb_region_code", "pcn_region_code",
        "country_code", "survey_acronym", "survey_coverage",
        "survey_comparability", "comparable_spell",
        "surveyid_year", "reporting_year",
        "survey_year", "survey_time", "welfare_type",
        "survey_mean_lcu", "survey_mean_ppp", #' survey_pop',
        "reporting_pop", "ppp", "cpi", "pop_data_level",
        "gdp_data_level", "pce_data_level",
        "cpi_data_level", "ppp_data_level", "reporting_level",
        "distribution_type", "gd_type",
        "is_interpolated", "is_used_for_line_up",
        "is_used_for_aggregation", "display_cp"
      )
  ]

  # Add aggregated mean for surveys split by Urban/Rural
  dt <- add_aggregated_mean(dt)

  # Sort rows
  data.table::setorder(dt, survey_id)

  # change factors to characters
  nn <- names(dt[, .SD, .SDcols = is.factor])
  dt[, (nn) := lapply(.SD, as.character),
     .SDcols = nn]

  return(dt)
}

#' add_aggregated_mean
#'
#' Compute and add aggregated (national) rows for surveys where the mean
#' calculation is split by urban / rural population level.
#'
#' @param dt data.table: A table with deflated survey means.
#' @return data.table
#' @noRd
add_aggregated_mean <- function(dt) {

  # Select rows w/ non-national pop_data_level
  dt_sub <- dt[is_used_for_aggregation == TRUE]

  # Compute aggregated mean (weighted population average)
  dt_agg <-
    dt_sub[, .(
      # survey_id       = unique(survey_id),
      # cache_id        = unique(cache_id),
      wb_region_code       = unique(wb_region_code),
      pcn_region_code      = unique(pcn_region_code),
      country_code         = unique(country_code),
      survey_acronym       = unique(survey_acronym),
      survey_coverage      = unique(survey_coverage),
      survey_comparability = unique(survey_comparability),
      comparable_spell     = unique(comparable_spell),
      surveyid_year        = unique(surveyid_year),
      reporting_year       = unique(reporting_year),
      survey_year          = unique(survey_year),
      survey_time          = unique(survey_time),
      welfare_type         = unique(welfare_type),
      survey_mean_lcu      = collapse::fmean(
        x = survey_mean_lcu,
        w = reporting_pop
      ),
      survey_mean_ppp      = collapse::fmean(
        x = survey_mean_ppp,
        w = reporting_pop
      ),
      reporting_pop           = collapse::fsum(reporting_pop),
      ppp                     = NA,
      cpi                     = NA,
      pop_data_level          = "national",
      gdp_data_level          = "national",
      pce_data_level          = "national",
      cpi_data_level          = "national",
      ppp_data_level          = "national",
      reporting_level         = "national",
      distribution_type       = unique(distribution_type),
      gd_type                 = unique(gd_type),
      is_interpolated         = FALSE,
      is_used_for_line_up     = FALSE,
      is_used_for_aggregation = FALSE,
      display_cp              = unique(display_cp)

    ),
    by = .(survey_id, cache_id)
    ]

  dt <- rbind(dt_agg, dt)

  # Sort rows
  data.table::setorder(dt, survey_id, cache_id)
  
  # fix data lvel vars for cases like IDN 1984
  dt_vars <- grep("data_level$", names(dt), value = TRUE)
  
  dt <- funique(dt, 
                 cols =  c("country_code",
                           "reporting_level",
                           "welfare_type",
                           "survey_year")
  )
  
  
  dt[,
      (dt_vars) := lapply(.SD, \(.) {
        fifelse(reporting_level == "national", reporting_level, .)
      }),
      .SDcols = dt_vars
  ]
  
  
  return(dt)
}


