#' Calculate survey means
#'
#' Calculate survey mean in local currency units (LCU) for a single survey
#' dataset.
#'
#' @inheritParams db_clean_data
#' @param gd_mean numeric: Mean to use for grouped data surveys.
#'
#' @return data.table
#' @export
db_compute_survey_mean <- function(dt, gd_mean = NULL) {
  tryCatch(
    expr = {

      # Get distribution type
      dist_type <- as.character(unique(dt$distribution_type))

      # Calculate weighted welfare mean
      dt <- compute_survey_mean[[dist_type]](dt, gd_mean)

      # Order columns
      data.table::setcolorder(
        dt, c(
          "survey_id", "country_code", "surveyid_year", "survey_acronym",
          "survey_year", "welfare_type", "survey_mean_lcu"
        )
      )

      return(dt)
    }, # end of expr section

    error = function(e) {
     cli::cli_alert_danger("Survey mean caluclation failed. Returning NULL.")

      return(NULL)
    } # end of error
  ) # End of trycatch
}

#' compute_survey_mean
#'
#' A wrapper for choosing the correct survey mean calculation, depending on the
#' distribution type.
#'
#' @inheritParams db_compute_survey_mean
#' @return data.table
#' @noRd
compute_survey_mean <- list(
  micro     = function(dt, gd_mean) md_compute_survey_mean(dt, gd_mean),
  group     = function(dt, gd_mean) gd_compute_survey_mean(dt, gd_mean),
  aggregate = function(dt, gd_mean) gd_compute_survey_mean(dt, gd_mean),
  imputed   = function(dt, gd_mean) md_compute_survey_mean(dt, gd_mean)
)

#' md_compute_survey_mean
#' @inheritParams db_compute_survey_mean
#' @return data.table
#' @noRd
md_compute_survey_mean <- function(dt, gd_mean = NULL) {

  
  uvars <- c( "survey_id",
              "cache_id",
              "country_code",
              "surveyid_year",
              "survey_acronym",
              "survey_year",
              "welfare_type",
              "distribution_type",
              "gd_type")
  
  data_level_vars <-  c("cpi_data_level",
                        "ppp_data_level",
                        "gdp_data_level",
                        "pce_data_level",
                        "pop_data_level")
  
  urep_lev <- unique(dt$reporting_level)
  if (length(urep_lev) == 1) {
    # uvars_inx <- which(names(dt) %in% uvars)
    dt <- dt |> 
      # By imputations
      fgroup_by(imputation_id, reporting_level) |> 
      fsummarise(across(uvars, funique), 
                 survey_mean_lcu = fmean(welfare, w= weight), 
                 weight          = fsum(weight)) |>
      # after imputation 
      fgroup_by(reporting_level) |> 
      fsummarise(across(uvars, funique), 
                 survey_mean_lcu = fmean(survey_mean_lcu, w= weight)) |>
      fungroup() |> 
      # data level vars should be exactly like reporting level when length is 1
      _[, 
        (data_level_vars) := reporting_level]
    
  } else {
    dt <- dt |> 
      fgroup_by(c("imputation_id", "reporting_level", data_level_vars)) |> 
      fsummarise(across(uvars, funique), 
                 survey_mean_lcu = fmean(welfare, w= weight), 
                 weight          = fsum(weight)) |>
      # after imputation 
      fgroup_by(c("reporting_level", data_level_vars)) |> 
      fsummarise(across(uvars, funique), 
                 survey_mean_lcu = fmean(survey_mean_lcu, w= weight)) |>
      fungroup()
  }
  

  return(dt)  
}

#' gd_compute_survey_mean
#' @inheritParams db_compute_survey_mean
#' @return data.table
#' @noRd
gd_compute_survey_mean <- function(dt, gd_mean) {

  # Sort rows by pop_data_level to ensure that means are
  # assigned correctly
  data.table::setorder(dt, pop_data_level)

  # Assign mean to aggregated dataset
  dt <-
    dt[, .(
      survey_id         = unique(survey_id),
      cache_id          = unique(cache_id),
      country_code      = unique(country_code),
      surveyid_year     = unique(surveyid_year),
      survey_acronym    = unique(survey_acronym),
      survey_year       = unique(survey_year),
      welfare_type      = unique(welfare_type),
      distribution_type = unique(distribution_type),
      gd_type           = unique(gd_type),
      survey_mean_lcu   = gd_mean,
      cpi_data_level    = unique(cpi_data_level),
      ppp_data_level    = unique(ppp_data_level),
      gdp_data_level    = unique(gdp_data_level),
      pce_data_level    = unique(pce_data_level),
      pop_data_level    = unique(pop_data_level),
      reporting_level   = unique(reporting_level)
    )]

  return(dt)
}

#' id_compute_survey_mean
#' @inheritParams db_compute_survey_mean
#' @return data.table
#' @noRd
id_compute_survey_mean <- function(dt, gd_mean = NULL) {

  # Slit by imputation id
  dl <- split(dt, f = list(dt$imputation_id))

  # Compute mean by group
  dl_mean <- purrr::map(dl, function(dt) {

    # Compute mean by data levels
    dt[, .(
      survey_id = unique(survey_id),
      cache_id = unique(cache_id),
      country_code = unique(country_code),
      surveyid_year = unique(surveyid_year),
      survey_acronym = unique(survey_acronym),
      survey_year = unique(survey_year),
      welfare_type = unique(welfare_type),
      distribution_type = unique(distribution_type),
      gd_type = unique(gd_type),
      survey_mean_lcu =
        collapse::fmean(
          x = welfare,
          w = weight,
          na.rm = TRUE
        )
    ),
    by = .(
      cpi_data_level, ppp_data_level,
      gdp_data_level, pce_data_level,
      pop_data_level, reporting_level
    )
    ]
  })
  dt <- data.table::rbindlist(dl_mean)

  # Compute mean by data levels
  dt <-
    dt[, .(
      survey_id = unique(survey_id),
      cache_id = unique(cache_id),
      country_code = unique(country_code),
      surveyid_year = unique(surveyid_year),
      survey_acronym = unique(survey_acronym),
      survey_year = unique(survey_year),
      welfare_type = unique(welfare_type),
      distribution_type = unique(distribution_type),
      gd_type = unique(gd_type),
      survey_mean_lcu =
        collapse::fmean(survey_mean_lcu)
    ),
    by = .(
      cpi_data_level,
      ppp_data_level,
      gdp_data_level,
      pce_data_level,
      pop_data_level,
      reporting_level
    )
    ]

  return(dt)
}
