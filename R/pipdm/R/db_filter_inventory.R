#' Filter inventory
#'
#' Filter the PIP-Data inventory to select surveys in the Price Framework.
#'
#' @param dt data.table: A table with the inventory.
#' @param pfw_table data.table: A table with the price framework file.
#'
#' @return data.table
#' @export
db_filter_inventory <- function(dt, pfw_table) {

  #--------- Prepare data for merge ---------

  # fix datatype for merge
  pfw_table[
    ,
    surveyid_year := as.numeric(surveyid_year)
  ]

  dt[
    ,
    surveyid_year := as.numeric(surveyid_year)
  ]


  # Get original names + "new_filename_
  orig_names <- c(names(dt), "cache_id")

  dcols <- c(
    "cpi_domain",
    "ppp_domain",
    "gdp_domain",
    "pce_domain",
    "pop_domain"
  )

  #--------- filter and merge data ---------
  dt <-
    pfw_table[
      # filter inpovcal data
      inpovcal == 1
    ][dt, # Merge with inventory data
      on = c(
        "country_code",
        "surveyid_year",
        "survey_acronym"
      )
    ][,
      # Find MAX domain per obs
      reporting_level := apply(.SD, MARGIN = 1, max),
      .SDcols = dcols
    ]

  #--------- Create extra rows for alternative welfare ---------

  # Get obs with alternative welfare
  alt <- dt[oth_welfare1_type != ""]

  # all data
  dt[
    ,
    oth_welfare1_type := NULL # remove variable
  ][
    ,
    is_alt_welf := FALSE
  ]

  if (nrow(alt) != 0) {
    alt[
      ,
      welfare_type := fcase(
        grepl("^([Cc])", oth_welfare1_type), "consumption",
        grepl("^([Ii])", oth_welfare1_type), "income",
        default = ""
      )
    ][
      ,
      oth_welfare1_type := NULL # remove variable
    ][
      ,
      is_alt_welf := TRUE
    ]

    dt <- rbindlist(list(dt, alt),
      use.names = TRUE,
      fill = TRUE
    )
  } # end of confition alt != 0

  #--------- Create new name ---------

  # correspondence data
  crr <- dt[
    ,
    wt := fcase(
      welfare_type == "income", "INC",
      welfare_type == "consumption", "CON",
      default = ""
    )
  ][
    ,
    cache_id := paste(country_code,
      surveyid_year,
      survey_acronym,
      paste0("D", reporting_level),
      wt,
      source,
      sep = "_"
    )
  ][
    ,
    ..orig_names
  ]


  return(crr)
}
