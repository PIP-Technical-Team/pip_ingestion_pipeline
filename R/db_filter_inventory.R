#' Filter inventory
#'
#' Filter the PIP-Data inventory to select surveys to be used in the PIP
#' ingestion pipeline.
#'
#' Selects the latest version of surveys in the Price Framework file (with
#' `inpovcal = 1`) and belong to the Poverty Calculator (PC) module. Also
#' selects the right source for each country-survey_id year (e.g. GPWG is
#' preferred over BIN).
#'
#' @param raw_inventory data.table: A table with the raw inventory. Output of
#'   `read_inventory()`.
#' @param pfw_table data.table: A table with the price framework file.
#' @param dsm_in character: File path of DSM input data
#'
#' @return data.table
#' @export
db_filter_inventory <- function(raw_inventory,
                                pfw_table,
                                dsm_in) {

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   parameters   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  pfw      <- data.table::as.data.table(pfw_table)
  ri       <- data.table::as.data.table(raw_inventory)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   Filter according to PFW   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  pfw[,
      surveyid_year := as.character(surveyid_year)]

  ri[pfw,
     on = c('country_code', 'surveyid_year',
            'survey_acronym'),
     inpovcal := i.inpovcal
     ]

  # dt <- data.table::merge.data.table(
  #   dt, pfw_table, all.x = TRUE,
  #   by = c('country_code', 'surveyid_year',
  #          'survey_acronym'))
  #
  # # Select surveys in PovcalNet
  ri <- ri[inpovcal == 1]


  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   filter according to DSM table   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  ri <- ri[,
           survey_id := gsub("\\.dta", "", filename)
          ]

  if (fs::file_exists(dsm_in)) {
    # Inventory in Use
    csdm <- fst::read_fst(dsm_in)
    setDT(csdm)

    iu <- csdm[, "survey_id"]

    # Get only those that are not in use
    ni <- ri[!iu,
             on = .(survey_id)]

  } else {

    # If deflated svy file does not exist use the whole raw inventory
    ni <- ri
  }

  # To DELETE
  # ni <- ni[country_code %chin% c("HND", "PER", "PRY", "KGZ", "AGO", "POL")]
  # ni <- ni[country_code %chin% c("CHL")]

  if (nrow(ni) == 0) {
    dt <- NULL
  } else {
    dt <- ni[["survey_id"]]
  }


  return(dt)

}
