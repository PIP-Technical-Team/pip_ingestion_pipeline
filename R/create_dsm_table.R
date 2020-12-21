##' .. content for \description{} (no empty lines) ..
##'
##' .. content for \details{} ..
##'
##' @title
##'
##' @param cpi Data frame with most latest version of CPI
##' @param ppp Data frame with most latest version of PPP
##' @param dt  Updated table with LCU means
create_dsm_table <- function(cpi = aux_cpi, 
                             ppp = aux_ppp, 
                             dt  = updated_lcum) {

  # Make sure everything is in data.table format
  setDT(dt)
  setDT(cpi)
  setDT(ppp)
  
  #--------- merge CPI ---------
  cpi_keys <- c("country_code", "surveyid_year", "survey_acronym", "cpi_data_level")
  cpi[, 
      surveyid_year := as.character(surveyid_year)]
  
  dt[cpi,
     on = cpi_keys,
     `:=`(
       cpi = i.cpi,
       ccf = i.ccf
     )
  ]
  
  #--------- merge PPP ---------
  ppp_keys <- c("country_code", "ppp_data_level")
  dt[ppp[ppp_default == TRUE],  # just default values
     on = ppp_keys,
     `:=`(
       ppp = i.ppp
     )
  ]
  
  
  #--------- Welfare to PPP values ---------
  dt[,
      dsm_mean := lcu_mean/cpi/ppp/ccf
  ]
  
  return(dt)

}
