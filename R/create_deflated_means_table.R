##' .. content for \description{} (no empty lines) ..
##'
##' .. content for \details{} ..
##'
##' @title
##'
##' @param dt 
##' @param cpi 
##' @param ppp 
##' @param inventory
create_deflated_means_table <- function(dt        = microdata, 
                                        cpi       = aux_data[["cpi"]], 
                                        ppp       = aux_data[["ppp"]], 
                                        inventory = inventory) {

  # Make sure everything is in data.table format
  setDT(dt)
  setDT(cpi)
  setDT(ppp)
  
  #--------- merge CPI ---------
  cpi_keys <- c("country_code", "surveyid_year", "survey_acronym", "cpi_data_level")
  dt[cpi,
     on = cpi_keys,
     `:=`(
       cpi = i.cpi,
       ccf = i.ccf
     )
  ]
  
  #--------- merge PPP ---------
  ppp_keys <- c("country_code", "ppp_data_level")
  dt[ppp[ppp_default == TRUE],  # just defatul values
     on = ppp_keys,
     `:=`(
       ppp = i.ppp
     )
  ]
  
  
  #--------- Welfare to PPP values ---------
  dt[,
     welfare_ppp := welfare/cpi/ppp/ccf
  ]
  
  #--------- calculate survey mean ---------
  
  # Survey-Mean
  sm <- dt[,
           .(svy_mean = weighted.mean(welfare_ppp, weight, na.rm = TRUE)),
           by = survey_id
  ]
  
  cnames <-
    c(
      "country_code",
      "year",
      "survey_acronym",
      "vermast",
      "M",
      "veralt",
      "A",
      "collection",
      "module"
    )
  
  # check number of items Pick third one by random (it could be any other row)
  # linv <- inventory[[3]]
  # nobj <- length(strsplit(linv, "/")[[1]])
  
  sm[,
     
     # Name sections of filename into variables
     (cnames) := tstrsplit(survey_id, "_", fixed=TRUE)
  ][,
    # create tool and source
    c("tool", "source") := tstrsplit(module, "-", fixed = TRUE)
  ][,
    # change to lower case
    c("vermast", "veralt") := lapply(.SD, tolower),
    .SDcols = c("vermast", "veralt")
  ][
    ,
    # Remove unnecessary variables
    c("M", "A", "collection") := NULL
  ][
    # Remove unnecessary rows
    !(is.na(survey_id))
  ]
  
  return(sm)

}
