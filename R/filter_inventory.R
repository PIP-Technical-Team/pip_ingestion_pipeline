##' .. content for \description{} (no empty lines) ..
##'
##' .. content for \details{} ..
##'
##' @title
##' @param raw_inventory
filter_inventory <- function(raw_inventory) {
  
  # Raw Inventory
  ri <- data.table::as.data.table(raw_inventory)
  ri <- ri[,
     survey_id := gsub("\\.dta", "", filename)
     ][toupper(tool) == "PC"
       ]
  
  if (fs::file_exists("output/deflated_svy_means.fst")) {
    # Inventory in Use
    csdm <- fst::read_fst("output/deflated_svy_means.fst")
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
  ni <- ni[country_code %chin% c("CHL")]
  
  return(ni)

}
