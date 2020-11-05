##' .. content for \description{} (no empty lines) ..
##'
##' .. content for \details{} ..
##'
##' @title
##' @param ud
##' @param old
join_dsm_tables <- function(ud, 
                            old = NULL) {
  setDT(ud)
  
  if (is.null(old)) {
    df <- ud
  } else {
    setDT(old)
    new_id <- ud[, .(survey_id = unique(survey_id))]
    
    #remove in old in case there is an update
    old <- old[!new_id,
               on = .(survey_id)]
    
    # append data
    df <- rbindlist(list(ud, old), 
                    use.names = TRUE, 
                    fill = TRUE)
  }
  setorder(df, country_code, surveyid_year, module, vermast, veralt)
  
  return(df)

}
