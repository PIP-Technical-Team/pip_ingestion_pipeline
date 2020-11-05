##' .. content for \description{} (no empty lines) ..
##'
##' .. content for \details{} ..
##'
##' @title
##' @param new_dsm
##' @param nameme1
save_dsm <- function(new_dsm, 
                     pipedir,
                     output) {
  
  # time for vintage
  time <- format(Sys.time(), "%Y%m%d%H%M%S") 
  attr(new_dsm, "datetime") <- time
  
  # make sure ingestion pipeline directory exists
  dsm_dir      <- paste0(pipedir, "dsm/")
  dsm_vint_dir <- paste0(dsm_dir, "_vintage/")
  
  if (!fs::dir_exists(dsm_dir)) {
    fs::dir_create(dsm_vint_dir, 
                   recurse = TRUE)
  }
  
  # modify output
  plain_output  <- gsub("\\.fst", "", output)
  nodir_output  <- gsub("output/", "", plain_output)
  vt_output     <- paste0(nodir_output, "_",time)
  
  
  #--------- Save files ---------
  
  fst::write_fst(x    = new_dsm,
                 path = output)
  
  haven::write_dta(data = new_dsm,
                   path = paste0(plain_output, ".dta"))
  
  #vintages and backup
  fst::write_fst(x    = new_dsm,
                 path = paste0(dsm_dir, nodir_output, ".fst"))
  
  haven::write_dta(data = new_dsm,
                   path = paste0(dsm_dir, nodir_output, ".dta"))
  
  #vintages and backup
  fst::write_fst(x    = new_dsm,
                 path = paste0(dsm_vint_dir, vt_output, ".fst"))
  
  haven::write_dta(data = new_dsm,
                   path = paste0(dsm_vint_dir, vt_output, ".dta"))
  
  return(invisible(TRUE))
}
