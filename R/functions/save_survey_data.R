#' Save survey data
#' 
#' Save survey data to .fst file in specified output directory. 
#' 
#' @param dt Data frame from cache to be saved
#' @param cols character: Vector with columns to save. If NULL all columns are
#'  saved. 
#' @param output_dir character: Output folder. 
#' @param future_plan character: `future` plan to use. 
#' @param compress numeric: Compression level used in `fst::write_fst()`.  
#' @param chh_filename character: Vector with new names for microdata. 
#' 
save_survey_data <- function(dt, 
                             cols = NULL, 
                             output_dir,
                             chh_filename,
                             future_plan = c('sequential', 'multisession', 'callr'), 
                             compress) {
  
  # Select columns
  if (!is.null(cols)) {
    dt <- dt[, ..cols]
  }
  
  # Create paths
  chh_filename <- fifelse(!grepl("\\.fst$", chh_filename), 
                          paste0(chh_filename, ".fst"), 
                          chh_filename)
  
  svy_out_path <- paste(output_dir, chh_filename, sep = "/")
  
  fst_status <- 
    tryCatch(
      expr = {
        # Your code...
        fst::write_fst(x        = dt, 
                       path     = svy_out_path, 
                       compress = compress)
        "passed"
      }, # end of expr section
      
      error = function(e) {
        "error"
      }, # end of error section
      
      warning = function(w) {
        "warning"
      }
    ) # End of trycatch
    
  attr(svy_out_path, "fst_status") <- fst_status
  
  return(svy_out_path)
}

