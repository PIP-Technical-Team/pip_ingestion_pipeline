#' Title
#'
#' @param chh_filename character: Cache filename
#' @param survey_id character: Original Survey ID
#' @param pip_data_dir character: Input folder for the raw survey data.  
#' @param cols character: vector of variables to keep. Default is NULL.
#' @param cache_svy_dir character: Output directory
#' @param compress numeric: Compression level used in `fst::write_fst()`.  
#'
#' @return
#' @export
#'
#' @examples
process_data <- function(survey_id, 
                         chh_filename,
                         pip_data_dir,
                         cols = NULL,
                         cache_svy_dir,
                         compress) {
  
  
  #--------- Load data ---------
  
  tryCatch(
    expr = {
      # Load data
      df <- pipload::pip_load_data(
        survey_id = survey_id, 
        maindir = pip_data_dir, 
        noisy = FALSE)
    }, # end of expr section
    
    error = function(e) {
      df <- NULL
    }, # end of error section
    
    warning = function(w) {
      df <- NULL
    }
  ) # End of trycatch
  
  
  if (is.null(df)) {
    ret <- data.table(id     = survey_id, 
                      status = "error loading")
    return(ret)
  }
  
  #--------- Clean Data ---------
  
  tryCatch(
    expr = {
      # Clean DAta
      df <- pipdm::db_clean_data(df)
    }, # end of expr section
    
    error = function(e) {
      df <- NULL
    }, # end of error section
    
    warning = function(w) {
      df <- NULL
    }
  ) # End of trycatch
  
  
  if (is.null(df)) {
    ret <- data.table(id     = survey_id, 
                      status = "error cleaning")
    return(ret)
  }
  
  #--------- Saving data ---------
  tryCatch(
    expr = {
      # Your code...
      if (!is.null(cols)) {
        df <- df[, .SD, .SDcols = cols]
      }
      
      # Create paths
      
      chh_filename <- fifelse(!grepl("\\.fst$", chh_filename), 
                              paste0(chh_filename, ".fst"), 
                              chh_filename)
      
      svy_out_path <- paste(cache_svy_dir, chh_filename, sep = "/")
      
      write_fst(x = df, 
                path = svy_out_path, 
                compress = compress)
    }, # end of expr section
    
    error = function(e) {
      df <- NULL
    }, # end of error section
    
    warning = function(w) {
      df <- NULL
    }
  ) # End of trycatch
  
  
  if (is.null(df)) {
    ret <- data.table(id     = survey_id, 
                      status = "error saving")
    return(ret)
  }
  
  ret <- data.table(id     = survey_id, 
                    status = "success")
  
  return(ret)

}
