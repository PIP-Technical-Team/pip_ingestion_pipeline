create_globals <- function(root_dir = '//w1wbgencifs01/pip') {
  glbs <- list()
  
  # Input dir 
  glbs$PIP_DATA_DIR     <- paste0(root_dir, '/PIP-Data_QA/') 
  
  # '//w1wbgencifs01/pip/pip_ingestion_pipeline/' # Output dir
  glbs$PIP_PIPE_DIR     <- paste0(root_dir, '/pip_ingestion_pipeline/') 
  
  # Cached survey data dir
  glbs$CACHE_SVY_DIR    <- paste0(glbs$PIP_PIPE_DIR, 'pc_data/cache/clean_survey_data/') 
  
  # Final survey data output dir
  glbs$OUT_SVY_DIR      <- paste0(glbs$PIP_PIPE_DIR, 'pc_data/output/survey_data/') 
  
  #  Estimations output dir
  glbs$OUT_EST_DIR      <- paste0(glbs$PIP_PIPE_DIR, 'pc_data/output/estimations/') 
  
  # aux data output dir
  glbs$OUT_AUX_DIR      <- paste0(glbs$PIP_PIPE_DIR, 'pc_data/output/aux/')  
  
  time             <- format(Sys.time(), "%Y%m%d%H%M%S") 
  
  ### Max dates --------
  
  c_month  <- as.integer(format(Sys.Date(), "%m"))
  max_year <- ifelse(c_month >= 8,  # August
                     as.integer(format(Sys.Date(), "%Y")) - 1, # After august
                     as.integer(format(Sys.Date(), "%Y")) - 2) # Before August
  
  glbs$PIP_YEARS        <- 1977:(max_year + 1) # Years used in PIP 
  glbs$PIP_REF_YEARS    <- 1981:max_year # Years used in the interpolated means table
  
  glbs$FST_COMP_LVL     <- 100 # Compression level for .fst output files
  
  return(glbs)
}