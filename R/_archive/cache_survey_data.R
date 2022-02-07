#' Cache survey data
#'
#' Save a cleaned version of the survey data in the cache survey data directory.
#' 
#' @param pipeline_inventory data.table: Pipeline inventory table. 
#' @param pip_data_dir character: Input folder for the raw survey data.  
#' @param cache_svy_dir character: Output folder for the cached survey data. 
#' @param compress numeric: Compression level used in `fst::write_fst()`. 
#' @param verbose boolean: If TRUE additional messages are printed to the 
#' console. 
#' @param force logical: if TRUE, all files will be generates again.
#' 
cache_survey_data <- function(pipeline_inventory,
                              pip_data_dir, 
                              cache_svy_dir,
                              compress   = 100,
                              verbose    = TRUE, 
                              force      = FALSE) {
  
  
  #--------- Parameters ---------
  
  # correspondence file
  crr_dir      <- glue::glue("{cache_svy_dir}_crr_inventory/")
  crr_filename <- glue::glue("{crr_dir}crr_inventory.fst")
  
  # Get all survey ids
  if (verbose) {
    cli::cli_alert('Checking for new survey ids...')
  }
  
  #--------- Identify new Surveys ---------
  
  # real new files
  if (isFALSE(force)) {
    
  new_svy_ids <- find_new_data(pipeline_inventory, 
                               cache_svy_dir)
  if (verbose) {
    cli::cli_alert("Found {.field {nrow(new_svy_ids)}} new survey(s)...")
  }
  
  } else { # if force is TRUE
    
    new_svy_ids <- 
      pipeline_inventory[, 
                         .(filename, cache_id)
                         ][,
                           svy_ids := gsub('(.+)(\\.dta)', '\\1', filename)
                           ] 
    if (verbose) {
      cli::cli_alert("Since {.code force = TRUE}, {.field {nrow(new_svy_ids)}} 
                     cache files will be recreated.", wrap = TRUE)
    }
    
  }
  
  
  # Early return
  if (nrow(new_svy_ids) == 0) {
    
    
    if ( !(file.exists(crr_filename)) ) {
      
      cli::cli_alert_warning("Correspondence inventory file not found. 
                           It will be created", 
                           wrap = TRUE)
      
      pip_update_cache_inventory(pipeline_inventory,
                                        cache_svy_dir)
    } 
    
    crr    <- fst::read_fst(crr_filename, 
                            as.data.table = TRUE)
    
    return(invisible(list(processed_data = "No data processed",
                          data_available = crr)))
    
  }
  
  #--------- Process data: Load, clean, and save ---------
  if (verbose) {
    cli::cli_alert("Processing raw PIP data...")
  }
  
  pb <- progress::progress_bar$new(format = ":what [:bar] :percent eta: :eta",
                                   clear = , total = nrow(new_svy_ids), width = 80)
  
  df <- purrr::map2_df(.x = new_svy_ids$svy_ids,
                       .y = new_svy_ids$cache_id,
                       .f = ~ {
                         id_what <- gsub("([A-Z]+_[0-9]+)(.+)", "\\1", .x)
                         pb$tick(tokens = list(what = id_what))
                          process_svy_data_to_cache(
                           survey_id     = .x, 
                           chh_filename  = .y,
                           pip_data_dir  = pip_data_dir,
                           cache_svy_dir = cache_svy_dir,
                           compress      = 100) 
                       })
  
  #--------- Save correspondence file ---------
  crr_status <- pip_update_cache_inventory(pipeline_inventory,
                                                  cache_svy_dir)
  if (verbose && crr_status) {
    
    cli::cli_alert_success('Correspondence inventory file saved')
    
  } else {
    
    cli::cli_alert_danger('Correspondence inventory file 
                          {.strong {col_red("NOT")}} saved', 
                          wrap = TRUE)
  }
  
  # load correspondence file
  crr          <- fst::read_fst(crr_filename, 
                                as.data.table = TRUE)
  
  #--------- DONE ---------
  if (verbose) {
    cli::cli_alert("Done!")
  }
  
  return(invisible(list(processed_data = df,
                        data_available = crr)))
}



