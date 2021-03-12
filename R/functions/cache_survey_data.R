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
#' @param compendium boolean: Should create compendium?
#' 
cache_survey_data <- function(pipeline_inventory,
                              pip_data_dir, 
                              cache_svy_dir,
                              compress   = 100,
                              verbose    = TRUE, 
                              compendium = TRUE) {
  
  
  #--------- Parameters ---------
  
  # correspondence file
  crr_dir      <- glue::glue("{cache_svy_dir}_crr_inventory/")
  crr_filename <- glue::glue("{crr_dir}crr_inventory.fst")
  
  # Get all survey ids
  if (verbose) {
    cli::cli_alert('Checking for new survey ids...')
  }
  
  #--------- Identify new Surveys ---------
  new_svy_ids <- find_new_data(pipeline_inventory, 
                               cache_svy_dir)
  
  if (verbose) {
    cli::cli_alert("Found {.field {nrow(new_svy_ids)}} new survey(s)...")
  }
  
  # Early return
  if (nrow(new_svy_ids) == 0) {
    
    crr          <- fst::read_fst(crr_filename, 
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
                         process_data(survey_id     = .x, 
                                      chh_filename  = .y,
                                      pip_data_dir  = pip_data_dir,
                                      cache_svy_dir = cache_svy_dir,
                                      compress      = 100) 
                       })
  
  #--------- Save correspondence file ---------
  crr_status <- update_crr_inventory(pipeline_inventory,
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
  
  #--------- compendium ---------
  
  if (compendium == TRUE ) {
    
    if (verbose) {
      cli::cli_alert("Creating compendium...")
    }
    
    dl_svy_data <- 
      load_cached_survey_data(
        survey_id     = NULL,
        cache_svy_dir = cache_svy_dir)
    
    if (verbose) {
      cli::cli_alert("Saving compendium...")
    }
    
    qs::qsave(x    = dl_svy_data,  
              file = glue::glue("{cache_svy_dir}_compendium/compendium.qs"))
    
    if (verbose) {
      cli::cli_alert("Compendium saved...")
    }
  }
  
  
  #--------- DONE ---------
  if (verbose) {
   cli::cli_alert("Done!")
  }
  
  return(invisible(list(processed_data = df,
                        data_available = crr)))
}



