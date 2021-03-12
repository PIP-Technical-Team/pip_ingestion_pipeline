#' Update correspondence inventory file
#'
#' @param pipeline_inventory data.table: Pipeline inventory table. 
#' @param cache_svy_dir character: Output folder for the cached survey data. 
#'
#' @return
#' @export
#'
#' @examples
update_crr_inventory <- function(pipeline_inventory,
                                 cache_svy_dir) {
  
  # get surveys available in cache dir
  
  cch  <- data.table(cache_file = list.files(cache_svy_dir, 
                                              full.names = TRUE, 
                                              pattern = "\\.fst$"))
  cch[, cache_id := gsub('(.+/)([^/]+)(\\.fst)', '\\2', cache_file)]
  cch <- cch[!grepl("^_", cache_id)]
  
  # Filter pipeline inventory and select relevant variables
  cols  <- c("orig", "filename", "survey_id")
  icols <- paste0("i.", cols)
  crr   <- cch[pipeline_inventory,
             on = "cache_id", 
             (cols) := mget(icols)
             ][,
               survey_id := gsub("\\.dta", "", filename)
               ]
  
  # Vintage
  time <- format(Sys.time(), "%Y%m%d%H%M%S") # find a way to account for time zones
  
  crr_dir      <- glue::glue("{cache_svy_dir}_crr_inventory/")
  crr_filename <- glue::glue("{crr_dir}crr_inventory")
  crr_vintage  <- glue::glue("{crr_dir}vintage/crr_inventory_{time}")
  
  # FST
  fst::write_fst(crr, glue::glue("{crr_filename}.fst"))
  fst::write_fst(crr, glue::glue("{crr_vintage}.fst"))
  
  #dta
  haven::write_dta(crr, glue::glue("{crr_filename}.dta"))
  haven::write_dta(crr, glue::glue("{crr_vintage}.dta"))
  
  return(invisible(TRUE))
}

