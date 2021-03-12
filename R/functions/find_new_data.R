#' Find which data is not available in cache folder or has changed SVY ID
#'
#' @param pipeline_inventory data.table: Pipeline inventory table. 
#' @param cache_svy_dir character: Output folder for the cached survey data. 
#'
#' @return
#' @export
#'
#' @examples
find_new_data <- function(pipeline_inventory, 
                          cache_svy_dir) {
  # get svy id available in PFW
  cch_ids <- pipeline_inventory$cache_id # Cache ID
  
  # Svy ID
  svy_ids <- pipeline_inventory$filename
  
  # Get existing survey ids 
  existing_chh_ids <- gsub('(.+)(\\.fst)', '\\1', list.files(cache_svy_dir))
  
  #--------- Identify new Surveys ---------
  
  # Select new surveys
  new_svy_ids <- svy_ids[!(cch_ids %in% existing_chh_ids)]
  
  #--------- find data whose SVY id has changed ---------
  # load correspondence inventory
  crr_dir <- glue::glue("{cache_svy_dir}_crr_inventory/")
  crr_inv <- fst::read_fst(glue::glue("{crr_dir}crr_inventory.fst"))
  data.table::setDT(crr_inv)
  
  chd_svy <- 
    crr_inv[pipeline_inventory, 
            on = "cache_id", 
            filename_svy := i.filename
    ][
      filename_svy != filename
    ][,
      filename
    ]
  
  if (length(chd_svy) > 0) {
    new_svy_ids <- c(new_svy_ids, chd_svy)
  }
  
  df <- pipeline_inventory[filename %chin% new_svy_ids
                           ][,
                             .(filename, cache_id)
                             ][,
                               svy_ids := gsub('(.+)(\\.dta)', '\\1', filename)
                               ]
  
  return(df)
}