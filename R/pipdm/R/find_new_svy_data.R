#' Find which data is not available in cache folder or has changed SVY ID
#'
#' @param pipeline_inventory data.table: Pipeline inventory table.
#' @param pip_data_dir character: Input folder for the raw survey data.
#' @param filename character: vector with original dta file names
#' @param tool character: Either "PC" or "TB" for poverty calculator and Table
#'   Baker, receptively
#'
#' @return
#' @export
find_new_svy_data <- function(pipeline_inventory,
                              pip_data_dir,
                              tool = c("PC", "TB"),
                              cache_svy_dir, 
                              pfw_table) {

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  ## check parameters --------
  tool <- match.arg(tool)
  
  
  ## filter first by data available in PFW
  pipeline_inventory <- 
    pipeline_inventory[pfw_table, 
                     on = c("country_code", "surveyid_year", "survey_acronym"), 
                     inpovcal := i.inpovcal
                     ][
                       inpovcal == 1
                       ][,
                         inpovcal := NULL]
  
  
  cache_id <- pipeline_inventory$cache_id
  filename <- pipeline_inventory$filename
  
  
  # Get existing survey ids
  existing_chh_ids <- gsub("(.+)(\\.fst)", "\\1", list.files(cache_svy_dir))

  #--------- Identify new Surveys ---------

  # Select new surveys
  new_svy_ids <- filename[!(cache_id %in% existing_chh_ids)]

  # working data.table
  pipe <- data.table::data.table(
    cache_id = get("cache_id"),
    filename = get("filename")
  )

  #--------- find data whose SVY id has changed ---------

  # load correspondence inventory
  crr_dir <- fs::path(cache_svy_dir, "_crr_inventory/")
  crr_inv_file <- fs::path(crr_dir, "crr_inventory", ext = "fst")
  if (file.exists(crr_inv_file)) {
    crr_inv <- fst::read_fst(crr_inv_file)
    data.table::setDT(crr_inv)

    chd_svy <-
      crr_inv[pipe,
        on = "cache_id",
        filename_svy := i.filename
      ][
        filename_svy != filename
      ][
        ,
        filename
      ]

    if (length(chd_svy) > 0) {
      new_svy_ids <- c(new_svy_ids, chd_svy)
    }
  } else {
    pip_update_cache_inventory(
      pipeline_inventory = pipeline_inventory,
      cache_svy_dir = cache_svy_dir,
      pip_data_dir = pip_data_dir,
      tool = tool
    )
  }

  df <- pipe[filename %chin% new_svy_ids][
    ,
    svy_ids := gsub("(.+)(\\.dta)", "\\1", filename)
  ]

  return(df)
}
