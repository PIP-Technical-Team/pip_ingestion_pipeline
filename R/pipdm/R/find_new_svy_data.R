#' Find which data is not available in cache folder or has changed SVY ID
#'
#' @param cache_svy_dir character: Output folder for the cached survey data.
#' @param cache_id  character: vector with cache Ids from pipeline inventory
#' @param filename character: vector with original dta file names
#' @param tool character: Either "PC" or "TB" for poverty calculator and Table
#'   Baker, receptively
#'
#' @return
#' @export
find_new_svy_data <- function(cache_id,
                              filename,
                              tool = c("PC", "TB"),
                              cache_svy_dir) {

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  ## check parameters --------
  tool <- match.arg(tool)

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
      cache_svy_dir = cache_svy_dir,
      tool = tool
    )
  }

  df <- pipe[filename %chin% new_svy_ids][
    ,
    svy_ids := gsub("(.+)(\\.dta)", "\\1", filename)
  ]

  return(df)
}
