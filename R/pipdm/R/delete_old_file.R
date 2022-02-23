#' Delete cache files that are no longer in use
#'
#' @param target_dir character: directory where the cache files are stored. If
#'   could be a patch, like `gls$OUT_SVY_DIR_PC` from
#' @param cache_ids character:vector with  Cache_ids. default from
#'   `db_create_pipeline_inventory()`
#'   `pipload::add_gls_to_env()` or it could be one of the two shortcuts, "in"
#'   or "out". The former refers to `gls$CACHE_SVY_DIR_PC`. the latter refers to
#'   `gls$OUT_SVY_DIR_PC`.
#' @param verbose logical: whether to display messages
#' @param delete logical or NULL. If Null, menu with choices will be displayed
#'   (default). If false, no file will be deleleted. If TRUE, all old files not
#'   available in cache_id will be deleted
#' @param gls list: List of globals.
#' @return
#' @export
#'
delete_old_file <- function(target_dir = "in",
                            gls,
                            cache_ids  = NULL,
                            verbose    = FALSE,
                            delete     = NULL) {

  if (is.null(cache_ids)) {
    dt <- db_create_pipeline_inventory()
    cache_ids <- dt[, unique(cache_id)]
  }

  if (target_dir == "in") {
    target_dir <- gls$CACHE_SVY_DIR_PC
  }
  if (target_dir == "out") {
    target_dir <- gls$OUT_SVY_DIR_PC
  }


  dir_files <- list.files(path       = target_dir,
                          pattern    = "^[^_]", # whatever does not start with _
                          full.names = TRUE,
                          recursive  = FALSE)

  file_names <- gsub("(//.+/)|([\\.].+$)", "", dir_files)

  names_delete <- file_names[!(file_names %in% cache_ids)]

  if (length(names_delete) == 0) {
    if (verbose) {
      cli::cli_alert_info("No old files found")
    }
    return(invisible(NA))
  }

  cli::cli_alert_danger("The following files will be deleted?")
  cli::cli_ul(names_delete)

  if (is.null(delete)) {
    delete <- usethis::ui_yeah("Do you want to delete them?")
  }


  if (isTRUE(delete)) {
    to_delete <- dir_files[!(file_names %in% cache_ids)]
    deleted <- file.remove(to_delete)
  } else {
    deleted <- NA
  }

  return(invisible(deleted))
}



