# Add global variables to avoid NSE notes in R CMD check
if (getRversion() >= '2.15.1')
  utils::globalVariables('dsm_file')

#' Save DSM table
#'
#' Save the deflated survey mean table to multiple locations.
#'
#' @param dsm_out data.table: A data frame containing both the old and newly
#'   estimated DSM tables.
#' @param pipedir character: Path to the DSM file.
#'
#' @return logical
#' @export
save_dsm <- function(dsm_out, pipedir) {


  # Make sure ingestion pipeline directory exists
  namef         <-  "deflated_svy_means"
  dsmdir        <-  paste0(pipedir, "dsm/")
  dsm_vint_dir  <-  paste0(dsmdir, "_vintage/")


  if (fs::dir_exists(dsm_vint_dir)) {
    fs::dir_create(dsm_vint_dir, recurse = TRUE)
  }

  # Early returns ------
  if (is.null(dsm_out)) {
    return(paste0(dsmdir, namef, ".fst"))
  }

  # Set time (for vintage)
  time <- format(Sys.time(), "%Y%m%d%H%M%S")
  attr(dsm_out, "datetime") <- time
  vt_output     <-  paste0(namef, "_", time)


  #--------- Save files ---------

  fst::write_fst(x    = dsm_out,
                 path = paste0(dsmdir, namef, ".fst"))

  fst::write_fst(x    = dsm_out,
                 path = paste0(dsmdir, namef, "_in.fst"))

  haven::write_dta(data = dsm_out,
                   path = paste0(dsmdir, namef, ".dta"))

  # Vintages and backup
  fst::write_fst(x    = dsm_out,
                 path = paste0(dsm_vint_dir, vt_output, ".fst"))

  haven::write_dta(data = dsm_out,
                   path = paste0(dsm_vint_dir, vt_output, ".dta"))

  return( paste0(dsmdir, namef, ".fst"))
}


