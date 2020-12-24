#' Bind DSM table in memory with the temporal DSM table just created
#'
#' @param dsm_in character: File path of DSM input data
#' @param tmp_dsm dataframe with dsm of new or changing surveys
#'
#' @return
#' @export
#'
#' @examples
db_bind_dsm_tables <- function(dsm_in, tmp_dsm) {

  # Early returns ------
  if (is.null(tmp_dsm)) {
    dt <- fst::read_fst(dsm_in)
    data.table::setDT(dt)
    return(dt)
  }


  if (fs::file_exists(dsm_in)) {
    # Inventory in Use
    old <- fst::read_fst(dsm_in)
    data.table::setDT(old)

    new_id <- tmp_dsm[, .(survey_id = unique(survey_id))]

    #remove in old in case there is an update
    old <- old[!new_id,
               on = .(survey_id)]

    # append data
    df <- rbindlist(list(tmp_dsm, old),
                    use.names = TRUE,
                    fill = TRUE)

  } else {
    # If deflated svy file does not exist use the whole raw inventory
    df <- tmp_dsm
  }

  setorder(df, country_code, surveyid_year, module, vermast, veralt)

  return(df)
}
