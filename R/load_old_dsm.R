##' .. content for \description{} (no empty lines) ..
##'
##' .. content for \details{} ..
##'
##' @title
##' @param nameme1
load_old_dsm <- function(fin = "output/deflated_svy_means.fst") {

  if (fs::file_exists(fin)) {
    df <- fst::read_fst(fin)
    setDT(df)
  } else {
    df <- NULL
  }
  
  return(df)
}

