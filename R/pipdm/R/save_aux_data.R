#' Save auxiliary data.
#'
#' @param x dataframe
#' @param filename final name of aux data
#' @param compress logical. iF TRUE it compress output
#'
#' @return
#' @export
save_aux_data <- function(x,
                          filename,
                          compress = FALSE) {


  type <- gsub("(.+)(\\.)([a-z]{3}$)", "\\3", filename)

  if (type == 'fst') {

    fst::write_fst(x, filename, compress = compress)

  } else if (type == 'rds') {

    saveRDS(x, filename,compress = compress)

  }

  return(filename)

}
