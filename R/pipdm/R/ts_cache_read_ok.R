#' Check that all cache files load correctly
#'
#' @param inv_path character: directory path where cache files are stores
#'
#' @return data.table
#' @export
ts_cache_read_ok <- function(inv_path) {
  inv <- fst::read_fst(inv_path, as.data.table = TRUE)
  # inv <- inv[grepl("ARG.+201[7-9]", filename)]

  cch <- inv$cache_file

  df <- purrr::map(
    .x = cch,
    .f = safe_fst
  )
  df <- unlist(df)
  inv[, status := df]
  inv[is.na(status), cache_id]

  if (requireNamespace("pushoverr", quietly = TRUE)) {
    pushoverr::pushover("Finish testing loading")
  }
  return(inv)
}


status_fst <- function(x) {
  fst::read_fst(x)
  "good"
}


safe_fst <- purrr::possibly(
  .f = status_fst,
  otherwise = NA
)
