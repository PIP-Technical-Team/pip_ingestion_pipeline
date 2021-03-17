#' Save survey data
#' 
#' Save survey data to .fst file in specified output directory. 
#' 
#' @param dl list: List with survey datasets. 
#' @param cols character: Vector with columns to save. If NULL all columns are
#'  saved. 
#' @param output_dir character: Output folder. 
#' @param future_plan character: `future` plan to use. 
#' @param compress numeric: Compression level used in `fst::write_fst()`.  
#' @param chh_filename character: Vector with new names for microdata. 
#' 
save_survey_data <- function(dl, 
                             cols = NULL, 
                             output_dir,
                             chh_filename,
                             future_plan = c('sequential', 'multisession', 'callr'), 
                             compress) {
  
  # Set future plan 
  future_plan <- match.arg(future_plan)
  plan(future_plan)
  
  p <- progressr::progressor(steps = length(dl))
  
  # Select columns
  if (!is.null(cols)) {
    dl <- purrr::map(dl, function(x) 
      x[, .SD, .SDcols = cols])
  }
  
  # Create paths
  
  chh_filename <- fifelse(!grepl("\\.fst$", chh_filename), 
                          paste0(chh_filename, ".fst"), 
                          chh_filename)
  
  svy_out_paths <- paste(output_dir, chh_filename, sep = "/")
  
  # Write files 
  report <- furrr::future_map2(
    .x = dl, 
    .y = svy_out_paths,
    .f = ~{
      p()
      ps_fst(.x, .y, compress = compress)
    },
    .options = furrr::furrr_options(seed = NULL)
  )
  
  report <- keep(report, ~is.null(.x))
  report <- names(report)
  
  
  if (future_plan == 'multisession') {
    close_workers()
  }
  
  return(invisible(report))
}

# safe writing of data
ps_fst <-  purrr::possibly(.f = fst::write_fst,
                           otherwise = NULL)

