#' Save survey data
#'
#' Save survey data to .fst file in specified output directory.
#'
#' @param dt Data frame from cache to be saved
#' @param cols character: Vector with columns to save. If NULL all columns are
#'   saved.
#' @param output_dir character: Output folder.
#' @param future_plan character: `future` plan to use.
#' @param compress numeric: Compression level used in `fst::write_fst()`.
#' @param save logical: if TRUE save data file. Default is TRUE
#' @param load logical: if TRUE loads data. Default is FALSE and returns file
#'   directory path
#' @param cache_filename character: Vector with new names for microdata.
#' @param verbose logical: Whether to display messages. Default is TRUE
#'
#' @export
save_survey_data <- function(dt,
                             cols        = NULL,
                             output_dir,
                             cache_filename,
                             future_plan = c('sequential', 'multisession', 'callr'),
                             compress,
                             save        = TRUE,
                             load        = FALSE,
                             verbose     = FALSE) {
  
  
  # TEMP: solution to account for imputed population -------
  dt_names <- names(dt)
  if ("imputation_id" %in% dt_names) {
    n_ids <- dt[, uniqueN(imputation_id)]
    if (n_ids > 1) {
      dt[, weight := weight/n_ids]
    }
  }
  
  # END of TEMP -------------
  
  
  # Select columns    ---------
  if (!is.null(cols)) {
    dt <- dt[, ..cols]
  }

  # optimize size --------
  dt[,
      area := factor(area, levels = c("rural", "urban"))
      ]

  byvars <- names(dt)[names(dt) != "weight"]
  
  # collapse by area and welfare to optimize size. 
  dt <- dt |>
    fgroup_by(byvars) |>
    fsum()
  
  
  # dt <- tryCatch(
  #   expr = {
  #     
  #     ht <- replicate_households(dt)
  #     if (attr(ht, "iterations") == 0) {
  #       has_replicated_hhds <- FALSE
  #     } else {
  #       has_replicated_hhds <- TRUE
  #     }
  #     setattr(ht, "has_replicated_hhds", has_replicated_hhds)
  #     
  #   }, # end of expr section
  #   
  #   error = function(e) {
  #     setattr(dt, "has_replicated_hhds", FALSE)
  #     
  #   }, # end of error section
  #   
  #   warning = function(w) {
  #     setattr(dt, "has_replicated_hhds", FALSE)
  #   } # end of finally section
  #   
  # ) # End of trycatch  
  
  
  # save -------
  ## Create paths -------------
  cache_filename <- fifelse(!grepl("\\.fst$", cache_filename),
                          paste0(cache_filename, ".fst"),
                          cache_filename)

  svy_out_path <- paste(output_dir, cache_filename, sep = "/")

  ## find if file has changed --------------
  if (file.exists(svy_out_path)) {
    odt <- fst::read_fst(svy_out_path)

    if (identical(odt, dt)) {
      if (verbose) {
        cli::cli_alert_info("{cache_filename} has not changed")
      }
      save <- FALSE
    } else {
      if (verbose)
        cli::cli_alert_warning("{cache_filename} has changed")
    }

  }

  if (save) {
    fst_status <-
      tryCatch(
        expr = {
          # Your code...
          fst::write_fst(x        = dt,
                         path     = svy_out_path,
                         compress = compress)
          "passed"
        }, # end of expr section

        error = function(e) {
          "error"
        }, # end of error section

        warning = function(w) {
          "warning"
        }
      ) # End of trycatch
    attr(svy_out_path, "fst_status") <- fst_status
  }  else {
    attr(svy_out_path, "fst_status") <- "Not saved"
  }

  if (load) {
    return(dt)
  } else {
    return(svy_out_path)
  }

}

