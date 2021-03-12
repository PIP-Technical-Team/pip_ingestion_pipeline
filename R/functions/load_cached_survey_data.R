#' Load cached survey data 
#' 
#' Load files form the cached survey data directory. 
#' 
#' @param survey_id character: Vector with survey ids to load. If NULL all 
#'  surveys are loaded. 
#' @param cache_svy_dir character: Input folder. 
#' @param future_plan character: `future` plan to use. 
#' 
#' @return list 
load_cached_survey_data <- function(survey_id = NULL, 
                                    cache_svy_dir) {
  
  if (is.null(cache_svy_dir)) {
    msg     <- "`cache_svy_dir` cannot be null"
    rlang::abort(c(msg))
  }
  
  # Get list of files 
  if (!is.null(survey_id)) {
    
    svy_files <- sprintf('%s/%s.fst', cache_svy_dir, survey_id)
    
  } else {
    
    svy_files <- list.files(cache_svy_dir, pattern = '[.]fst', 
                            full.names = TRUE)   
  }
  
  # Read files
  cli::cli_alert("Loading data...")
  pb <- progress_bar$new(format = ":what [:bar] :percent eta: :eta",
                         clear = , total = length(svy_files), width = 80)
  
  dl <- 
    purrr::map(
      .x = svy_files, 
      .f = ~ {
        id_what <- gsub("(.+)/([A-Z]+_[0-9]+)(.+)", "\\2", .x)
        pb$tick(tokens = list(what = id_what))
        dt <- safe_read_fst(.x)
        data.table::setDT(dt)
      }
      )
  
  names(dl) <- gsub("(.+/)([^/]+)(\\.fst$)",  "\\2", svy_files)
  
  report <- keep(dl, ~is.null(.x))
  report <- names(report)
  
  if (length(report) != 0) {
    cli::cli_alert_warning(("Did NOT find {.field {length(report)}} file{?s}."))
    cli::cli_ul(report)
  }
  
  dl <- keep(dl, ~!is.null(.x))
  
  return(dl)
}


#' safe_read_fst 
safe_read_fst <-
  purrr::possibly(.f = fst::read_fst,
                  otherwise = NULL)


