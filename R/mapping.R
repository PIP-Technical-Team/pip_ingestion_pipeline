mp_svy_mean_lcu <- function(cache, gd_means) {
  purrr::map2(cache, gd_means,
              db_compute_survey_mean)
}


#' Create list of cache files
#'
#' @param cache_dir character: direcoty path of cache files
#' @param load logical: whether to load the list of cache files
#' @param save logical: whether to create the list again and save it
#' @param gls list: globals from `pip_create_globals()`
#' @param cache_ppp numeric: PPP year. 
#'
#' @return
#' @export
#'
#' @examples
mp_cache <- 
  function(cache_dir = NULL, 
           load = TRUE, 
           save = FALSE, 
           gls, 
           cache_ppp) {
    
    dir <- fs::path(gls$PIP_PIPE_DIR, "pc_data/cache/global_list/")
    
    # global_file <- paste0(dir, "global_list.rds")
    global_name <- paste0("global_list_", cache_ppp)
    global_file <- fs::path(dir, global_name , ext = "qs")
    
    if (!fs::file_exists(global_file)) {
      save <- TRUE
      cli::cli_alert("file {.file {global_file}} does not exist. 
                     It will be created and saved")
    }
    
    if (isTRUE(save)) {
      
      if (is.null(cache_dir)) {
        cli::cli_abort("You must provide a {.code cache_dir} vector")
      }
      
      ch_names <- gsub("(.+/)([A-Za-z0-9_\\-]+)(\\.fst$)", "\\2", cache_dir)
      names(ch_names) <- NULL
      names(cache_dir) <- ch_names
      
      cli::cli_progress_step("Creating list")
      y <- purrr::map(.x = cli::cli_progress_along(ch_names), 
                      .f = ~{
                        tryCatch(
                          expr = {
                            # Your code...
                            fst::read_fst(path = cache_dir[.x],
                                          as.data.table = TRUE)
                          }, # end of expr section
                          
                          error = function(e) {
                            NULL
                          }, # end of error section
                          
                          warning = function(w) {
                            NULL
                          } # end of finally section
                          
                        ) # End of trycatch
                        
                      })
      
      names(y) <- ch_names
      qs::qsave(y, global_file)
      # readr::write_rds(y, global_file)
      
    }
    
    if (isTRUE(load)) {
      if (isTRUE(save)) { # load from process above
        
        cli::cli_progress_step("loading from saving process")
        x <- y
        
      } else { # load from file
        
        if (file.exists(global_file)) {
          
          cli::cli_progress_step("Loading list from file")
          
          # x <- readr::read_rds(global_file)
          x <- qs::qread(global_file)
          
        } else {
          cli::cli_abort("file {.file {global_file}} does not exist. 
                       Use option {.code save = TRUE} instead")
        }
        
      }
      
    } else {
      cli::cli_progress_step("Returning TRUE")
      x <- TRUE
    }
    
    return(x)
    
  }


#' Title
#'
#' @param cache 
#'
#' @return
#' @export
#'
#' @examples
mp_lorenz <- function(cache) {
  d <- purrr::map(cache, db_compute_lorenz)
  purrr::keep(d, ~!is.null(.x))
  
}






#' Title
#'
#' @param dt 
#' @param mean_table 
#' @param pop_table 
#' @param cache_id 
#'
#' @return
#' @export
#'
#' @examples
mp_dl_dist_stats <- function(dt         ,
                             mean_table ,
                             pop_table  ,
                             cache_id   ) {
  
  purrr::map2(.x = dt, 
              .y = cache_id, 
              .f = ~{
                db_compute_dist_stats(dt         = .x,
                                      mean_table = mean_table,
                                      pop_table  = pop_table,
                                      cache_id   = .y)
              })
  
}


mp_survey_files <- function(cache          ,
                            cache_ids      ,
                            output_dir     ,
                            cols           ,
                            compress       ) {
  
  x <- purrr::map(.x = cli::cli_progress_along(cache_ids), 
                   .f = ~{
                     save_survey_data(
                       dt              = cache[[.x]],
                       cache_filename  = cache_ids[[.x]],
                       output_dir      = output_dir,
                       cols            = cols,
                       compress        = compress) 
                     
                   })
  return(x)
  
}


