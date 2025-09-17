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
  function(cache_dir, 
           load = TRUE, 
           save = FALSE, 
           gls, 
           cache_ppp) {
    
    dir <- fs::path(gls$PIP_PIPE_DIR, "pc_data/cache/global_list/")
    
    # global_file <- paste0(dir, "global_list.rds")
    global_name      <- paste0("global_list_", cache_ppp)
    global_name_inv  <- paste0("global_list_inventory", cache_ppp)
    global_file      <- fs::path(dir, global_name , ext = "qs")
    global_inv       <- fs::path(dir, global_name_inv, ext = "qs")
    
    if (!fs::file_exists(global_file)) {
      save <- TRUE
      cli::cli_alert("file {.file {global_file}} does not exist. 
                     It will be created and saved")
    }
    
    # Compare names of each file with the ones already saved
    # if different, then create cache again and save both cache 
    # and new inventory
    ch_names <- names(cache_dir)
    if (!fs::file_exists(global_inv)) {
      save <- TRUE
      cli::cli_alert("file {.file {global_inv}} does not exist. 
                     cache will be created and saved")
    } else {
      inv <- qs::qread(global_inv)
      if (!identical(inv, ch_names)) {
        cli::cli_alert("current cache inventory in file  does not match with 
                       the new one. Cache will be recreated")
        save <- TRUE
      }
    }
    
    if (isTRUE(save)) {
      
      
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
      qs::qsave(y, global_file, preset =  "fast")
      qs::qsave(ch_names, global_inv, preset =  "fast")
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
          x <- qs::qread(global_file, nthreads = 2)
          
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





#' Create list of cache files
#'
#' @param cache_dir character: direcoty path of cache files
#' @param load logical: whether to load the list of cache files
#' @param gls list: globals from `pip_create_globals()`
#' @param cache_ppp numeric: PPP year. 
#'
#' @return
#' @export
create_cache <- 
  function(cache_dir, 
           gls, 
           cache_ppp, 
           cache_status) {
    
    dir <- fs::path(gls$PIP_PIPE_DIR, "pc_data/cache/global_list/")
    
    # global_file <- paste0(dir, "global_list.rds")
    global_name <- paste0("global_list_", cache_ppp)
    global_file <- fs::path(dir, global_name , ext = "qs")
    
    cache_ids   <- cache_dir |> 
      fs::path_file() |> 
      fs::path_ext_remove()
    
    tfs <- cache_status[status != "unchanged"]
    if (nrow(tfs) == 0L) return(global_file)
    
    cache <- purrr::map(.x = cache_dir, 
                    .f = ~{
                      tryCatch(
                        expr = {
                          # Your code...
                          fst::read_fst(path = .x,
                                        as.data.table = TRUE)
                        }, # end of expr section
                        
                        error = function(e) {
                          NULL
                        }, # end of error section
                        
                        warning = function(w) {
                          NULL
                        } # end of finally section
                        
                      ) # End of trycatch
                      
                    }, 
                    .progress = TRUE) |> 
      setNames(cache_ids) |> 
      purrr::compact() 
    
    qs::qsave(cache, global_file, preset =  "fast")

    
    global_file
    
  }


update_global_cache_inv <- \(cache_dir, gls, cache_ppp) {
  
  dir <- fs::path(gls$PIP_PIPE_DIR, "pc_data/cache/global_list/")
  
  cache_ids   <- cache_dir |> 
    fs::path_file() |> 
    fs::path_ext_remove()
  
  global_name_inv  <- paste0("global_list_inventory", cache_ppp)
  global_inv       <- fs::path(dir, global_name_inv, ext = "qs")
  qs::qsave(cache_ids, global_inv, preset =  "fast")
  global_inv
}





load_cache <- \(cache_file) {
   # load from file
  
  if (file.exists(cache_file)) {
    
    cli::cli_progress_step("Loading global cache file",  spinner = TRUE)
    
    # x <- readr::read_rds(global_file)
    x <- qs::qread(cache_file, nthreads = 2)
    
    cli::cli_progress_step("Done!")
    
  } else {
    cli::cli_abort("file {.file {global_file}} does not exist. 
                   Use option {.code save = TRUE} instead")
  }
  
  x
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
                             cache_id   , 
                             ppp_year) {
  
  purrr::map2(.x = dt, 
              .y = cache_id, 
              .f = ~{
                db_compute_dist_stats(dt         = .x,
                                      mean_table = mean_table,
                                      pop_table  = pop_table,
                                      cache_id   = .y, 
                                      ppp_year   = ppp_year)
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


