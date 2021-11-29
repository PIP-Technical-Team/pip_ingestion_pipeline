mp_svy_mean_lcu <- function(cache, gd_means) {
  purrr::map2(cache, gd_means,
              db_compute_survey_mean)
}


mp_cache <- function(cache_dir) {
    purrr::map(.x = cache_dir, 
               .f = ~{
                 fst::read_fst(path = .x,
                               as.data.table = TRUE)
               })
  
}


mp_lorenz <- function(cache) {
  d <- purrr::map(cache, db_compute_lorenz)
  purrr::keep(d, ~!is.null(.x))
  
}






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




