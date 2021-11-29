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
