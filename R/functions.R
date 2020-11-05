load_aux_data <- function() {
  
  ll <- list(
    cpi = pipload::pip_load_aux(measure = "cpi"),
    ppp = pipload::pip_load_aux(measure = "ppp"),
    pop = pipload::pip_load_aux(measure = "pop"),
    gdp = pipload::pip_load_aux(measure = "gdp"),
    pce = pipload::pip_load_aux(measure = "pce"),
    pfw = pipload::pip_load_aux(measure = "pfw")
  )
  
  return(ll)
}

load_aux <- function(auxdir, dr) {
  file_to_load <- paste0(auxdir, dr, "/pip_", dr, ".fst")
  fst::read_fst(file_to_load)
}


find_aux_data <- function(auxdir) {
  files <- fs::dir_ls(auxdir,
             type    = "file", 
             recurse = TRUE,
             regexp  = ".*/[a-z]+\\.fst")
  
  files <- as.character(files)
  return(files)
}


aux_names <- function(auxdir, aux_files_to_load) {
  
  aux_indicators <- as.character(gsub(auxdir, "", aux_files_to_load))
  aux_indicators <- as.character(gsub("/.*", "", aux_indicators))
  return(aux_indicators)

}

import_file <- function(file) {
  fst::read_fst(file)
}



