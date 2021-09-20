save_estimations <- function(dt, dir, name, 
                             time = format(Sys.time(), "%Y%m%d%H%M%S"), 
                             compress) {
  
  fst::write_fst(x        = dt,
                 path     = paste0(dir, name, ".fst"),
                 compress = compress)
  
  fst::write_fst(x        = dt,
                 path     = paste0(dir,"_vintage/", name, "_", time, ".fst"),
                 compress = compress)
  
  haven::write_dta(data     = dt,
                   path     = paste0(dir, name, ".dta"))
  
  haven::write_dta(data     = dt,
                   path     = paste0(dir,"_vintage/", name, "_", time, ".dta"))
  
  return(paste0(dir, name, ".fst"))
}

create_input_folder_structure <- function(dir) {
  
  # Create sub directories if needed 
  
  # _aux folder
  if (!dir.exists(paste0(dir, '_aux')))
    dir.create(paste0(dir, '_aux'))
  
  # _inventory folder 
  if (!dir.exists(paste0(dir, '_inventory')))
    dir.create(paste0(dir, '_inventory'))
  
  # _log folders 
  if (!dir.exists(paste0(dir, '_log')))
    dir.create(paste0(dir, '_log'))
  if (!dir.exists(paste0(dir, '_log/info')))
    dir.create(paste0(dir, '_log/info'))
  if (!dir.exists(paste0(dir, '_log/info/vintage')))
    dir.create(paste0(dir, '_log/info/vintage'))
  
  # Create other files if needed 
  
  # Create empty _log/info .dtasig files  
  if (!file.exists(paste0(dir, '_log/info/pip_info_gd.dtasig')))
    file.create(paste0(dir, '_log/info/pip_info_gd.dtasig'))
  if (!file.exists(paste0(dir, '_log/info/pip_info_md.dtasig')))
    file.create(paste0(dir, '_log/info/pip_info_md.dtasig'))
  
}

check_missing_input_files <- function(dir) {
  
  # Check that _a_ WEO.xls file is available 
  # The most recent version of this file needs to be 
  # manually downloaded from IMF's webpage. 
  weo_path <- '_aux/weo/WEO_[0-9]{4}-[0-9]{2}-[0-9]{2}.xls'
  if (!file.exists(paste0(dir, weo_path)))
    rlang::abort('WEO_[YYYY-MM-DD].xls file not found.')
  
  # Check that _the_ 2021-01-14 version of NAS special.xlsx
  # ia available. The use of this specific file is currently
  # hardcoded in pipaux::pip_gdp_update and pipaux::pip_pce_update. 
  nas_path <- '_aux/sna/NAS special_2021-01-14.xlsx'
  if (!file.exists(paste0(dir, nas_path)))
    rlang::abort('NAS special_2021-01-14.xlsx file not found.')
  
}

cache_inventory_path <- function(CACHE_SVY_DIR){
  paste0(CACHE_SVY_DIR, "_crr_inventory/crr_inventory.fst")
}

get_cache_files <- function(x) {
  x$cache_file
}

get_cache_id <- function(x) {
  x$cache_id
}

get_survey_id <- function(x) {
  x$survey_id
}


aux_out_files_fun <- function(OUT_AUX_DIR, aux_names) {
  purrr::map_chr(aux_names, ~ paste0(OUT_AUX_DIR, .x, ".fst"))
}

temp_cleaning_ref_table <- function(dt) {
  
  dt <- dt[!(is.null(survey_mean_ppp) | is.na(survey_mean_ppp))]
  dt <- dt[!(is.null(predicted_mean_ppp) | is.na(predicted_mean_ppp))]
  return(dt)
  
}

named_mean <- function(dt) {
  mvec        <- dt[, survey_mean_lcu]
  names(mvec) <- dt[, pop_data_level]
  return(mvec)
}

prep_aux_data <- function(PIP_DATA_DIR) {
  
  PIP_DATA_DIR = gls$PIP_DATA_DIR
  
  auxdir <- paste0(PIP_DATA_DIR, "_aux/")
  
  aux_files <- list.files(auxdir,
                          pattern    = "[a-z]+\\.(rds|fst)",
                          recursive  = TRUE,
                          full.names = TRUE)
  
  # remove double // in the middle of path
  aux_files        <- gsub("(.+)//(.+)", "\\1/\\2", aux_files)
  
  aux_indicators   <- gsub(auxdir, "", aux_files)
  aux_indicators   <- gsub(".*[/]|([.].*)", "", aux_indicators)
  names(aux_files) <- aux_indicators
  
  aux_tb <- data.table::data.table(
    auxname  = aux_indicators,
    auxfiles = aux_files
  )
  
  # filter 
  aux_tb <- aux_tb[!(auxname %chin% 
                       c("weo", "maddison", "cpicpi_vintage"))]
  
  return(aux_tb)
}
