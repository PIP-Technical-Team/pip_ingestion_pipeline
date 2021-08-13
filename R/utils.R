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