save_estimations <- function(dt, dir, name, 
                             time = format(Sys.time(), "%Y%m%d%H%M%S"), 
                             compress) {
  
  vintage     <- paste0(name, "_", time)
  vintage_dir <- fs::path(dir,"_vintage")
  if (!fs::dir_exists(vintage_dir)) {
    fs::dir_create(vintage_dir)
  }
  
  fst::write_fst(x        = dt,
                 path     = fs::path(dir, name, ext = "fst"),
                 compress = compress)
  
  fst::write_fst(x        = dt,
                 path     = fs::path(vintage_dir, vintage, ext = "fst"),
                 compress = compress)
  
  haven::write_dta(data     = dt,
                   path     = fs::path(dir, name, ext = "dta"))
  
  haven::write_dta(data     = dt,
                   path     = fs::path(vintage_dir,vintage, ext = "dta"))
  
  return(fs::path(dir, name, ext = "fst"))
}

create_input_folder_structure <- function(dir) {
  
  # Create sub directories if needed 
  
  # _aux folder
  if (!dir.exists(fs::path(dir, '_aux')))
    dir.create(fs::path(dir, '_aux'))
  
  # _inventory folder 
  if (!dir.exists(fs::path(dir, '_inventory')))
    dir.create(fs::path(dir, '_inventory'))
  
  # _log folders 
  if (!dir.exists(fs::path(dir, '_log')))
    dir.create(fs::path(dir, '_log'))
  if (!dir.exists(fs::path(dir, '_log/info')))
    dir.create(fs::path(dir, '_log/info'))
  if (!dir.exists(fs::path(dir, '_log/info/vintage')))
    dir.create(fs::path(dir, '_log/info/vintage'))
  
  # Create other files if needed 
  
  # Create empty _log/info .dtasig files  
  if (!file.exists(fs::path(dir, '_log/info/pip_info_gd.dtasig')))
    file.create(fs::path(dir, '_log/info/pip_info_gd.dtasig'))
  if (!file.exists(fs::path(dir, '_log/info/pip_info_md.dtasig')))
    file.create(fs::path(dir, '_log/info/pip_info_md.dtasig'))
  
}

check_missing_input_files <- function(dir) {
  
  # Check that _a_ WEO.xls file is available 
  # The most recent version of this file needs to be 
  # manually downloaded from IMF's webpage. 
  weo_path <- '_aux/weo/WEO_[0-9]{4}-[0-9]{2}-[0-9]{2}.xls'
  if (!file.exists(fs::path(dir, weo_path)))
    rlang::abort('WEO_[YYYY-MM-DD].xls file not found.')
  
  # Check that _the_ 2021-01-14 version of NAS special.xlsx
  # ia available. The use of this specific file is currently
  # hardcoded in pipaux::pip_gdp_update and pipaux::pip_pce_update. 
  nas_path <- '_aux/sna/NAS special_2021-01-14.xlsx'
  if (!file.exists(fs::path(dir, nas_path)))
    rlang::abort('NAS special_2021-01-14.xlsx file not found.')
  
}

cache_inventory_path <- function(CACHE_SVY_DIR){
  fs::path(CACHE_SVY_DIR, "_crr_inventory/crr_inventory.fst")
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
  purrr::map_chr(aux_names, ~ fs::path(OUT_AUX_DIR, .x,  ext = "fst"))
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
  
  auxdir <- fs::path(PIP_DATA_DIR, "_aux/")
  
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

#' Create framework data from Price FrameWork data (subsample)
#'
#' @param pfw 
#'
#' @return
#' @export
#'
#' @examples
create_framework <- function(pfw) {
  meta_vars <- 
    c(
      "wb_region_code",
      "country_code",
      "pcn_region_code",
      "ctryname",
      "year",
      "surveyid_year",
      "reporting_year",
      "survey_year",
      "survey_acronym",
      "survey_coverage",
      "welfare_type",
      "use_imputed",
      "use_microdata",
      "use_bin",
      "use_groupdata",
      "survey_comparability"
    )
  
  pfw <- pfw[, ..meta_vars]
  setnames(pfw, "ctryname", "country_name")
  
  return(pfw)
}

