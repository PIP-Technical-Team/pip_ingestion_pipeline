
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#               Step 0: Start up   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(targets)
library(tarchetypes)

# tar_option_set(debug = "svy_mean_lcu_table")
# tar_cue(mode = "never")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##   Set initial parameters  --------
# remotes::install_github("PIP-Technical-Team/pipdm")
# remotes::install_github("PIP-Technical-Team/pipload")
# remotes::install_github("PIP-Technical-Team/wbpip@ineq_using_synth")
# remotes::install_github("PIP-Technical-Team/pipdm@development")
# remotes::install_github("randrescastaneda/joyn")
### defaults ---------

# Input dir 
PIP_DATA_DIR     <- '//w1wbgencifs01/pip/PIP-Data_QA/' 

# '//w1wbgencifs01/pip/pip_ingestion_pipeline/' # Output dir
PIP_PIPE_DIR     <- '//w1wbgencifs01/pip/pip_ingestion_pipeline/' 

# Cached survey data dir
CACHE_SVY_DIR    <- paste0(PIP_PIPE_DIR, 'pc_data/cache/clean_survey_data/') 

# Final survey data output dir
OUT_SVY_DIR      <- paste0(PIP_PIPE_DIR, 'pc_data/output/survey_data/') 

#  Estimations output dir
OUT_EST_DIR      <- paste0(PIP_PIPE_DIR, 'pc_data/output/estimations/') 

# aux data output dir
OUT_AUX_DIR      <- paste0(PIP_PIPE_DIR, 'pc_data/output/aux/')  

time             <- format(Sys.time(), "%Y%m%d%H%M%S") 

### Max dates --------

c_month  <- as.integer(format(Sys.Date(), "%m"))
max_year <- ifelse(c_month >= 8,  # August
                   as.integer(format(Sys.Date(), "%Y")) - 1, # After august
                   as.integer(format(Sys.Date(), "%Y")) - 2) # Before August

PIP_YEARS        <- 1977:(max_year+1) # Years used in PIP 
PIP_REF_YEARS    <- 1981:max_year # Years used in the interpolated means table

FST_COMP_LVL     <- 100 # Compression level for .fst output files
APPLY_GC         <- TRUE # Apply garbage collection 
PIP_SAFE_WORKERS <- FALSE # Open/close workers after each future call

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##           Packages --------

pkgs <- 
  c('pipload', 
    'pipdm',
    'wbpip',
    'fst',
    'qs',
    'magrittr',
    'data.table',
    'dplyr',
    'cli',
    'progress',
    'glue',
    'purrr'
  )

  
# Set targets options 
tar_option_set(
  garbage_collection = TRUE,
  memory = 'transient',
  format = 'qs', #'fst_dt',
  packages = pkgs,
  imports  = c('pipload',
               'pipdm',
               'wbpip')
  )



# Load pipeline helper functions 
source('_packages.R')
source('R/_common.R')

options(joyn.verbose = FALSE) # make sure joyn does not display messages
# Set future plan (for targets::tar_make_future)
# plan(multisession)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#      Step 1: Define short useful functions   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

cache_inventory_path <- function(){
  paste0(CACHE_SVY_DIR, "_crr_inventory/crr_inventory.fst")
}
  
get_cache_files <- function(x) {
  x$cache_file
}

get_cache_id <- function(x) {
  x$cache_id
}


aux_out_files_fun <- function(OUT_AUX_DIR, aux_names) {
  purrr::map_chr(aux_names, ~ paste0(OUT_AUX_DIR, .x, ".fst"))
}

temp_cleaning_ref_table <- function(dt) {
  
  dt <- dt[!(is.null(survey_mean_ppp) | is.na(survey_mean_ppp))]
  dt <- dt[!(is.null(predicted_mean_ppp) | is.na(predicted_mean_ppp))]
  return(dt)
  
}

save_estimations <- function(dt, dir, name, time, compress) {
  
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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#       Step 2: Prepare data                     ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##  Create list of AUX files  -------

auxdir <- paste0(PIP_DATA_DIR, "_aux/")

aux_files <- list.files(auxdir,
                        pattern    = "[a-z]+\\.fst",
                        recursive  = TRUE,
                        full.names = TRUE)

# remove double // in the middle of path
aux_files        <- gsub("(.+)//(.+)", "\\1/\\2", aux_files)

aux_indicators   <- gsub(auxdir, "", aux_files)
aux_indicators   <- gsub("(.*/)([^/]+)(\\.fst)", "\\2", aux_indicators)

names(aux_files) <- aux_indicators


aux_tb <- data.table(
  auxname  = aux_indicators,
  auxfiles = aux_files
)

# filter 
aux_tb <- aux_tb[!(auxname %chin% c("weo", "maddison"))]


# # Load PFW file
pfw_glo <- pipload::pip_load_aux(
    file_to_load = aux_files['pfw'],
    apply_label = FALSE)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##        Inventories        --------

# Load PIP inventory
pip_inventory <-
  pipload::pip_find_data(
    inv_file = paste0(PIP_DATA_DIR, '_inventory/inventory.fst'),
    filter_to_pc = TRUE,
    maindir = PIP_DATA_DIR)

# Create pipeline inventory
pipeline_inventory <-
  pipdm::db_filter_inventory(
    pip_inventory,
    pfw_table = pfw_glo)

# Ad hoc filtering for testing purposes
# pipeline_inventory <-
#   pipeline_inventory[country_code == 'ZMB'
#                      & surveyid_year == 1996]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##      Create cache files --------
# pipeline_inventory <- 
#   pipeline_inventory[cache_id == "PHL_2018_FIES_D1_CON_GPWG"]

cache_info <- 
  cache_survey_data(
  pipeline_inventory = pipeline_inventory,
  pip_data_dir       = PIP_DATA_DIR,
  cache_svy_dir      = CACHE_SVY_DIR,
  compress           = FST_COMP_LVL,
  # force              = TRUE,
  verbose            = TRUE) 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#            Step 3:   Run pipeline   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

list(

#~~~~~~~~~~~~~~~~~~~~~~~
##  Load AUX data ---- 
  tar_map(
    values = aux_tb, 
    names  = "auxname", 
    
    # create dynamic name
    tar_target(
      aux_dir,
      auxfiles, 
      format = "file"
    ), 
    tar_target(
      aux,
      pipload::pip_load_aux(file_to_load = aux_dir,
                            apply_label = FALSE)
    )
    
  ),

#~~~~~~~~~~~~~~~~~~~~~~~
## Cache inventory file ----
  tar_target(
    cache_inventory_dir, 
    cache_inventory_path(),
    format = "file"
  ),

  tar_target(
    cache_inventory, 
    {
    x <- fst::read_fst(cache_inventory_dir, 
                  as.data.table = TRUE)
    # to filter temporarily
    # x <- x[grepl("^(PRY|ARE)", survey_id)
    #        ][gsub("([A-Z]+)_([0-9]+)_(.*)", "\\2", survey_id) > 2010
    #        ]
    
    },
  ),
  
  tar_target(cache_files, 
             get_cache_files(cache_inventory)),
  
  tar_target(cache_ids, 
             get_cache_id(cache_inventory)),

  tar_files(cache_dir, cache_files),
  tar_target(cache, 
             fst::read_fst(path = cache_dir, 
                           as.data.table = TRUE), 
             pattern = map(cache_dir), 
             iteration = "list"),

#~~~~~~~~~~~~~~~~~~~~~~~
##  LCU survey means ---- 
   
### Fetch GD survey means and convert them to daily values ----
  tar_target(
    gd_means, {
      
      cache_inventory[aux_gdm,
                      on = "survey_id",
                      # Convert to daily values (PCN is in monthly) 
                      survey_mean_lcu := i.survey_mean_lcu* (12/365) 
                      ][,
                        survey_mean_lcu
                        ]
       
      }, 
    iteration = "list"
    ) ,

### Calculate LCU survey mean ----

  tar_target(
    svy_mean_lcu,
    db_compute_survey_mean(cache, gd_means),
    pattern =  map(cache, gd_means), 
    iteration = "list"
     ),
#   
###  Remove surveys where the mean calculation failed ----
  # tar_target(
  #   svy_mean_lcu_clean,
  #   svy_mean_lcu %>%
  #     filter(!is.na(survey_mean_lcu)), 
  #   iteration = "list"
  # ),

### Remove survey data where the LCU mean calculation failed
  # tar_target(
  #   dl_svy_data_w_mean,
  #   dl_svy_data[
  #     names(dl_svy_data) %in%
  #       names(dl_svy_mean_lcu_w_mean)]
  # ),

#~~~~~~~~~~~~~~~~~~~~~~~
## Dist  stats ---- 

###  Lorenz curves -----

# Calculate Lorenz curves (for microdata)  
  tar_target(
    lorenz_all,
    db_compute_lorenz(cache),
    pattern = map(cache), 
    iteration = "list"
  ), 
# clean Group Data obs
  tar_target(
    lorenz, 
    purrr::keep(lorenz_all, ~!is.null(.x))
  ),

# get mean 
  tar_target(
    dl_mean, # name vectors. 
    svy_mean_lcu$survey_mean_lcu,
    pattern = map(svy_mean_lcu),
    iteration = "list"
  ),

### Calculate distributional statistics
  tar_target(
    name      = dl_dist_stats,
    command   = db_compute_dist_stats(cache, dl_mean, aux_pop), 
    pattern   =  map(cache, dl_mean), 
    iteration = "list"
    ),
  
## Create LCU table ------

  tar_target(
    svy_mean_lcu_table,
    db_create_lcu_table(
     dl        = svy_mean_lcu,
     pop_table = aux_pop,
     pfw_table = aux_pfw)
    ),

#~~~~~~~~~~~~~~~~~~~~~~~~
##  Create DSM table ---- 

  tar_target(svy_mean_ppp_table,
             db_create_dsm_table(
               lcu_table = svy_mean_lcu_table,
               cpi_table = aux_cpi,
               ppp_table = aux_ppp)),
   
## Create dist stat table ------
#   
#   # Covert dist stat list to table
  tar_target(dt_dist_stats,
             db_create_dist_table(
               dl        = dl_dist_stats,
               dsm_table = svy_mean_ppp_table, 
               crr_inv   = cache_inventory)
             ),
#   
#   
##Create reference year table ------
#   
#   # Create a reference year table with predicted means 
#   # for each year in PIP_REF_YEARS. 
#   # 
#   # Due to a change in naming convention this table is 
#   # saved as interpolated-means.qs in 
#   # PIP_PIPE_DIR/aux_data/.
#   
  tar_target(dt_ref_mean_pred,
             db_create_ref_year_table(
               gdp_table = aux_gdp,
               pce_table = aux_pce,
               pop_table = aux_pop,
               pfw_table = aux_pfw,
               dsm_table = svy_mean_ppp_table,
               ref_years = PIP_REF_YEARS,
               pip_years = PIP_YEARS,
               region_code = 'pcn_region_code')),
  
  # tar_target(dt_ref_mean_pred_tmp,
  #            db_create_ref_year_table(
  #              gdp_table = aux_gdp,
  #              pce_table = aux_pce,
  #              pop_table = aux_pop,
  #              pfw_table = aux_pfw,
  #              dsm_table = svy_mean_ppp_table,
  #              ref_years = PIP_REF_YEARS,
  #              pip_years = PIP_YEARS,
  #              region_code = 'pcn_region_code')),
  # 
  # tar_target(dt_ref_mean_pred, 
  #            temp_cleaning_ref_table(
  #              dt_ref_mean_pred_tmp
  #            )),


## Create All-estimations table
  tar_target(dt_prod_estimation_all,
             db_create_estimation_table(
               ref_year_table = dt_ref_mean_pred, 
               dist_table     = dt_dist_stats)
             ),
 
 
## Create coverage table -------

#  Create coverage table by region
  tar_target(
    dt_coverage,
    db_create_coverage_table(
      ref_year_table    = dt_ref_mean_pred,
      pop_table         = aux_pop,
      ref_years         = PIP_REF_YEARS,
      special_countries = c("ARG", "CHN", "IDN", "IND"),
      digits            = 2
      )
    ),
   
## Create aggregated POP table ----
  
  tar_target(
    dt_pop_region,
    db_create_reg_pop_table(
      pop_table   = aux_pop,
      cl_table    = aux_country_list, 
      region_code = 'pcn_region_code',
      pip_years   = PIP_YEARS)),

##  Clean AUX data ------
   
# Clean and transform the AUX tables to the format
# used on the PIP webpage.
  tar_target(all_aux,
             list(aux_cpi, aux_gdp, aux_pop, aux_ppp, aux_pce),
             iteration = "list"
             ),

  
  tar_target(aux_names,
             c("cpi", "gdp", "pop", "ppp", "pce"),
             iteration = "list"),

  
  tar_target(
    aux_clean,
    db_clean_aux(all_aux, aux_names, pip_years = PIP_YEARS),
    pattern = map(all_aux, aux_names), 
    iteration = "list"
  ),

##  Save data ---- 

### Save microdata for production ------
  tar_target(
    survey_files,
    save_survey_data(
      dt              = cache,
      cache_filename  = cache_ids,
      output_dir      = OUT_SVY_DIR,
      cols            = c('welfare', 'weight', 'area'),
      compress        = FST_COMP_LVL,), 
    pattern = map(cache, cache_ids)
  ),

### Save Basic AUX data----
  tar_target(aux_out_files,
             aux_out_files_fun(OUT_AUX_DIR, aux_names)
             ),

  tar_files(aux_out_dir, aux_out_files),
  tar_target(aux_out, 
             fst::write_fst(x        = aux_clean,
                            path     = aux_out_dir,
                            compress =  FST_COMP_LVL), 
             pattern = map(aux_clean, aux_out_dir), 
             iteration = "list"),

### Save additional aux files----
  # tar_target(
  #   add_aux_files,
  #   paste0(
  #     OUT_AUX_DIR,
  #     c("pop-region", "coverage"),".fst"
  #   )
  # ),
  # 
  # tar_files(add_aux_dir, add_aux_files),
  # 
  # tar_target(add_aux,
  #            list(dt_pop_region, dt_coverage), 
  #            iteration = "list"
  #            ),
  tar_target(
    pop_region_out,
    save_aux_data(
      dt_pop_region,
      paste0(OUT_AUX_DIR, "coverage.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  tar_target(
    coverage_out,
    save_aux_data(
      dt_coverage,
      paste0(OUT_AUX_DIR, "pop-region.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),

  # tar_target(
  #   add_aux_out,
  #   save_aux_data(
  #     add_aux,
  #     add_aux_dir,
  #     compress = FST_COMP_LVL
  #     ), 
  #   format = 'file',
  #   pattern = map(add_aux, add_aux_dir)
  # ),

### Save Lorenz list ----
  tar_target(
    lorenz_out,
    save_aux_data(
      lorenz,
      paste0(OUT_AUX_DIR, "lorenz.rds"),
      compress = TRUE
      ),
    format = 'file',
    ),

### Save table with mean and dist stats -------
  tar_target(
    prod_estimation_file,
    format = 'file', 
    save_estimations(dt       = dt_prod_estimation_all, 
                     dir      = OUT_EST_DIR, 
                     name     = "prod_estimation_all", 
                     time     = time, 
                     compress = FST_COMP_LVL)
  ),

### Save dist stats table----
  tar_target(
    dist_file,
    format = 'file',
    save_estimations(dt       = dt_dist_stats, 
                     dir      = OUT_EST_DIR, 
                     name     = "dist_stats", 
                     time     = time, 
                     compress = FST_COMP_LVL)
  ),

### Save survey means table ----
  tar_target(
    survey_mean_file,
    format = 'file', 
    save_estimations(dt       = svy_mean_ppp_table, 
                     dir      = OUT_EST_DIR, 
                     name     = "survey_means", 
                     time     = time, 
                     compress = FST_COMP_LVL)
  ),
   
### Save interpolated means table ----
  tar_target(
    interpolated_means_file,
    format = 'file', 
    save_estimations(dt       = dt_ref_mean_pred, 
                     dir      = OUT_EST_DIR, 
                     name     = "interpolated_means", 
                     time     = time, 
                     compress = FST_COMP_LVL)
  )

)

