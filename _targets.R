
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#               Step 0: Start up   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(targets)
library(tarchetypes)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##   Set initial parameters  --------
# remotes::install_github("PIP-Technical-Team/pipdm@development")
### defaults ---------

# Input dir 
PIP_DATA_DIR     <- '//w1wbgencifs01/pip/PIP-Data/_testing/pipdp_testing/' 

# '//w1wbgencifs01/pip/pip_ingestion_pipeline/' # Output dir
PIP_PIPE_DIR     <- '//w1wbgencifs01/pip/PIP-Data/_testing/pip_ingestion_pipeline/' 

# Cached survey data dir
CACHE_SVY_DIR    <- paste0(PIP_PIPE_DIR, 'pc_data/cache/alt_clean_survey_data/') 

# Final survey data output dir
OUT_SVY_DIR      <- paste0(PIP_PIPE_DIR, 'pc_data/survey_data/') 

#  Estimations output dir
OUT_EST_DIR      <- paste0(PIP_PIPE_DIR, 'pc_data/estimations_output/') 

# aux data output dir
OUT_AUX_DIR      <- paste0(PIP_PIPE_DIR, 'pc_data/aux_output/')  

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
  imports  = c('pipdm','pipload', 'wbpip'))



# Load pipeline helper functions 
source('_packages.R')
source('R/_common.R')

# Set future plan (for targets::tar_make_future)
# plan(multisession)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#      Step 1: Define short useful functions   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# compendium path
compendium_path <- function(){
  paste0(CACHE_SVY_DIR, "_compendium/compendium.qs")
}
cache_inventory_path <- function(){
  paste0(CACHE_SVY_DIR, "_crr_inventory/crr_inventory.fst")
}
  
get_cache_files <- function(x) {
  x$cache_file
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
aux_tb <- aux_tb[auxname != "maddison"]


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
cache_info <- 
  cache_survey_data(
  pipeline_inventory = pipeline_inventory,
  pip_data_dir       = PIP_DATA_DIR,
  cache_svy_dir      = CACHE_SVY_DIR,
  compress           = FST_COMP_LVL,
  verbose            = TRUE, 
  compendium         = FALSE)

# correspondence inventory
# cache_inventory <- cache_info$data_available
# cache_inventory <- 
#   cache_inventory[grepl("^(PRY|ARE)", survey_id)
#                  ][gsub("([A-Z]+)_([0-9]+)_(.*)", "\\2", survey_id) > 2010
#                    ]

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
    x <- x[grepl("^(PRY|ARE)", survey_id)
           ][gsub("([A-Z]+)_([0-9]+)_(.*)", "\\2", survey_id) > 2010
           ]
    
    },
  ),
  
  tar_target(cache_files, 
             get_cache_files(cache_inventory)),

  tar_files(chh_dir, cache_files),
  tar_target(cch, 
             fst::read_fst(path = chh_dir, 
                           as.data.table = TRUE), 
             pattern = map(chh_dir), 
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
    pipdm::db_compute_survey_mean(cch, gd_means),
    pattern =  map(cch, gd_means), 
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
    pipdm::db_compute_lorenz(cch),
    pattern = map(cch), 
    iteration = "list"
  ), 
# clean Group Data obs
  tar_target(
    lorenz, 
    purrr::keep(lorenz_all, ~!is.null(.x))
  ),

# get mean 
  tar_target(
    dl_mean,
    svy_mean_lcu$survey_mean_lcu,
    pattern = map(svy_mean_lcu),
    iteration = "list"
  ),

### Calculate distributional statistics
  tar_target(
    dl_dist_stats,
    pipdm::db_compute_dist_stats(cch, dl_mean), 
    map(cch, dl_mean), 
    iteration = "list"
    ),
  
## Create LCU table ------

  tar_target(
    svy_mean_lcu_table,
    pipdm::db_create_lcu_table(
     dl        = svy_mean_lcu,
     pop_table = aux_pop,
     pfw_table = aux_pfw)
    ),

#~~~~~~~~~~~~~~~~~~~~~~~~
##  Create DSM table ---- 

  tar_target(svy_mean_ppp_table,
             pipdm::db_create_dsm_table(
               lcu_table = svy_mean_lcu_table,
               cpi_table = aux_cpi,
               ppp_table = aux_ppp)),
   
## Create dist stat table ------
#   
#   # Covert dist stat list to table
  tar_target(dt_dist_stats,
             pipdm::db_create_dist_table(
               dl_dist_stats,
               survey_id = cache_inventory$survey_id,
               dsm_table = svy_mean_ppp_table)),
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
             pipdm::db_create_ref_year_table(
               gdp_table = aux_gdp,
               pce_table = aux_pce,
               pop_table = aux_pop,
               pfw_table = aux_pfw,
               dsm_table = svy_mean_ppp_table,
               ref_years = PIP_REF_YEARS,
               pip_years = PIP_YEARS,
               region_code = 'pcn_region_code')),

# 
## Create coverage table -------

#  Create coverage table by region
  tar_target(
    dt_coverage,
    pipdm::db_create_coverage_table(
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
    pipdm::db_create_reg_pop_table(
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
    pipdm::db_clean_aux(all_aux, aux_names, pip_years = PIP_YEARS),
    pattern = map(all_aux, aux_names), 
    iteration = "list"
  ),

##  Save data ---- 

## Save AUX data
  tar_target(aux_out_files,
             paste0(OUT_AUX_DIR, aux_names, ".fst"),
             pattern   = map(aux_names)
             ),

  tar_files(aux_out_dir, aux_out_files),
  tar_target(aux_out, 
             fst::write_fst(x        = aux_clean,
                            path     = aux_out_dir,
                            compress =  FST_COMP_LVL), 
             pattern = map(aux_clean, aux_out_dir), 
             iteration = "list")

#   # Save POP region table
#   tar_target(
#     pop_region_file,
#     format = 'file',
#     save_aux_data(
#       dt_pop_region,
#       filename = 'pop-region',
#       outdir = OUT_AUX_DIR,
#       compress = FST_COMP_LVL)
#   ),
# 
#   # Save coverage table
#   tar_target(
#     coverage_file,
#     format = 'file',
#     save_aux_data(
#       dt_coverage,
#       filename = 'coverage',
#       outdir = OUT_AUX_DIR,
#       compress = FST_COMP_LVL)
#   ),
# #   
#   # Save Lorenz list
#   tar_target(
#     lorenz_file,
#     format = 'file',
#     save_aux_data(
#       dl_lorenz,
#       filename = 'lorenz',
#       outdir = OUT_AUX_DIR,
#       compress = TRUE,
#       type = 'rds')
#   ),
#   
#   # Save dist stats table
#   tar_target(
#     dist_file,
#     format = 'file',
#     save_aux_data(
#       dt_dist_stats,
#       filename = 'dist-stats',
#       outdir = OUT_AUX_DIR,
#       compress = FST_COMP_LVL,
#       type = 'fst')
#   ),
#     
#   # Save survey means table
#   tar_target(
#     survey_mean_file,
#     format = 'file',
#     save_aux_data(
#       dt_svy_mean_ppp,
#       filename = 'survey-mean',
#       outdir = OUT_AUX_DIR,
#       compress = FST_COMP_LVL,
#       type = 'fst')
#   ),
#   
#   # Save interpolated means table 
#   tar_target(
#     interpolated_mean_file,
#     format = 'file',
#     save_aux_data(
#       dt_svy_mean_ppp,
#       filename = 'interpolated-mean',
#       outdir = OUT_AUX_DIR,
#       compress = FST_COMP_LVL,
#       type = 'fst')
#   )
  
)

