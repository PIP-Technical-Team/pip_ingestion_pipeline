#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Set initial parameters  --------
# remotes::install_github("PIP-Technical-Team/pipdm")
# remotes::install_github("PIP-Technical-Team/pipdm@development")
# remotes::install_github("PIP-Technical-Team/pipload@development")
# remotes::install_github("PIP-Technical-Team/wbpip@halfmedian_spl")
# remotes::install_github("randrescastaneda/joyn")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Start up   ---------
## Load packages ----
source("./_packages.R")
options(joyn.verbose = FALSE) # make sure joyn does not display messages

## Load R files ----
lapply(list.files("./R", full.names = TRUE, pattern = "\\.R$"), source)

## Set-up global variables ----
globals <- create_globals(root_dir = '//w1wbgencifs01/pip')

# Check that the correct _targets store is used 
if (identical(
  tar_config_get('store'),
  paste0(globals$PIP_PIPE_DIR, 'pc_data/_targets/'))) {
  stop('The store specified in _targets.yaml doesn\'t match with the pipeline directory')
}

# Set targets options 
tar_option_set(
  garbage_collection = TRUE,
  memory = 'transient',
  format = 'qs', #'fst_dt',
  imports  = c('pipload',
               'pipdm',
               'wbpip')
  )

# Set future plan (for targets::tar_make_future)
# plan(multisession)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1: Prepare data ---------
## Load PIP inventory 
pip_inventory <- 
  pipload::pip_find_data(
    inv_file = paste0(globals$PIP_DATA_DIR, '_inventory/inventory.fst'),
    filter_to_pc = TRUE,
    maindir = globals$PIP_DATA_DIR)
## Create list of AUX files  -------
aux_tb <- prep_aux_data(globals$PIP_DATA_DIR)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2: Run pipeline ---------

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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Inventory and cache files --------

### Load PIP inventory -----
# tar_target(pip_inventory, 
#            pipload::pip_find_data(
#              inv_file = paste0(PIP_DATA_DIR, '_inventory/inventory.fst'),
#              filter_to_pc = TRUE,
#              maindir = PIP_DATA_DIR)),
  
  # Create pipeline inventory
  tar_target(pipeline_inventory, {
             x <- pipdm::db_filter_inventory(
                        dt = pip_inventory,
                        pfw_table = aux_pfw)
             
             # Uncomment for specific countries
             # x <- x[country_code == 'IDN' & surveyid_year == 2015]
             }
             ),

### Create cache files ---------
  tar_target(status_cache_files_creation, 
             pipdm::create_cache_file(
               pipeline_inventory = pipeline_inventory,
               pip_data_dir       = globals$PIP_DATA_DIR,
               tool               = "PC",
               cache_svy_dir      = globals$CACHE_SVY_DIR,
               compress           = globals$FST_COMP_LVL,
               force              = FALSE,
               verbose            = FALSE,
               cpi_dt             = aux_cpi,
               ppp_dt             = aux_ppp)
             ),

### Cache inventory file ----
  tar_target(
    cache_inventory_dir, 
    cache_inventory_path(globals$CACHE_SVY_DIR),
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
  
### identifiers ---------
  tar_target(cache_ids, 
             get_cache_id(cache_inventory)),
  
  tar_target(survey_ids, 
             get_survey_id(cache_inventory)),
  
  tar_target(cache_files,
             get_cache_files(cache_inventory)),
  
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

# get mean 
  tar_target(
    dl_mean, # name vectors. 
    named_mean(svy_mean_lcu),
    pattern = map(svy_mean_lcu),
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

### Create reference year table ------
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
             ref_years = globals$PIP_REF_YEARS,
             pip_years = globals$PIP_YEARS,
             region_code = 'pcn_region_code')),


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


### Calculate distributional statistics ----
  tar_target(
    name      = dl_dist_stats,
    command   = db_compute_dist_stats(dt       = cache, 
                                      mean     = dl_mean, 
                                      pop      = aux_pop, 
                                      cache_id = cache_ids), 
    pattern   =  map(cache, dl_mean, cache_ids), 
    iteration = "list"
    ),
   
### Create dist stat table ------
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
  
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Output tables --------

## Create estimations table
  tar_target(dt_prod_estimation_all,
             db_create_ref_estimation_table(
               ref_year_table = dt_ref_mean_pred, 
               dist_table     = dt_dist_stats)
             ),

  tar_target(dt_prod_ref_estimation,
             db_create_ref_estimation_table(
               ref_year_table = dt_ref_mean_pred, 
               dist_table     = dt_dist_stats)
  ),

  tar_target(dt_prod_svy_estimation,
             db_create_svy_estimation_table(
               dsm_table = svy_mean_ppp_table, 
               dist_table     = dt_dist_stats)
  ),
   
 
### Create coverage table -------

#  Create coverage table by region
  tar_target(
    dt_coverage,
    db_create_coverage_table(
      ref_year_table    = dt_ref_mean_pred,
      pop_table         = aux_pop,
      ref_years         = globals$PIP_REF_YEARS,
      special_countries = c("ARG", "CHN", "IDN", "IND"),
      digits            = 2
      )
    ),
   
### Create aggregated POP table ----
  
  tar_target(
    dt_pop_region,
    db_create_reg_pop_table(
      pop_table   = aux_pop,
      cl_table    = aux_country_list, 
      region_code = 'pcn_region_code',
      pip_years   = globals$PIP_YEARS)),

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##  Save data ---- 

### Save microdata for production ------
  tar_target(
    survey_files,
    save_survey_data(
      dt              = cache,
      cache_filename  = cache_ids,
      output_dir      = globals$OUT_SVY_DIR,
      cols            = c('welfare', 'weight', 'area'),
      compress        = globals$FST_COMP_LVL,), 
    pattern = map(cache, cache_ids)
  ),

### Save Basic AUX data----
  tar_target(aux_out_files,
             aux_out_files_fun(globals$OUT_AUX_DIR, aux_names)
             ),

  tar_files(aux_out_dir, aux_out_files),
  tar_target(aux_out, 
             fst::write_fst(x        = aux_clean,
                            path     = aux_out_dir,
                            compress =  globals$FST_COMP_LVL), 
             pattern = map(aux_clean, aux_out_dir), 
             iteration = "list"),

### Save additional aux files----
  tar_target(
    pop_region_out,
    save_aux_data(
      dt_pop_region,
      paste0(globals$OUT_AUX_DIR, "pop-region.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  tar_target(
    coverage_out,
    save_aux_data(
      dt_coverage,
      paste0(globals$OUT_AUX_DIR, "coverage.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),

### Save Lorenz list ----
  tar_target(
    lorenz_out,
    save_aux_data(
      lorenz,
      paste0(globals$OUT_AUX_DIR, "lorenz.rds"),
      compress = TRUE
      ),
    format = 'file',
    ),

### Save table with mean and dist stats -------
  tar_target(
    prod_estimation_file,
    format = 'file', 
    save_estimations(dt       = dt_prod_estimation_all, 
                     dir      = globals$OUT_EST_DIR, 
                     name     = "prod_estimation_all", 
                     time     = time, 
                     compress = globals$FST_COMP_LVL)
  ),

  tar_target(
    prod_ref_estimation_file,
    format = 'file', 
    save_estimations(dt       = dt_prod_ref_estimation, 
                     dir      = globals$OUT_EST_DIR, 
                     name     = "prod_ref_estimation", 
                     time     = time, 
                     compress = globals$FST_COMP_LVL)
  ),

  tar_target(
    prod_svy_estimation_file,
    format = 'file', 
    save_estimations(dt       = dt_prod_svy_estimation, 
                     dir      = globals$OUT_EST_DIR, 
                     name     = "prod_svy_estimation", 
                     time     = time, 
                     compress = globals$FST_COMP_LVL)
  ),


### Save dist stats table----
  tar_target(
    dist_file,
    format = 'file',
    save_estimations(dt       = dt_dist_stats, 
                     dir      = globals$OUT_EST_DIR, 
                     name     = "dist_stats", 
                     time     = time, 
                     compress = globals$FST_COMP_LVL)
  ),

### Save survey means table ----
  tar_target(
    survey_mean_file,
    format = 'file', 
    save_estimations(dt       = svy_mean_ppp_table, 
                     dir      = globals$OUT_EST_DIR, 
                     name     = "survey_means", 
                     time     = time, 
                     compress = globals$FST_COMP_LVL)
  ),
   
### Save interpolated means table ----
  tar_target(
    interpolated_means_file,
    format = 'file', 
    save_estimations(dt       = dt_ref_mean_pred, 
                     dir      = globals$OUT_EST_DIR, 
                     name     = "interpolated_means", 
                     time     = time, 
                     compress = globals$FST_COMP_LVL)
  )

)

