# ---- Install packages ----

# remotes::install_github("PIP-Technical-Team/pipload@development",
#                         dependencies = FALSE)
# remotes::install_github("PIP-Technical-Team/wbpip@synth_vector",
#                        dependencies = FALSE)
# remotes::install_github("PIP-Technical-Team/wbpip",
#                        dependencies = FALSE)
# remotes::install_github("PIP-Technical-Team/pipdm",
#                         dependencies = FALSE)
# remotes::install_github("PIP-Technical-Team/pipdm@development",
#                         dependencies = FALSE)

# ---- Start up ----

# Load packages
source("./_packages.R")
options(joyn.verbose = FALSE) # make sure joyn does not display messages

# Load R files
lapply(list.files("./R", full.names = TRUE,
                  pattern = "\\.R$"), source)

# Set-up global variables
pipload::add_gls_to_env()

# Check that the correct _targets store is used 
if (identical(
  tar_config_get('store'),
  paste0(gls$PIP_PIPE_DIR, 'pc_data/_targets/'))) {
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

# ---- Step 1: Prepare data ----

## Load PIP inventory 
pip_inventory <- 
  pipload::pip_find_data(
    inv_file = paste0(gls$PIP_DATA_DIR, '_inventory/inventory.fst'),
    filter_to_pc = TRUE,
    maindir = gls$PIP_DATA_DIR)

# Load AUX data
aux_tb <- prep_aux_data(gls$PIP_DATA_DIR)

dl_aux <- lapply(aux_tb$auxname, function(x) {
  pipload::pip_load_aux(measure     = x, 
                        apply_label = FALSE,
                        maindir     = gls$PIP_DATA_DIR, 
                        verbose     = FALSE)
})
names(dl_aux) <- aux_tb$auxname                

# ---- Step 2: Run pipeline -----

list(
  
  ## Inventory and cache files ----
  
  ### Create pipeline inventory ----
  
  tar_target(pipeline_inventory, {
    x <- db_filter_inventory(
      dt        = pip_inventory,
      pfw_table = dl_aux$pfw)
     # Uncomment for specific countries
    # x <- x[country_code == 'PHL' & surveyid_year == 2000]
    # x <- x[country_code %in% c("IND", "IDN")]
  }
  ),
  
  ### Create cache files ----
  
  tar_target(status_cache_files_creation, 
             create_cache_file(
               pipeline_inventory = pipeline_inventory,
               pip_data_dir       = gls$PIP_DATA_DIR,
               tool               = "PC",
               cache_svy_dir      = gls$CACHE_SVY_DIR_PC,
               compress           = gls$FST_COMP_LVL,
               force              = FALSE,
               verbose            = FALSE,
               cpi_dt             = dl_aux$cpi,
               ppp_dt             = dl_aux$ppp, 
               pfw_dt             = dl_aux$pfw)
  ),
  
  ### Cache inventory file ----
  
  tar_target(
    cache_inventory, 
    {
      x <- pip_update_cache_inventory(
        pipeline_inventory = pipeline_inventory,
        pip_data_dir       = gls$PIP_DATA_DIR,
        cache_svy_dir      = gls$CACHE_SVY_DIR_PC,
        tool               = "PC", 
        save               = TRUE, 
        load               = TRUE, 
        verbose            = TRUE
      )
      # to filter temporarily
      # x <- x[grepl("^(CHN|IDN)", survey_id)
      #        ][gsub("([A-Z]+)_([0-9]+)_(.*)", "\\2", survey_id) > 2010
      #        ]
    }
  ),
  
  ### Identifiers -----
  
  tar_target(cache_ids, 
             get_cache_id(cache_inventory)),
  
  tar_target(survey_ids, 
             get_survey_id(cache_inventory)),
  
  tar_target(cache_files,
             get_cache_files(cache_inventory)),
  
  tar_files(cache_dir, cache_files), # label as files, not targets
  
  tar_target(cache,
             fst::read_fst(path = cache_dir,
                           as.data.table = TRUE),
             pattern = map(cache_dir),
             iteration = "list"),
  # 
  ## LCU survey means ---- 
  
  ### Fetch GD survey means and convert them to daily values ----
  tar_target(
    gd_means, {
      
      dt. <- joyn::merge(x          = cache_inventory,
                         y          = dl_aux$gdm,
                         by         = c("survey_id", "welfare_type"),
                         match_type = "1:m",
                         yvars      = c("survey_mean_lcu", "pop_data_level"),
                         keep       = "left")
      
      data.table::setorder(dt., cache_id, pop_data_level)
      
      
      gd_means        <- dt.[, survey_mean_lcu]
      gd_means        <- gd_means * (12/365)
      
      names(gd_means) <- dt.[, cache_id]
      ## convert to list by name
      gd_means        <- split(unname(gd_means),names(gd_means)) 
      
      return(gd_means)
      
    }, 
    iteration = "list"
  ) ,
  
  ## Calculate LCU survey mean ----
  
  tar_target(
    svy_mean_lcu,
    db_compute_survey_mean(cache, gd_means),
    pattern =  map(cache, gd_means),
    iteration = "list"
  ),
  
  
  ## Create LCU table ------
  tar_target(
    svy_mean_lcu_table,
    db_create_lcu_table(
      dl        = svy_mean_lcu,
      pop_table = dl_aux$pop,
      pfw_table = dl_aux$pfw)
  ),
  
  ##  Create DSM table ---- 
  
  tar_target(svy_mean_ppp_table,
             db_create_dsm_table(
               lcu_table = svy_mean_lcu_table,
               cpi_table = dl_aux$cpi,
               ppp_table = dl_aux$ppp)),
  
  ## Create reference year table ------
  
  tar_target(dt_ref_mean_pred,
             db_create_ref_year_table(
               gdp_table = dl_aux$gdp,
               pce_table = dl_aux$pce,
               pop_table = dl_aux$pop,
               pfw_table = dl_aux$pfw,
               dsm_table = svy_mean_ppp_table,
               ref_years = gls$PIP_REF_YEARS,
               pip_years = gls$PIP_YEARS,
               region_code = 'pcn_region_code')),
  
  ## Distributional stats ---- 
  
  # Calculate Lorenz curves (for microdata)
  tar_target(
    lorenz_all,
    db_compute_lorenz(cache),
    pattern = map(cache),
    iteration = "list"
  ),

  # Clean group data
  tar_target(
    lorenz,
    purrr::keep(lorenz_all, ~!is.null(.x))
  ),
  
  ### Calculate distributional statistics ----
  
  # Get mean 
  # tar_target(
  #   dl_mean, # name vectors. 
  #   named_mean(svy_mean_lcu),
  #   pattern = map(svy_mean_lcu),
  #   iteration = "list"
  # ),
  
   
  tar_target(
    name      = dl_dist_stats,
    command   = db_compute_dist_stats(dt         = cache,
                                      mean_table = svy_mean_ppp_table,
                                      pop_table  = dl_aux$pop,
                                      cache_id   = cache_ids),
    pattern   =  map(cache, cache_ids),
    iteration = "list"
  ),
  
  ### Create dist stat table ------

  # Covert dist stat list to table
  tar_target(dt_dist_stats,
             db_create_dist_table(
               dl        = dl_dist_stats,
               dsm_table = svy_mean_ppp_table, 
               crr_inv   = cache_inventory)
  ),

  ## Output tables --------
  
  ### Create estimations tables ----
  
  tar_target(dt_prod_ref_estimation,
             db_create_ref_estimation_table(
               ref_year_table = dt_ref_mean_pred, 
               dist_table     = dt_dist_stats)
  ),
  
  tar_target(dt_prod_svy_estimation,
             db_create_svy_estimation_table(
               dsm_table = svy_mean_ppp_table, 
               dist_table = dt_dist_stats,
               gdp_table = dl_aux$gdp,
               pce_table = dl_aux$pce)
  ),
  
  ### Create coverage table -------
  
  # Create coverage table by region
  tar_target(
    dt_coverage,
    db_create_coverage_table(
      ref_year_table    = dt_ref_mean_pred,
      pop_table         = dl_aux$pop,
      ref_years         = gls$PIP_REF_YEARS,
      special_countries = c("ARG", "CHN", "IDN", "IND"),
      digits            = 2
    )
  ),
  
  ### Create regional population table ----
  
  tar_target(
    dt_pop_region,
    db_create_reg_pop_table(
      pop_table   = dl_aux$pop,
      cl_table    = dl_aux$country_list, 
      region_code = 'pcn_region_code',
      pip_years   = gls$PIP_YEARS)),
  
  ### Create decomposition table ----
  
  tar_target(
    dt_decomposition,
    db_create_decomposition_table(
      dsm_table = svy_mean_ppp_table)),
  
  ##  Clean AUX data ------
  
  # Clean and transform the AUX tables to the format
  # used on the PIP webpage.
  tar_target(all_aux,
             list(dl_aux$cpi, dl_aux$gdp, dl_aux$pop,
                  dl_aux$ppp, dl_aux$pce),
             iteration = "list"
  ),
  
  tar_target(aux_names,
             c("cpi", "gdp", "pop", "ppp", "pce"),
             iteration = "list"),
  
  tar_target(
    aux_clean,
    db_clean_aux(all_aux, aux_names, pip_years = gls$PIP_YEARS),
    pattern = map(all_aux, aux_names), 
    iteration = "list"
  ),
  
  # Create Framework data
  tar_target(
    dt_framework,
    create_framework(dl_aux$pfw)
  ),
  
  ## Save data ---- 
  
  ### Save survey data ------
  
  tar_target(
    survey_files,
    save_survey_data(
      dt              = cache,
      cache_filename  = cache_ids,
      output_dir      = gls$OUT_SVY_DIR_PC,
      cols            = c('welfare', 'weight', 'area'),
      compress        = gls$FST_COMP_LVL), 
    pattern = map(cache, cache_ids)
  ),
  
  ### Save basic AUX data ----
  
  tar_target(aux_out_files,
             aux_out_files_fun(gls$OUT_AUX_DIR_PC, aux_names)
  ),
  tar_files(aux_out_dir, aux_out_files),
  
  tar_target(aux_out,
             fst::write_fst(x        = aux_clean,
                            path     = aux_out_dir,
                            compress = gls$FST_COMP_LVL),
             pattern   = map(aux_clean, aux_out_dir),
             iteration = "list"),

  ### Save additional AUX files ----
  
  # Countries
  tar_target(
    countries_out,
    save_aux_data(
      dl_aux$countries %>% 
        data.table::setnames('pcn_region_code', 'region_code'),
      paste0(gls$OUT_AUX_DIR_PC, "countries.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Regions
  tar_target(
    regions_out,
    save_aux_data(
      dl_aux$regions,
      paste0(gls$OUT_AUX_DIR_PC, "regions.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Country profiles 
  tar_target(
    country_profiles_out,
    save_aux_data(
      dl_aux$cp,
      paste0(gls$OUT_AUX_DIR_PC, "country_profiles.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Poverty lines
  tar_target(
    poverty_lines_out,
    save_aux_data(
      dl_aux$pl,
      paste0(gls$OUT_AUX_DIR_PC, "poverty_lines.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Survey metadata (for Data Sources page)
  tar_target(
    survey_metadata_out,
    save_aux_data(
      dl_aux$metadata,
      paste0(gls$OUT_AUX_DIR_PC, "survey_metadata.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Indicators master
  tar_target(
    indicators_out,
    save_aux_data(
      dl_aux$indicators,
      paste0(gls$OUT_AUX_DIR_PC, "indicators.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Regional population
  tar_target(
    pop_region_out,
    save_aux_data(
      dt_pop_region,
      paste0(gls$OUT_AUX_DIR_PC, "pop_region.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Coverage 
  tar_target(
    coverage_out,
    save_aux_data(
      dt_coverage,
      paste0(gls$OUT_AUX_DIR_PC, "coverage.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Decomposition master
  tar_target(
    decomposition_out,
    save_aux_data(
      dt_decomposition,
      paste0(gls$OUT_AUX_DIR_PC, "decomposition.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Framework data
  tar_target(
    framework_out,
    save_aux_data(
      dt_framework,
      paste0(gls$OUT_AUX_DIR_PC, "framework.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  ### Save estimation tables -------
  
  tar_target(
    prod_ref_estimation_file,
    format = 'file', 
    save_estimations(dt       = dt_prod_ref_estimation, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "prod_ref_estimation", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  tar_target(
    prod_svy_estimation_file,
    format = 'file', 
    save_estimations(dt       = dt_prod_svy_estimation, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "prod_svy_estimation", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  ### Save Lorenz list ----
  
  tar_target(
    lorenz_out,
    save_aux_data(
      lorenz,
      paste0(gls$OUT_AUX_DIR_PC, "lorenz.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  ### Save dist stats table ----
  
  tar_target(
    dist_file,
    format = 'file',
    save_estimations(dt       = dt_dist_stats, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "dist_stats", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  ### Save survey means table ----
  
  tar_target(
    survey_mean_file,
    format = 'file', 
    save_estimations(dt       = svy_mean_ppp_table, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "survey_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  ### Save interpolated means table ----
  
  tar_target(
    interpolated_means_file,
    format = 'file', 
    save_estimations(dt       = dt_ref_mean_pred, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "interpolated_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  )
  
)

