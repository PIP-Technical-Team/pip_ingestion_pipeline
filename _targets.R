# ---- Install packages ----
# 
# remotes::install_github("PIP-Technical-Team/pipload@dev",
#                         dependencies = FALSE)

# remotes::install_github("PIP-Technical-Team/wbpip@synth_vector",
#                        dependencies = FALSE)
# remotes::install_github("PIP-Technical-Team/wbpip",
#                        dependencies = FALSE)

# ---- Start up ----

# Load packages
source("./_packages.R")
options(joyn.verbose = FALSE, # make sure joyn does not display messages
        pipload.verbose = FALSE) 

# Load R files
purrr::walk(fs::dir_ls(path = "./R", 
                       regexp = "\\.R$"), source)

# Read pipdm functions
purrr::walk(fs::dir_ls(path = "./R/pipdm/R", 
                       regexp = "\\.R$"), source)




#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Select PPP year   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


py <- 2011  # PPP year 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Load globals   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



gls <- pipload::pip_create_globals(
  root_dir   = Sys.getenv("PIP_ROOT_DIR"), 
  # out_dir    = fs::path("y:/pip_ingestion_pipeline/temp/"),
  vintage    = list(release = "20220408", 
                    ppp_year = py, 
                    identity = "PROD"), 
  create_dir = TRUE
)


cli::cli_text("Vintage directory {.file {gls$vintage_dir}}")

# pipload::add_gls_to_env(vintage = "20220408")

# pipload::add_gls_to_env(vintage = "new",
#                         out_dir = fs::path("y:/pip_ingestion_pipeline/temp/"))
# 
# Check that the correct _targets store is used 

if (!identical(fs::path(tar_config_get('store')),
               fs::path(gls$PIP_PIPE_DIR, 'pc_data/_targets'))) {
  stop('The store specified in _targets.yaml doesn\'t match with the pipeline directory')
}

# Set targets options 
tar_option_set(
  garbage_collection = TRUE,
  memory = 'transient',
  format = 'qs', #'fst_dt',
  # imports  = c('pipload',
  #              'wbpip'), 
  workspace_on_error = TRUE
)

# Set future plan (for targets::tar_make_future)
# plan(multisession)

# ---- Step 1: Prepare data ----


## Load AUX data -----
aux_tb <- prep_aux_data(maindir = gls$PIP_DATA_DIR)

dl_aux <- purrr::map(.x = aux_tb$auxname, 
                     .f = ~{
                       pipload::pip_load_aux(measure     = .x, 
                                             apply_label = FALSE,
                                             maindir     = gls$PIP_DATA_DIR, 
                                             verbose     = FALSE)
                     })
names(dl_aux) <- aux_tb$auxname                

# temporal change. 
dl_aux$pop$year <- as.numeric(dl_aux$pop$year)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Select PPP year --------

vars     <- c("ppp_year", "release_version", "adaptation_version")
ppp_v    <- unique(dl_aux$ppp[, ..vars], by = vars)
data.table::setnames(x = ppp_v,
                     old = c("release_version", "adaptation_version"),
                     new = c("ppp_rv", "ppp_av"))

# max release version
m_rv <- ppp_v[ppp_year == py, max(ppp_rv)]

# max adaptation year
m_av <- ppp_v[ppp_year == py & ppp_rv == m_rv, 
              max(ppp_av)]


dl_aux$ppp <- dl_aux$ppp[ppp_year == py 
                         & release_version    == m_rv
                         & adaptation_version == m_av
                         ][, 
                           ppp_default := TRUE]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Select the right CPI --------

cpivar <- paste0("cpi", py)

dl_aux$cpi[, cpi := get(cpivar)]



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Load PIP inventory ----
pip_inventory <- 
  pipload::pip_find_data(
    inv_file = fs::path(gls$PIP_DATA_DIR, '_inventory/inventory.fst'),
    filter_to_pc = TRUE,
    maindir = gls$PIP_DATA_DIR)



## Create pipeline inventory ----

pipeline_inventory <- 
  db_filter_inventory(dt        = pip_inventory,
                      pfw_table = dl_aux$pfw)


# pipeline_inventory <-
#   pipeline_inventory[grepl("UGA_2019", cache_id)]

# pipeline_inventory <-
#   pipeline_inventory[grepl("^NIC", cache_id)]

# Uncomment for specific countries
# pipeline_inventory <-
# pipeline_inventory[country_code == 'PHL' & surveyid_year == 2000]


# cts_filter <- c('COL', 'IND', "CHN")
# pipeline_inventory <-
#    pipeline_inventory[country_code %in% cts_filter
#                       ][!(country_code == 'CHN' & surveyid_year >= 2017)]

# pipeline_inventory <-
#    pipeline_inventory[!(country_code == 'CHN' & surveyid_year >= 2017)]


# pipeline_inventory <-
#    pipeline_inventory[country_code == 'CHN' & surveyid_year == 2019]

# 
# pipeline_inventory <-
#    pipeline_inventory[country_code == 'CRI' & surveyid_year == 1989]


## --- Create cache files ----

status_cache_files_creation <- 
  create_cache_file(
    pipeline_inventory = pipeline_inventory,
    pip_data_dir       = gls$PIP_DATA_DIR,
    tool               = "PC",
    cache_svy_dir      = gls$CACHE_SVY_DIR_PC,
    compress           = gls$FST_COMP_LVL,
    force              = FALSE,
    verbose            = TRUE,
    cpi_table          = dl_aux$cpi,
    ppp_table          = dl_aux$ppp, 
    pfw_table          = dl_aux$pfw, 
    pop_table          = dl_aux$pop)


## bring cache our of pipeline -----

cache_inventory <- 
  pip_update_cache_inventory(
    pipeline_inventory = pipeline_inventory,
    pip_data_dir       = gls$PIP_DATA_DIR,
    cache_svy_dir      = gls$CACHE_SVY_DIR_PC,
    tool               = "PC", 
    save               = FALSE, 
    load               = TRUE, 
    verbose            = TRUE
  )

# to filter temporarily
# 
# cache_inventory  <-
#   cache_inventory[!(grepl("^(CHN)", survey_id) &
#                       stringr::str_extract(survey_id, "([0-9]{4})") >= 2017)
#                   ]

# reg <- paste0("^(", paste(cts_filter, collapse = "|"),")")
# 
# cache_inventory <- cache_inventory[grepl(reg, survey_id)]


cache_ids <- get_cache_id(cache_inventory)
cache_dir <- get_cache_files(cache_inventory)

cache   <- mp_cache(cache_dir = cache_dir, 
                    load      = TRUE, 
                    save      = FALSE, 
                    gls       = gls, 
                    py        = py)

# selected_files <- which(grepl(reg, names(cache)))
# cache <- cache[selected_files]

# remove CHN 2017 and 2018 manually
# cache[grep("CHN_201[78]", names(cache), value = TRUE)] <- NULL

# notify that cache has finished loading (please do NOT delete)
if (requireNamespace("pushoverr")) {
  pushoverr::pushover("Finished loading or creating cache list")
}

length(cache)
length(cache_dir)


# ---- Step 2: Run pipeline -----

list(
  
  ## LCU survey means ---- 
  # tar_target(cache, cache_o, iteration = "list"),
  
  ### Fetch GD survey means and convert them to daily values ----
  tar_target(
    gd_means, 
    get_groupdata_means(cache_inventory = cache_inventory, 
                        gdm            = dl_aux$gdm), 
    iteration = "list"
  ),
  
  ## Calculate LCU survey mean ----
  
  tar_target(
    svy_mean_lcu,
    mp_svy_mean_lcu(cache, gd_means)
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
               dsm_table = svy_mean_ppp_table,
               gdp_table = dl_aux$gdp,
               pce_table = dl_aux$pce,
               pop_table = dl_aux$pop,
               ref_years = gls$PIP_REF_YEARS,
               pip_years = gls$PIP_YEARS,
               region_code = 'pcn_region_code')),
  
  ## Distributional stats ---- 
  
  # Calculate Lorenz curves (for microdata)
  tar_target(
    lorenz,
    mp_lorenz(cache)
  ),
  
  # Calculate Lorenz curves (for microdata)
  # tar_target(
  #   lorenz_all,
  #   db_compute_lorenz(cache),
  #   pattern = map(cache),
  #   iteration = "list"
  # ),
  
  # Clean group data
  # tar_target(
  #   lorenz,
  #   purrr::keep(lorenz_all, ~!is.null(.x))
  # ),
  
  
  ### Calculate distributional statistics ----
  
  
  tar_target(dl_dist_stats,
             mp_dl_dist_stats(dt         = cache,
                              mean_table = svy_mean_ppp_table,
                              pop_table  = dl_aux$pop,
                              cache_id   = cache_ids)
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
    dl_coverage,
    db_create_coverage_table(
      ref_year_table        = dt_ref_mean_pred,
      pop_table             = dl_aux$pop,
      cl_table              = dl_aux$country_list,
      incgrp_table          = dl_aux$income_groups, 
      ref_years             = gls$PIP_REF_YEARS,
      urban_rural_countries = c("ARG", "CHN", "IDN", "IND"),
      digits                = 2
    )
  ),
  
  
  ### Create censoring table -------
  
  # Create censoring list
  tar_target(
    dl_censored,
    db_create_censoring_table(
      censored           = dl_aux$censoring,
      coverage_list      = dl_coverage,
      coverage_threshold = 50
    )
  ),
  
  ### Create regional population table ----
  
  tar_target(
    dt_pop_region,
    db_create_reg_pop_table(
      pop_table   = dl_aux$pop,
      cl_table    = dl_aux$country_list, 
      region_code = 'pcn_region_code',
      pip_years   = gls$PIP_YEARS)
  ),
  
  ### Create decomposition table ----
  
  tar_target(
    dt_decomposition,
    db_create_decomposition_table(
      dsm_table = svy_mean_ppp_table)
  ),
  
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
             iteration = "list"
  ),
  
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
  
  #~~~~~~~~~~~~~~~~~~~~~~
  ## Save data ---- 
  
  ### survey data ------
  
  tar_target(
    survey_files,
    mp_survey_files(
      cache       = cache,
      cache_ids   = cache_ids,
      output_dir  = gls$OUT_SVY_DIR_PC,
      cols        = c('welfare', 'weight', 'area'),
      compress    = gls$FST_COMP_LVL)
  ),
  
  ### Basic AUX data ----
  
  tar_target(aux_out_files,
             aux_out_files_fun(gls$OUT_AUX_DIR_PC, aux_names)
  ),
  tar_target(aux_out,
             fst::write_fst(x        = aux_clean,
                            path     = aux_out_files,
                            compress = gls$FST_COMP_LVL),
             pattern   = map(aux_clean, aux_out_files),
             iteration = "list"),
  
  tar_files(aux_out_dir, aux_out_files),
  
  ### Additional AUX files ----
  
  # Countries
  tar_target(
    countries_out,
    save_aux_data(
      dl_aux$countries %>% 
        data.table::setnames('pcn_region_code', 'region_code'),
      fs::path(gls$OUT_AUX_DIR_PC, "countries.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Regions
  tar_target(
    regions_out,
    save_aux_data(
      dl_aux$regions,
      fs::path(gls$OUT_AUX_DIR_PC, "regions.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Country profiles 
  tar_target(
    country_profiles_out,
    save_aux_data(
      dl_aux$cp,
      fs::path(gls$OUT_AUX_DIR_PC, "country_profiles.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Poverty lines
  tar_target(
    poverty_lines_out,
    save_aux_data(
      dl_aux$pl,
      fs::path(gls$OUT_AUX_DIR_PC, "poverty_lines.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Survey metadata (for Data Sources page)
  tar_target(
    survey_metadata_out,
    save_aux_data(
      dl_aux$metadata,
      fs::path(gls$OUT_AUX_DIR_PC, "survey_metadata.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Indicators master
  tar_target(
    indicators_out,
    save_aux_data(
      dl_aux$indicators,
      fs::path(gls$OUT_AUX_DIR_PC, "indicators.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Regional population
  tar_target(
    pop_region_out,
    save_aux_data(
      dt_pop_region,
      fs::path(gls$OUT_AUX_DIR_PC, "pop_region.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  ### Coverage files ----
  
  # Regional coverage 
  tar_target(
    region_year_coverage_out,
    save_aux_data(
      dl_coverage$region,
      fs::path(gls$OUT_AUX_DIR_PC, "region_coverage.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Regional coverage 
  tar_target(
    incomeGroup_year_coverage_out,
    save_aux_data(
      dl_coverage$incgrp,
      fs::path(gls$OUT_AUX_DIR_PC, "incgrp_coverage.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Country year coverage
  tar_target(
    country_year_coverage_out,
    save_aux_data(
      dl_coverage$country_year_coverage,
      fs::path(gls$OUT_AUX_DIR_PC, "country_coverage.fst"),
      compress = TRUE)
  ),
  
  # Censoring 
  tar_target(
    censored_out,
    save_aux_data(
      dl_censored,
      fs::path(gls$OUT_AUX_DIR_PC, "censored.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  
  # Decomposition master
  tar_target(
    decomposition_out,
    save_aux_data(
      dt_decomposition,
      fs::path(gls$OUT_AUX_DIR_PC, "decomposition.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Framework data
  tar_target(
    framework_out,
    save_aux_data(
      dt_framework,
      fs::path(gls$OUT_AUX_DIR_PC, "framework.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  # Dictionary
  tar_target(
    dictionary_out,
    save_aux_data(
      dl_aux$dictionary,
      fs::path(gls$OUT_AUX_DIR_PC, "dictionary.fst"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  ### Estimation tables -------
  
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
  
  ###  Lorenz list ----
  
  tar_target(
    lorenz_out,
    save_aux_data(
      lorenz,
      fs::path(gls$OUT_AUX_DIR_PC, "lorenz.rds"),
      compress = TRUE
    ),
    format = 'file',
  ),
  
  ### Dist stats table ----
  
  tar_target(
    dist_file,
    format = 'file',
    save_estimations(dt       = dt_dist_stats, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "dist_stats", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  ###Survey means table ----
  
  tar_target(
    survey_mean_file,
    format = 'file', 
    save_estimations(dt       = svy_mean_ppp_table, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "survey_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  tar_target(
    survey_mean_file_aux,
    format = 'file', 
    save_estimations(dt       = svy_mean_ppp_table, 
                     dir      = gls$OUT_AUX_DIR_PC, 
                     name     = "survey_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  ### Interpolated means table ----
  
  tar_target(
    interpolated_means_file,
    format = 'file', 
    save_estimations(dt       = dt_ref_mean_pred, 
                     dir      = gls$OUT_EST_DIR_PC, 
                     name     = "interpolated_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  tar_target(
    interpolated_means_file_aux,
    format = 'file', 
    save_estimations(dt       = dt_ref_mean_pred, 
                     dir      = gls$OUT_AUX_DIR_PC, 
                     name     = "interpolated_means", 
                     time     = gls$TIME, 
                     compress = gls$FST_COMP_LVL)
  ),
  
  ### Data timestamp file ----
  
  tar_target(
    data_timestamp_file,
    # format = 'file', 
    writeLines(as.character(Sys.time()), 
               fs::path(gls$OUT_DIR_PC, 
                        gls$vintage_dir, 
                        "data_update_timestamp", 
                        ext = "txt"))
  )
  
)


