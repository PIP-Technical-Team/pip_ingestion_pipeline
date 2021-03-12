# ---- Step 0: Start up ---- 

# Load packages 
library(pipload)
library(pipdm)
library(wbpip)
library(future)
library(future.apply)
library(future.callr)
library(magrittr)
library(progress)
library(glue)
library(cli)
suppressPackageStartupMessages(library(fst))
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(purrr))

# Load functions
source('R/_common.R')

message('Start up.')

# Set initial parameters 
PIP_DATA_DIR     <- '//w1wbgencifs01/pip/PIP-Data/_testing/pipdp_testing/' # Input dir 

# '//w1wbgencifs01/pip/pip_ingestion_pipeline/' # Output dir
PIP_PIPE_DIR     <- '//w1wbgencifs01/pip/PIP-Data/_testing/pip_ingestion_pipeline/' 

# Cached survey data dir
CACHE_SVY_DIR    <- paste0(PIP_PIPE_DIR, 'pc_data/cache/alt_clean_survey_data/') 


OUT_SVY_DIR      <- paste0(PIP_PIPE_DIR, 'pc_data/survey_data/') # Final survey data output dir
OUT_AUX_DIR      <- paste0(PIP_PIPE_DIR, 'pc_data/aux_data/') #  # Final aux data output dir 
PIP_YEARS        <- 1977:2020 # Years used in PIP 
PIP_REF_YEARS    <- 1981:2019 # Years used in the interpolated means table 
FST_COMP_LVL     <- 100 # Compression level for .fst output files
APPLY_GC         <- TRUE # Apply garbage collection 
PIP_SAFE_WORKERS <- FALSE # Open/close workers after each future call



# ---- Step 1: Load AUX data ---- 

# Load a list of auxiliary datasets used in the pipeline.
# 
# * The PFW table is used to add survey metadata to the 
#   different output tables. It is also used to filter 
#   outputs to only included surveys in the price framework. 
# * The PPP and CPI tables are used to calculate deflated 
#   survey means. 
# * The GDP and PCE tables are used to calculate predicted
#   reference year means. 
# * The POP table is used to add population values to the
#   main output tables, as well as calculating regional
#   survey coverage. 

# Create vector with paths to all AUX input datasets 
aux_indicators <-   c('gdp', 'pce', 'pop', 'ppp', 'cpi',
                      'pfw', 'gdm', 'country_list')
aux_files <- sprintf('%1$s_aux/%2$s/%2$s.fst',
                     PIP_DATA_DIR, aux_indicators)

# Read AUX data into list 
dl_aux <- 
  purrr::map(aux_files, function(x) {
    suppressMessages(pipload::pip_load_aux(
      file_to_load = x, apply_label = FALSE))
  })
names(dl_aux) <- aux_indicators

# Remove GDM PCN surveys not in PIP_DATA_DIR 
dl_aux$gdm <- dl_aux$gdm[!is.na(survey_id)]


# ---- Step 2: Load PIP inventory ---- 

# Load inventory table of surveys in PIP_DATA_DIR.
#
# Note that this file needs to updated if new 
# surveys have been added to PIP_DATA_DIR at 
# the pipdp stage. This can be done by running 
# pipload::pip_update_inventory() manually or 
# setting PIP_UPDATE_INVENTORY to TRUE. 
# 
# The inventory table needs to be filtered to 
# account for potential changes in surveys 
# included in PFW. This is done with 
# pipdm::db_filter_inventory() below. 

# Load table with the whole inventory
pip_inventory <- 
  pipload::pip_find_data(
    inv_file = paste0(PIP_DATA_DIR, '_inventory/inventory.fst'),
    filter_to_pc = TRUE,
    maindir = PIP_DATA_DIR)

# Select surveys in PFW 
pipeline_inventory <- 
  pipdm::db_filter_inventory(
    pip_inventory,
    pfw_table = dl_aux$pfw)

# PIP survey ids in PFW 
pfw_svy_ids <- 
  sub('[.]dta', '', pipeline_inventory$filename)


# ---- Step X: Cache new survey data ----

# Clean and cache new surveys 
status <- cache_survey_data(
  pipeline_inventory = pipeline_inventory, 
  pip_data_dir       = PIP_DATA_DIR,
  cache_svy_dir      = CACHE_SVY_DIR,
  compress           = FST_COMP_LVL,
  verbose            = TRUE)



# ---- Step 3: Load survey data ----

# Load cached survey data (.fst)

# svy_files  <- list.files(CACHE_SVY_DIR, pattern = '[.]fst')  
# survey_ids <- gsub("\\.fst", "", svy_files)
# survey_ids <- survey_ids[1:40]
# 
# dl_svy_data <- 
#   load_cached_survey_data(
#     survey_id     = survey_ids,
#     cache_svy_dir = CACHE_SVY_DIR)


dl_svy_data <- 
  load_cached_survey_data(
    survey_id     = NULL,
    cache_svy_dir = CACHE_SVY_DIR)


# Filter for surveys not in PFW 
dl_svy_data <- 
  dl_svy_data[names(dl_svy_data) %in% pfw_svy_ids]

# Pipeline survey ids
survey_ids <- names(dl_svy_data)

# Filter against PFW / pipeline_inventory


# ---- Step 4: Calculate survey means ---- 

# Open workers 
# if (PIP_SAFE_WORKERS) open_workers()
open_workers()

# Fetch GD survey means and convert them to daily values
dl_gd_means <-
  purrr::map(survey_ids, function(x) {
    x <- dl_aux$gdm[survey_id == x][['survey_mean_lcu']]
    x <- x * (12/365) # Convert to daily values (PCN is in monthly)
    return(x)
  })

# Calculate LCU survey mean 
dl_svy_mean_lcu <-
  future.apply::future_Map(
    f = pipdm::db_compute_survey_mean,
    dl_svy_data, 
    dl_gd_means,
    future.packages = c('data.table', 'pipdm', 'wbpip'),
    future.seed     = NULL)

names(dl_svy_mean_lcu) <- names(dl_svy_data)

# Remove surveys that failed 
dl_svy_mean_lcu <- 
  dl_svy_mean_lcu[lengths(dl_svy_mean_lcu) != 0]

dl_svy_mean_lcu <- 
  dl_svy_mean_lcu[purrr::map_lgl(dl_svy_mean_lcu,
                                 function(x) !any(is.na(x$survey_mean_lcu))
                                 )]

# Remove survey data where the LCU mean calculation failed
dl_svy_data_w_mean <- 
  dl_svy_data[
  names(dl_svy_data) %in% names(dl_svy_mean_lcu)
  ]
rm(dl_svy_data)

# Run garbage collection 
if (APPLY_GC) gc()

# Close workers
if (PIP_SAFE_WORKERS) close_workers()


# ---- Step 5: Calculate Lorenz curves ---- 

# Open workers 
if (PIP_SAFE_WORKERS) open_workers()

# Calculate Lorenz curves (for microdata)
dl_lorenz <-
  future.apply::future_lapply(
    dl_svy_data_w_mean,
    FUN             = pipdm::db_compute_lorenz,
    future.packages = c('data.table', 'pipdm'),
    future.seed     = NULL)

# Add names to list 
names(dl_lorenz) <- names(dl_svy_data_w_mean)

# Remove surveys where the Lorenz calculation failed 
dl_lorenz <- 
  dl_lorenz[lengths(dl_lorenz) != 0]

# Close workers
if (PIP_SAFE_WORKERS) close_workers()


# ---- Step 6: Calculate dist stats ---- 

# Open workers 
if (PIP_SAFE_WORKERS) open_workers()

# Make sure list are in the same order 
dl_svy_data_w_mean <-
  dl_svy_data_w_mean[order(names(dl_svy_data_w_mean))]

dl_svy_mean_lcu <-
  dl_svy_mean_lcu[order(names(dl_svy_mean_lcu))]

# Get mean (used for grouped data)
dl_mean <- 
  purrr::map(dl_svy_mean_lcu,
             function(x) x$survey_mean_lcu)

# Calculate distributional statistics
dl_dist_stats <-
  future.apply::future_Map(
    f               = pipdm::db_compute_dist_stats,
    dt              = dl_svy_data_w_mean,
    mean            = dl_mean,
    future.packages = c('data.table', 'pipdm', 'wbpip'),
    future.seed     = NULL
  )

names(dl_dist_stats) <- names(dl_mean)
dl_dist_stats <- 
  dl_dist_stats[lengths(dl_dist_stats) != 0]

# Close workers
if (PIP_SAFE_WORKERS) close_workers()


# ---- Step 7: Create LCU table ---- 

# Make sure survey year is numeric
# Why is it converted to character? 
dl_svy_mean_lcu <-
  purrr::map(dl_svy_mean_lcu, function(dt) {
    dt$survey_year <- as.numeric(dt$survey_year)
    return(dt)
  })

# Create new LCU table 
dt_svy_mean_lcu <- 
  pipdm::db_create_lcu_table(
    dl_svy_mean_lcu, 
    pop_table = dl_aux$pop,
    pfw_table = dl_aux$pfw) 

# Remove surveys with missing LCU means 
# (This should be removed once validation checks
# are implemented.)
dt_svy_mean_lcu <- 
  dt_svy_mean_lcu[!is.na(survey_mean_lcu)]


# ---- Step 8: Create DSM table ---- 

# Create survey mean table with deflated survey means
# for each survey and pop_data_level. 
# 
# Due to a change in naming convention this table is 
# saved as survey-means.qs in PIP_PIPE_DIR/aux_data/.

# Create survey mean table 
dt_svy_mean_ppp <-
  pipdm::db_create_dsm_table(
    lcu_table = dt_svy_mean_lcu,
    cpi_table = dl_aux$cpi, 
    ppp_table = dl_aux$ppp)


# ---- Step 9: Create dist stat table ----  

# Covert dist stat list to table
dt_dist_stats <- 
  pipdm::db_create_dist_table(
    dl_dist_stats, 
    survey_id = names(dl_dist_stats),
    dsm_table = dt_svy_mean_ppp)


# ---- Step 10: Create refyear table ---- 

# Create a reference year table with predicted means 
# for each year in PIP_REF_YEARS. 
# 
# Due to a change in naming convention this table is 
# saved as interpolated-means.qs in 
# PIP_PIPE_DIR/aux_data/.

dt_ref_mean_pred <-
  pipdm::db_create_ref_year_table(
    gdp_table   = dl_aux$gdp,
    pce_table   = dl_aux$pce,
    pop_table   = dl_aux$pop,
    pfw_table   = dl_aux$pfw,
    dsm_table   = dt_svy_mean_ppp,
    ref_years   = PIP_REF_YEARS,
    pip_years   = PIP_YEARS,
    region_code = 'pcn_region_code')


# ---- Step 11: Create coverage table ---- 

# Create coverage table by region 
dt_coverage <- 
  pipdm::db_create_coverage_table(
    ref_year_table    = dt_ref_mean_pred,
    pop_table         = dl_aux$pop,
    ref_years         = PIP_REF_YEARS,
    special_countries = c("ARG", "CHN", "IDN", "IND"),
    digits            = 2
  )


# ---- Step 12: Create aggregated POP table ----

dt_pop_region <- 
  pipdm::db_create_reg_pop_table(
    pop_table   = dl_aux$pop, 
    cl_table    = dl_aux$country_list, 
    region_code = 'pcn_region_code',
    pip_years   = PIP_YEARS)


# ---- Step 13: Clean AUX data ---- 

# Clean and transform the AUX tables to the format
# used on the PIP webpage. 

# Remove PFW, GDM and CL tables
dl_aux$gdm <- NULL
dl_aux$pfw <- NULL
dl_aux$country_list <- NULL

# Clean and transform AUX tables 
dl_aux_clean <- 
  purrr::map2(dl_aux, names(dl_aux), 
              .f = pipdm::db_clean_aux,
              pip_years = PIP_YEARS)


# ---- Step 14: Save output files ----

# Save survey data to drive (in .fst format)
save_survey_data(
  dl          = dl_svy_data_w_mean, 
  output_dir  = OUT_SVY_DIR,
  cols        = c('welfare', 'weight', 'area'),
  compress    = FST_COMP_LVL, 
  future_plan = 'multisession', 
  new_names   = names(dl_svy_data_w_mean))

# Save aux data
aux_out_path <-  
  sprintf('%s%s.fst',
          OUT_AUX_DIR,
          names(dl_aux_clean))
purrr::map2(
  dl_aux_clean, aux_out_path, function(x, y) {
    fst::write_fst(x, path = y, compress = FST_COMP_LVL)
  }) %>% 
  invisible()

# Save POP region table
fst::write_fst(
  dt_pop_region, 
  path = sprintf('%spop-region.fst', OUT_AUX_DIR),
  compress = FST_COMP_LVL)

# Save coverage table
fst::write_fst(
  dt_coverage, 
  path = sprintf('%scoverage.fst', OUT_AUX_DIR),
  compress = FST_COMP_LVL)

# Save LCU table
fst::write_fst(
  dt_svy_mean_lcu, 
  path = sprintf('%slcu.fst', OUT_AUX_DIR),
  compress = FST_COMP_LVL)

# Save Lorenz list 
saveRDS(
  dl_lorenz, 
  file = sprintf('%slorenz.RDS', OUT_AUX_DIR),
  compress = TRUE)

# Save dist stats table
fst::write_fst(
  dt_dist_stats, 
  path = sprintf('%sdist-stats.fst', OUT_AUX_DIR),
  compress = FST_COMP_LVL)

# Save survey means table
fst::write_fst(
  dt_svy_mean_ppp, 
  path = sprintf('%ssurvey-mean.fst', OUT_AUX_DIR),
  compress = FST_COMP_LVL)

# Save interpolated means table 
fst::write_fst(
  dt_ref_mean_pred, 
  path = sprintf('%sinterpolated-mean.fst', OUT_AUX_DIR),
  compress = FST_COMP_LVL)


# ---- Step 15: Done ----

# Done! 
cat('Done. Code is poetry.')
