# #### ---- NOTE ---- #### 
# 
# * Pipeline settings can be edited in .Rprofile
# * Internal pipeline functions can be edited in R/_common.R
# * Make sure packages are up to date by running renv::status()

# Start up ----------------------------------------------------------------

# Clean workspace
rm(list = ls())

# Load project specific .Rprofile
if (file.exists('.Rprofile')) {
  base::sys.source('.Rprofile', envir = environment())
} else {
  rlang::abort('Project .Rprofile not found.')
}

# Load functions 
source('R/_common.R')

# Load packages 

# PIP packages 
library(wbpip)
library(pipdm)
library(pipload)
library(pipaux)

# Future packages (for parallel processing)
library(future)
library(future.apply)
library(future.callr)

# File storage packages
suppressPackageStartupMessages(library(qs))
suppressPackageStartupMessages(library(arrow))

# Miscellaneous packages
library(wbstats)
library(magrittr)
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(purrr))


# Update AUX --------------------------------------------------------------

# Update all auxiliary datasets in PIP_DATA_DIR.
#
# The AUX tables are usually updated prior to 
# running the pipeline, but the PIP_UPDATE_AUX
# flag can be used to make sure that this is 
# checked. 
# 
# Running pipaux::pip_update_all_aux() with 
# force = FALSE will check the signature hash
# for all tables, but only overwrite files 
# with any changes. 
# 
# Files will be saved to PIP_DATA_DIR/_aux/<...>.  

# Update AUX tables
if (PIP_UPDATE_AUX) {
  pipaux::pip_update_all_aux(
    maindir = PIP_DATA_DIR, force = FALSE)
}

# Update inventory  -------------------------------------------------------

# Update inventory table of surveys in PIP_DATA_DIR.
#
# The PIP inventory will usually be updated prior 
# to running the pipeline, but setting 
# PIP_UPDATE_INVENTORY to TRUE can be used to 
# make sure that this is checked. 
# 
# Note that inventory.fst MUST be updated if new 
# surveys have been added to PIP_DATA_DIR at 
# the pipdp stage. 
# 
# The output file will be saved to 
# PIP_DATA_DIR/_inventory/inventory.fst. 
# 

# Update raw inventory file
if (PIP_UPDATE_INVENTORY) {
  pipload::pip_update_inventory(
    maindir = PIP_DATA_DIR, force = FALSE)
}


# Load current data -------------------------------------------------------

# Load existing datasets in PIP_PIPE_DIR. 
# 
# Load existing LCU, Distributional statistics and 
# Lorenz datasets from PIP_PIPE_DIR, as well as
# a vector with previously completed survey ids. 
# 
# If PIP_RUN_ALL = TRUE empty (NULL) vectors will 
# be created as placeholders. 

if (PIP_RUN_ALL) {
  # Create empty values when parsing all surveys from scratch 
  dt_current_lcu_table <- NULL
  dt_current_dist_table <- NULL 
  dl_current_lorenz <- NULL
  existing_svy_ids <-  NULL
} else {
  # Else load existing tables and vectors 
  dt_current_lcu_table <-
    qs::qread(sprintf('%s/pc_data/aux_data/lcu.qs', PIP_PIPE_DIR))
  dt_current_dist_table <-
    qs::qread(sprintf('%s/pc_data/aux_data/dist-stats.qs', PIP_PIPE_DIR))
  dl_current_lorenz <-
    qs::qread(sprintf('%s/pc_data/aux_data/lorenz.qs', PIP_PIPE_DIR))
  existing_svy_ids <-
    qs::qread(sprintf('%s/pc_data/aux_data/survey-ids.qs', PIP_PIPE_DIR))
  
}


# Load AUX data -----------------------------------------------------------

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

# Initialize future plan 
# plan('callr')

# Create vector with paths to all AUX input datasets 
aux_indicators <- 
  c('gdp', 'pce', 'pop', 'ppp', 'cpi', 'pfw')
aux_files <-
  sprintf('%1$s_aux/%2$s/%2$s.fst',
          PIP_DATA_DIR, aux_indicators)

# Read AUX data into list 
dl_aux <- 
  future.apply::future_lapply(
    aux_files, future.seed = NULL, function(x) 
      suppressMessages(pipload::pip_load_aux(
        file_to_load = x, apply_label = FALSE)))
names(dl_aux) <- aux_indicators

# Clean up 
rm(aux_indicators, aux_files)

# Run garbage collection 
if (PIP_GC_MILD) gc()

# Load inventory ----------------------------------------------------------

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
pipeline_inventory <- 
  pipload::pip_find_data(
    inv_file = paste0(PIP_DATA_DIR, '_inventory/inventory.fst'),
    filter_to_pc = TRUE,
    maindir = PIP_DATA_DIR)

# Select surveys in PFW 
pipeline_inventory <- 
  pipdm::db_filter_inventory(
    pipeline_inventory,
    pfw_table = dl_aux$pfw)


# Load grouped data means -------------------------------------------------

# Load a table with grouped data means from the 
# PCN Masterfile (SurveyMean sheet).
#
# This is needed since means can't be calculated 
# for grouped data. Once PCN goes out of production 
# the Masterfile should be replaced by a PIP specific
# file. 
#
# If PIP_UPDATE_GD_MEANS = TRUE the table will also
# be updated and saved to the PIP_PIPE_DIR. This is 
# only necessary if there has been changes to the 
# grouped data surveys in the SurveyMean sheet. 

if (PIP_UPDATE_GD_MEANS) {
  # Update grouped data means table 
  
  # Get list of files
  m_files <- list.files(PCN_MASTER_DIR, 
                        pattern = 'Master_2021[0-9]{10}.xlsx')
  
  # Find latest masterfile
  pcn_master_path <- m_files %>%
    gsub('Master_|.xlsx', '', .) %>%
    as.POSIXlt(format = '%Y%m%d%H%M%S') %>%
    max(na.rm = TRUE) %>%
    as.character() %>%
    gsub('-|:| ', '', .) %>%
    sprintf('%s/Master_%s.xlsx', PCN_MASTER_DIR, .)
  
  # Create GD survey mean table for PIP 
  dt_gd_svy_mean <- 
    pipdm::db_create_gd_svy_mean_table(
      pcn_master_path, 
      pfw_table = dl_aux$pfw,
      inventory = pipeline_inventory)
  
  # Save to disk
  qs::qsave(dt_gd_svy_mean, 
            file = sprintf('%spc_data/aux_data/gd-mean.qs', PIP_PIPE_DIR),
            algorithm = PIP_COMPRESSION,
            compress_level = PIP_COMPRESSION_LVL)
  
  rm(m_files, pcn_master_path)
  
} else {
  
  # Else load existing table 
  dt_gd_svy_mean <-
    qs::qread(sprintf('%spc_data/aux_data/gd-mean.qs', 
                      PIP_PIPE_DIR))
  
}

# Remove PCN surveys not in PIP_DATA_DIR 
dt_gd_svy_mean <- 
  dt_gd_svy_mean[!is.na(survey_id)]


# Load WDI metadata -------------------------------------------------------

# Load WDI country-region metadata. 
#
# This is currently used for creating the
# regional population table. 
# See also PIP-Technical-Team/TMP_pipeline#18 

if (PIP_UPDATE_WDI_META) {
  dt_wdi_meta <- wbstats::wb_countries() %>% setDT()
  qs::qsave(dt_wdi_meta, file =
              sprintf('%spc_data/aux_data/wdi-meta.qs',
                      PIP_PIPE_DIR) )
} else {
  dt_wdi_meta <- 
    qs::qread(sprintf('%s/pc_data/aux_data/wdi-meta.qs', 
                      PIP_PIPE_DIR))
}


# New survey ids ----------------------------------------------------------

# Create a vector of new survey ids to parsed through 
# the pipeline, based on the file names in the 
# pipeline_inventory and the existing_svy_ids.  
# 
# The maximum number of surveys allowed in a single
# run is determined by PIP_N_MAX. The purpose of this 
# setting is to avoid that the server runs out of 
# memory. 

# Create vector w/ full survey paths
svy_ids <- sub('[.]dta', '', pipeline_inventory$filename)

# Filter for surveys already parsed 
new_svy_ids <- 
  svy_ids[!svy_ids %in% existing_svy_ids]
new_svy_ids <- sort(new_svy_ids)
if (length(new_svy_ids) > PIP_N_MAX) {
  new_svy_ids <- new_svy_ids[sample(length(new_svy_ids), PIP_N_MAX)]
  rlang::warn(
    sprintf('The number of new survey ids is larger than PIP_N_MAX.\nSelecting a sample of %s surveys.', 
            PIP_N_MAX))
}
# new_svy_ids <- new_svy_ids[sample(length(new_svy_ids), 5)]
# new_svy_ids <- new_svy_ids[grepl('GROUP', new_svy_ids)]
# new_svy_ids <- new_svy_ids[grepl('IND_1987|IND_1993', new_svy_ids)]


# Load survey data --------------------------------------------------------

# Initialize future plan 
plan('callr')
# open_workers()

# Load .dta survey data files from PIP_DATA_DIR
dl_svy_data_raw <- 
  future.apply::future_lapply(
    new_svy_ids, future.seed = NULL, function(x) {
      pipload::pip_load_data(survey_id = x, 
                             maindir = PIP_DATA_DIR, 
                             noisy = FALSE)
    })

# Add names to list 
names(dl_svy_data_raw) <- new_svy_ids

# Remove surveys that failed 
dl_svy_data_raw <-
  dl_svy_data_raw[lengths(dl_svy_data_raw) != 0]


# Clean survey data -------------------------------------------------------

# Initialize future plan
open_workers()

# n <- PIP_N_MAX / 2

# Clean survey data
if (length(new_svy_ids) <= 1000L) {
  
  dl_svy_data_clean <-
    future.apply::future_lapply(
      dl_svy_data_raw, function(x) {
        pipdm::db_clean_data(x)
      }
    )
  
} else if (length(new_svy_ids) > 1000L) {
  
  # Run first 1000 surveys 
  dl_svy_data_clean_1 <-
    future.apply::future_lapply(
      dl_svy_data_raw[1:1000], function(x) {
        pipdm::db_clean_data(x) 
      }
    )
  
  # Close workers 
  close_workers()
  
  # Re-open workers 
  open_workers()
  
  # Run the rest 
  dl_svy_data_clean_2 <-
    future.apply::future_lapply(
      dl_svy_data_raw[1001:length(dl_svy_data_raw)], function(x) {
        pipdm::db_clean_data(x)
      }
    )
  
  # Append 
  dl_svy_data_clean <-
    append(dl_svy_data_clean_1, dl_svy_data_clean_2)
  dl_svy_data_clean <-
    dl_svy_data_clean[order(names(dl_svy_data_clean))]
  
  rm(dl_svy_data_clean_1, dl_svy_data_clean_2)
  
}

# Add names to list 
names(dl_svy_data_clean) <- names(dl_svy_data_raw)

# Remove surveys where the cleaning failed 
dl_svy_data_clean <- 
  dl_svy_data_clean[lengths(dl_svy_data_clean) != 0]

# Clean up
rm(dl_svy_data_raw)

# Run garbage collection 
if (PIP_GC_MILD) gc()

# Close workers
if (PIP_SAFE_WORKERS) close_workers()


# Calculate survey means --------------------------------------------------

# Open workers 
if (PIP_SAFE_WORKERS) open_workers()

# Fetch GD survey means 
dl_gd_means <-
  purrr::map(new_svy_ids, function(x) {
    x <- dt_gd_svy_mean[survey_id == x][['survey_mean_lcu']]
    x <- x * (12/365) # Convert to daily values (PCN is in monthly)
    return(x)
  })

# Calculate LCU survey mean 
dl_svy_mean_lcu <-
  future.apply::future_Map(
    f = pipdm::db_compute_survey_mean,
    dl_svy_data_clean, dl_gd_means,
    # gc = PIP_GC_TORTURE, 
    future.packages = c('data.table', 'pipdm', 'wbpip'),
    future.seed = NULL)
names(dl_svy_mean_lcu) <- names(dl_svy_data_clean)
dl_svy_mean_lcu <- 
  dl_svy_mean_lcu[lengths(dl_svy_mean_lcu) != 0]
dl_svy_mean_lcu <- dl_svy_mean_lcu[
  purrr::map_lgl(dl_svy_mean_lcu, 
                 function(x) !any(is.na(x$survey_mean_lcu)))]

# Remove survey data where the LCU mean calculation failed
dl_svy_data_w_mean <- dl_svy_data_clean[
  names(dl_svy_data_clean) %in% 
    names(dl_svy_mean_lcu)]
rm(dl_svy_data_clean)

# Run garbage collection 
if (PIP_GC_MILD) gc()

# Close workers
if (PIP_SAFE_WORKERS) close_workers()


# Calculate Lorenz curves -------------------------------------------------

# Open workers 
if (PIP_SAFE_WORKERS) open_workers()

# Calculate Lorenz curves (for microdata)
dl_lorenz <-
  future.apply::future_lapply(
    dl_svy_data_w_mean,
    FUN = pipdm::db_compute_lorenz,
    # gc = PIP_GC_TORTURE, 
    future.packages = c('data.table', 'pipdm'),
    future.seed = NULL)

# Add names to list 
names(dl_lorenz) <- names(dl_svy_data_w_mean)

# Remove surveys where the Lorenz calculation failed 
dl_lorenz <- 
  dl_lorenz[lengths(dl_lorenz) != 0]

# Append with current data
dl_lorenz <-
  append(dl_current_lorenz, dl_lorenz)
dl_lorenz <-
  dl_lorenz[order(names(dl_lorenz))]

# Close workers
if (PIP_SAFE_WORKERS) close_workers()


# Calculate distributional stats ------------------------------------------

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
    f = pipdm::db_compute_dist_stats,
    dt = dl_svy_data_w_mean,
    mean = dl_mean,
    # gc = PIP_GC_TORTURE,
    future.packages = c('data.table', 'pipdm', 'wbpip'),
    future.seed = NULL
  )
names(dl_dist_stats) <- names(dl_mean)
dl_dist_stats <- 
  dl_dist_stats[lengths(dl_dist_stats) != 0]

# Close workers
if (PIP_SAFE_WORKERS) close_workers()


# Create LCU table --------------------------------------------------------

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

# Bind together with current LCU table  
dt_svy_mean_lcu <- 
  rbind(dt_svy_mean_lcu, 
        dt_current_lcu_table)

# Remove surveys not in PFW 
dt_svy_mean_lcu <- 
  dt_svy_mean_lcu[dt_svy_mean_lcu$survey_id
                  %in% svy_ids,]

# Remove surveys with missing LCU means 
# (This should be removed once validation checks
# are implemented.)
dt_svy_mean_lcu <- 
  dt_svy_mean_lcu[!is.na(survey_mean_lcu)]


# Create DSM table --------------------------------------------------------

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

# Temporary quick fix for is_used_for_aggregation column,
# see issue PIP-Technical-Team/TMP_pipeline#14 
dt_svy_mean_ppp$is_used_for_aggregation <- 
  ifelse(dt_svy_mean_ppp$pop_data_level != 'national',
         TRUE, FALSE)

# Create dist stat table --------------------------------------------------

# Covert dist stat list to table
dt_dist_stats <- 
  pipdm::db_create_dist_table(
    dl_dist_stats, 
    survey_id = names(dl_dist_stats),
    dsm_table = dt_svy_mean_ppp)

# Bind together with current dist stat table  
dt_dist_stats <- 
  rbind(dt_dist_stats, 
        dt_current_dist_table)

# Remove surveys not in PFW 
dt_dist_stats <- 
  dt_dist_stats[dt_dist_stats$survey_id
                %in% svy_ids,]


# Create reference year table  --------------------------------------------

# Create a reference year table with predicted means 
# for each year in PIP_REF_YEARS. 
# 
# Due to a change in naming convention this table is 
# saved as interpolated-means.qs in 
# PIP_PIPE_DIR/aux_data/.

dt_ref_mean_pred <-
  pipdm::db_create_ref_year_table(
    gdp_table = dl_aux$gdp,
    pce_table = dl_aux$pce,
    pop_table = dl_aux$pop,
    pfw_table = dl_aux$pfw,
    dsm_table = dt_svy_mean_ppp,
    ref_years = PIP_REF_YEARS,
    pip_years = PIP_YEARS)

# Temporary quick fix for is_used_for_aggregation column,
# see issue PIP-Technical-Team/TMP_pipeline#14 
dt_ref_mean_pred$is_used_for_aggregation <- 
  ifelse(dt_ref_mean_pred$pop_data_level != 'national',
         TRUE, FALSE)


# Create metadata table -------------------------------------

# Create a metadata table
dt_metadata <-
  pipdm::db_create_metadata_table(
    pfw_table = dl_aux$pfw,
    lcu_table = dt_svy_mean_lcu
  )


# Create aggregated pop table ---------------------------------------------

dt_pop_region <- 
  pipdm::db_create_reg_pop_table(
    pop_table = dl_aux$pop,
    wb_meta = dt_wdi_meta,
    pip_years = PIP_YEARS)


# Clean AUX data ----------------------------------------------------------

# Clean and transform the AUX tables to the format
# used on the PIP webpage. 

# Remove PFW table from list 
dl_aux$pfw <- NULL

# Clean and transform AUX tables 
dl_aux_clean <- 
  purrr::map2(dl_aux, names(dl_aux), .f = pipdm::db_clean_aux,
              pip_years = PIP_YEARS)

# Clean up
rm(dl_aux)

# Run garbage collection 
if (PIP_GC_MILD) gc()

# Save files --------------------------------------------------------------

# Open workers 
if (PIP_SAFE_WORKERS) open_workers()

# Save survey data to drive (in .parquet format)
svy_out_paths <- 
  sprintf('%spc_data/survey_data/%s.%s.parquet',
          PIP_PIPE_DIR,
          names(dl_svy_data_w_mean),
          PIP_COMPRESSION)
# future.apply::future_Map(
#   function(
#     x, y,
#     compression = PIP_COMPRESSION,
#     compression_level = PIP_COMPRESSION_LVL) {
#     arrow::write_parquet(
#       x[, c('welfare', 'weight', 'area')],
#       sink = y)
#     },
#   dl_svy_data_w_mean, svy_out_paths,
#   future.seed = NULL) %>% invisible()

# Save aux data
aux_out_path <-  
  sprintf('%spc_data/aux_data/%s.qs',
          PIP_PIPE_DIR,
          names(dl_aux_clean))
future.apply::future_Map(
  function(x, y) {
    qs::qsave(x, file = y,
              algorithm = PIP_COMPRESSION,
              compress_level = PIP_COMPRESSION_LVL,
    )},
  dl_aux_clean, aux_out_path,
  future.seed = NULL) %>% invisible()

# Close workers
if (PIP_SAFE_WORKERS) close_workers()

# Save POP region table
qs::qsave(dt_pop_region, 
          file = sprintf('%spc_data/aux_data/pop-region.qs', PIP_PIPE_DIR),
          algorithm = PIP_COMPRESSION,
          compress_level = PIP_COMPRESSION_LVL)

# Save LCU table
qs::qsave(dt_svy_mean_lcu, 
          file = sprintf('%spc_data/aux_data/lcu.qs', PIP_PIPE_DIR),
          algorithm = PIP_COMPRESSION,
          compress_level = PIP_COMPRESSION_LVL)

# Save Lorenz list 
qs::qsave(dl_lorenz, 
          file = sprintf('%spc_data/aux_data/lorenz.qs', PIP_PIPE_DIR),
          algorithm = PIP_COMPRESSION,
          compress_level = PIP_COMPRESSION_LVL)

# Save dist stats table
qs::qsave(dt_dist_stats, 
          file = sprintf('%spc_data/aux_data/dist-stats.qs', PIP_PIPE_DIR),
          algorithm = PIP_COMPRESSION,
          compress_level = PIP_COMPRESSION_LVL)

# Save survey means table
qs::qsave(dt_svy_mean_ppp, 
          file = sprintf('%spc_data/aux_data/survey-mean.qs', PIP_PIPE_DIR),
          algorithm = PIP_COMPRESSION,
          compress_level = PIP_COMPRESSION_LVL)

# Save interpolated means table 
qs::qsave(dt_ref_mean_pred, 
          file = sprintf('%spc_data/aux_data/interpolated-mean.qs', PIP_PIPE_DIR),
          algorithm = PIP_COMPRESSION,
          compress_level = PIP_COMPRESSION_LVL)

# Save metadata table 
qs::qsave(dt_metadata,
          file = sprintf('%spc_data/aux_data/metadata.qs', PIP_PIPE_DIR),
          algorithm = PIP_COMPRESSION,
          compress_level = PIP_COMPRESSION_LVL)

# Save survey ids 
ids <- sort(c(existing_svy_ids, names(dl_svy_data_w_mean)))
qs::qsave(ids, 
          file = sprintf('%spc_data/aux_data/survey-ids.qs', PIP_PIPE_DIR),
          algorithm = PIP_COMPRESSION)

# Done! 
cat('Done. Code is poetry.')
