# source("renv/activate.R")
# ---- RENV ---- 

library(targets)
library(tarchetypes)
# ---- MISCELLANEOUS ---- 

# Number of future() worker sessions
PIP_N_WORKERS = length(future::availableWorkers()) - 1 # Number of cores minus 1.  

# Compression algorithm 
PIP_COMPRESSION = 'ztsd' # Used for both .qs and .parquet output files.

# # Compression level 
PIP_COMPRESSION_LVL = 22 # Used for both .qs and .parquet output files. 

# Maximum number of new surveys 
PIP_N_MAX = 1000L # Maximum number of new surveys handled by the pipeline. Change with caution. 

# ---- FLAGS ---- 

# Re-run everything 
PIP_RUN_ALL = FALSE # Ignore already parsed survey data.

# Update raw inventory file
PIP_UPDATE_INVENTORY = FALSE # Update PIP_DATA_DIR/_inventory/inventory.fst. 

# # Update auxiliary data
PIP_UPDATE_AUX = FALSE  # Update all aux files in PIP_DATA_DIR/_aux/<...>. 

# Update grouped data LCU means 
PIP_UPDATE_GD_MEANS = FALSE # Update PIP_PIPE_DIR/aux_data/gd-mean.qs (read from PCN Masterfile). 

# Update WDI country-region metadata
PIP_UPDATE_WDI_META = FALSE # Update PIP_PIPE_DIR/aux_data/wdi-meta.qs (query wbstats). 

# Apply garbage collection 
PIP_GC_MILD = TRUE # Apply gc() after each future call. Always recommended. 
PIP_GC_TORTURE = FALSE # Apply gc() _within_ all pipdm future calls. Recommended for large updates. 

# Apply "safe" worker sessions 
PIP_SAFE_WORKERS = FALSE # Start/close workers after each future call. Recommended for large updates. 
