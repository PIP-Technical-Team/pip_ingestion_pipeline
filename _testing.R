


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1 ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Select Defaults ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

py                 <- 2017  # PPP year
branch             <- "main"
branch             <- "DEV"
release            <- "20240326"
release            <- "20240429"
identity           <- "PROD"
identity           <- "INT"
max_year_country   <- 2022
max_year_aggregate <- 2022

force_create_cache_file         <- FALSE
save_pip_update_cache_inventory <- FALSE
force_gd_2_synth                <- TRUE
save_mp_cache                   <- FALSE


## Start up ----
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Load packages

# Load R files
purrr::walk(fs::dir_ls(path = "./R", 
                       regexp = "\\.R$"), source)

# Read pipdm functions
purrr::walk(fs::dir_ls(path = "./R/pipdm/R", 
                       regexp = "\\.R$"), source)


## Run common R code   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


fs::path("_common.R") |> 
  source(echo = FALSE)

# check synth creation for a particular country/year

cts <- NULL
yrs <- NULL

cts <- "IDN"
yrs <- 1984


# 
fs::path("_cache_loading_saving.R") |>
  source(echo = FALSE)

