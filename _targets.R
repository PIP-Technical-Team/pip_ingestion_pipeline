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


# Set-up global variables

# pipload::add_gls_to_env(vintage = "20220408")

# pipload::add_gls_to_env(vintage = "new",
#                         out_dir = fs::path("y:/pip_ingestion_pipeline/temp/"))
# 
# Check that the correct _targets store is used 

tdir <- "//w1wbgencifs01/pip/pip_ingestion_pipeline"
if (identical(fs::path(tar_config_get('store')),
             fs::path(tdir, 'pc_data/_targets'))
    ) {
  stop('The store specified in _targets.yaml doesn\'t match with the pipeline directory')
}


# Set future plan (for targets::tar_make_future)
# plan(multisession)

# ---- Step 1: Prepare data ----


gdm <- data.frame(a = 1, b  = 2)



# ---- Step 2: Run pipeline -----

list(
  
  ## LCU survey means ---- 
  # tar_target(cache, cache_o, iteration = "list"),
  
  ### Fetch GD survey means and convert them to daily values ----
  tar_target(
    gd_means, 
    gdm)
)

