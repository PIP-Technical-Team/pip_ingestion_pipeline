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

gls <- pipload::pip_create_globals(
  root_dir   = Sys.getenv("PIP_ROOT_DIR"), 
  out_dir    = fs::path("y:/pip_ingestion_pipeline/temp/"),
  vintage    = c("new", "test"), 
  create_dir = TRUE
  )

# pipload::add_gls_to_env(vintage = "20220408")

# pipload::add_gls_to_env(vintage = "new",
#                         out_dir = fs::path("y:/pip_ingestion_pipeline/temp/"))
# 
# Check that the correct _targets store is used 
if (identical(tar_config_get('store'),
              paste0(gls$PIP_PIPE_DIR, 'pc_data/_targets/'))
    ) {
  stop('The store specified in _targets.yaml doesn\'t match with the pipeline directory')
}

# Set targets options 
tar_option_set(
  garbage_collection = TRUE,
  memory = 'transient',
  format = 'qs', #'fst_dt',
  imports  = c('pipload',
               'wbpip'), 
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

gdm <- as.data.frame(dl_aux$gdm)

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

