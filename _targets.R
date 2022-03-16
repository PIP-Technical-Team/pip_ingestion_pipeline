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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Select PPP year --------

py <- 2017

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
                         & adaptation_version == m_av]




## Load PIP inventory ----
pip_inventory <- 
  pipload::pip_find_data(
    inv_file = fs::path(gls$PIP_DATA_DIR, '_inventory/inventory.fst'),
    filter_to_pc = TRUE,
    maindir = gls$PIP_DATA_DIR)

# pip_inventory <- 
#   pipload::pip_find_data(filter_to_pc = TRUE)



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
    verbose            = FALSE,
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
                      gls       = gls)
 
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
  )
)

