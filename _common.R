
branch <- "main"
branch <- "DEV"

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Load globals   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

gls <- pipfun::pip_create_globals(
  root_dir   = Sys.getenv("PIP_ROOT_DIR"), 
  # out_dir    = fs::path("y:/pip_ingestion_pipeline/temp/"),
  vintage    = list(release = "20230626", 
                    ppp_year = py, 
                    identity = "TEST"), 
  create_dir = TRUE
)


# to delete and modify in pipfun code
gls$FST_COMP_LVL <- 20

cli::cli_text("Vintage directory {.file {gls$vintage_dir}}")

# pipload::add_gls_to_env(vintage = "20220408")

# pipload::add_gls_to_env(vintage = "new",
#                         out_dir = fs::path("y:/pip_ingestion_pipeline/temp/"))
# 

# Set targets options 
tar_option_set(
  garbage_collection = TRUE,
  memory = 'transient',
  format = 'qs', #'fst_dt',
  workspace_on_error = TRUE, 
  error = "stop"  # or "null"
)


# make sure joyn does not display messages
options(joyn.verbose = FALSE, 
        pipload.verbose = FALSE) 



# Set future plan (for targets::tar_make_future)
# plan(multisession)

# ---- Step 1: Prepare data ----

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##  AUX data -----
aux_tb <- prep_aux_data(maindir = gls$PIP_DATA_DIR)
# filter 
aux_tb <- aux_tb[!(auxname %chin% c("maddison"))]

aux_ver <- rep("00", length(aux_tb$auxname))

# aux_ver[which(aux_tb$auxname == "cpi")] <- -1 # remove for march update

dl_aux <- purrr::map2(.x = aux_tb$auxname,
                      .y =  aux_ver,
                      .f = ~ {
                        pipload::pip_load_aux(
                          measure     = .x, 
                          apply_label = FALSE,
                          maindir     = gls$PIP_DATA_DIR, 
                          verbose     = FALSE, 
                          version     = .y, 
                          branch      = branch)
                      }
)

names(dl_aux) <- aux_tb$auxname    

aux_versions <- purrr::map_df(aux_tb$auxname, ~{
  y <- attr(dl_aux[[.x]], "version")
  w <- data.table(aux = .x, 
                  version = y)
  w
})


# temporal change. 
dl_aux$pop$year <- as.numeric(dl_aux$pop$year)


### Select PPP year --------

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


### Select the right CPI --------

cpivar <- paste0("cpi", py)

dl_aux$cpi[, cpi := get(cpivar)]

### Select right Poverty lines table ------

dl_aux$pl <- dl_aux$pl[ppp_year == py
][, 
  ppp_year := NULL]

### Select right Country Profile ------


dl_aux$cp <-
  lapply(dl_aux$cp,
         \(.) { # for each list *key indicators and charts
           lapply(.,
                  \(x) { # for each table inside each list
                    if ("ppp_year" %in% names(x)) {
                      x <-
                        x[ppp_year == py][,
                                          ppp_year := NULL]
                    }
                    x
                  })
         })

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
##  PIP inventory ----
pip_inventory <- 
  pipload::pip_find_data(
    inv_file = fs::path(gls$PIP_DATA_DIR, '_inventory/inventory.fst'),
    filter_to_pc = TRUE,
    maindir = gls$PIP_DATA_DIR)


### pipeline inventory ----

pipeline_inventory <- 
  db_filter_inventory(dt        = pip_inventory,
                      pfw_table = dl_aux$pfw)

### Filter pipline inventory ---- 

# pipeline_inventory <-
#   pipeline_inventory[grepl("^SOM", cache_id)]
# 

# pipeline_inventory <-
#   pipeline_inventory[grepl("^IND_201[5-9]", cache_id)]

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
#    pipeline_inventory[surveyid_year >= 2021]

# cts_filter <- c("CHN")
# pipeline_inventory <-
#    pipeline_inventory[country_code == 'TWN' 
#                       & surveyid_year >= 2022]
