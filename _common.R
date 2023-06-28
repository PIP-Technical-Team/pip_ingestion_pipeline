
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
#    pipeline_inventory[country_code == 'VEN']


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Cache files ----

### Create Cache files --------
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


### bring cache our of pipeline -----

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


### Load or create cache list -----------

cache_ppp <- gls$cache_ppp
cache_ids <- get_cache_id(cache_inventory)
cache_dir <- get_cache_files(cache_inventory)
names(cache_dir) <-  cache_ids

cache   <- mp_cache(cache_dir = cache_dir, 
                    load      = TRUE, 
                    save      = FALSE, 
                    gls       = gls, 
                    cache_ppp = cache_ppp)

cache <- purrr::compact(cache)

# selected_files <- which(grepl(reg, names(cache)))
# cache <- cache[selected_files]

# remove CHN 2017 and 2018 manually
# cache[grep("CHN_201[78]", names(cache), value = TRUE)] <- NULL


### remove all the surveyar that are not available in the PFW ----

svy_in_pfw <- dl_aux$pfw[, link] 

pattern <-  "([[:alnum:]]{3}_[[:digit:]]{4}_[[:alnum:]\\-]+)(.*)"
cache_names <- 
  names(cache) |> 
  gsub(pattern = pattern, 
       replacement = "\\1", 
       x = _)

cache_dir_names <- 
  names(cache_dir) |> 
  gsub(pattern = pattern, 
       replacement = "\\1", 
       x = _)

to_drop_cache     <- which(!cache_names %in% svy_in_pfw)
to_drop_cache_dir <- which(!cache_dir_names %in% svy_in_pfw)

cache[to_drop_cache]         <- NULL
cache_dir <- cache_dir[-to_drop_cache_dir]

cache_inventory[,
                cache_names := gsub(pattern = pattern, 
                                    replacement = "\\1", 
                                    x = cache_id)]

cache_inventory <- cache_inventory[cache_names %chin% svy_in_pfw]

cache_ids <- names(cache_dir)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## notify that cache has finished loading (please do NOT delete) ---
stopifnot(
  "Lengths of cache list and cache directory are not the same" = 
    length(cache) == length(cache_dir)
)

if (requireNamespace("pushoverr")) {
  pushoverr::pushover("Finished loading or creating cache list")
}

