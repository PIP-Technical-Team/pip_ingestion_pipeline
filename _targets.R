# ==================================================
# project:       PIP ingestrion pipline using Targets
# Author:        Andres Castaneda
# Dependencies:  The World Bank
# ----------------------------------------------------
# Creation Date:    2020-12-22
# Modification Date: 
# Script version:    01
# References:
# 
# 
# Output:             
# ==================================================

#----------------------------------------------------------
#   Load libraries
#----------------------------------------------------------

# devtools::install_github("PIP-Technical-Team/wbpip")
# devtools::install_github("PIP-Technical-Team/pipload@development")
# devtools::install_github("PIP-Technical-Team/pipdm@for_targets")

library(here)
library(targets)
library(tarchetypes)

#----------------------------------------------------------
#   subfunctions
#----------------------------------------------------------

# Set target-specific options such as packages.
pkgs <- c("data.table", 
          "pipload", 
          "dplyr",
          "purrr",
          "pipdm", 
          "pipload", 
          "wbpip")

tar_option_set(packages = pkgs)

# tar_option_set(debug = "dt_load_clean")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#---------   initial parameters   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#--------- Main Directory ---------
maindir <- "//w1wbgencifs01/pip/PIP-Data/_testing/pipdp_testing/"
pipedir <- "//w1wbgencifs01/pip/pip_ingestion_pipeline/"

#--------- Auxiliary indicators ---------
auxdir <- paste0(maindir, "_aux/")
aux_files_to_load <- as.character(
  fs::dir_ls(auxdir,
             type    = "file",
             recurse = TRUE,
             regexp  = ".*/[a-z]+\\.fst")
)


aux_indicators <- as.character(gsub(auxdir, "", aux_files_to_load))
aux_indicators <- as.character(gsub("/.*", "", aux_indicators))

aux_tb <- tibble::tibble(
  auxname  = aux_indicators,
  auxfiles = aux_files_to_load
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#---------   Pipeline   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#--------- CReate batch of targets for auxiliary data ---------

aux_targ <- tar_map(
  values = aux_tb, 
  names  = "auxname", 
  
  # create dynamic name
  tar_target(
    rawaux,
    auxfiles, 
    format = "file"
  ), 
  # load data using pipload
  tar_target(
    aux,
    pipload::pip_load_aux(file_to_load = rawaux)
  )
)


#--------- define targets and pipeline ---------

tar_pipeline(
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   Auxiliary data   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  aux_targ,
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   Inventory   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # Declare inventory file as dynamic
  tar_target(raw_inventory_file, 
             paste0(maindir, "_inventory/inventory.fst"),
             format = "file"),
  
  # Load RAW inventory file and filter with `filter_to_pc`
  tar_target(raw_inventory, 
             pipload::pip_find_data(inv_file     = raw_inventory_file,
                                    filter_to_pc = TRUE)),
  tar_target(inventory,
             pipdm::db_filter_inventory(raw_inventory = raw_inventory, 
                                        pfw_table = aux_pfw,
                                        pipedir = pipedir)[1:5] # To delete
             
             ),
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   Survey mean   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # load and clean welfare data
  tar_target(dt_load_clean,
             pipdm::db_load_and_clean(survey_id = inventory, 
                                      maindir   = maindir)),
  
  # LCU mean for unused surveys 
  tar_target(tmp_lcu_mean,
             pipdm::db_create_lcu_table(dlc     = dt_load_clean,
                                        pop     = aux_pop,
                                        maindir = maindir)),
  
  # Deflated mean for tmp 
  tar_target(tmp_dsm,
             pipdm::db_create_dsm_table(lcu_table = tmp_lcu_mean,
                                        cpi_table = aux_cpi,
                                        ppp_table = aux_ppp))
)


#----------------------------------------------------------
# Drake Plan   
#----------------------------------------------------------

## dsm stands for deflated_svy_means
## lcu stands for Local Currency Unit

# the_plan <-
#   drake_plan(
#     
#     
#     ## STEP 1: Load Inventory of microdata
#     raw_inventory =  fst::read_fst(file_in(!!paste0(maindir, "_inventory/inventory.fst"))),
#     inventory     =  filter_inventory(raw_inventory),
#     
#     ## STEP 2: Load auxiliary data (statics branching)
#     aux = target(
#       import_file(file_in(file)),
#       transform = map(file  = !!aux_files_to_load,
#                       label = !!aux_indicators,
#                       .id = label)
#     ),
#     
#     ## STEP 3: Deflate welfare means (survey years) 
#     ## Creates a table of deflated survey means
#     updated_lcum = calculate_lcum(inv = inventory$survey_id),
#     
#     updated_dsm = create_dsm_table(cpi = aux_cpi,
#                                    ppp = aux_ppp,
#                                    dt  = updated_lcum),
#     
#     old_dsm = load_old_dsm(),
#     new_dsm = join_dsm_tables(ud  = updated_dsm,
#                               old = old_dsm),
#     
#     out_dsm = save_dsm(new_dsm,
#                        pipedir,
#                        file_out("output/deflated_svy_means.fst")) 
#     
#   ) 
