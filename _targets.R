# ==================================================
# project:       PIP ingestion pipeline using Targets
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

library(targets)
library(tarchetypes)

# Load packages for the sake of renv
# library(dplyr)
# library(tidyr)
# library(data.table)
# library(pipload)
# library(wbpip)
# library(pipdm)

#----------------------------------------------------------
#   subfunctions
#----------------------------------------------------------

# Set target-specific options such as packages.
pkgs <- c("dplyr", 
          "tidyr",
          "data.table", 
          "pipload", 
          # "pipdm",
          "wbpip")

tar_option_set(packages = pkgs)

# read all files in R folder
rfiles <- fs::dir_ls(path = "R/",
           type = "file")

purrr::walk(rfiles, source)


# tar_option_set(debug = "dt_load_clean")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#---------   initial parameters   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#--------- Main Directory ---------
maindir <- "//w1wbgencifs01/pip/PIP-Data/_testing/pipdp_testing/"
pipedir <- "//w1wbgencifs01/pip/pip_ingestion_pipeline/"


# Years

ref_years <- c(1981:2019)
pip_years <- c(1981:2019)



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

aux_tb <- data.table::data.table(
  auxname  = aux_indicators,
  auxfiles = aux_files_to_load
)

# filter 
aux_tb <- aux_tb[auxname != "maddison"]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#---------   Pipeline   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
runit <- TRUE
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
  # tar_force(
  #   aux,
  #   pipload::pip_load_aux(file_to_load = rawaux), 
  #   force = runit
  # )
  tar_target(
    aux,
    pipload::pip_load_aux(file_to_load = rawaux)
  )
  
)


#--------- define targets and pipeline ---------
tar_pipeline(
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   Auxiliary and input data   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Aux
  aux_targ,
  
  # input dsm data
  tar_target(dsm_in, 
             paste0(pipedir, "dsm/deflated_svy_means_in.fst"),
             format = "file"),
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   Inventory   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # Declare inventory file as dynamic
  tar_target(raw_inventory_file, 
             paste0(maindir, "_inventory/inventory.fst"),
             format = "file"),
  
  # Load RAW inventory file and filter with `filter_to_pc`
  tar_target(raw_inventory, 
             pip_find_data(inv_file     = raw_inventory_file,
                          filter_to_pc  = TRUE)
             ),
  # tar_force(inventory,
  #            db_filter_inventory(raw_inventory = raw_inventory,
  #                                pfw_table     = aux_pfw,
  #                                dsm_in        = dsm_in),
  #           force = runit
  #            ),
  
  tar_target(inventory,
             db_filter_inventory(raw_inventory = raw_inventory,
                                 pfw_table     = aux_pfw,
                                 dsm_in        = dsm_in)
             ),
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #---------   Survey mean   ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # load and clean welfare data
  tar_target(dt_load_clean,
             db_load_and_clean(survey_id = inventory, 
                               maindir   = maindir)),
  
  # LCU mean for unused surveys 
  tar_target(tmp_lcu_mean,
             db_create_lcu_table(dlc     = dt_load_clean,
                                 pop     = aux_pop,
                                 maindir = maindir)),
  
  # Deflated mean of  tmp  LCU
  tar_target(dsm_tmp,
             db_create_dsm_table(lcu_table = tmp_lcu_mean,
                                 cpi_table = aux_cpi,
                                 ppp_table = aux_ppp)),
  
  # append new dsm to final file
  tar_target(dsm_out,
             db_bind_dsm_tables(dsm_in  = dsm_in,
                                tmp_dsm = dsm_tmp)),
  
  # tar_force(dsm_out,
  #            db_bind_dsm_tables(dsm_in  = dsm_in, 
  #                               tmp_dsm = dsm_tmp),
  #           force = runit),
  
  # save dsm table
  tar_target(dsm_file,
             save_dsm(dsm_out = dsm_out, 
                      pipedir = pipedir),
             format = "file"),
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #--  adjusted welfare means for each reference year -------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # Reference year table
  tar_target(ref_year_table,
             db_create_ref_year_table(gdp_table = aux_gdp,
                                      pce_table = aux_pce,
                                      pop_table = aux_pop,
                                      pfw_table = aux_pfw,
                                      dsm_table = dsm_out,
                                      ref_years = ref_years,
                                      pip_years = pip_years)),
  
  # National accounts table
  tar_target(nac_table,
             db_create_nac_table(gdp_table = aux_gdp, 
                                 pce_table = aux_pce, 
                                 pip_years = pip_years))
  
)

