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
# devtools::install_github("PIP-Technical-Team/pipdm@improvments")

library(here)
library(targets)
library(tarchetypes)

#----------------------------------------------------------
#   subfunctions
#----------------------------------------------------------

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#---------   initial parameters   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#--------- Main Directory ---------
maindir <- "//w1wbgencifs01/pip/PIP-Data/_testing/pipdp_testing/"
pipedir <- paste0(maindir, "//w1wbgencifs01/pip/pip_ingestion_pipeline/")

#--------- Auxiliary indicators ---------
auxdir <- paste0(maindir, "_aux/")
aux_files_to_load <- as.character(
  fs::dir_ls(auxdir,
             type    = "file",
             recurse = TRUE,
             regexp  = ".*/[a-z]+\\.fst")
)

# in case we need to filter the aux data
filt <- FALSE
aux_to_keep <- c("cpi", "ppp")
if (filt == TRUE) {
  to_keep <- paste(aux_to_keep,
                   collapse = "|")
  
  aux_files_to_load <- grep(to_keep, aux_files_to_load, value = TRUE)
}

aux_indicators <- as.character(gsub(auxdir, "", aux_files_to_load))
aux_indicators <- as.character(gsub("/.*", "", aux_indicators))


#----------------------------------------------------------
#   Set up
#----------------------------------------------------------
# Set target-specific options such as packages.
pkgs <- c("data.table", 
          "pipload", 
          "dplyr",
          "purrr",
          "pipdm")

tar_option_set(packages = pkgs,
               imports = pkgs
)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#---------   PLAN   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Define targets
targets <- list(
  # Declare inventory file as dynamic
  tar_target(raw_inventory_file, 
             paste0(maindir, "_inventory/inventory.fst"),
             format = "file"),
  
  # Load RAW inventory file
  tar_target(raw_inventory, 
             fst::read_fst(raw_inventory_file)),
  
  
)

# End with a call to tar_pipeline() to wrangle the targets together.
# This target script must return a pipeline object.
tar_pipeline(targets)


#----------------------------------------------------------
# Drake Plan   
#----------------------------------------------------------

## dsm stands for deflated_svy_means
## lcu stands for Local Currency Unit

the_plan <-
  drake_plan(
    
    
    ## STEP 1: Load Inventory of microdata
    raw_inventory =  fst::read_fst(file_in(!!paste0(maindir, "_inventory/inventory.fst"))),
    inventory     =  filter_inventory(raw_inventory),
    
    ## STEP 2: Load auxiliary data (statics branching)
    aux = target(
      import_file(file_in(file)),
      transform = map(file  = !!aux_files_to_load,
                      label = !!aux_indicators,
                      .id = label)
    ),
    
    ## STEP 3: Deflate welfare means (survey years) 
    ## Creates a table of deflated survey means
    updated_lcum = calculate_lcum(inv = inventory$survey_id),
    
    updated_dsm = create_dsm_table(cpi = aux_cpi,
                                   ppp = aux_ppp,
                                   dt  = updated_lcum),
    
    old_dsm = load_old_dsm(),
    new_dsm = join_dsm_tables(ud  = updated_dsm,
                              old = old_dsm),
    
    out_dsm = save_dsm(new_dsm,
                       pipedir,
                       file_out("output/deflated_svy_means.fst")) 
    
  ) 