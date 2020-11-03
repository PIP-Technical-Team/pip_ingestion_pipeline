# ==================================================
# project:       PIP ingestion workflow (Drake Plan)
# Author:        PIP Technical Team
# Dependencies:  The World Bank
# ----------------------------------------------------
# Creation Date:    2020-11-03
# Modification Date: 
# Script version:    01
# References:
# 
# 
# Output:             output
# ==================================================


#----------------------------------------------------------
#   Set up
#----------------------------------------------------------

maindir <- "//w1wbgencifs01/pip/PIP-Data/"
auxdir <- paste0(maindir, "_aux/")

#----------------------------------------------------------
#   
#----------------------------------------------------------

the_plan <-
  drake_plan(

   ## STEP 1: Load microdata
    inventory =  fst::read_fst(file_in(!!paste0(maindir, "_inventory/inventory.fst"))),
       
    # inventory_list = target({
    #    df <- fst::read_fst(file_in(!!paste0(maindir, "_inventory/inventory.fst")))
    #    df$orig
    # },
    # format = "file"
    # ),
    
    microdata = pip_load_data(country =  c("KGZ", "AGO", "PRY"),
                              tool = "PC"),
    
   ## STEP 2: Load auxiliary data
    # aux_data = load_aux_data(),
    aux_data = target(),
    
   ## STEP 3: Deflate welfare means (survey years) 
   ## Creates a table of deflated survey means
   deflated_svy_means = target(
     create_deflated_means_table(dt        = microdata,
                                 cpi       = aux_data[["cpi"]],
                                 ppp       = aux_data[["ppp"]],
                                 inventory = inventory),
     format = "fst"
   )

) 

