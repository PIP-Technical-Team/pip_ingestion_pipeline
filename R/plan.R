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

#--------- Main Directory ---------
maindir <- "//w1wbgencifs01/pip/PIP-Data/"
pipedir <- paste0(maindir, "_pip_ingestion_pipeline/")

#--------- Auxiliary indicators ---------
auxdir <- paste0(maindir, "_aux/")
aux_files_to_load <- as.character(
   fs::dir_ls(auxdir,
              type    = "file",
              recurse = TRUE,
              regexp  = ".*/[a-z]+\\.fst")
)

# in case we need to filter the aux data
filt <- TRUE
aux_to_keep <- c("cpi", "ppp")
if (filt == TRUE) {
   to_keep <- paste(aux_to_keep,
                    collapse = "|")
   
   aux_files_to_load <- grep(to_keep, aux_files_to_load, value = TRUE)
}

aux_indicators <- as.character(gsub(auxdir, "", aux_files_to_load))
aux_indicators <- as.character(gsub("/.*", "", aux_indicators))

#----------------------------------------------------------
# Drake Plan   
#----------------------------------------------------------

the_plan <-
  drake_plan(
     
   ## dsm stands for deflated_svy_means
   ## lcu stands for Lucal Currency Unit

   ## STEP 1: Load Inventory of microdata
    raw_inventory =  fst::read_fst(file_in(!!paste0(maindir, "_inventory/inventory.fst"))),
    inventory     =  filter_inventory(raw_inventory),

    # 
   ## STEP 2: Load auxiliary data (statics branching)
   # include files into plan
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

