the_plan <-
  drake_plan(

   ## STEP 1: Load microdata
    inventory = pip_load_inventory(),
    microdata = pip_load_data(country =  c("COL", "BRA", "PRY"),
                              tool = "PC"),
    
   ## STEP 2: Load auxiliary data
    aux_data = load_aux_data(),
    
   ## STEP 3: Deflate welfare means (survey years) 
   ## Creates a table of deflated survey means
   deflated_svy_means = create_deflated_means_table(microdata = microdata, 
                                                    aux_data  = aux_data,
                                                    inventory = inventory)

)
