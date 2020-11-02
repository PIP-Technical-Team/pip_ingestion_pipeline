the_plan <-
  drake_plan(

   ## STEP 1: Load microdata
    inventory = pip_load_inventory(),
    microdata = pip_load_data(country =  c("COL", "BRA", "PRY"),
                              tool = "PC"),
    
   ## STEP 2: Load auxiliary data
    cpi = pipload::pip_load_aux(measure = "cpi"),
    ppp = pipload::pip_load_aux(measure = "ppp"),
    pop = pipload::pip_load_aux(measure = "pop"),
    gdp = pipload::pip_load_aux(measure = "gdp"),
    pce = pipload::pip_load_aux(measure = "pce"),
    pfw = pipload::pip_load_aux(measure = "pfw"),
    
    
   ## STEP 3: Deflate welfare means (survey years) 
   ## Creates a table of deflated survey means
   # deflated_svy_means = create_deflated_means_table()

)
