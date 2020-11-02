load_aux_data <- function() {
  
  ll <- list(
    cpi = pipload::pip_load_aux(measure = "cpi"),
    ppp = pipload::pip_load_aux(measure = "ppp"),
    pop = pipload::pip_load_aux(measure = "pop"),
    gdp = pipload::pip_load_aux(measure = "gdp"),
    pce = pipload::pip_load_aux(measure = "pce"),
    pfw = pipload::pip_load_aux(measure = "pfw")
  )
  
  return(ll)
}
