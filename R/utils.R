save_estimations <- function(dt, dir, name, 
                             time = format(Sys.time(), "%Y%m%d%H%M%S"), 
                             compress) {
  
  fst::write_fst(x        = dt,
                 path     = paste0(dir, name, ".fst"),
                 compress = compress)
  
  fst::write_fst(x        = dt,
                 path     = paste0(dir,"_vintage/", name, "_", time, ".fst"),
                 compress = compress)
  
  haven::write_dta(data     = dt,
                   path     = paste0(dir, name, ".dta"))
  
  haven::write_dta(data     = dt,
                   path     = paste0(dir,"_vintage/", name, "_", time, ".dta"))
  
  return(paste0(dir, name, ".fst"))
}