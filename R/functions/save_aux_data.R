save_aux_data <- function(x, 
                          filename, 
                          compress) {
  
  
  type <- gsub("(.+)(\\.)([a-z]{3}$)", "\\3", filename)
  
  if (type == 'fst') {
    
    fst::write_fst(x, filename, compress = compress)
    
  } else if (type == 'rds') {
    
    saveRDS(x, filename,compress = compress)
  
  }
  
  return(filename)
  
}
