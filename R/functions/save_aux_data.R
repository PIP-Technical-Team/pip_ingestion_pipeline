save_aux_data <- function(x, 
                          filename, 
                          outdir,
                          compress,
                          type = c('fst', 'rds')) {
  
  type <- match.arg(type)
  path <- sprintf('%s%s.%s', outdir, filename, type)
  if (type == 'fst')
    fst::write_fst(x, path, compress = compress)
  else if (type == 'rds')
    saveRDS(x, path, compress = compress)
  
  return(path)
  
}
