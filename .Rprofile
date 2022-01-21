source("renv/activate.R")
# ---- RENV ---- 

library(targets)
library(tarchetypes)


if (requireNamespace("gert", quietly = TRUE)) {
  library(gert)
}

if (requireNamespace("pushoverr", quietly = TRUE)) {
  
  
  run_tar <- function() {
    # names <- rlang::enquo(names)
    s     <- Sys.time()
    start <-  format(s, "%H:%M")
    try(tar_make())
    
    f      <- Sys.time()
    finish <- format(f, "%H:%M")
    
    d <- f - s
    
    msg <- paste0("Finished pipeline. \nStarted at ", start, 
                  "\nFinished at ", finish, 
                  "\nDifference ", d)
    pushoverr::pushover(msg)
    cli::cli_alert(msg)
    
    return(invisible(TRUE))
  }
  
}

