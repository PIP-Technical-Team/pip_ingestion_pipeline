source("renv/activate.R")
# ---- RENV ---- 

library(targets)
library(tarchetypes)


if (requireNamespace("gert", quietly = TRUE)) {
  library(gert)
}

if (requireNamespace("pushoverr", quietly = TRUE)) {
  
  
  run_tar <- function() {
    start <-  format(Sys.time(), "%H:%M")
    try(tar_make())
    finish <- format(Sys.time(), "%H:%M")
    msg <- paste0("Finished pipeline. \nStarted at ", start, 
                  " \nFinished at ", finish)
    pushoverr::pushover(msg)
    return(invisible(TRUE))
  }
  
}

