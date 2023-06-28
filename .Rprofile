source("renv/activate.R")



library(targets)
library(tarchetypes)
library(gittargets)


if (requireNamespace("gert", quietly = TRUE)) {
  library(gert)
  gca <- function(x, ...) {
    gert::git_commit_all(x, ...)
  }
  
  gp <- function(x = NULL, ...) {
    gert::git_push(x, ...)
  }
  
  ga <- function(...) {
    gert::git_add(gert::git_status(...)$file)
  }
  
  gi <- function() {
    gert::git_info()$upstream
  }
  gs <- function() {
    gert::git_status()
  }
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

