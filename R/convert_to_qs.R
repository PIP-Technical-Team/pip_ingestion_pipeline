

#' Convert any file in directory to qs format
#'
#' @param dir 
#'
#' @return
#' @export
#'
#' @examples
convert_to_qs <- function(dir = gls$OUT_AUX_DIR_PC) {
  
  #   ____________________________________________________________________________
  #   on.exit                                                                 ####
  on.exit({
    
  })
  
  #   ____________________________________________________________________________
  #   Defenses                                                                ####
  stopifnot( exprs = {
    fs::dir_exists(dir)
  }
  )
  
  #   ____________________________________________________________________________
  #   Early returns                                                           ####
  if (FALSE) {
    return()
  }
  
  #   ____________________________________________________________________________
  #   Computations                                                            ####
  
  ## get all available files ---------------
  
  path <- 
    fs::dir_ls(dir, 
               recurse = 0, 
               type = "file")
  
  exts <- fs::path_ext(path)
  
  #remove qs files
  path <- path[exts != "qs"]
  
  saved <- 
    purrr::map_chr(path, load_and_qsave)
  
  
  
  #   ____________________________________________________________________________
  #   Return                                                                  ####
  return(invisible(saved))
  
}



#' Load file by extension and save it as qs
#'
#' @param path character: path of original file
#' @param ... additional parameters to `qs::qsave()`
#'
#' @return
#' @export
#'
#' @examples
load_and_qsave <- function(path, ...) {
  
  #   ____________________________________________________________________________
  #   on.exit                                                                 ####
  on.exit({
    
  })
  
  #   ____________________________________________________________________________
  #   Defenses                                                                ####
  stopifnot( exprs = {
    
  }
  )
  
  #   ____________________________________________________________________________
  #   Early returns                                                           ####
  if (FALSE) {
    return()
  }
  
  #   ____________________________________________________________________________
  #   Computations                                                            ####
  saved <- 
    tryCatch(
      expr = {
        df <- load_by_ext(path = path)
        path_qs <- 
          fs::path_ext_remove(path) |> 
          fs::path(ext = "qs")
        
        qs::qsave(df, path_qs, , ...)
        "saved"
        
      }, # end of expr section
      
      error = function(e) {
        glue::glue("error: {e$message}")
      }, # end of error section
      
      warning = function(w) {
        glue::glue("warning: {w$message}")
      }
      
    ) # End of trycatch
  
  
  #   ____________________________________________________________________________
  #   Return                                                                  ####
  return(invisible(saved))
  
}




#' Load according to extension
#'
#' @param path character: file path
#' @param ... additional paramters parsed to the corresponding loading function
#'
#' @return
#' @export
#'
#' @examples
load_by_ext <- function(path, ...) {
  
  #   ____________________________________________________________________________
  #   on.exit                                                                 ####
  on.exit({
    
  })
  
  #   ____________________________________________________________________________
  #   Defenses                                                                ####
  stopifnot( exprs = {
    fs::file_exists(path)
  }
  )
  
  #   ____________________________________________________________________________
  #   Early returns                                                           ####
  if (FALSE) {
    return()
  }
  
  #   ____________________________________________________________________________
  #   Computations                                                            ####
  
  ## Get extension of file -------
  ext <- fs::path_ext(path)
  
  
  ## load file by extension --------------
  tryCatch(
    expr = {
      # load depending of the extension
      df <-  suppressMessages(  # suppress any loading message
        
        if (ext == "csv") {
          
          # readr::read_csv(path, ...)
          data.table::fread(path, ...)
          
        } else if (ext  %in% c("xls", "xlsx")) {
          
          # temp_file <- tempfile(fileext = ext)
          # req <- httr::GET(path,
          #                  # write result to disk
          #                  httr::write_disk(path = temp_file))
          # 
          # readxl::read_excel(path = temp_file, ...)
          
          readxl::read_excel(path = path, ...)
          
        } else if (ext == "dta") {
          
          haven::read_dta(path, ...)
          
        } else if (ext == "qs") {
          
          qs::qread(path, ...)
          
        } else if (ext == "fst") {
          
          fst::read_fst(path)
          
        } else if (ext == "yaml") {
          
          yaml::read_yaml(path, ...)
          
        } else if (tolower(ext) == "rds") {
          
          readr::read_rds(path, ...)
          
        } else {
          msg <- paste("format", ext, "not available")
          stop(msg)
        }
        
      )
      
      if (is.data.frame(df)) {
        data.table::setDT(df)
      }
    },
    # end of expr section
    
    error = function(e) {
      
      file <- fs::path_file(path)
      
      msg     <- glue::glue(
        "file: {file} could not be loaded. Reason: \n {e$message}"
      )
      # 
      # cli::cli_abort(msg)
    }
    
  ) # End of trycatch
  
  
  
  
  #   ____________________________________________________________________________
  #   Return                                                                  ####
  return(df)
  
}


