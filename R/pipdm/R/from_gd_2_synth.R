from_gd_2_synth <- function(dl_aux, gls,
                            pipeline_inventory,
                            cts = NULL,
                            yrs = NULL, 
                            force = FALSE) {
  
  withr::local_options(list(joyn.verbose = FALSE))
  # prepare data --------
  ## load data ---------
  sv_dir <- gls$CACHE_SVY_DIR_PC
  
  gdm <- copy(dl_aux$gdm)
  pop <- copy(dl_aux$pop)
  cpi <- copy(dl_aux$cpi)
  ppp <- copy(dl_aux$ppp)
  pfw <- copy(dl_aux$pfw)
  
  
  setnames(gdm, 
           c("pop_data_level", "surveyid_year"), 
           c("data_level", "year"))
  
  setnames(pop, 
           c("pop_data_level"), 
           c("data_level"))
  
  setnames(cpi, 
           c("cpi_year"), 
           c("year"))
  
  
  # 
  ## find reporting level ---------
  dcols <- c(
    "cpi_domain",
    "ppp_domain",
    "gdp_domain",
    "pce_domain",
    "pop_domain"
  )
  
  pfw[,
      # Find MAX domain per obs
      reporting_level := apply(.SD, MARGIN = 1, max),
      .SDcols = dcols
  ][, 
    # welfare type 
    wt := fcase(welfare_type == "consumption", "CON", 
                welfare_type == "income", "INC",
                default =  "")
  ]
  
  
  
  
  ## merge data ---------
  
  gpfw <- joyn::joyn(pfw[use_groupdata == 1], gdm, 
                     by = c("country_code", "year"),
                     match_type = "1:m", 
                     reportvar = FALSE, 
                     keep = "inner") |> 
    joyn::joyn(pop,
               by = c("country_code", "data_level", "year"),
               match_type = "1:1", 
               reportvar = FALSE, 
               keep = "inner") 
  
  gpfw[, 
       cpi_data_level := fifelse(cpi_domain == 1, "national", data_level)
  ][, 
    ppp_data_level := fifelse(ppp_domain == 1, "national", data_level)]
  
  gpfw <- joyn::joyn(gpfw, cpi, 
                     by = c("country_code", "cpi_data_level", "survey_acronym", "year"),
                     match_type = "m:1", 
                     reportvar = FALSE, 
                     keep = "inner") |> 
    joyn::joyn( ppp, 
                by = c("country_code", "ppp_data_level"),
                match_type = "m:1", 
                reportvar = FALSE, 
                keep = "inner")
  
  # filter data -----------
  
  if (!is.null(cts)) {
    gpfw <- gpfw[country_code %in% cts]
  }
  
  if (!is.null(yrs)) {
    gpfw <- gpfw[year %in% yrs]
  }
  
  
  # Process in functional programming -----------
  
  # unique framework
  uvars <- c("country_code", "year", "survey_acronym", "wt", "reporting_level")
  ugpfw <- unique(gpfw[, ..uvars]) |> 
    _[, 
      cache_id := paste(country_code,
                        year,
                        survey_acronym,
                        paste0("D", reporting_level),
                        wt,
                        "SYNTH",
                        sep = "_"
      )]
  
  
  # filter if it is already available -------
  if (force == FALSE) {
    syn_cache_ids <- 
      fs::dir_ls(sv_dir, regexp = "fst$", type = "file") |> 
      fs::path_file() |> 
      fs::path_ext_remove()
    
    ugpfw <- ugpfw[!cache_id %in% syn_cache_ids]
  }
  
  # early return  -----
  
  if (nrow(ugpfw) == 0) {
    return(invisible(pipeline_inventory))
  }
  
  
  # define length of inventory
  j <- 1
  seq_pfw <- seq_len(nrow(ugpfw))
  ldt <- purrr::map(cli::cli_progress_along(seq_pfw), \(j) {
    ugpfw_j <- ugpfw[j]
    gpfw_j  <- gpfw[ugpfw_j, on = uvars]
    
    ## local cache data ------------
    dt <- pipload::pip_load_data(ugpfw_j$country,
                                 ugpfw_j$year, 
                                 verbose = FALSE)
    
    area_levels <- dt[, unique(area)]
    lal         <- length(area_levels)
    if (lal == 1) {
      if (area_levels == "" || is.na(area_levels) || is.null(area_levels)) {
        area_levels <- "national"
        dt[, area := "national"]
      }
    }
    lsyn        <- vector("list", length = lal)
    names(lsyn) <- area_levels
    
    
    # i <- area_levels[1]
    for (i in area_levels) {
      
      ## Filter data -------------
      dt_area   <- dt[area == i]
      gpfw_ji    <- gpfw_j[data_level == i]
      
      gd_type   <- sub("\\D", "", gpfw_ji$gd_type) |> # remove eveything not numeric
        as.numeric() 
      
      ## clean group data ------------
      
      dt_area <- wbpip:::gd_clean_data(dt_area, 
                                       welfare     = "welfare",
                                       population  = "weight", 
                                       gd_type     = gd_type, 
                                       quiet       = TRUE)
      
      ## Create synthetic data ------------
      # empty data.table when error or warning
      emp <- data.table(country_code = gpfw_ji$country,
                        year = gpfw_ji$year, 
                        area = i) |> 
        _[, c("welfare", "weight", "welfare_lcu", "welfare_ppp") := NA]
      
      # computation
      ls <- tryCatch(
        expr = {
          ## Bottom censoring 25 cents ---------
          # PPP year
          py <- gls$CACHE_SVY_DIR_PC |> 
            fs::path_file() |> 
            sub("(^[0-9]+_)([0-9]{4})(_.*)", "\\2", x = _) |> 
            as.numeric()
          
          # threshold
          if (py == 2017) {
            bc <- .25
          } else if (py == 2011) {
            bc <- .25
          } else {
            bc <- 0
          }
          
          
          wbpip:::sd_create_synth_vector(welfare     = dt_area$welfare, 
                                         population  = dt_area$weight, 
                                         mean        = gpfw_ji$survey_mean_lcu,
                                         pop         = gpfw_ji$pop) |> 
            _[, `:=`(
              welfare_lcu       = welfare,
              area              = i,
              country_code      = gpfw_ji$country,
              year              = gpfw_ji$year, 
              distribution_type = "micro", 
              welfare_type      = gpfw_ji$welfare_type
            )][, 
               welfare_ppp := wbpip:::deflate_welfare_mean(welfare_mean = welfare, 
                                                           ppp = gpfw_ji$ppp, 
                                                           cpi = gpfw_ji$cpi)
          # apply censoring
            ][welfare_ppp <= bc, 
              welfare_ppp := bc]
          
        }, # end of expr section
        
        error = function(e) {
          emp
        }, # end of error section
        
        warning = function(w) {
          emp
        }
      ) # End of trycatch
      
      lsyn[[i]] <- ls
    }
    
    ## Combine data ------------
    dt <- rbindlist(lsyn)
    
    return(dt)
  })
  
  
  
  ## inventory file -------
  inv <- vector("list", length = nrow(ugpfw))
  
  for (j in 1:nrow(ugpfw)) {
    ugpfw_j <- ugpfw[j]
    inv[[j]] <- pipload::pip_find_data(ugpfw_j$country,
                                       ugpfw_j$year)
  }
  
  cache_ids <- 
    with(ugpfw, {
      paste(country_code,
            year,
            survey_acronym,
            paste0("D", reporting_level),
            wt,
            "SYNTH",
            sep = "_"
      )
    })
  
  synth_inv <- 
    rbindlist(inv) |> 
    _[, 
      `:=`(
        cache_id = cache_ids,
        survey_id = sub("\\.dta", "", filename)
      ) ]
  
  # save data ---------
  
  
  ## organize and save -----------
  names(ldt) <- cache_ids
  
  
  purrr::walk(cli::cli_progress_along(cache_ids), \(i) {
    
    fs::path(sv_dir, cache_ids[[i]], ext = "fst") |> 
      fst::write_fst(ldt[[i]], path = _)
  })
  
  
  pipeline_inventory <- pipeline_inventory[!synth_inv, # remove old cache
                                           on = "cache_id"] |> 
    rowbind(synth_inv)
  
  return(invisible(pipeline_inventory))
  
}