Rcpp::sourceCpp("src/SelectCumGrowth.cpp")

#' Title
#'
#' @param z
#' @param x
#' @param y
#' @param preferred
#'
#' @return
#' @export
#'
#' @examples
#' library(fastverse)
#' x <- c(1:7) |>
#'   na_insert(.4)
#'
#' y <- c(4:10) |>
#'   na_insert(.4)
#'
#' z <- c(6:12) |>
#'   na_insert(.8)
#'
#' x
#' y
#' z
#' na_subs(z, x, y)
#' na_subs(z, x, y, "y")
#' na_subs(z, x, y, "none")
#' na_subs(z, x, y, 5)
na_subs <- function(z, x, y, preferred = "x") {
  
  stopifnot(length(x) == length(y))
  stopifnot(length(x) == length(z))
  
  na_z <- is.na(z) |>
    which()
  
  na_x <- (!is.na(x)) |>
    which() |>
    intersect(na_z)
  
  na_y <- (!is.na(y)) |>
    which() |>
    intersect(na_z)
  
  # What value to use in case of conflict?
  x_y <- NULL
  if (preferred == "x") {
    
    na_y <- setdiff(na_y, na_x)
    
  } else if (preferred == "y") {
    
    na_x <- setdiff(na_x, na_y)
    
  } else {
    x_y  <- intersect(na_y, na_x)
    na_y <- setdiff(na_y, x_y)
    na_x <- setdiff(na_x, x_y)
    
  }
  
  # replace
  z[na_y] <- y[na_y]
  z[na_x] <- x[na_x]
  
  # If selected by user
  if (preferred != "none") {
    if (!is.null(x_y) & length(x_y) != 0) {
      z[x_y] <- preferred
    }
  }
  
  z
}



#' Title
#'
#' @param x
#' @param y
#'
#' @return
#' @export
#'
#' @examples
`%?:%` <- function(x, y) {
  
  stopifnot(length(x) == length(y))
  na_x <- !is.na(x)
  na_y <- !is.na(y)
  
  z <- rep(NA, length(x))
  z[na_y] <- y[na_y]
  z[na_x] <- x[na_x]
  z
}









lineup_mean2 <- \(growth,
                  svy_mean,
                  direction = c("forward", "backward")
) {
  
  direction <- match.arg(direction)
  
  if (direction == "forward") {
    o <- 1:length(svy_mean)
  } else {
    o <- length(svy_mean):1
  }
  
  l = list(svy_mean = svy_mean[o],
           growth   = growth[o])
  
  na_to_mean <- \(prev, i) {
    
    if (!is.na(l$svy_mean[i])) {
      
      return(l$svy_mean[i])
      
    } else if (is.na(prev)) {
      
      return(NA)
      
    } else {
      return(prev*l$growth[i])
    }
    
  }
  
  rt <-
    Reduce(f          = na_to_mean,
           x          = seq_len(length(l$svy_mean)),
           init       = l$svy_mean[1],
           accumulate = TRUE)
  
  if (direction == "backward") rt <- rev(rt)
  
  rt
}




lineup_mean <- \(growth,
                 svy_mean,
                 direction = c("forward", "backward")
) {
  
  direction <- match.arg(direction)
  stopifnot(length(svy_mean) == length(growth))
  #vector length
  vl <- length(svy_mean)
  
  
  if (direction == "backward") {
    svy_mean <-  rev(svy_mean)
    growth   <-  rev(growth)
  }
  
  rt <- vector("numeric", length = vl)
  
  for (i in seq_along(svy_mean)) {
    
    if (i == 1) {
      rt[i] <- if (!is.na(svy_mean[i])) svy_mean[i] else  NA
      next
    }
    
    rt[i] <-
      if (!is.na(svy_mean[i])) {
        
        svy_mean[i]
        
      } else if (is.na(rt[i - 1])) {
        
        NA
        
      } else {
        
        if (direction == "backward") {
          rt[i - 1] / growth[i - 1]
        } else  {
          rt[i - 1] * growth[i]
        }
      }
    
  } # end of loop
  
  if (direction == "backward") rt <- rev(rt)
  
  rt
}




# svy_mean <- c(NA, NA, NA, NA, NA, NA, NA, NA, NA, 7.933157550406, NA, NA, NA, NA, NA, 8.108228791612, NA, NA, 9.165974569466, NA, NA, 10.038169339119, NA, NA, NA, 9.517231461151, NA, 10.141309877656, 12.055372745893, 12.54109311046, 12.411218948203, 13.31650707986, 13.749830812247, 13.952189784018, NA, NA)
#
# growth <- c(0.980753525343573, 0.97716234365872, 1.04846532099561, 0.921684662907275, 0.807032254141012, 0.953642141513516, 1.07160964547194, 1.06278833160985, 1.09817147150909, 1.06846125915512, 0.927472265203412, 1.06661539106756, 1.09525648386599, 1.05341015707559, 1.06520077997575, 1.03395332733934, 1.0414694807972, 1.04169598717327, 1.0424751770755, 1.04601046087654, 1.04751060255121, 1.05829675896047, 1.0097632063508, 1.01523498332799, 1.01975149718598, 1.00206138666227, 1.01416330925895, 1.02118925922242, 1.0103446425499, 1.01603161981399, 1.0177639650511, 1.02429362541907, 1.0262127497992, 0.98010705095038, 1.03947182227088, 1.04343692775827)
#
#
# lineup_mean(growth, svy_mean)
# lineup_mean(growth, svy_mean, "backward")
#



#' is_monotonic makes sure that the reference value= is in between the survey
#' values
#'
#' @param svy vector of values that correspond to the survey years.
#' @param ref vector of reference values. it should be the same size as `svy`
#'   and, in theory, should have the same value in both observations.
#'
#' @return logical
#' @noRd
is_monotonic <- function(svy, ref) {
  r <- unique(ref)
  if (length(svy)  == 1)
    return(FALSE)
  
  stopifnot(exprs = {
    length(r) == 1
    length(svy) == 2
  })
  
  ((r - svy[1]) * (svy[2] - r)) > 0
}




#' convert to FALSE if not TRUE
#'
#' it makes use of `fifelse()` because it is intended to be use in pipe calls
#'
#' @param x value to convert
#'
#' @return
#' @export
#'
#' @examples
false_if_not_true <- \(x) {
  fifelse(!isTRUE(x), FALSE, TRUE)
}







#' Title
#'
#' @param svy_mean
#' @param svy_nac
#' @param ref_nac
#'
#' @return
#' @export
#'
#' @examples
same_dir_mean <- \(svy_mean, svy_nac, ref_nac) {
  
  ref_value <- unique(ref_nac)
  if (length(svy_mean)  == 1)
    return(svy_mean)
  
  stopifnot(exprs = {
    length(ref_value) == 1
    length(svy_nac)   == 2
    length(svy_mean)  == 2
  })
  
  
  
  # estimated reference mean for both years
  (svy_mean[2] - svy_mean[1]) *
    ((ref_value - svy_nac[1]) / (svy_nac[2] - svy_nac[1])) +  svy_mean[1]
  
}



#' Select cumulive growth with passthrough.
#'
#' @param inc_group
#' @param passthrough
#' @param condition
#' @param gdpg GDP growth from `collapse::G(gdp, scale = 1) + 1`
#' @param pceg PCE growth from `collapse::G(pce, scale = 1) + 1`
#'
#' @return
#' @export
#'
#' @examples
select_cum_growth_R <- \(gdpg, pceg,
                         inc_group,
                         passthrough =1,
                         condition = c("LIC", "LMIC")) {
  stopifnot(exprs = {
    length(gdpg) == length(pceg)
    is.character(inc_group)
    passthrough <= 1 & passthrough >= 0
  })
  
  
  out <- vector("numeric", length(gdpg))
  
  gdpg <- (gdpg - 1)*passthrough + 1
  pceg <- (pceg - 1)*passthrough + 1
  
  
  for (i in seq_along(gdpg)) {
    if (i == 1) {
      out[i] <- 1
      next
    }
    
    # NAs conditions
    out[i] <-
      if (is.na(pceg[i])) {
        
        gdpg[i]*out[i - 1]
        
      } else if (is.na(gdpg[i])) {
        
        pceg[i]*out[i - 1]
        
        # income group conditions
      } else if (inc_group[i] %in% condition) {
        
        gdpg[i]*out[i - 1]
        
      } else {
        pceg[i]*out[i - 1]
      }
    
  }
  
  out
  
}


select_cum_growth <- \(gdpg, pceg,
                       inc_group,
                       passthrough =1,
                       condition = c("LIC", "LMIC")) {
  stopifnot(exprs = {
    length(gdpg) == length(pceg)
    is.character(inc_group)
    passthrough <= 1 & passthrough >= 0
  })
  
  
  gdpg <- (gdpg - 1)*passthrough + 1
  pceg <- (pceg - 1)*passthrough + 1
  
  SelectCumGrowth(gdpg, pceg, inc_group)
  
}






#' Find the relative distance from survey years to reference year to interpolate
#'
#' @param ref_year
#' @param svy_year
#'
#' @return
#' @examples
relative_distance <- \(ref_year, svy_year) {
  
  ls <- length(svy_year)
  ry <- unique(ref_year)
  
  stopifnot(exprs = {
    length(ry) == 1
    ls %in% c(1, 2)
  })
  
  if (ls == 1)
    return(1)
  
  
  dist <-  abs(svy_year - ry)
  1 - dist/sum(dist)
}












#' SAve estimations file
#'
#' @param dt 
#' @param dir 
#' @param name 
#' @param time 
#' @param compress 
#'
#' @return
#' @export
#'
#' @examples
save_estimations <- function(dt, dir, name, 
                             time = format(Sys.time(), "%Y%m%d%H%M%S"), 
                             compress) {
  
  vintage     <- paste0(name, "_", time)
  vintage_dir <- fs::path(dir,"_vintage")
  if (!fs::dir_exists(vintage_dir)) {
    fs::dir_create(vintage_dir)
  }
  
  fst::write_fst(x        = dt,
                 path     = fs::path(dir, name, ext = "fst"),
                 compress = compress)
  
  fst::write_fst(x        = dt,
                 path     = fs::path(vintage_dir, vintage, ext = "fst"),
                 compress = compress)
  
  haven::write_dta(data     = dt,
                   path     = fs::path(dir, name, ext = "dta"))
  
  haven::write_dta(data     = dt,
                   path     = fs::path(vintage_dir,vintage, ext = "dta"))
  
  return(fs::path(dir, name, ext = "fst"))
}

create_input_folder_structure <- function(dir) {
  
  # Create sub directories if needed 
  
  # _aux folder
  if (!dir.exists(fs::path(dir, '_aux')))
    dir.create(fs::path(dir, '_aux'))
  
  # _inventory folder 
  if (!dir.exists(fs::path(dir, '_inventory')))
    dir.create(fs::path(dir, '_inventory'))
  
  # _log folders 
  if (!dir.exists(fs::path(dir, '_log')))
    dir.create(fs::path(dir, '_log'))
  if (!dir.exists(fs::path(dir, '_log/info')))
    dir.create(fs::path(dir, '_log/info'))
  if (!dir.exists(fs::path(dir, '_log/info/vintage')))
    dir.create(fs::path(dir, '_log/info/vintage'))
  
  # Create other files if needed 
  
  # Create empty _log/info .dtasig files  
  if (!file.exists(fs::path(dir, '_log/info/pip_info_gd.dtasig')))
    file.create(fs::path(dir, '_log/info/pip_info_gd.dtasig'))
  if (!file.exists(fs::path(dir, '_log/info/pip_info_md.dtasig')))
    file.create(fs::path(dir, '_log/info/pip_info_md.dtasig'))
  
}

check_missing_input_files <- function(dir) {
  
  # Check that _a_ WEO.xls file is available 
  # The most recent version of this file needs to be 
  # manually downloaded from IMF's webpage. 
  weo_path <- '_aux/weo/WEO_[0-9]{4}-[0-9]{2}-[0-9]{2}.xls'
  if (!file.exists(fs::path(dir, weo_path)))
    rlang::abort('WEO_[YYYY-MM-DD].xls file not found.')
  
  # Check that _the_ 2021-01-14 version of NAS special.xlsx
  # ia available. The use of this specific file is currently
  # hardcoded in pipaux::pip_gdp_update and pipaux::pip_pce_update. 
  nas_path <- '_aux/sna/NAS special_2021-01-14.xlsx'
  if (!file.exists(fs::path(dir, nas_path)))
    rlang::abort('NAS special_2021-01-14.xlsx file not found.')
  
}

cache_inventory_path <- function(CACHE_SVY_DIR){
  fs::path(CACHE_SVY_DIR, "_crr_inventory/crr_inventory.fst")
}

get_cache_files <- function(x) {
  x$cache_file
}

get_cache_id <- function(x) {
  x$cache_id
}

get_survey_id <- function(x) {
  x$survey_id
}


aux_out_files_fun <- function(OUT_AUX_DIR, aux_names) {
  purrr::map_chr(aux_names, ~ fs::path(OUT_AUX_DIR, .x,  ext = "fst"))
}

temp_cleaning_ref_table <- function(dt) {
  
  dt <- dt[!(is.null(survey_mean_ppp) | is.na(survey_mean_ppp))]
  dt <- dt[!(is.null(predicted_mean_ppp) | is.na(predicted_mean_ppp))]
  return(dt)
  
}

named_mean <- function(dt) {
  mvec        <- dt[, survey_mean_lcu]
  names(mvec) <- dt[, pop_data_level]
  return(mvec)
}

prep_aux_data <- function(maindir = PIP_DATA_DIR) {
  
  auxdir <- fs::path(maindir, "_aux/")
  
  aux_dirs <- fs::dir_ls(auxdir,
                         recurse = FALSE,
                         type = "directory")
  
  aux_indicators <- stringr::str_extract(aux_dirs, "[^/]+$")
  aux_indicators   <-  tolower(unique(aux_indicators))
  
  # keep only those that exist
  dd <-
    purrr::map2_lgl(.x = aux_dirs,
                .y = aux_indicators,
                .f = ~{
                  ffst <- fs::path(.x, .y, ext = "fst")
                  frds <- fs::path(.x, .y, ext = "rds")
                  
                  f_exists <- purrr::map_lgl(c(ffst, frds), fs::file_exists)
                  any(f_exists)
                  
                })
  names(dd) <- aux_indicators
  
  aux_indicators <- aux_indicators[dd]
  aux_dirs       <- aux_dirs[dd]
  
  names(aux_dirs) <- aux_indicators
  
  aux_tb <- data.table::data.table(
    auxname  = aux_indicators,
    auxfiles = aux_dirs
  )
  
  return(aux_tb)
}

#' Create framework data from Price FrameWork data (subsample)
#'
#' @param pfw 
#'
#' @return
#' @export
#'
#' @examples
create_framework <- function(pfw) {
  meta_vars <- 
    c(
      "wb_region_code",
      "country_code",
      "pcn_region_code",
      "ctryname",
      "year",
      "surveyid_year",
      "reporting_year",
      "survey_year",
      "survey_acronym",
      "survey_coverage",
      "welfare_type",
      "use_imputed",
      "use_microdata",
      "use_bin",
      "use_groupdata",
      "survey_comparability",
      "survey_time",
      "surv_title",
      "surv_producer"
    )
  
  pfw <- pfw[, ..meta_vars]
  setnames(pfw, "ctryname", "country_name")
  
  return(pfw)
}


get_groupdata_means <- function(cache_inventory, 
                                gdm) {
  
  dt. <- joyn::joyn(x          = cache_inventory,
                     y          = gdm,
                     by         = c("survey_id", "welfare_type"),
                     match_type = "1:m",
                     y_vars_to_keep = c("survey_mean_lcu", "pop_data_level"),
                     keep       = "left")
  
  data.table::setorder(dt., cache_id, pop_data_level)
  
  
  gd_means        <- dt.[, survey_mean_lcu]
  gd_means        <- gd_means * (12/365)
  
  names(gd_means) <- dt.[, cache_id]
  ## convert to list by name
  gd_means        <- split(unname(gd_means),names(gd_means)) 
  
  return(gd_means)

}

