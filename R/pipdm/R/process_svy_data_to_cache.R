#' Process survey data to cache file
#'
#' @param survey_id character: Original Survey ID
#' @param cache_id  character: cache id vector
#' @param pip_data_dir character: Input folder for the raw survey data.
#' @param cols character: vector of variables to keep. Default is NULL.
#' @param cache_svy_dir character: Output directory
#' @param compress numeric: Compression level used in `fst::write_fst()`.
#' @inheritParams db_create_ref_year_table
#' @inheritParams db_create_dsm_table
#' @inheritParams create_cache_file
#'
#' @return data frame with status of process
#' @export
process_svy_data_to_cache <- function(survey_id,
                                      cache_id,
                                      pip_data_dir,
                                      cache_svy_dir,
                                      compress      = 100,
                                      cols          = NULL,
                                      cpi_table,
                                      ppp_table,
                                      pfw_table,
                                      pop_table) {


  #--------- Load data ---------
  chh_filename <- fifelse(
    grepl("\\.fst$", cache_id),
    cache_id,
    paste0(cache_id, ".fst")
  )

  df <- tryCatch(
    expr = {
      # Load data
      df <- pipload::pip_load_data(
        survey_id = survey_id,
        maindir   = pip_data_dir,
        verbose   = FALSE
      )
    }, # end of expr section

    error = function(e) {
      NULL
    }
  ) # End of trycatch

  if (is.null(df)) {
    ret <- data.table(
      id = survey_id,
      status = "error loading"
    )
    return(ret)
  }

  #--------- Clean Data ---------

  df <- tryCatch(
    expr = {
      # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      # Clean data   ---------
      # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

      
      #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      ## check if there is alternative welfare to use --------

      # make sure the right welfare type is in the microdata.
      wt <- gsub("(.+_)([A-Z]{3})(_[A-Z\\-]+)(\\.fst)?$", "\\2", chh_filename)
      wt <- fifelse(wt == "INC", "income", "consumption")

      # get the right observations in pfw
      vars <-
        c(
          "country_code",
          "surveyid_year",
          "survey_acronym",
          "reporting_level",
          "welfare_type",
          "source"
        )
      dt_id <- data.table(cache_id = get("cache_id"))

      dt_id[,
            (vars) := data.table::tstrsplit(cache_id,
                                            split = "_",
                                            names = TRUE,
                                            fixed = TRUE)
      ][,
        surveyid_year := as.integer(surveyid_year)
      ]

      pfw <- joyn::joyn(pfw_table,
                         dt_id,
                         by = c("country_code", "surveyid_year", "survey_acronym"),
                         match_type = "1:1",
                         keep = "inner", 
                        reportvar = FALSE)

      if (pfw$oth_welfare1_type != "" && !is.na(pfw$oth_welfare1_type)) {
        if (substr(wt, 1, 1) == pfw$oth_welfare1_type) {
          # replace alternative welfare
          df[, welfare := alt_welfare]

        }
      }

      # stadanrdize and change weflare type
      df[, welfare_type := wt]
      
      # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      ## Standard cleaning --------
      
      df <- db_clean_data(df)
      

      #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      # additional variables   ---------
      #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


      # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      ## reporting level variable --------

      dl_var        <- grep("data_level", names(df), value = TRUE) # data_level vars
      ordered_level <- purrr::map_dbl(dl_var, ~ get_ordered_level(df, .x))
      select_var    <- dl_var[which.max(ordered_level)]

      df[, reporting_level := get(select_var)]

      data.table::setorder(df, reporting_level)

      # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      ## Deflate data --------
      # ppp_table <- ppp_table[ppp_default == TRUE]

      # Merge survey table with PPP (left join)
      df <- joyn::joyn(df, ppp_table,
        by         = c("country_code", "ppp_data_level"),
        match_type = "m:1",
        y_vars_to_keep = "ppp",
        keep       = "left",
        reportvar  = FALSE,
        verbose    = FALSE
      )

      # Merge survey table with CPI (left join)
      ## Round survey_year to 3 decimal places to guarantee the merge
      
      df[, survey_year := round(survey_year, 2)]
      
      
      df <- joyn::joyn(df, cpi_table,
        by = c(
          "country_code", "survey_year",
          "survey_acronym", "cpi_data_level"
        ),
        match_type = "m:1",
        y_vars_to_keep = "cpi",
        keep = "left",
        reportvar = FALSE,
        verbose = FALSE
      )
      
      ## Bottom censoring 25 cents ---------
      # PPP year
      py <- cache_svy_dir |> 
        fs::path_file() |> 
        sub("(^[0-9]+_)([0-9]{4})(_.*)", "\\2", x = _) |> 
        as.numeric()
      
      if (py == 2021) {
        bc <- 0.2831
      } else if (py == 2017) {
        bc <- .25
      } else if (py == 2011) {
        bc <- .22
      } else {
        bc <- 0
      }
      
    
      df[,
         # convert bottom censoring threshold to LCU
         bc_lcu := get("bc")*ppp*cpi
         # apply censoring
         ][welfare <= bc_lcu, 
           welfare := bc_lcu
           ][,
             # deflate LCU welfare to PPP
             welfare_lcu := welfare
             ][
               ,
               welfare_ppp := wbpip::deflate_welfare_mean(
                 welfare_mean = welfare_lcu,
                 ppp          = ppp,
                 cpi          = cpi
               )
             ]

      #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      ## scale subnational population to National accounts (WDI) --------


      nrl <- length(df[, unique(reporting_level)]) # number of reporting level
      dst <- df[, unique(distribution_type)]       # distribution type

      if ( nrl > 1  &&  !(dst %in% c("group", "aggregate")))  {
        # sd <- split(df, by = "imputation_id")
        # lf <- purrr::map(.x = sd, 
        #                  adjust_population, 
        #                  pop_table = pop_table)
        # 
        df <- adjust_population(df, pop_table)
      }  # end of population adjustment


      #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      ## Convert character to factors --------

      chr_vars <- names(df)[sapply(df, is.character)]

      df[,
         (chr_vars) := lapply(.SD, as.factor),
         .SDcols = chr_vars
         ]
      

    }, # end of expr section in trycatch

    error = function(e) {
      NULL
    }, # end of error section

    warning = function(w) {
      NULL
    }
  ) # End of trycatch

  if (is.null(df)) {
    ret <- data.table(
      id = survey_id,
      status = "error cleaning"
    )
    return(ret)
  }

  #--------- Saving data ---------
  df <- tryCatch(
    expr = {
      # Your code...
      if (!is.null(cols)) {
        df <- df[, ..cols]
      }

      df[, cache_id := (cache_id)]

      # Create paths

      svy_out_path <- paste(cache_svy_dir, chh_filename, sep = "/")

      fst::write_fst(
        x = df,
        path = svy_out_path,
        compress = compress
      )
      TRUE
    }, # end of expr section

    error = function(e) {
      NULL
    }, # end of error section

    warning = function(w) {
      NULL
    }
  ) # End of trycatch


  if (is.null(df)) {
    ret <- data.table(
      id = survey_id,
      status = "error saving"
    )
    return(ret)
  }

  ret <- data.table(
    id = survey_id,
    status = "success"
  )

  return(ret)
}

#' get ordered level of data_level variables
#'
#' @param dt cleaned dataframe
#' @param x data_level variable name
#'
#' @return integer
#' @noRd
get_ordered_level <- function(dt, x) {
  x_level <- unique(dt[[x]])
  d1 <- c("national")
  d2 <- c("rural", "urban")

  if (identical(x_level, d1)) {
    1
  } else if (identical(x_level, d2)) {
    2
  } else {
    3
  }
}

#' Adjust microdata to WDI population levels when the number of reporting levels
#' is equal or greater than 2
#'
#' @param df dataframe with microdata
#' @param pop_table population data from WDI.
#'
#' @return dataframe
adjust_population <- function(df, pop_table) {

  spop <- df |> 
    fgroup_by(c("imputation_id", "country_code", "survey_year", "reporting_level")) |> 
    fselect(weight) |> 
    fsum()
  
  to_filter <- df |> 
    fselect(country_code, survey_year, pop_data_level = reporting_level) |> 
    funique()
  
  
  dpop <- joyn::joyn(pop_table, to_filter, 
                     by = c("country_code", "pop_data_level"), 
                     keep = "inner", 
                     match_type = "m:1",
                     reportvar = FALSE, 
                     verbose = FALSE)
  
  
  dpop <-
    dpop[,
         # Abs difference in year
         to_keep := years_to_keep(year, survey_year)
    ][to_keep == TRUE
    ][,
      # get weights for weighted mean
      wght := 1 - abs(year - survey_year)
    ][, 
      # population at the right year
      .(wdipop = weighted.mean(pop, wght)), 
      by = pop_data_level
    ]
  setnames(dpop, "pop_data_level", "reporting_level")
  
  fact <- joyn::joyn(spop, dpop,
                     by = "reporting_level", 
                     match_type = "m:1", 
                     reportvar = FALSE, 
                     verbose = FALSE) |> 
    ftransform(pop_fact  = wdipop/weight) |> 
    fselect(imputation_id, reporting_level, pop_fact)
  
  df <- joyn::joyn(x  = df,
                   y  = fact,
                   by = c("imputation_id", "reporting_level"),
                   match_type = "m:1",
                   reportvar = FALSE, 
                   verbose = FALSE) |> 
    ftransform(weight = weight*pop_fact)

  return(df)
}


years_to_keep <- \(year, survey_year) {
  sy  <- unique(survey_year)
  fcy <- c(floor(sy), ceiling(sy))
  year %in% fcy
}

