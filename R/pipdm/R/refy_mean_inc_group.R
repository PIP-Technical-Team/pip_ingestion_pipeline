refy_mean_inc_group <- \(dsm, gls, dl_aux) {
  # dsm = svy_mean_ppp_table
  # ref_years = gls$PIP_REF_YEARS
  # pip_years = gls$PIP_YEARS
  
  
  # opt <- options("joyn.verbose" = FALSE)
  withr::local_options(list(joyn.verbose = FALSE))
  # 2.National accounts growth ----------
  
  ## Load data  ------------
  
  gdp <- dl_aux$gdp  |>
    frename(data_level = gdp_data_level) |>
    fselect(-gdp_domain) 
  pce <- dl_aux$pce |>
    frename(data_level = pce_data_level) |>
    fselect(-pce_domain) 
  
  ig <- dl_aux$income_groups |>
    fselect(country_code, year, income_group_code)
  
  pfw <- dl_aux$pfw
  setorder(pfw, country_code, welfare_type, reporting_year)
  
  ## Get Cumulative Growth and pass through ------------
  
  passthrough <- .7
  nac <- joyn::joyn(gdp, pce,
                    by = c("country_code", "data_level", "year"),
                    match_type = "1:1",
                    reportvar = FALSE)  |>
    joyn::joyn(ig,
               by = c("country_code", "year"),
               match_type = "m:1",
               reportvar  = FALSE,
               keep = "left") |>
    fgroup_by(country_code, data_level) |>
    fmutate(income_group_code =
              income_group_code |>
              na_locf() |>
              na_focb()) |>
    fungroup() |>
    findex_by(country_code, data_level, year) |>
    ftransform(gdp_gr =  (G(gdp, scale = 1) + 1),
               pce_gr =  (G(pce, scale = 1) + 1)) |>
    unindex() |>
    fgroup_by(country_code, data_level) |>
    fmutate(inc_growth = select_cum_growth(gdp_gr, pce_gr,
                                           inc_group  = income_group_code),
            con_growth = select_cum_growth(gdp_gr, pce_gr,
                                           inc_group   = income_group_code,
                                           passthrough = passthrough)) |>
    fungroup()
  
  # fsubset(nac, country_code == "AGO")
  # fsubset(country_code == "AGO") |>
  
  
  w2k <-
    pfw |>
    fselect(country_code, welfare_type, year = reporting_year)
  
  w2k <-
    w2k[
      , ny := seq_len(.N)
      , by = .(country_code, year)
    ][
      ### remove alternative welfare
      ny == 1
    ][
      , ny := NULL
    ]
  
  
  ## Load survey mean ---------
  
  
  mnt <- dsm |>
    fselect(country_code, 
            year = reporting_year ,
            reporting_level,
            survey_year,
            welfare_type,
            mean = survey_mean_ppp) |>
    joyn::joyn(w2k,
               by = c("country_code", "welfare_type", "year"),
               match_type = "m:1",
               keep = "inner",
               reportvar = FALSE) |>
    # convert to national those with only one obs per year
    # number of data level
    _[, ndl := .N,
      by = c("country_code", "welfare_type", "year")
    ][, data_level := fifelse(ndl == 1, "national", reporting_level)
    ][,
      ndl := NULL]
  
  ## remove national if urb/rur data level is available for the same and remve
  # urb/rur if national available is subsequent years
  # we need to sort to create the correct id... very inefficient
  setorder(mnt, country_code, reporting_level, year)
  
  mn <- mnt |> 
    # number of reporting levels within each country
    fgroup_by(country_code) |>
    fmutate(rlid = groupid(reporting_level), 
            maxrlid  = fmax(rlid)) |> 
    fungroup() |> 
    copy() |> 
    _[,
      # number of rows per year
      nry := .N, 
      by = .(country_code, year)
    ][,
      # identify obs to drop
      # we drop: 
      tokeep := fcase(
        # 1. urban or rural when there is only one obs per year and there is at
        # least one National reporting level in the series (e.g., URY, BOL).
        maxrlid > 1 & nry == 1 & reporting_level != "national", FALSE,
        # 2. National if there are more than one obs per year (e.g., IND).
        maxrlid > 1 & nry > 1 & reporting_level == "national", FALSE, 
        # 3. All the others should not be droped
        default = TRUE
      )
    ]  |> 
    fsubset(tokeep == TRUE) |> 
    fselect(-c(rlid, maxrlid, nry, tokeep)) 
  
  
  
  
  ## expand those with decimal years ----
  
  sy <- mn |>
    fmutate(freq = fifelse(survey_year %% 1 > 0, 2, 1),
            freq = fifelse(is.na(freq), 1, freq)) |>
    # expand for decimnals
    _[rep(seq_len(.N), freq), !"freq"] |>
    # use  as reference year the floow of survey_year
    ftransform(year_floor   = floor(survey_year),
               year_orginal = year) |>
    fgroup_by(country_code, year, data_level, welfare_type) |>
    # Decimals years are in between two regular years, so `yid`
    # identifies them
    fmutate(yid = seqid(survey_year),
            year = fifelse(yid == 2, year_floor + 1, year_floor)) |>
    fungroup() |>
    # fsubset(country_code == "ALB")
    fselect(-c(year_floor))
  
  ## get growth at decimal years ----------
  
  dynac <- joyn::joyn(sy, nac,
                      by = c("country_code", "data_level", "year"),
                      match_type = "m:1",
                      reportvar = FALSE,
                      keep = "left") |>
    ftransform(nac = fifelse(welfare_type == "consumption",
                             con_growth, inc_growth)) |>
    ftransform(dist_weight = fifelse(yid == 1,
                                     1 - (survey_year %% 1),
                                     survey_year %% 1)) |>
    fgroup_by(country_code, reporting_level, data_level, welfare_type, survey_year, mean) |>
    fsummarise(nac_sy = fmean(nac,dist_weight)) |>
    fungroup()
  
  ## expand to reference years --------------
  
  rynac <- tidyr::expand_grid(dynac,
                              reference_year = gls$PIP_REF_YEARS) |>
    qDT() |>
    joyn::joyn(nac,
               by = c("country_code", "data_level", "reference_year = year"),
               match_type = "m:1",
               reportvar = FALSE,
               keep = "left") |>
    ftransform(nac = fifelse(welfare_type == "consumption",
                             con_growth, inc_growth)) |>
    roworderv(c("country_code", "data_level", "reference_year", "survey_year")) |>
    fgroup_by(country_code, data_level) |>
    fmutate(income_group_code =
              income_group_code |>
              na_locf() |>
              na_focb()) |>
    fungroup()
  
  
  # 3. Growth in Reference year ----------
  
  ## svy years per ref year --------
  
  
  byvars <-
    c("country_code",
      "reference_year",
      "data_level"
      # ,
      # "welfare_type"
    )
  
  ## closest surveys
  cvy <- copy(rynac) |> 
    _[
      # Get differences between reference year and svy year
      , diff_year := reference_year - survey_year
    ][ # find if it will be below or above
      , lineup_case := fcase(all(diff_year < 0), "below",
                             all(diff_year > 0), "above",
                             any(diff_year == 0), "svy_year",
                             default = "mixed")
      , by =  byvars
    ][  # create sign for groupoing in next step
      , sign :=
        fcase(
          # "negative",
          lineup_case != "svy_year" & diff_year < 0, -1,
          #"positive",
          lineup_case != "svy_year" & diff_year > 0, 1,
          default = 0)
      , by =  byvars
    ][,
      # find the closest and keep
      keep := fmin(abs(diff_year))*sign == diff_year,
      by = c(byvars, "sign")
    ][
      keep == TRUE
    ][
      , c("diff_year", "sign", "keep") := NULL
    ]
  
  # 4. Mean at the reference year --------
  
  rm <- cvy |>
    fgroup_by(country_code, data_level, welfare_type, reference_year) |>
    ## is growth monotonic ? -------------
  ## # This function is not in `wbpip`
  fmutate(monotonic =
            is_monotonic(svy = nac_sy, ref = nac) |>
            false_if_not_true(),
          # nac and median grow in the same direction
          same_direction =
            wbpip:::is_same_direction(nac_sy, mean) |>
            false_if_not_true()) |>
    
    ### interpolates mean when Same direction happens --------
  fmutate(ref_mean =
            fifelse(monotonic       == TRUE &
                      same_direction  == TRUE &
                      lineup_case     == "mixed",
                    yes = same_dir_mean(svy_mean = mean,
                                        svy_nac  = nac_sy,
                                        ref_nac  = nac),
                    no  = NA)
  ) |>
    fungroup() |>
    
    # Extrapolated and interpolated mean for diverging case ------------
  ftransform(ref_mean = fifelse(is.na(ref_mean),
                                mean*nac/nac_sy,
                                ref_mean)) |>
    # get relative distance for weighted mean
    fgroup_by(country_code,
              data_level,
              welfare_type,
              income_group_code,
              reference_year) |>
    fmutate(relative_distance = relative_distance(ref_year = reference_year,
                                                  svy_year = survey_year)) |>
    fungroup() |>
    # New reference mean
    fselect(
      country_code      ,
      reporting_level,
      welfare_type      ,
      income_group_code ,
      survey_year       ,
      reporting_year = reference_year,
      nac               ,
      ref_mean          ,
      relative_distance
    )
  
}


