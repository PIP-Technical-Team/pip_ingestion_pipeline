refy_mean_inc_group <- \(dsm, gls, dl_aux, pinv) {
  
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
  
  pop <- dl_aux$pop |>
    # THis naming is different because it is merged way down in the process
    frename(reporting_level = pop_data_level, 
            reference_year  = year) |>
    fselect(-pop_domain) 
  
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
    roworderv(c("country_code", "data_level", "year")) |> 
    findex_by(country_code, data_level, year) |>
    ftransform(gdp_gr =  (G(gdp, scale = 1, shift = "row") + 1),
               pce_gr =  (G(pce, scale = 1, shift = "row") + 1)) |>
    unindex() |>
    fgroup_by(country_code, data_level) |>
    fmutate(inc_growth = select_cum_growth(gdp_gr, pce_gr,
                                           inc_group  = income_group_code),
            con_growth = select_cum_growth(gdp_gr, pce_gr,
                                           inc_group   = income_group_code,
                                           passthrough = passthrough)) |>
    fungroup()
  
  #temporal solution
  setkey(nac, NULL)
  
  # reduced Price framework data
  rpfw <- pfw |>
    fselect(country_code, survey_acronym, surveyid_year, year = reporting_year) |> 
    collapse::funique()
  
  w2k <- pinv |> 
    ftransform(welfare_type = fifelse(grepl("_INC_", cache_id), "income", "consumption")) |> 
    fselect(country_code, welfare_type, survey_acronym, surveyid_year) |> 
    joyn::joyn(rpfw,
               by = c("country_code", "survey_acronym", "surveyid_year"), 
               match_type = "m:1", 
               keep = "inner", 
               reportvar = FALSE
    ) |>
    fselect(country_code, welfare_type, year) |> 
    roworder(country_code, welfare_type, year) 
  
  #temporal solution
  setkey(w2k, NULL)
  
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
  
  #temporal solution
  setkey(mnt, NULL)
  
  
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
  
  #temporal solution
  setkey(dynac, NULL)
  
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
  
  #temporal solution
  setkey(rynac, NULL)
  
  
  # 3. Growth in Reference year ----------
  
  ## svy years per ref year --------
  
  byvars <-
    c("country_code",
      "data_level",
      "reference_year"
      # ,
      # "welfare_type"
    )
  rr <- 
    rynac |> 
    # Get differences between reference year and svy year
    ftransform(diff_year = reference_year - survey_year) |> 
    fgroup_by(c(byvars)) |>
    fmutate(survey_select = fifelse(any(diff_year == 0), TRUE, FALSE)) |> 
    fgroup_by(c(byvars, "welfare_type")) |> 
    fmutate(lineup_case = fcase(all(diff_year < 0), "below",
                                all(diff_year > 0), "above",
                                default = "mixed")) |> 
    fungroup() |>
    ftransform(lineup_case = fifelse(survey_select == TRUE & lineup_case == "mixed",
                                     "svy_year", lineup_case)) |> 
    ftransform(sign = fcase(
      lineup_case != "svy_year" & diff_year < 0, -1,
      lineup_case != "svy_year" & diff_year > 0, 1,
      default = 0)) |> 
    
    # get all min differences between years
    fgroup_by(c(byvars,"sign")) |>  # 
    fmutate(min_diff_sign = fmin(abs(diff_year))) |> 
    
    fgroup_by(c(byvars,"welfare_type")) |>  # 
    fmutate(min_diff_welfare = fmin(abs(diff_year))) |> 
    
    fgroup_by(c(byvars,"lineup_case")) |>  # 
    fmutate(min_diff_case = fmin(abs(diff_year))) |> 
    
    fgroup_by(c(byvars, "welfare_type", "lineup_case")) |>  # 
    fmutate(min_diff_welfare_case = fmin(abs(diff_year))) |> 
    
    fgroup_by(c(byvars, "welfare_type", "sign")) |>  # 
    fmutate(min_diff_welfare_sign = fmin(abs(diff_year))) |> 
    
    fgroup_by(c(byvars)) |>  # 
    fmutate(min_diff = fmin(abs(diff_year))) |> 
    fungroup()
  
  # Obs to keep
  cvy <- copy(rr) |> 
    ftransform(keep = fcase(
      #min diff by welfare and sign if mixed (to filter more later)
      lineup_case == "mixed" & min_diff_welfare_sign*sign == diff_year, TRUE,
      # min diff if above or below (independent of welfare type)
      lineup_case %in% c("below","above") & min_diff*sign == diff_year, TRUE,
      # survey year
      lineup_case == "svy_year" & diff_year == 0, TRUE,
      default = FALSE)) |>
    # Duplicate welfare type in mixed cases
    # fgroup_by(c(byvars, "keep")) |> 
    fgroup_by(c(byvars, "keep")) |> 
    fmutate(dup_wt = NROW(lineup_case)) |> 
    fgroup_by(c(byvars, "keep", "lineup_case")) |> 
    fmutate(ulc = NROW(lineup_case)) |> 
    fungroup() |> 
    # drop income cases where there ref year has both income and consumption
    # surveys below and above
    ftransform(keep = fifelse(lineup_case == "mixed" 
                              & keep == TRUE 
                              & dup_wt == 4
                              & welfare_type != "consumption", 
                              FALSE, keep)) |>
    # drop cases where only one welfare type is close to the ref year but the
    # other welfare type is both above and below (mixed)
    ftransform(keep = fifelse(lineup_case != "mixed" 
                              & keep == TRUE 
                              & dup_wt == 3
                              & ulc == 1, 
                              FALSE, keep)) |>
    # select consumption over income when both inc and consumption are equally
    # appart from the ref year
    ftransform(keep = fifelse(keep == TRUE 
                              & dup_wt == 2
                              & ulc == 1 
                              & welfare_type != "consumption",  
                              FALSE, keep)) |>
    fsubset(keep == TRUE) |> 
    ## Add population 
    joyn::joyn(pop,
               by = c("country_code", "reporting_level", "reference_year"),
               match_type = "m:1",
               keep       = "inner", 
               reportvar = FALSE)  
  
  #temporal solution
  setkey(cvy, NULL)
  
  # Delte temporary vars
  vars_to_del <-
    c(
      "dup_wt",
      "ulc",
      "keep",
      "survey_select",
      "diff_year",
      "sign",
      grep("^min_diff", names(cvy), value = TRUE)
    )
  
  get_vars(cvy, vars_to_del) <- NULL
  
  
  
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
    ftransform(estimation_type  = fcase(lineup_case == "mixed", "interpolation", 
                                        lineup_case == "svy_year" , "survey", 
                                        default = "extrapolation")) |> 
    ftransform(interpolation_id = paste(country_code, reference_year, reporting_level, 
                                        sep =  "_"))  |> 
    # New reference mean
    fselect(
      country_code      ,
      reporting_level,
      welfare_type      ,
      income_group_code ,
      survey_year       ,
      reporting_year = reference_year,
      nac               ,
      nac_sy,
      relative_distance, 
      estimation_type, 
      interpolation_id, 
      predicted_mean_ppp = ref_mean,
      reporting_gdp = gdp, 
      reporting_pce = pce, 
      reporting_pop = pop,
      monotonic, 
      same_direction
    )
  
  
  
  out <- joyn::joyn(
    x = rm,
    y = dsm |> # remove survey year population to merge reference year pop
      fselect(-reporting_pop),
    by = c(
      "country_code",
      "reporting_level",
      "welfare_type",
      "survey_year"),
    match_type = "m:1",
    reportvar = FALSE,
    keep = "left"
  ) |> 
    ftransform(is_interpolated = fifelse(estimation_type  != "survey", TRUE, FALSE)) |> 
    _[, 
      reporting_pop := fmean(reporting_pop, w = relative_distance),
      by = c("country_code", "reporting_level", "reporting_year", "welfare_type")]
  
  #temporal solution
  setkey(out, NULL)
  
  out
}


