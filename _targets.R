#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# PIP Ingestion Pipeline — Unified _targets.R
#
# This single script serves both PPP years (2017 and 2021).
# The PPP year is determined by the TAR_PROJECT environment variable:
#
#   Sys.setenv(TAR_PROJECT = "ppp2021")
#   targets::tar_make()
#
# See _targets.yaml for project configuration.
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1: Determine PPP year from TAR_PROJECT ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

tar_project <- Sys.getenv("TAR_PROJECT", unset = "ppp2021")

py <- switch(
  tar_project,
  ppp2017 = 2017,
  ppp2021 = 2021,
  stop(
    "Unknown TAR_PROJECT: '",
    tar_project,
    "'. Must be 'ppp2017' or 'ppp2021'."
  )
)

cli::cli_alert_info(
  "Running pipeline for PPP year {.val {py}} (TAR_PROJECT = {.val {tar_project}})"
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2: Set defaults ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

branch <- "DEV"
release <- "20260324"
identity <- "PROD"
max_year_country <- 2025
max_year_aggregate <- 2026
max_year_lineup <- 2026

## CMD (Cross-country Missing Data) settings
cmd_coeff_branch <- "2026_03_update" # branch in aux_missing_countries repo
cmd_n_quantiles <- 10000L # number of CMD quantiles per distribution

## Filter creation of synth data (NULL = all)
cts <- yrs <- NULL

## Save/force flags
force_create_cache_file <- FALSE
save_pip_update_cache_inventory <- FALSE
force_gd_2_synth <- FALSE


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3: Source R functions ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Load all R functions (includes packages via 00.packages.R)
purrr::walk(
  fs::dir_ls(path = "./R", regexp = "\\.R$"),
  source
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4: Load globals and set options ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

source("./_common.R", echo = FALSE)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 5: Run pipeline ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

list(
  # AUX data ------------------

  tar_target(
    aux_tb,
    prep_aux_data(maindir = gls$PIP_DATA_DIR, branch = branch)
  ),

  # Load aux data
  # NOTE: cue=always because aux files live on network share and can change
  # without {targets} detecting it (no format="file" tracking on inputs)
  tar_target(dl_aux1, load_aux_data(aux_tb), cue = tar_cue(mode = "always")),

  # Format data (filter by PPP year)
  tar_target(dl_aux, format_aux_data(dl_aux1, py)),

  tar_target(aux_versions, get_aux_versions(dl_aux)),

  # CACHE data ------------

  # Load PIP inventory
  tar_target(
    pip_inventory_file,
    fs::path(gls$PIP_DATA_DIR, '_inventory/inventory.fst'),
    format = "file"
  ),

  tar_target(pip_inventory, load_pip_inventory(pip_inventory_file)),

  # Load PIPELINE inventory file
  tar_target(
    pipeline_inventory,
    db_filter_inventory(dt = pip_inventory, pfw_table = dl_aux$pfw) |>
      _[module != "PC-GROUP"]
  ),

  # NOTE: cue=always because this detects external filesystem changes
  # (file modification times) that {targets} cannot track
  tar_target(
    fs_status,
    check_fs_status(
      dir_path = fs::path(gls$CACHE_SVY_DIR_PC),
      fs_paths = as.character(pipeline_inventory$orig),
      name = "fs_status"
    ),
    cue = tar_cue(mode = "always")
  ),

  tar_target(
    old_cache_id_deleted,
    delete_old_cache_id(
      fs_status = fs_status,
      pipeline_inventory = pipeline_inventory,
      gls
    )
  ),

  # Create microdata cache files
  tar_target(
    status_cache_files_creation,
    create_cache_file(
      pipeline_inventory = pipeline_inventory,
      pip_data_dir = gls$PIP_DATA_DIR,
      tool = "PC",
      cache_svy_dir = gls$CACHE_SVY_DIR_PC,
      compress = gls$FST_COMP_LVL,
      force = force_create_cache_file,
      verbose = TRUE,
      cpi_table = dl_aux$cpi,
      ppp_table = dl_aux$ppp,
      pfw_table = dl_aux$pfw,
      pop_table = dl_aux$pop
    )
  ),

  # Create synthetic cache files
  tar_target(
    pipeline_inventory2,
    from_gd_2_synth(
      dl_aux = dl_aux,
      gls = gls,
      pipeline_inventory = pipeline_inventory,
      force = force_gd_2_synth,
      cts = cts,
      yrs = yrs
    )
  ),

  tar_target(
    cache_inventory1,
    pip_update_cache_inventory(
      pipeline_inventory = pipeline_inventory2,
      pip_data_dir = gls$PIP_DATA_DIR,
      cache_svy_dir = gls$CACHE_SVY_DIR_PC,
      tool = "PC",
      save = save_pip_update_cache_inventory,
      load = TRUE,
      verbose = TRUE
    )
  ),

  # Filter cache inventory with PFW
  tar_target(cache_inventory, filter_cache_inventory(cache_inventory1, dl_aux)),

  tar_target(cache_ids, get_cache_id(cache_inventory)),
  tar_target(cache_dir, get_cache_files(cache_inventory)),

  # ---- Lazy cache: no monolithic .qs serialize/deserialize ----
  # Downstream targets (svy_mean_lcu, lorenz, dl_dist_stats,
  # survey_files) now read each .fst file on demand via
  # read_cache_survey(), eliminating the create_cache/load_cache
  # bottleneck. cache_dir and cache_ids are used directly.

  ## Mean estimates ------------

  ### Fetch GD survey means and convert them to daily values ----
  tar_target(
    gd_means,
    get_groupdata_means(cache_inventory = cache_inventory, gdm = dl_aux$gdm),
    iteration = "list"
  ),

  ### LCU survey mean list ----
  tar_target(
    svy_mean_lcu,
    mp_svy_mean_lcu(cache_dir, cache_ids, gd_means)
  ),

  ### LCU table ------
  tar_target(
    svy_mean_lcu_table,
    db_create_lcu_table(
      dl = svy_mean_lcu,
      pop_table = dl_aux$pop,
      pfw_table = dl_aux$pfw
    )
  ),

  ### Deflated survey mean (DSM) table ----
  tar_target(
    svy_mean_ppp_table,
    db_create_dsm_table(
      lcu_table = svy_mean_lcu_table,
      cpi_table = dl_aux$cpi,
      ppp_table = dl_aux$ppp
    )
  ),

  ### Reference year mean table ------
  tar_target(
    dt_ref_mean_pred,
    refy_mean_inc_group(
      dsm = svy_mean_ppp_table,
      gls = gls,
      dl_aux = dl_aux,
      pinv = pipeline_inventory2
    )
  ),

  ## Distributional stats ----

  ### Lorenz curves (for microdata) ----
  tar_target(
    lorenz,
    mp_lorenz(cache_dir, cache_ids)
  ),

  ### Dist statistics list ------
  tar_target(
    dl_dist_stats,
    mp_dl_dist_stats(
      cache_dir = cache_dir,
      cache_ids = cache_ids,
      mean_table = svy_mean_ppp_table,
      pop_table = dl_aux$pop,
      ppp_year = py
    )
  ),

  ### Dist stat table ------
  tar_target(
    dt_dist_stats,
    db_create_dist_table(
      dl = dl_dist_stats,
      dsm_table = svy_mean_ppp_table,
      crr_inv = cache_inventory
    )
  ),

  ## Output tables --------

  ### Reference year estimation table ----
  tar_target(
    dt_prod_ref_estimation,
    db_create_ref_estimation_table(
      ref_year_table = dt_ref_mean_pred,
      dist_table = dt_dist_stats
    )
  ),

  ### Survey year estimation table ----
  tar_target(
    dt_prod_svy_estimation,
    db_create_svy_estimation_table(
      dsm_table = svy_mean_ppp_table,
      dist_table = dt_dist_stats,
      gdp_table = dl_aux$gdp,
      pce_table = dl_aux$pce
    )
  ),

  ## Coverage and censoring -------

  ### Coverage table by region ----
  tar_target(
    dl_coverage,
    db_create_coverage_table(
      ref_year_table = dt_ref_mean_pred,
      pop_table = dl_aux$pop,
      cl_table = dl_aux$country_list,
      incgrp_table = dl_aux$income_groups,
      ref_years = gls$PIP_REF_YEARS,
      urban_rural_countries = c("ARG", "CHN"),
      digits = 2,
      gls = gls
    )
  ),

  ### Censoring table -------
  tar_target(
    dl_censored,
    db_create_censoring_table(
      censored = dl_aux$censoring,
      coverage_list = dl_coverage,
      coverage_threshold = 50
    )
  ),

  ### Regional population table ----
  tar_target(
    dt_pop_region,
    db_create_reg_pop_table(
      pop_table = dl_aux$pop,
      cl_table = dl_aux$country_list,
      region_code = 'region_code',
      pip_years = gls$PIP_REF_YEARS
    )
  ),

  ### Decomposition table ----
  tar_target(
    dt_decomposition,
    db_create_decomposition_table(
      dsm_table = svy_mean_ppp_table
    )
  ),

  ##  Clean AUX data ------

  tar_target(
    all_aux,
    list(dl_aux$cpi, dl_aux$gdp, dl_aux$pop, dl_aux$ppp, dl_aux$pce),
    iteration = "list"
  ),

  tar_target(
    aux_names,
    c("cpi", "gdp", "pop", "ppp", "pce"),
    iteration = "list"
  ),

  tar_target(
    aux_clean,
    db_clean_aux(all_aux, aux_names, pip_years = gls$PIP_YEARS),
    pattern = map(all_aux, aux_names),
    iteration = "list"
  ),

  # Framework data (raw PFW)
  tar_target(
    dt_framework,
    (dl_aux$pfw)
  ),

  #~~~~~~~~~~~~~~~~~~~~~~
  ## Save data ----

  ### Survey data ------
  tar_target(
    survey_files,
    mp_survey_files(
      cache_dir = cache_dir,
      cache_ids = cache_ids,
      output_dir = gls$OUT_SVY_DIR_PC,
      cols = c('welfare', 'weight', 'area'),
      compress = gls$FST_COMP_LVL
    )
  ),

  ### Basic AUX data ----
  tar_target(aux_out_files, aux_out_files_fun(gls$OUT_AUX_DIR_PC, aux_names)),
  tar_target(
    aux_out,
    fst::write_fst(
      x = aux_clean,
      path = aux_out_files,
      compress = gls$FST_COMP_LVL
    ),
    pattern = map(aux_clean, aux_out_files),
    iteration = "list"
  ),

  ### Additional AUX files ----

  #### Countries -----------
  tar_target(
    countries_out,
    save_aux_data(
      dl_aux$countries,
      fs::path(gls$OUT_AUX_DIR_PC, "countries.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Countries with missing data ------------
  tar_target(
    missing_data_out,
    save_aux_data(
      dl_aux$missing_data,
      fs::path(gls$OUT_AUX_DIR_PC, "missing_data.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Country List ---------
  tar_target(
    country_list_out,
    save_aux_data(
      dl_aux$country_list,
      fs::path(gls$OUT_AUX_DIR_PC, "country_list.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Regions -----------
  tar_target(
    regions_out,
    save_aux_data(
      dl_aux$regions,
      fs::path(gls$OUT_AUX_DIR_PC, "regions.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Country profiles  ------------
  tar_target(
    country_profiles_out,
    save_aux_data(
      dl_aux$cp,
      fs::path(gls$OUT_AUX_DIR_PC, "country_profiles.rds"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Poverty lines ---------
  tar_target(
    poverty_lines_out,
    save_aux_data(
      dl_aux$pl,
      fs::path(gls$OUT_AUX_DIR_PC, "poverty_lines.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  tar_target(
    national_poverty_lines_out,
    save_aux_data(
      dl_aux$npl,
      fs::path(gls$OUT_AUX_DIR_PC, "national_poverty_lines.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Survey metadata (for Data Sources page) --------
  tar_target(
    survey_metadata_out,
    save_aux_data(
      dl_aux$metadata,
      fs::path(gls$OUT_AUX_DIR_PC, "survey_metadata.rds"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Indicators ----------
  tar_target(
    indicators_out,
    save_aux_data(
      dl_aux$indicators,
      fs::path(gls$OUT_AUX_DIR_PC, "indicators.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  ### Coverage files ----

  #### Regional population ---------
  tar_target(
    pop_region_out,
    save_aux_data(
      dt_pop_region,
      fs::path(gls$OUT_AUX_DIR_PC, "pop_region.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Regional coverage  ----------
  tar_target(
    region_year_coverage_out,
    save_aux_data(
      dl_coverage$region,
      fs::path(gls$OUT_AUX_DIR_PC, "region_coverage.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Income group coverage ---------
  tar_target(
    incomeGroup_year_coverage_out,
    save_aux_data(
      dl_coverage$incgrp,
      fs::path(gls$OUT_AUX_DIR_PC, "incgrp_coverage.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Country year coverage --------
  tar_target(
    country_year_coverage_out,
    save_aux_data(
      dl_coverage$country_year_coverage,
      fs::path(gls$OUT_AUX_DIR_PC, "country_coverage.fst"),
      compress = TRUE
    )
  ),

  ### Censored  -----
  tar_target(
    censored_out,
    save_aux_data(
      dl_censored,
      fs::path(gls$OUT_AUX_DIR_PC, "censored.rds"),
      compress = TRUE
    ),
    format = 'file'
  ),

  ### Decomposition master --------
  tar_target(
    decomposition_out,
    save_aux_data(
      dt_decomposition,
      fs::path(gls$OUT_AUX_DIR_PC, "decomposition.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  #### Framework data --------------
  tar_target(
    framework_out,
    save_aux_data(
      dt_framework,
      fs::path(gls$OUT_AUX_DIR_PC, "framework.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  ### Dictionary -------------
  tar_target(
    dictionary_out,
    save_aux_data(
      dl_aux$dictionary,
      fs::path(gls$OUT_AUX_DIR_PC, "dictionary.fst"),
      compress = TRUE
    ),
    format = 'file'
  ),

  ### Estimation tables -------

  tar_target(
    prod_ref_estimation_file,
    save_estimations(
      dt = dt_prod_ref_estimation,
      dir = gls$OUT_EST_DIR_PC,
      name = "prod_ref_estimation",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = 'file'
  ),

  tar_target(
    prod_svy_estimation_file,
    save_estimations(
      dt = dt_prod_svy_estimation,
      dir = gls$OUT_EST_DIR_PC,
      name = "prod_svy_estimation",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = 'file'
  ),

  ###  Lorenz list ----
  tar_target(
    lorenz_out,
    save_aux_data(
      lorenz,
      fs::path(gls$OUT_AUX_DIR_PC, "lorenz.rds"),
      compress = TRUE
    ),
    format = 'file'
  ),

  ### Dist stats table ----
  tar_target(
    dist_file,
    save_estimations(
      dt = dt_dist_stats,
      dir = gls$OUT_EST_DIR_PC,
      name = "dist_stats",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = 'file'
  ),

  ### Survey means table ----
  tar_target(
    survey_mean_file,
    save_estimations(
      dt = svy_mean_ppp_table,
      dir = gls$OUT_EST_DIR_PC,
      name = "survey_means",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = 'file'
  ),

  tar_target(
    survey_mean_file_aux,
    save_estimations(
      dt = svy_mean_ppp_table,
      dir = gls$OUT_AUX_DIR_PC,
      name = "survey_means",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = 'file'
  ),

  tar_target(
    aux_versions_out,
    save_estimations(
      dt = aux_versions,
      dir = gls$OUT_AUX_DIR_PC,
      name = "aux_versions",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = 'file'
  ),

  ### Interpolated means table ----
  tar_target(
    interpolated_means_file,
    save_estimations(
      dt = dt_ref_mean_pred,
      dir = gls$OUT_EST_DIR_PC,
      name = "interpolated_means",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = 'file'
  ),

  tar_target(
    interpolated_means_file_aux,
    save_estimations(
      dt = dt_ref_mean_pred,
      dir = gls$OUT_AUX_DIR_PC,
      name = "interpolated_means",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = 'file'
  ),

  ### Metaregion --------------
  tar_target(
    metaregion_file_aux,
    save_estimations(
      dt = dl_aux$metaregion,
      dir = gls$OUT_AUX_DIR_PC,
      name = "metaregion",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = 'file'
  ),

  ### Data timestamp file ----
  # NOTE: cue=always so timestamp reflects actual run time
  tar_target(
    run_time,
    as.character(Sys.time()),
    cue = tar_cue(mode = "always")
  ),

  tar_target(
    data_timestamp_file,
    writeLines(
      run_time,
      fs::path(
        gls$OUT_DIR_PC,
        gls$vintage_dir,
        "data_update_timestamp",
        ext = "txt"
      )
    )
  ),

  ## Convert AUX files to qs ---------
  tar_target(
    aux_qs_out,
    convert_to_qs(dir = gls$OUT_AUX_DIR_PC)
  ),

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # CMD (Cross-country Missing Data) targets ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  # Download CMD coefficients from GitHub
  tar_target(
    cmd_coeff,
    load_cmd_coeff(branch = cmd_coeff_branch)
  ),

  # Record the resolved GitHub commit SHA for the coefficient branch.
  # This pins the exact data version used and ensures reproducibility even
  # if the branch HEAD moves.
  tar_target(
    cmd_coeff_version,
    {
      ref_url <- paste(
        "https://api.github.com/repos/PIP-Technical-Team/aux_missing_countries",
        "git/ref/heads",
        cmd_coeff_branch,
        sep = "/"
      )
      resp <- httr::GET(
        ref_url,
        httr::add_headers(Accept = "application/vnd.github+json")
      )
      if (!httr::http_error(resp)) {
        httr::content(resp)$object$sha
      } else {
        NA_character_
      }
    }
  ),

  # Build logit-transformed quantile vector
  tar_target(
    cmd_qs,
    calc_cmd_quantiles(n = cmd_n_quantiles)
  ),

  # Identify missing-data countries from the auxiliary missing_data table.
  # Columns: country_code, year, reporting_pop, region_code, welfare_type.
  # The id column ("CC_YYYY") is used as the .fst filename key downstream.
  tar_target(
    cmd_md_countries,
    dl_aux$missing_data |>
      fmutate(id = paste(country_code, year, sep = "_"))
  ),

  # Estimate CMD distributions, write .fst files to lineup_data/, and
  # accumulate per-country dist_stats; return as a hashable data.table.
  tar_target(
    cmd_dist_out,
    {
      fs::dir_create(gls$OUT_LINEUP_DIR_PC)
      # Select the PPP-year-specific coefficient table
      CF <- cmd_coeff[[paste0("ppp", py)]]
      env_acc <- new.env(parent = emptyenv())
      estimate_and_write_full_cmd(
        md = cmd_md_countries,
        CF = CF,
        qs = cmd_qs,
        py = py,
        dir = fs::path(gls$OUT_DIR_PC, gls$vintage_dir),
        env_acc = env_acc
      )
      rowbind(as.list(env_acc))
    }
  ),

  # Assemble CMD dist-stats and join welfare_type from the missing_data aux
  tar_target(
    cmd_dist_stats,
    joyn::left_join(
      x = cmd_dist_out,
      y = cmd_md_countries[, .(
        country_code,
        reporting_year = year,
        welfare_type
      )],
      by = c("country_code", "reporting_year"),
      reportvar = FALSE,
      verbose = FALSE
    )
  ),

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Lineup distribution targets ---------
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  # Prepare the reference-year table for use in lineup estimation
  tar_target(
    df_refy_lineups,
    prep_df_refy_for_lineups(dt_prod_ref_estimation)
  ),

  # Compute multiplication factors (extrapolation / interpolation)
  # Uses the full dt_prod_ref_estimation (not the deduplicated df_refy_lineups)
  # because cache_id must remain intact for loading survey microdata.
  tar_target(
    df_refy_mult,
    get_refy_mult_factor(dt_prod_ref_estimation)
  ),

  # Reference-year table formatted for the lineup estimations output file
  tar_target(
    lineup_prod_refy_estimation,
    prep_df_refy_for_lineups(data.table::copy(df_refy_mult))
  ),

  # Lookup table of all lineup reference years
  tar_target(
    lineup_years_dt,
    data.table(lineup_years = gls$PIP_LINEUP_YEARS)
  ),

  # Build the full list of country-year combinations to estimate.
  # iteration = "list" is required so that pattern = map(lineup_full_list)
  # in lineup_dist_country receives one element per branch.
  tar_target(
    lineup_full_list,
    get_full_list(
      lineup_years = gls$PIP_LINEUP_YEARS,
      df_refy = df_refy_mult
    ),
    iteration = "list"
  ),

  # Per-country dynamic branch: one branch per element of lineup_full_list
  # (172 branches total). Each branch processes all reference years for one
  # country, writes the .fst files, and returns a data.table of dist-stats.
  # Only countries whose upstream data changed are re-executed on incremental
  # runs, cutting re-run time from ~58 min to seconds.
  tar_target(
    lineup_dist_country,
    {
      process_country_lineup(
        country_entry = lineup_full_list,
        df_refy = df_refy_mult,
        path = gls$OUT_LINEUP_DIR_PC,
        gls = gls,
        dl_aux = dl_aux,
        py = py
      )
    },
    pattern = map(lineup_full_list),
    iteration = "list"
  ),

  # Combine per-country dist-stats into a single table.
  # Produces the same output shape as the former monolithic lineup_dist_out.
  tar_target(
    lineup_dist_out,
    rowbind(lineup_dist_country, use.names = TRUE)
  ),

  # Add welfare_type to lineup dist-stats from the reference-year table
  # (get_dist_stats() does not include welfare_type; pipeline-run.R
  #  joins it from df_refy afterwards)
  tar_target(
    lineup_dist_out_wt,
    {
      wt_lookup <- df_refy_mult[,
        .(country_code, reporting_year, welfare_type, reporting_level)
      ] |>
        funique()

      joyn::left_join(
        x = lineup_dist_out,
        y = wt_lookup,
        by = c("country_code", "reporting_year", "reporting_level"),
        reportvar = FALSE,
        verbose = FALSE
      )
    }
  ),

  # Merge lineup dist-stats with CMD dist-stats into a single table
  tar_target(
    lineup_dist_stats,
    rowbind(
      lineup_dist_out_wt,
      cmd_dist_stats,
      use.names = TRUE,
      fill = TRUE
    ) |>
      setorder(country_code, reporting_year)
  ),

  # Save the merged dist-stats summary table to estimations/
  tar_target(
    lineup_dist_stats_file,
    save_estimations(
      dt = lineup_dist_stats,
      dir = gls$OUT_EST_DIR_PC,
      name = "lineup_dist_stats",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = "file"
  ),

  # Save the reference-year estimation table used for lineups
  tar_target(
    lineup_prod_refy_file,
    save_estimations(
      dt = lineup_prod_refy_estimation,
      dir = gls$OUT_EST_DIR_PC,
      name = "prod_refy_estimation",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = "file"
  ),

  # Save the lineup reference-year lookup table
  tar_target(
    lineup_years_file,
    save_estimations(
      dt = lineup_years_dt,
      dir = gls$OUT_EST_DIR_PC,
      name = "lineup_years",
      time = gls$TIME,
      compress = gls$FST_COMP_LVL
    ),
    format = "file"
  )
)
