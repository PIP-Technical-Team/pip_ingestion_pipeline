# Phase 2: Flatten pipdm + Adopt Canonical DAG

**Status**: COMPLETE  
**Branch**: `refactor/unified-pipeline`  
**Working dir**: `E:\PIP\pipeline_refactor\`

## What was done

### 2a. Flattened R/pipdm/R/ into R/

- Moved 46 R files from `R/pipdm/R/` to `R/`
- Renamed `R/pipdm/R/utils.R` → `R/pipdm_utils.R` to avoid conflict with existing `R/utils.R`
  - pipdm's `utils.R` has: `create_line_up_check`, `check_no_national_survey`, `check_inputs_*`, `uniq_vars_to_attr`
  - main `utils.R` has: `lineup_mean`, `save_estimations`, `get_cache_id`, etc.
- Skipped 2 boilerplate files (not needed when source()'ing directly):
  - `pipdm-package.R` — only `@importFrom` directives and `.onLoad`
  - `utils-data-table.R` — only `@import data.table` roxygen directive
- Removed `R/pipdm/` directory entirely

### 2b. Adopted 2021 _targets.R as canonical

- Backed up old `_targets.R` → `_targets_old_2017.R.bak`
- Created new unified `_targets.R` based on the 2021 version
- Key changes from 2021 original:
  - **PPP year**: derived from `TAR_PROJECT` env var via `switch()` instead of hardcoded
  - **No `withr::with_dir()`**: sources from `./R/` directly (no remote base_dir)
  - **Single `purrr::walk`**: over `./R/` only (no second walk for pipdm)
  - **Sources `_common.R`** directly at `"./_common.R"`
- Full 2021 DAG preserved:
  - `refy_mean_inc_group()` (not old `db_create_ref_year_table`)
  - Targetized cache creation (`create_cache` → `load_cache` → `assert_cache_length`)
  - `region_code` (not `pcn_region_code`)
  - `c("ARG","CHN")` for `urban_rural_countries`
  - `ppp_year = py` in `mp_dl_dist_stats()`
  - Raw PFW for `dt_framework` (not processed)
  - `metaregion` target included
  - No SPL targets

### 2c. Updated _targets.yaml

- Replaced single `main:` project pointing to network share
- Now defines two named projects with local SSD stores:
  ```yaml
  ppp2017:
    store: E:/PIP/pipeline_targets_cache/store_2017
  ppp2021:
    store: E:/PIP/pipeline_targets_cache/store_2021
  ```

### 2d. Fixed mapping.R

- Added missing `force = FALSE` parameter to `create_cache()` function
- Added `&& force == FALSE` to early-return guard (was lost in dev branch divergence)

## Verification

- All 36 functions referenced in `_targets.R` confirmed present in `R/*.R` or `_common.R`
- Parenthesis balance verified: 197 open, 197 close
- 47 R files in `R/` directory (including `00.packages.R` for package loading)

## File inventory (R/)

```
00.packages.R, adjust_aux_values.R, convert_to_qs.R, create_cache_file.R,
db_clean_aux.R, db_clean_data.R, db_compute_dist_stats.R,
db_compute_lineup_headcount.R, db_compute_lineup_median.R,
db_compute_lorenz.R, db_compute_predicted_means.R, db_compute_spl.R,
db_compute_survey_mean.R, db_create_censoring_table.R,
db_create_coverage_table.R, db_create_decomposition_table.R,
db_create_dist_table.R, db_create_dsm_table.R,
db_create_gd_svy_mean_table.R, db_create_lcu_table.R,
db_create_lkup_table.R, db_create_metadata_table.R,
db_create_nac_table.R, db_create_ref_estimation_table.R,
db_create_ref_year_table.R, db_create_reg_pop_table.R,
db_create_svy_estimation_table.R, db_filter_inventory.R,
db_finalize_ref_year_table.R, db_get_closest_surveys.R,
db_merge_anchor_nac.R, db_ppp_list.R, db_select_lineup_surveys.R,
delete_old_file.R, find_new_svy_data.R, from_gd_2_synth.R,
get_ref_mean_pred.R, mapping.R, pip_update_cache_inventory.R,
pipdm_utils.R, process_svy_data_to_cache.R, refy_mean_inc_group.R,
replicate_households.R, save_aux_data.R, save_survey_data.R,
select_proxy.R, ts_cache_read_ok.R, utils.R
```

## Next: Phase 3

Phase 3 items are already partially done (named projects in _targets.yaml).
Remaining Phase 3 work:
- Remove the `stopifnot` store validation from `_common.R` (or update it)
- Update `_common.R` to handle `TAR_PROJECT` properly
- Smoke test: `Sys.setenv(TAR_PROJECT="ppp2021"); source("_targets.R")` should parse without error
- Validate `tar_manifest()` for each project
