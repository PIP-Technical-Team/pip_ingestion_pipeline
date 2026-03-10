# PIP Ingestion Pipeline — Copilot Instructions

## What This System Does

This is the **PIP (Poverty and Inequality Platform) ingestion pipeline**,
maintained by the World Bank's PIP Technical Team. It transforms raw household
survey microdata and auxiliary economic indicators (CPI, PPP, GDP, PCE,
population) into poverty and inequality estimation tables used by the PIP API and
website (<https://pip.worldbank.org>).

## Architecture

The pipeline uses the R `{targets}` package for orchestration. Key components:

### Data Flow (DAG)

```
AUX data (CPI, PPP, GDP, PCE, pop) ─┐
                                     ├─> format_aux_data(py) ─> dl_aux
Survey microdata (.dta files)  ──────┤
                                     ├─> create_cache_file() ─> .fst cache
                                     ├─> from_gd_2_synth() ─> synthetic data
                                     └─> cache_inventory ─> cache (list of DTs)
                                              │
                    ┌────────────────────────┬─┴──────────────────────┐
                    │                        │                        │
              gd_means + cache         cache only               cache only
                    │                        │                        │
              svy_mean_lcu              mp_lorenz            mp_dl_dist_stats
                    │                        │                        │
           svy_mean_lcu_table          lorenz (list)         dl_dist_stats
                    │                                              │
          svy_mean_ppp_table (DSM) ──────────────────> dt_dist_stats
                    │                                              │
         refy_mean_inc_group() ─────────> dt_ref_mean_pred         │
                    │                           │                  │
                    │                    dt_prod_ref_estimation <───┘
                    │                           │
                    └────────> dt_prod_svy_estimation
                                        │
                               Save to .fst / .dta / .qs
```

### Multi-PPP-Year Support

The pipeline must produce outputs for **two PPP base years** (2017 and 2021).
This is implemented using `{targets}` **named projects** configured in
`_targets.yaml`:

- **Project `ppp2017`:** `py = 2017`, store at
  `E:\PIP\pipeline_targets_cache\store_2017`
- **Project `ppp2021`:** `py = 2021`, store at
  `E:\PIP\pipeline_targets_cache\store_2021`

To run a specific PPP year:

```r
Sys.setenv(TAR_PROJECT = "ppp2021")
targets::tar_make()
```

The PPP year (`py`) is determined from `TAR_PROJECT` at the top of `_targets.R`
and flows through:

- `pipfun::pip_create_globals()` → all directory paths
- `format_aux_data()` → CPI column, PPP table, poverty lines, country profiles
- `from_gd_2_synth()` → bottom censoring threshold
- `mp_dl_dist_stats()` → distributional statistics

### Directory Structure

```
R/                      # All R functions (flattened from former R/pipdm/R/)
  00.packages.R         # Package loading and conflict resolution
  mapping.R             # Core mapping functions (mp_cache, mp_lorenz, etc.)
  utils.R               # Utility functions (lineup_mean, save_estimations, etc.)
  convert_to_qs.R       # File format conversion
  db_*.R                # Pipeline computation functions (from former pipdm)
  create_cache_file.R   # Cache creation
  from_gd_2_synth.R     # Group data to synthetic microdata
  refy_mean_inc_group.R # Reference year mean prediction (canonical algorithm)
  ...
src/                    # C++ source (SelectCumGrowth.cpp)
_targets.R              # Pipeline DAG definition (parameterized by py)
_targets.yaml           # Named project configuration (ppp2017, ppp2021)
_common.R               # Shared setup: globals, tar_option_set, helper functions
_packages.R             # Package loading (same as 00.packages.R, legacy)
.process/               # Refactoring documentation
  hand_off.md           # Master hand-off for AI session continuity
  phases/               # Per-phase logs
```

### Key External Packages

| Package | Source | Purpose |
|---------|--------|---------|
| `pipfun` | PIP-Technical-Team/pipfun | `pip_create_globals()` — path construction |
| `pipload` | PIP-Technical-Team/pipload | `pip_load_aux()`, `pip_find_data()` — data loading |
| `wbpip` | PIP-Technical-Team/wbpip | `gd_clean_data()`, `sd_create_synth_vector()` |
| `joyn` | randrescastaneda/joyn | Data joins with match-type validation |
| `data.table` / `collapse` | CRAN | Core data manipulation |
| `fst` / `qs` | CRAN | Serialization formats |

### Data Locations

- **Input data:** `//w1wbgencifs01/pip/PIP-Data_QA/` (network share, read-only)
- **Cache files:** `//w1wbgencifs01/pip/pip_ingestion_pipeline/pc_data/cache/`
- **Output data:** `//w1wbgencifs01/pip/pip_ingestion_pipeline/pc_data/output-tfs-sync/`
  (under vintage subdirectory like `20260324_2021_01_02_PROD/`)
- **Target stores:** `E:\PIP\pipeline_targets_cache\store_{year}` (local SSD)

## Coding Conventions

- **Data manipulation:** `data.table` syntax (`:=`, `.SD`, `.()`), not
  `dplyr`/tidyverse
- **Joins:** `joyn::joyn()` with explicit match types, not `merge()` or
  `*_join()`
- **Pipes:** Base R `|>` pipe (not `%>%`)
- **Iteration:** `purrr::map()` family
- **File I/O:** `fst::read_fst()` / `write_fst()` for tabular data,
  `qs::qsave()` / `qread()` for complex objects
- **CLI output:** `cli::cli_alert()`, `cli::cli_progress_step()` for user
  feedback
- **Error handling:** `tryCatch()` with explicit error/warning handlers
- **Package conflicts:** Resolved via `conflicted` in `00.packages.R`

## Refactoring Plan (Current)

The system is being refactored from three duplicate repositories into one
unified, parameterized pipeline. See `.process/hand_off.md` for current status
and phase details.

### Phases

1. ✅ Infrastructure (worktree, docs)
2. Flatten `R/pipdm/R/` into `R/`
3. Named projects + parameterize `py`
4. Remove `cue = "always"`, fix dependency chains
5. Lazy cache loading with dynamic branching
6. Validation against production outputs
