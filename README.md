# PIP Ingestion Pipeline

Unified ingestion pipeline for the Poverty and Inequality Platform (PIP).
Produces poverty estimation tables for **both PPP base years (2017 and 2021)**
from a single codebase using the R [`{targets}`](https://docs.ropensci.org/targets/) package.

> Full background documentation is in [Chapter 9 of the PIP Technical Manual](https://pip-technical-team.github.io/PIPmanual/pcpipeline.html#pcpipeline).

---

## Directory structure

```
pipeline_refactor/
├── _targets.R          # Pipeline DAG — single file for both PPP years
├── _targets.yaml       # Named project configs (ppp2017, ppp2021)
├── _common.R           # Globals (gls), tar_option_set(), aux data helpers
├── .Rprofile           # Convenience helpers (run_tar, gca, gp)
├── validate.R          # Post-run comparison against original pipeline outputs
├── R/                  # All pipeline functions (sourced automatically)
│   ├── 00.packages.R   # package::function declarations — no explicit library()
│   ├── mapping.R       # mp_* wrappers + read_cache_survey() lazy reader
│   ├── utils.R         # get_cache_id(), get_cache_files(), check_fs_status(), ...
│   └── *.R             # Remaining pipeline functions (db_*, from_gd_2_synth, ...)
├── doc/
│   └── pipeline_report.Rmd
├── src/
│   └── SelectCumGrowth.cpp
└── .process/           # Refactoring documentation (hand_off.md, phase logs)
```

**Target stores** (local — not in git):

```
E:/PovcalNet/01.personal/wb384996/PIP/pipeline_targets_cache/
├── store_2017/   # {targets} store for ppp2017 project
└── store_2021/   # {targets} store for ppp2021 project
```

---

## How to run

### Prerequisites

- Set `PIP_ROOT_DIR` to the root of the PIP data directory
  (e.g. `//w1wbgencifs01/pip/`).
- Required packages: `targets`, `tarchetypes`, `pipfun`, `pipload`, `wbpip`,
  `joyn`, `data.table`, `collapse`, `fst`, `qs`, `cli`, `fs`, `purrr`.
  No `renv` — packages are managed at the system/user library level.

### Run PPP 2021

```r
Sys.setenv(TAR_PROJECT = "ppp2021")
targets::tar_make()
```

### Run PPP 2017

```r
Sys.setenv(TAR_PROJECT = "ppp2017")
targets::tar_make()
```

### Run both years sequentially

```r
for (proj in c("ppp2021", "ppp2017")) {
  Sys.setenv(TAR_PROJECT = proj)
  targets::tar_make()
}
```

### Inspect the pipeline (without running)

```r
Sys.setenv(TAR_PROJECT = "ppp2021")
targets::tar_outdated()     # which targets are stale
targets::tar_visnetwork()   # interactive DAG
targets::tar_manifest()     # table of all targets and their commands
```

### Convenience helper (defined in `.Rprofile`)

```r
# run_tar("ppp2021") is equivalent to the Sys.setenv + tar_make() pair
run_tar("ppp2021")
run_tar("ppp2017")
```

---

## How it works

The pipeline is parameterised by the `TAR_PROJECT` environment variable:

```
TAR_PROJECT = "ppp2021"  →  py = 2021  →  PPP 2021 tables  →  store_2021/
TAR_PROJECT = "ppp2017"  →  py = 2017  →  PPP 2017 tables  →  store_2017/
```

Both years share one `_targets.R` and one set of `R/` functions.
The two named projects keep their stores fully isolated, so running one year
never invalidates the other.

### Pipeline stages (~66 targets)

| Stage | Key targets | Description |
|-------|-------------|-------------|
| AUX data | `aux_tb`, `dl_aux1`, `dl_aux` | Load and clean auxiliary tables (CPI, PPP, PFW, …) |
| Inventory | `pip_inventory`, `pipeline_inventory`, `cache_inventory` | Build the list of surveys to process |
| Cache files | `cache_ids`, `cache_dir`, `status_cache_files_creation` | Create/validate per-survey `.fst` microdata files |
| Survey means | `gd_means`, `svy_mean_lcu`, `svy_mean_ppp_table` | LCU and PPP-deflated survey means |
| Distributions | `lorenz`, `dl_dist_stats`, `dt_dist_stats` | Lorenz curves and distributional statistics |
| Reference years | `dt_ref_mean_pred` | Interpolated/extrapolated means at reference years |
| Output tables | `dt_prod_ref_estimation`, `dt_prod_svy_estimation` | Final poverty estimation tables |
| Coverage | `dl_coverage`, `dl_censored`, `dt_pop_region` | Coverage and censoring tables |
| Save | `survey_files`, `prod_ref_estimation_file`, … | Write all outputs to the network share |

### Lazy cache loading

Each `mp_*` function reads individual `.fst` microdata files on demand via
`read_cache_survey(path)` instead of loading all ~2500 files into a single
object. This eliminates the ~8 GB memory spike and serialize/deserialize
overhead of the old `cache` target.

---

## Key files

| File | Purpose |
|------|---------|
| `_targets.R` | Full pipeline DAG — edit here to add/modify/remove targets |
| `_common.R` | `gls` globals, `tar_option_set()`, aux helpers |
| `_targets.yaml` | Store paths per named project — edit to change store location |
| `R/mapping.R` | `mp_*` survey-iteration wrappers, `read_cache_survey()` |
| `R/utils.R` | Low-level helpers: `get_cache_id()`, `get_cache_files()`, `check_fs_status()` |
| `validate.R` | Compares this pipeline's outputs against the original pipeline |

---

## Outputs

All outputs are written to the **network share** via `gls$OUT_*` paths
set by `pipfun::pip_create_globals()`. Local stores hold only `{targets}`
metadata and intermediate R objects.

| Target | File | Description |
|--------|------|-------------|
| `prod_ref_estimation_file` | `OUT_EST_DIR_PC/prod_ref_estimation_*.fst` | Reference-year poverty estimates |
| `prod_svy_estimation_file` | `OUT_EST_DIR_PC/prod_svy_estimation_*.fst` | Survey-year poverty estimates |
| `dist_file` | `OUT_EST_DIR_PC/dist_stats_*.fst` | Distribution statistics |
| `survey_mean_file` | `OUT_EST_DIR_PC/survey_means_*.fst` | Survey means |
| `survey_files` | `OUT_SVY_DIR_PC/<cache_id>.fst` | Per-survey welfare/weight/area data |

---

## Development notes

**Branch:** `refactor/unified-pipeline` (git worktree from `pip_ingestion_pipeline`).

**No renv.** Install or update packages manually.

**`cue = tar_cue(mode = "always")` targets:** Three targets intentionally
re-run on every `tar_make()` because `{targets}` cannot detect their upstream
changes automatically:

| Target | Reason |
|--------|--------|
| `dl_aux1` | Reads files from the network share — mtime changes are undetected |
| `fs_status` | Detects new/modified `.fst` cache files (uses custom hashing) |
| `run_time` | Timestamp target; always stale by design |

**Adding a new PPP year:** Add a named project entry to `_targets.yaml`,
create the corresponding store directory, and add the new `py` value to the
`switch()` at the top of `_targets.R`.
