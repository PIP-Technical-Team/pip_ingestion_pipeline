# Phase 3: Smoke Test + Named Projects Validation

**Status**: COMPLETE
**Branch**: `refactor/unified-pipeline`
**Working dir**: `E:\PIP\pipeline_refactor\`

## What was done

### 3a. Syntax validation

- `parse("_targets.R")` — ✔ passes
- `parse("_common.R")` — ✔ passes
- All 48 R files in `R/` — ✔ all parse successfully

### 3b. DAG validation via tar_manifest()

- `TAR_PROJECT = "ppp2021"`: ✔ 71 targets found
- `TAR_PROJECT = "ppp2017"`: ✔ 71 targets found
- Target names are **identical** between both projects (confirmed via `identical()`)
- Vintage directories are correctly differentiated:
  - ppp2021 → `20260324_2021_01_02_PROD`
  - ppp2017 → `20260324_2017_01_02_PROD`

### 3c. _common.R verification

- No `stopifnot` for store validation — no fix needed
- `gls` is correctly constructed from `py` (passed from `_targets.R`)
- `tar_option_set()` configures: `format = "qs"`, `memory = "transient"`,
  `garbage_collection = TRUE`, `workspace_on_error = TRUE`
- Helper functions (`load_aux_data`, `format_aux_data`, `get_aux_versions`,
  `load_pip_inventory`) all use `gls` from enclosing environment correctly

### 3d. _targets.yaml named projects

Already configured in Phase 2:
```yaml
ppp2017:
  store: E:/PIP/pipeline_targets_cache/store_2017
ppp2021:
  store: E:/PIP/pipeline_targets_cache/store_2021
```

## No code changes required

Phase 3 was purely a validation phase. All smoke tests passed without
modifications to the codebase.
