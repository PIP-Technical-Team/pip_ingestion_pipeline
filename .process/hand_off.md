# PIP Ingestion Pipeline — Refactoring Hand-Off Document

## Purpose

This document enables any AI session (or human collaborator) to pick up where
the previous session left off. Read this file first when resuming work.

## System Overview

The PIP (Poverty and Inequality Platform) ingestion pipeline transforms raw
survey data and auxiliary economic indicators into poverty estimation tables. It
uses the R `{targets}` package for pipeline orchestration.

### The Problem

The pipeline must produce outputs for **two PPP (Purchasing Power Parity) base
years**: 2017 and 2021. Because `{targets}` manages a single data store per
project, the original solution duplicated the entire repository into three
folders:

- `pip_ingestion_pipeline/` — canonical code + 2017 targets store
- `Old_ingestion_pipeline_2021/` — copy with `py <- 2021` + separate store
- `Old_ingestion_pipeline_2017/` — copy with `py <- 2017` + separate store

The 2021 copy evolved beyond the canonical: it targetized cache creation, adopted
a new reference-year algorithm (`refy_mean_inc_group()`), and added filesystem
change detection. The copies diverge, are hard to maintain, and the system fails
frequently.

### The Solution

Consolidate into a **single repository** using `{targets}` named projects. One
`_targets.R` script parameterized by `py`, two separate local stores (one per
PPP year), run independently via `Sys.setenv(TAR_PROJECT = "ppp2021")`.

## Refactoring Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Infrastructure: worktree, .process/, copilot-instructions.md | ✅ COMPLETE |
| 2 | Flatten `R/pipdm/R/` into `R/`, adopt 2021 DAG as canonical | ✅ COMPLETE |
| 3 | Named projects + parameterize `py` via `TAR_PROJECT` | IN PROGRESS |
| 4 | Remove `cue = tar_cue(mode = "always")`, fix dependency chains | NOT STARTED |
| 5 | Lazy cache loading with dynamic branching per survey | NOT STARTED |
| 6 | Validation against current 2021 production outputs | NOT STARTED |

## Working Environment

- **Main repo:** `E:\PovcalNet\01.personal\wb384996\PIP\pip_ingestion_pipeline`
  (stays on `dev` branch, untouched)
- **Worktree:** `E:\PIP\pipeline_refactor\` (branch: `refactor/unified-pipeline`)
- **Local target stores:** `E:\PIP\pipeline_targets_cache\store_2017` and
  `store_2021`
- **Old 2021 copy (reference only):**
  `E:\PovcalNet\01.personal\wb384996\PIP\Old_ingestion_pipeline_2021`

## Key Decisions Made

1. **2021 `_targets.R` is canonical.** All improvements from the 2021 copy
   (targetized cache, `refy_mean_inc_group()`, `region_code`) are adopted for
   both PPP years.
2. **Named projects over `tar_map()`.** Each PPP year gets its own store for
   full isolation. Debugging one year doesn't affect the other.
3. **`R/pipdm/R/` functions move to `R/`.** The vendored `pipdm` package no
   longer exists; functions are maintained directly in the repo.
4. **Local SSD stores.** `_targets` stores move from the network share to
   `E:\PIP\pipeline_targets_cache\` for speed and reliability.
5. **Lazy cache loading.** Replace monolithic `mp_cache()` with per-survey
   dynamic branching using `format = "file_fast"` for efficient change detection.

## Current State

**Last completed:** Phase 2 — flattened pipdm, adopted 2021 DAG, updated _targets.yaml
**Currently in progress:** Phase 3 — named projects are configured in _targets.yaml; need to update _common.R and smoke test
**Next action:** Check _common.R for store validation `stopifnot`, ensure `py` flows correctly from `TAR_PROJECT`, run smoke test with `tar_manifest()`.

### Phase 2 Summary
- Moved 46 files from `R/pipdm/R/` → `R/` (renamed `utils.R` → `pipdm_utils.R`)
- Created unified `_targets.R` from 2021 canonical version
- PPP year derived from `TAR_PROJECT` env var: `switch(tar_project, ppp2017=2017, ppp2021=2021)`
- Updated `_targets.yaml` with two named projects (`ppp2017`, `ppp2021`) using local SSD stores
- Fixed `create_cache()` in `mapping.R`: added missing `force` parameter

### Files changed since Phase 1 commit
- `_targets.R` — new unified version (old backed up as `_targets_old_2017.R.bak`)
- `_targets.yaml` — named projects with local stores
- `R/mapping.R` — `force` parameter fix in `create_cache()`
- `R/*.R` — 44 files moved from `R/pipdm/R/` (+ `pipdm_utils.R`)
- `R/pipdm/` — removed entirely

## Phase Logs

See individual phase files in `.process/phases/` for detailed records.
