# PIP Ingestion Pipeline - Refactoring Hand-Off Document

## Purpose

This document enables any AI session (or human collaborator) to pick up where
the previous session left off. Read this file first when resuming work.

## System Overview

The PIP (Poverty and Inequality Platform) ingestion pipeline transforms raw
survey data and auxiliary economic indicators into poverty estimation tables. It
uses the R {targets} package for pipeline orchestration.

### The Problem

The pipeline must produce outputs for **two PPP (Purchasing Power Parity) base
years**: 2017 and 2021. Because {targets} manages a single data store per
project, the original solution duplicated the entire repository into three
folders:

- pip_ingestion_pipeline/ - canonical code + 2017 targets store
- Old_ingestion_pipeline_2021/ - copy with py <- 2021 + separate store
- Old_ingestion_pipeline_2017/ - copy with py <- 2017 + separate store

### The Solution

Consolidate into a **single repository** using {targets} named projects. One
_targets.R script parameterized by py, two separate local stores (one per
PPP year), run independently via Sys.setenv(TAR_PROJECT = "ppp2021").

## Refactoring Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Infrastructure: worktree, .process/, copilot-instructions.md | COMPLETE (d1ee7b58) |
| 2 | Flatten R/pipdm/R/ into R/, adopt 2021 DAG as canonical | COMPLETE (ef94c3fb) |
| 3 | Smoke test: 	ar_manifest() for both projects | COMPLETE (0791012b) |
| 4 | Remove unnecessary cue = tar_cue(mode = "always") | COMPLETE (570b1dcd) |
| 5 | Lazy cache loading: eliminate monolithic .qs serialize/deserialize | COMPLETE |
| 6 | Validation against current 2021 production outputs | NOT STARTED |

## Working Environment

- **Main repo:** E:\PovcalNet\01.personal\wb384996\PIP\pip_ingestion_pipeline
  (stays on dev branch, untouched)
- **Worktree:** E:\PIP\pipeline_refactor\ (branch: efactor/unified-pipeline)
- **Local target stores:** E:\PIP\pipeline_targets_cache\store_2017 and
  store_2021

## Key Decisions Made

1. **2021 _targets.R is canonical.** All improvements from the 2021 copy
   (targetized cache, efy_mean_inc_group(), egion_code) are adopted for
   both PPP years.
2. **Named projects over 	ar_map().** Each PPP year gets its own store for
   full isolation. Debugging one year doesn't affect the other.
3. **R/pipdm/R/ functions move to R/.** The vendored pipdm package no
   longer exists; functions are maintained directly in the repo.
4. **Local SSD stores.** _targets stores move from the network share to
   E:\PIP\pipeline_targets_cache\ for speed and reliability.
5. **Lazy cache loading.** mp_* functions read .fst files on demand via
   ead_cache_survey() instead of loading a monolithic .qs file.
6. **No per-survey branching.** Avoided dynamic branching to prevent ~10K
   branch metadata entries and potential hashing performance issues.

## Current State

**Last completed:** Phase 5 - lazy cache loading
**Target count:** 66 (down from 71)
**Next action:** Phase 6 - run pipeline and validate outputs against production

### Phase 5 Summary (latest)
- Added ead_cache_survey() helper in R/mapping.R
- Modified 4 mp_* functions to accept cache_dir + cache_ids instead of pre-loaded cache list
- Removed 5 targets: cache_status, cache_file, cache, ssert_cache_length, cache_ppp
- Removed save_mp_cache flag
- gd_means lookup now by name instead of positional alignment
- 	ar_manifest() validates 66 targets for both ppp2017 and ppp2021

### Remaining cue=always targets (3)
- dl_aux1 - external network share aux files
- s_status - filesystem change detection
- un_time - timestamp

## Phase Logs

See individual phase files in .process/phases/ for detailed records.
