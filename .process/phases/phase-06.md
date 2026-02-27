# Phase 6: Validation Against Production Outputs

**Status:** PREPARED (awaiting user execution)
**Date:** 2026-02-27

## Goal

Run the refactored pipeline and compare outputs against the original pipeline's
stored results to confirm functional equivalence.

## What was done

1. Created alidate.R — a comparison script that reads target outputs from both
   the refactored store and the original store, then compares dimensions, column
   names, and values.

2. The script compares 9 key targets:
   - svy_mean_lcu_table, svy_mean_ppp_table (survey mean tables)
   - dt_dist_stats (distribution statistics)
   - dt_prod_ref_estimation, dt_prod_svy_estimation (estimation tables)
   - dt_ref_mean_pred (reference year means)
   - lorenz (Lorenz curves)
   - svy_mean_lcu, dl_dist_stats (intermediate lists)

## What the user needs to do

1. Open R in the worktree directory (E:\PIP\pipeline_refactor\)
2. Run the pipeline:
   `
   Sys.setenv(TAR_PROJECT = "ppp2021")
   targets::tar_make()
   `
3. After completion, run validation:
   `
   source("validate.R")
   `
4. Review comparison output for each target

## Expected outcomes

- All table dimensions should match
- Column names should be identical
- Numerical values should be identical (same code, same data)
- Any differences would indicate a bug in the refactoring

## Notes

- The original store is at pip_ingestion_pipeline/_targets
- The refactored store is at E:/PIP/pipeline_targets_cache/store_2021
- Full pipeline run may take several hours depending on network/disk speed
- The original store may have been produced with a different vintage/release;
  some numerical differences are expected if the input data has changed
