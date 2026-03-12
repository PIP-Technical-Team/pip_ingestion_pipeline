# Phase 5: Lazy Cache Loading

**Status:** COMPLETE
**Date:** 2026-02-27

## Goal

Replace the monolithic serialize/deserialize cycle (create_cache → global_list.qs → load_cache) with on-demand .fst file reading. Each mp_* function now reads individual .fst files as needed, eliminating the bottleneck of loading ~2500 data.tables into a single giant object.

## Problem

The old pattern:
1. create_cache() reads ALL ~2500 .fst files → writes a single global_list_{ppp}.qs (~8GB)
2. load_cache() reads the entire .qs back into memory
3. {targets} hashes the entire ~8GB named list — extremely slow
4. Downstream mp_* functions iterate over surveys one-at-a-time anyway

## Solution

**New pattern:** cache_dir (file paths) + cache_ids (names) are passed directly to mp_* functions, which read each .fst lazily via ead_cache_survey().

### Changes

#### R/mapping.R
- **Added:** ead_cache_survey(path) — reads a single .fst file with error handling
- **Modified:** mp_svy_mean_lcu(cache_dir, cache_ids, gd_means) — reads surveys on demand, looks up gd_mean by cache_id name
- **Modified:** mp_lorenz(cache_dir, cache_ids) — reads surveys on demand
- **Modified:** mp_dl_dist_stats(cache_dir, cache_ids, mean_table, pop_table, ppp_year) — reads surveys on demand
- **Modified:** mp_survey_files(cache_dir, cache_ids, ...) — reads surveys on demand

#### _targets.R
- **Removed targets (5):**
  - cache_status (cue=always, detected filesystem changes for create_cache)
  - cache_file (create_cache → writes monolithic .qs)
  - cache (load_cache → reads monolithic .qs)
  - ssert_cache_length (validation of cache vs cache_dir)
  - cache_ppp (only used by create_cache, now orphaned)
- **Removed flag:** save_mp_cache (no longer needed)
- **Modified targets (4):**
  - svy_mean_lcu → mp_svy_mean_lcu(cache_dir, cache_ids, gd_means)
  - lorenz → mp_lorenz(cache_dir, cache_ids)
  - dl_dist_stats → mp_dl_dist_stats(cache_dir, cache_ids, ...)
  - survey_files → mp_survey_files(cache_dir, cache_ids, ...)

## Target count: 71 → 66

## Validation
- 	ar_manifest() succeeds for both ppp2017 and ppp2021 (66 targets each)
- All R files parse without errors
- No stray references to removed targets

## Design decisions

1. **No per-survey branching:** Avoided dynamic branching with pattern = map(cache_dir) because ~2500 surveys × 4 targets = ~10,000 branch targets, which would cause metadata bloat and potentially the same hashing performance issue.

2. **Kept mp_* wrapper structure:** The wrappers now iterate over cache_dir indexes instead of a pre-loaded named list. This preserves the existing code structure while eliminating the serialize/deserialize bottleneck.

3. **Name-based gd_means lookup:** gd_means[[cache_ids[i]]] instead of positional alignment, making the code more robust.

4. **Kept create_cache and load_cache functions:** These remain in mapping.R for backward compatibility but are no longer called from the DAG.
