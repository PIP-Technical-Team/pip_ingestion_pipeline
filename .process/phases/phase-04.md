# Phase 4: Remove Unnecessary `cue = "always"`

**Status**: COMPLETE
**Branch**: `refactor/unified-pipeline`
**Working dir**: `E:\PIP\pipeline_refactor\`

## Analysis

Audited all 7 targets that had `cue = tar_cue(mode = "always")`. The user
explained these were workarounds for broken dependency chains where {targets}
couldn't detect external file changes.

## Decisions

### REMOVED `cue = "always"` (3 targets)

| Target | Reason for removal |
|--------|--------------------|
| `status_cache_files_creation` | Pure upstream dependencies; has internal early-return logic via `find_new_svy_data()` when `force = FALSE` |
| `svy_mean_lcu` | Pure function of upstream targets (`cache`, `gd_means`); no external reads |
| `aux_qs_out` | All input files are written by upstream pipeline targets; normal invalidation works |

### KEPT `cue = "always"` (4 targets)

| Target | Reason for keeping |
|--------|-------------------|
| `dl_aux1` | Reads external `.fst`/`.rds` files from network share; {targets} can't detect changes |
| `fs_status` | Filesystem change detector — must always run to detect modified survey files |
| `cache_status` | Filesystem change detector — must always run to detect modified cache files |
| `run_time` | Timestamp — legitimately must produce current time on every run |

## Changes Made

- Removed `cue = tar_cue(mode = "always")` from 3 targets in `_targets.R`
- Added explanatory `# NOTE:` comments to all 4 remaining `cue = "always"` targets
  documenting why forced re-execution is necessary
- Validated DAG: `tar_manifest()` still produces 71 targets for both PPP years

## Impact

The 3 removed targets will now be **skipped** when their upstream inputs haven't
changed, saving significant runtime on incremental pipeline runs:
- `status_cache_files_creation` — expensive (creates .fst cache files for all surveys)
- `svy_mean_lcu` — expensive (computes survey means for ~2500 surveys)
- `aux_qs_out` — moderate (converts all aux files to .qs format)
