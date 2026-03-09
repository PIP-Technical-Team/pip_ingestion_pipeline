#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Phase 6: Validation Script
#
# Compares ALL built targets shared between the refactored
# pipeline and the original pipeline's targets store.
#
# Steps:
#   1. Reads meta from both stores to discover built targets.
#   2. Reports coverage: which targets exist in each store.
#   3. For shared targets, uses all.equal() to detect diffs.
#   4. For differing targets, builds a tidy data.frame with
#      one row per difference message for further inspection.
#
# Usage:
#   Sys.setenv(TAR_PROJECT = "ppp2021")
#   source("validate.R")
#   # then inspect: diff_report, coverage_report
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(targets)
library(data.table)

# ---- Paths ----------------------------------------------------------------
refactored_store <- "E:/PovcalNet/01.personal/wb384996/PIP/pipeline_targets_cache/store_2021"
original_store   <- "y:/pip_ingestion_pipeline/pc_data/_targets2021/"

cli::cli_h1("PIP Pipeline Validation")

# ---- 1. Discover built targets in each store ------------------------------

cli::cli_h2("Discovering built targets")

get_built_stems <- function(store) {
  meta <- tryCatch(
    tar_meta(store = store, fields = c("name", "type", "time")),
    error = function(e) {
      cli::cli_alert_danger("Cannot read meta from {.path {store}}: {e$message}")
      return(data.table(name = character(), type = character(), time = as.POSIXct(NA)))
    }
  )
  setDT(meta)
  # Keep only targets that have actually been built (time is not NA)
  # and are not function definitions
  meta[!is.na(time) & type != "function"]
}

meta_new <- get_built_stems(refactored_store)
meta_old <- get_built_stems(original_store)

# Branch targets have hash suffixes; strip them to get the parent pattern name
# so we can match patterns across stores even when branch hashes differ.
strip_hash <- function(x) sub("_[0-9a-f]{16}$", "", x)

meta_new[, stem := strip_hash(name)]
meta_old[, stem := strip_hash(name)]

new_stems <- unique(meta_new$stem)
old_stems <- unique(meta_old$stem)

shared_stems   <- intersect(new_stems, old_stems)
new_only_stems <- setdiff(new_stems, old_stems)
old_only_stems <- setdiff(old_stems, new_stems)

# ---- 2. Coverage report ---------------------------------------------------

cli::cli_h2("Coverage report")
cli::cli_alert_info("New store built targets (unique stems): {length(new_stems)}")
cli::cli_alert_info("Old store built targets (unique stems): {length(old_stems)}")
cli::cli_alert_info("Shared stems (comparable):              {length(shared_stems)}")
cli::cli_alert_info("New-only stems:                         {length(new_only_stems)}")
cli::cli_alert_info("Old-only stems:                         {length(old_only_stems)}")

if (length(new_only_stems) > 0) {
  cli::cli_alert_warning("In new store only: {paste(new_only_stems, collapse = ', ')}")
}
if (length(old_only_stems) > 0) {
  cli::cli_alert_warning("In old store only: {paste(old_only_stems, collapse = ', ')}")
}

coverage_report <- data.table(
  stem     = sort(union(new_stems, old_stems)),
  in_new   = sort(union(new_stems, old_stems)) %in% new_stems,
  in_old   = sort(union(new_stems, old_stems)) %in% old_stems,
  in_both  = sort(union(new_stems, old_stems)) %in% shared_stems
)

# ---- 3. Compare shared targets --------------------------------------------

cli::cli_h2("Comparing {length(shared_stems)} shared targets")

# For targets with dynamic branches we read the parent pattern name directly;
# tar_read_raw() on a pattern name returns the aggregated result.
safe_read <- function(name, store) {
  tryCatch(
    tar_read_raw(name, store = store),
    error = function(e) structure(list(error = e$message), class = "read_error")
  )
}

is_read_error <- function(x) inherits(x, "read_error")

# Returns a character vector of all.equal() messages, or character(0) if equal.
compare_objects <- function(new_obj, old_obj) {
  result <- all.equal(new_obj, old_obj, check.attributes = FALSE)
  if (isTRUE(result)) character(0) else as.character(result)
}

# Accumulate results row by row into a list, then rbindlist once at the end.
diff_rows  <- list()
equal_tgts <- character()

for (stem in sort(shared_stems)) {

  cli::cli_progress_step("Comparing {.val {stem}}")

  new_obj <- safe_read(stem, refactored_store)
  old_obj <- safe_read(stem, original_store)

  # Read errors
  if (is_read_error(new_obj) || is_read_error(old_obj)) {
    diff_rows[[length(diff_rows) + 1L]] <- data.table(
      target    = stem,
      equal     = FALSE,
      new_class = if (is_read_error(new_obj)) "READ_ERROR" else paste(class(new_obj), collapse = "/"),
      old_class = if (is_read_error(old_obj)) "READ_ERROR" else paste(class(old_obj), collapse = "/"),
      new_dim   = NA_character_,
      old_dim   = NA_character_,
      diff_msg  = if (is_read_error(new_obj)) new_obj$error else old_obj$error
    )
    next
  }

  # Dimension helpers
  obj_dim <- function(x) {
    if (is.data.frame(x))  paste0(nrow(x), "x", ncol(x))
    else if (is.list(x))   paste0("list[", length(x), "]")
    else if (is.vector(x)) paste0("vec[", length(x), "]")
    else                   NA_character_
  }

  diffs <- compare_objects(new_obj, old_obj)

  if (length(diffs) == 0L) {
    equal_tgts <- c(equal_tgts, stem)
  } else {
    for (msg in diffs) {
      diff_rows[[length(diff_rows) + 1L]] <- data.table(
        target    = stem,
        equal     = FALSE,
        new_class = paste(class(new_obj), collapse = "/"),
        old_class = paste(class(old_obj), collapse = "/"),
        new_dim   = obj_dim(new_obj),
        old_dim   = obj_dim(old_obj),
        diff_msg  = msg
      )
    }
  }
}

cli::cli_progress_done()

# ---- 4. Build diff report -------------------------------------------------

diff_report <- if (length(diff_rows) > 0L) {
  rbindlist(diff_rows)
} else {
  data.table(
    target    = character(),
    equal     = logical(),
    new_class = character(),
    old_class = character(),
    new_dim   = character(),
    old_dim   = character(),
    diff_msg  = character()
  )
}

# ---- 5. Summary -----------------------------------------------------------

n_equal  <- length(equal_tgts)
n_diff   <- uniqueN(diff_report$target)
n_errors <- uniqueN(diff_report[new_class == "READ_ERROR" | old_class == "READ_ERROR", target])

cli::cli_h1("Validation Summary")
cli::cli_alert_info("Shared targets compared : {length(shared_stems)}")
cli::cli_alert_success("Identical               : {n_equal}")

if (n_diff > 0) {
  cli::cli_alert_warning("Differ                  : {n_diff}")
  cli::cli_alert_info(
    "Differing targets: {paste(unique(diff_report$target), collapse = ', ')}"
  )
} else {
  cli::cli_alert_success("All shared targets are identical!")
}

if (n_errors > 0) {
  cli::cli_alert_danger("Read errors             : {n_errors}")
}

cli::cli_alert_info("Coverage report  -> {.var coverage_report}")
cli::cli_alert_info("Difference report -> {.var diff_report}  ({nrow(diff_report)} rows)")
