#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Phase 6: Validation Script
# 
# Run this script after tar_make() completes for ppp2021.
# It compares key outputs against the original pipeline's
# results in the main repo's targets store.
#
# Usage:
#   Sys.setenv(TAR_PROJECT = "ppp2021")
#   source("validate.R")
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(targets)
library(data.table)

# Paths
refactored_store <- "E:/PIP/pipeline_targets_cache/store_2021"
original_store   <- "E:/PovcalNet/01.personal/wb384996/PIP/pip_ingestion_pipeline/_targets"

cli::cli_h1("PIP Pipeline Validation")

# ---- Helper function ----
compare_target <- function(name, 
                           new_store = refactored_store, 
                           old_store = original_store) {
  cli::cli_h2("Comparing target: {.val {name}}")
  
  new <- tryCatch(
    tar_read_raw(name, store = new_store),
    error = function(e) { cli::cli_alert_danger("Cannot read from refactored store"); NULL }
  )
  old <- tryCatch(
    tar_read_raw(name, store = old_store),
    error = function(e) { cli::cli_alert_danger("Cannot read from original store"); NULL }
  )
  
  if (is.null(new) || is.null(old)) {
    cli::cli_alert_warning("Skipping comparison (missing data)")
    return(invisible(NULL))
  }
  
  # Compare dimensions
  if (is.data.frame(new) && is.data.frame(old)) {
    cli::cli_alert_info("Dimensions: new={nrow(new)}x{ncol(new)}, old={nrow(old)}x{ncol(old)}")
    
    if (ncol(new) == ncol(old) && all(names(new) == names(old))) {
      cli::cli_alert_success("Column names match")
    } else {
      cli::cli_alert_danger("Column names differ")
      cli::cli_alert_info("New only: {setdiff(names(new), names(old))}")
      cli::cli_alert_info("Old only: {setdiff(names(old), names(new))}")
    }
    
    if (nrow(new) == nrow(old)) {
      cli::cli_alert_success("Row count matches: {nrow(new)}")
    } else {
      cli::cli_alert_warning("Row count differs: new={nrow(new)}, old={nrow(old)}")
    }
  } else if (is.list(new) && is.list(old)) {
    cli::cli_alert_info("List lengths: new={length(new)}, old={length(old)}")
    if (length(new) == length(old)) {
      cli::cli_alert_success("List lengths match")
    } else {
      cli::cli_alert_warning("List lengths differ")
    }
  }
  
  # Try exact comparison
  if (isTRUE(all.equal(new, old))) {
    cli::cli_alert_success("PASS: identical results")
  } else {
    msg <- all.equal(new, old)
    cli::cli_alert_warning("Differences found:")
    for (m in head(msg, 5)) cli::cli_alert_info("  {m}")
  }
  
  invisible(list(new = new, old = old))
}

# ---- Key targets to compare ----
# These are the main pipeline outputs

targets_to_compare <- c(
  # Survey mean tables
  "svy_mean_lcu_table",
  "svy_mean_ppp_table",
  
  # Distribution stats
  "dt_dist_stats",
  
  # Estimation tables
  "dt_prod_ref_estimation",
  "dt_prod_svy_estimation",
  
  # Reference year means
  "dt_ref_mean_pred",
  
  # Lorenz curves
  "lorenz",
  
  # Intermediate: survey means list
  "svy_mean_lcu",
  
  # Intermediate: dist stats list
  "dl_dist_stats"
)

results <- list()
for (tgt in targets_to_compare) {
  results[[tgt]] <- tryCatch(
    compare_target(tgt),
    error = function(e) {
      cli::cli_alert_danger("Error comparing {tgt}: {e}")
      NULL
    }
  )
}

cli::cli_h1("Validation Summary")
cli::cli_alert_info("Compared {length(targets_to_compare)} targets")
cli::cli_alert_info("Run tar_make() first if targets are missing from the refactored store")
