
library(targets)
library(tarchetypes)
library(gittargets)


if (requireNamespace("gert", quietly = TRUE)) {
  library(gert)
  gca <- function(x, ...) {
    gert::git_commit_all(x, ...)
  }

  gp <- function(x = NULL, ...) {
    gert::git_push(x, ...)
  }

  ga <- function(...) {
    gert::git_add(gert::git_status(...)$file)
  }

  gi <- function() {
    gert::git_info()$upstream
  }
  gs <- function() {
    gert::git_status()
  }
}

if (requireNamespace("pushoverr", quietly = TRUE)) {


  run_tar <- function(...) {
    s     <- Sys.time()
    start <- format(s, "%H:%M")

    # Helper: build a timestamped message with error details
    make_msg <- function(status, detail = NULL) {
      f      <- Sys.time()
      finish <- format(f, "%H:%M")
      d      <- f - s
      base   <- paste0(status, " in pipeline.",
                       "\nStarted at ",  start,
                       "\nFinished at ", finish,
                       "\nDifference ",  d)
      if (!is.null(detail)) paste0(base, "\n\nDetails: ", detail) else base
    }

    # Helper: run sync and return any error as a string (never throws)
    run_sync <- function() {
      tryCatch({
        # Load globals into the global env so gls and release are available
        tar_load_globals(envir = globalenv())
        gls <- get("gls", envir = globalenv())

        sync_status <- syncdr::compare_directories(
          left_path  = fs::path(gls$OUT_DIR_PC, gls$vintage_dir),
          right_path = fs::path("e:/PIP/pipapi_data", gls$vintage_dir) |>
            fs::dir_create(),
          by_date    = TRUE,
          by_content = FALSE,
          verbose    = FALSE,
          recurse    = TRUE)

        syncdr::common_files_asym_sync_to_right(
          sync_status = sync_status,
          force       = TRUE,
          verbose     = FALSE)

        syncdr::update_missing_files_asym_to_right(
          sync_status     = sync_status,
          copy_to_right   = TRUE,
          delete_in_right = FALSE,
          exclude_delete  = c("cache.duckdb",
                              "lineup_data",
                              "prod_refy_estimation.fst",
                              "lineup_dist_stats.fst",
                              "lineup_years.fst"),
          force           = TRUE,
          verbose         = FALSE)

        NULL  # no error
      }, error = function(e) {
        conditionMessage(e)
      })
    }

    msg <- tryCatch(
      expr = {
        # --- Step 1: run the pipeline ---
        tar_make(...)

        # --- Step 2: sync outputs ---
        sync_err <- run_sync()
        if (!is.null(sync_err)) {
          make_msg("WARNING", paste("tar_make() succeeded but sync failed:", sync_err))
        } else {
          make_msg("SUCCESS")
        }
      }, # end of expr section

      error = function(e) {
        # Captures errors from tar_make() AND any uncaught error above
        detail <- paste0(
          conditionMessage(e),
          "\n\nTraceback:\n",
          paste(
            vapply(sys.calls(), function(x) deparse(x)[[1L]], character(1L)),
            collapse = "\n"
          )
        )
        make_msg("ERROR", detail)
      }, # end of error section

      warning = function(w) {
        # tar_make() can signal warnings for certain pipeline states
        sync_err <- run_sync()
        detail   <- paste0("Pipeline warning: ", conditionMessage(w))
        if (!is.null(sync_err)) {
          detail <- paste0(detail, "\nSync failed: ", sync_err)
        }
        make_msg("WARNING", detail)
      } # end of warning section

    ) # End of tryCatch

    msg_safe <- gsub("\\}", "}}", gsub("\\{", "{{", msg))
    pushoverr::pushover(msg_safe)
    cli::cli_alert(msg_safe)

    return(invisible(TRUE))
  }

}


# ---- Tiny helpers (expose a few, keep rest optional) ----
if (Sys.info()[["user"]] == "wb384996") {
  tdirp <- fs::path("p:/02.personal/wb384996/temporal/R/")
  tdire <- fs::path("E:/PovcalNet/01.personal/wb384996/temp/R")
}

