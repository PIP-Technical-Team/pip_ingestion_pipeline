# Lineup utility functions
# Adapted from pip_lineups_pipeline/init.R

# ── Auxiliary data loading ─────────────────────────────────────────────────────

#' Read aux list for lineup estimation
#'
#' Reads all `.fst` files (and optionally `.qs` files) from the `_aux`
#' sub-folder of `path` and returns them as a named list.  Intended as
#' input for the lineup estimation process.
#'
#' @param path character / fs_path. Vintage output directory
#'   (e.g. `fs::path(gls$OUT_DIR_PC, gls$vintage_dir)`).
#'
#' @return Named list where each element corresponds to one aux file.
#' @export
read_aux_list <- function(path) {
  aux_path <- fs::path(path, "_aux")

  fst_files <- list.files(aux_path, pattern = "\\.fst$", full.names = TRUE)
  fst_names <- tools::file_path_sans_ext(basename(fst_files))

  if (!length(fst_files)) {
    cli::cli_abort("No .fst files found in {.path {aux_path}}.")
  }

  dl_aux <- lapply(fst_files, fst::read_fst, as.data.table = TRUE)
  names(dl_aux) <- fst_names

  dl_aux
}


# ── Country / year helpers ─────────────────────────────────────────────────────

#' Build full country-year list for lineup estimation
#'
#' Constructs a list of `list(country_code, year)` elements covering all
#' country-years present in `df_refy` up to `max(lineup_years)`.
#'
#' @param lineup_years integer vector. Reference years to include.
#' @param only_country character or NULL. If supplied, restrict to one country.
#' @param df_refy data.table. Reference-year table; must contain
#'   `country_code` and `reporting_year`.
#' @param rm_country character or NULL. Country to exclude.
#'
#' @return List of `list(country_code, year)` elements, one per country.
#' @export
get_full_list <- function(
  lineup_years,
  only_country = NULL,
  df_refy,
  rm_country = NULL
) {
  ctry <- df_refy |>
    fsubset(reporting_year <= max(lineup_years)) |>
    fselect(country_code, year = reporting_year) |>
    funique() |>
    qDT()

  full_list <- ctry[,
    .(year = list(sort(unique(year)))),
    by = country_code
  ][, lapply(.SD, as.list), .SDcols = c("country_code", "year")]

  full_list <- lapply(seq_len(nrow(full_list)), \(i) {
    list(country_code = full_list$country_code[[i]], year = full_list$year[[i]])
  })

  if (!is.null(only_country)) {
    idx <- which(
      vapply(full_list, `[[`, character(1), "country_code") == only_country
    )
    full_list <- full_list[idx]
  }

  if (!is.null(rm_country)) {
    idx <- which(
      vapply(full_list, `[[`, character(1), "country_code") == rm_country
    )
    full_list <- full_list[-idx]
  }

  full_list
}


# ── Attribute helpers ──────────────────────────────────────────────────────────

#' Identify variables with a single unique value
#'
#' @param x data.frame or data.table.
#' @return Character vector of column names whose `uniqueN` equals 1.
#' @export
uniq_vars <- function(x) {
  x <- check_data_table(x)
  N_vars <- x[, lapply(.SD, uniqueN)]
  names(N_vars)[N_vars == 1L]
}


#' Coerce to data.table if needed
#' @noRd
check_data_table <- function(x) {
  if (!is.data.table(x)) qDT(x) else x
}


#' Convert single-value columns to a named list
#'
#' @param x data.frame or data.table.
#' @return Named list of unique values from single-value columns.
#' @export
uniq_vars_to_list <- function(x) {
  x <- check_data_table(x)
  uni_vars <- uniq_vars(x)
  y <- x[, lapply(.SD, unique), .SDcols = uni_vars]
  as.list(y)
}


#' Build a named list of unique values for selected variables
#'
#' @param x data.table.
#' @param vars character vector. Column names to extract.
#' @param nm character vector or NULL. Optional names vector.
#' @return Named list.
#' @export
vars_to_list <- function(x, vars, nm = NULL) {
  var1 <- lapply(x[, ..vars], unique)
  if (!is.null(nm)) {
    var2 <- lapply(x[, ..nm], unique)
    if (!all(mapply(\(a, b) length(a) == length(b), var1, var2))) {
      cli::cli_abort(
        "Unique values in {.arg num_var} and {.arg name_var} are not equal."
      )
    }
    var1 <- Map(stats::setNames, var1, var2)
  }
  var1
}


#' Move single-value columns to attributes and drop them
#'
#' @param x data.frame or data.table.
#' @param exclude_vars character vector or NULL. Columns to skip.
#' @return data.table with single-value columns removed and stored as attributes.
#' @export
#' @examples
#' dt  <- data.table(a = 1, b = 1:10, c = 5)
#' out <- uniq_vars_to_attr(dt)
#' attr(out, "a")
uniq_vars_to_attr <- function(x, exclude_vars = NULL) {
  nm <- copy(names(x))
  x1 <- copy(x)

  if (!is.null(exclude_vars)) {
    missing_ev <- exclude_vars[!exclude_vars %in% nm]
    if (length(missing_ev)) {
      cli::cli_abort(
        "{.var {missing_ev}} {?is/are} not column name{?s} in data."
      )
    }
    x1[, (exclude_vars) := NULL]
  }

  uvl <- uniq_vars_to_list(x1)
  uni_vars <- names(uvl)
  mul_vars <- setdiff(nm, uni_vars)

  x <- change_vars_to_attr(x, uvl)
  x[, ..mul_vars]
}


#' Set a list of named values as attributes on a data.frame
#'
#' @param df data.frame or data.table.
#' @param uvl named list. Values to attach as attributes.
#' @return `df` with attributes added (invisibly modifies data.table by ref).
#' @export
change_vars_to_attr <- function(df, uvl) {
  for (i in seq_along(uvl)) {
    var <- names(uvl)[i]
    value <- uvl[[i]]
    if (inherits(df, "data.table")) {
      setattr(df, var, value)
    } else {
      attr(df, var) <- value
    }
  }
  df
}


#' Move selected columns to attributes and drop them
#'
#' @param df data.frame or data.table.
#' @param vars character vector. Columns to promote to attributes.
#' @return data.table without `vars` columns.
#' @export
#' @examples
#' \dontrun{
#' dt  <- data.table(a = c(1, 2), b = 1:10, c = 5)
#' out <- vars_to_attr(dt, "a")
#' }
vars_to_attr <- function(df, vars) {
  df <- check_data_table(df)
  uvl <- vars_to_list(df, vars)
  df <- change_vars_to_attr(df, uvl)
  df[, !..vars]
}


#' Create named-vector attributes from numeric and name columns
#'
#' @param df data.frame or data.table.
#' @param num_var character. Column with numeric values.
#' @param name_var character. Column with names.
#' @return data.table with `num_var` and `name_var` columns removed.
#' @export
#' @examples
#' \dontrun{
#' dt  <- data.table(a = c(1, 2), b = 1:10, c = c("x", "y"))
#' out <- num_vars_to_attr(dt, "a", "c")
#' }
num_vars_to_attr <- function(df, num_var, name_var) {
  dt <- check_data_table(df)

  if (length(num_var) != length(name_var)) {
    cli::cli_abort(
      "{.arg num_var} and {.arg name_var} must be the same length.",
      call = NULL
    )
  }

  uvl <- vars_to_list(dt, num_var, name_var)
  dt <- change_vars_to_attr(dt, uvl)
  c_col <- c(num_var, name_var)
  dt[, !..c_col]
}


# ── Level ordering ────────────────────────────────────────────────────────────

#' Map a data-level vector to an integer ordering code
#'
#' Returns 1 for national-only, 2 for rural/urban, 3 for mixed.
#'
#' @param dt data.table. Data containing the grouping variable.
#' @param x character. Column name of the data-level variable.
#' @return integer scalar.
#' @noRd
get_ordered_level <- function(dt, x) {
  x_level <- unique(dt[[x]])
  d1 <- c("national")
  d2 <- c("rural", "urban")

  if (identical(x_level, d1)) {
    1L
  } else if (identical(x_level, d2)) {
    2L
  } else {
    3L
  }
}


# ── Reference-year table prep ─────────────────────────────────────────────────

#' Prepare reference-year table for lineup use in API
#'
#' Ensures uniqueness of columns by country-year-welfare_type-reporting_level.
#' Non-unique columns are set to NA.  Returns a deduplicated table.
#'
#' @param df_refy data.table. Reference-year estimation table.
#' @return data.table with one row per country-year-level combination.
#' @export
prep_df_refy_for_lineups <- function(df_refy) {
  vars <- colnames(df_refy)
  group_vars <- c(
    "country_code",
    "reporting_year",
    "welfare_type",
    "reporting_level"
  )
  check_vars <- setdiff(vars, group_vars)

  df_refy$survey_comparability <- NA
  df_refy$comparable_spell <- NA
  df_refy$survey_mean_lcu <- NA
  df_refy$survey_mean_ppp <- NA
  df_refy$survey_median_lcu <- NA
  df_refy$survey_median_ppp <- NA

  nu_counts <- df_refy[,
    lapply(.SD, uniqueN, na.rm = TRUE),
    by = group_vars,
    .SDcols = check_vars
  ]

  cols_to_na <- check_vars[
    sapply(nu_counts[, ..check_vars], function(v) any(v > 1L))
  ]

  if (length(cols_to_na)) {
    df_refy[, (cols_to_na) := NA]
  }

  df_refy |> fselect(vars) |> funique()
}
