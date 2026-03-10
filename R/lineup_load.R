# Lineup distribution load functions
# Adapted from pip_lineups_pipeline/pipdata_pip_load_lineups.R
#
# NOTE: The function load_aux_data() from the source pipeline is renamed to
# load_lineup_aux_data() here to avoid a name conflict with the load_aux_data()
# helper defined in _common.R.

#' Load a lineup distribution for a single country-year
#'
#' @param country_code character. ISO3c country code (e.g. `"ZAF"`).
#' @param year numeric or character. Reference year.
#' @param path character / fs_path. Directory containing the `.qs` files.
#'
#' @return The data.table stored in `{path}/{country_code}_{year}.qs`,
#'   with all original attributes.
#' @export
#'
#' @examples
#' \dontrun{
#' zaf_2020 <- load_refy("ZAF", 2020, path = gls$OUT_LINEUP_DIR_PC)
#' }
load_refy <- function(country_code, year, path) {
  qs::qread(
    file = fs::path(path, paste0(country_code, "_", year), ext = "qs")
  )
}


#' Load a list of lineup distributions
#'
#' Transforms `input_list` into flat country-year pairs via
#' [transform_input()], then loads each with [load_refy()].
#'
#' @param input_list list with elements `country_code` (character vector) and
#'   `year` (single value or list of year vectors, one per country).
#' @param path character / fs_path. Directory containing `.qs` files.
#'
#' @return Named list of data.tables.
#' @export
#'
#' @examples
#' \dontrun{
#' input_list <- list(
#'   country_code = c("ZAF", "COL"),
#'   year         = list(c(2020, 2021), c(2015, 2016))
#' )
#' all_lineups <- load_list_refy(input_list, path = gls$OUT_LINEUP_DIR_PC)
#' }
load_list_refy <- function(input_list, path) {
  input_list <- transform_input(input_list)

  dl <- lapply(input_list, \(x) {
    load_refy(country_code = x$country_code, year = x$year, path = path)
  })

  names(dl) <- vapply(
    input_list,
    \(x) paste0(x$country_code, x$year),
    FUN.VALUE = character(1L)
  )
  dl
}


#' Load only the attributes of a lineup `.qs` file
#'
#' Faster than [load_refy()] when only metadata is needed.
#'
#' @inheritParams load_refy
#' @return Named list of attributes.
#' @export
#'
#' @examples
#' \dontrun{
#' zaf_attrs <- load_attr("ZAF", 2020, path = gls$OUT_LINEUP_DIR_PC)
#' }
load_attr <- function(country_code, year, path) {
  qs::qattributes(
    fs::path(path, paste0(country_code, "_", year, ".qs"))
  )
}


#' Load distributional statistics from a lineup file
#'
#' Reads only the `dist_stats` attribute from the `.qs` file.
#'
#' @inheritParams load_refy
#' @return Named list of distributional statistics.
#' @export
load_dist_stats <- function(country_code, year, path) {
  load_attr(country_code, year, path)$dist_stats
}


#' Load flat distributional-statistics data.table from a lineup file
#'
#' Reads only the `dt_dist_stats` attribute (a flat data.table).
#'
#' @inheritParams load_refy
#' @return data.table with one row per reporting level.
#' @export
load_dt_dist_stats <- function(country_code, year, path) {
  load_attr(country_code, year, path)$dt_dist_stats
}


#' Load and stack flat dist-stats for all country-years in a list
#'
#' @param full_list list. Each element is `list(country_code, year)` where
#'   `year` is a vector. See [get_full_list()].
#' @param path character / fs_path. Directory containing `.qs` files.
#'
#' @return data.table with one row per country-year-reporting-level.
#' @export
load_full_dt_dist_stats <- function(full_list, path) {
  outlist <- lapply(full_list, \(x) {
    cn <- x$country_code
    rowbind(lapply(x$year, \(y) load_dt_dist_stats(cn, y, path = path)))
  })

  rowbind(outlist)
}


#' Load auxiliary data attribute from a lineup file
#'
#' Reads only the `aux_data` attribute from the `.qs` file.
#' Renamed from `load_aux_data()` to avoid conflict with `_common.R`.
#'
#' @inheritParams load_refy
#' @return Named list of auxiliary data.
#' @export
load_lineup_aux_data <- function(country_code, year, path) {
  load_attr(country_code, year, path)$aux_data
}


#' Extract a named attribute from a lineup data.table
#'
#' @param df data.table. Output of [load_refy()] or [load_list_refy()].
#' @param dist_stats logical. If TRUE, look inside the `dist_stats` attribute.
#' @param aux_data logical. If TRUE, look inside the `aux_data` attribute.
#' @param attr character. Name of the attribute (or sub-attribute) to extract.
#' @param verbose logical. Emit diagnostic messages on error.
#'
#' @return The requested attribute value.
#' @export
extract_attr <- function(
  df,
  dist_stats = FALSE,
  aux_data = FALSE,
  attr,
  verbose = FALSE
) {
  # Always enforce mutual exclusion — this is a programming error, not a user
  # choice, so it must not be gated behind `verbose`.
  if (dist_stats && aux_data) {
    cli::cli_abort("`dist_stats` and `aux_data` cannot both be TRUE.")
  }

  l_attr <- base::attributes(df)

  a <- if (dist_stats) {
    l_attr$dist_stats[[attr]]
  } else if (aux_data) {
    l_attr$aux_data[[attr]]
  } else {
    l_attr[[attr]]
  }

  if (verbose) {
    n_attr <- if (dist_stats) {
      names(l_attr$dist_stats)
    } else if (aux_data) {
      names(l_attr$aux_data)
    } else {
      names(l_attr)
    }
    if (!attr %in% n_attr) {
      n_attr <- n_attr[
        !grepl("names|row.names|.internal.selfref|class", n_attr)
      ]
      cli::cli_abort(
        "With `dist_stats`={dist_stats} and `aux_data`={aux_data}, ",
        "`attr` must be one of: {.val {n_attr}}"
      )
    }
  }

  a
}


#' Move an attribute back into a column of a data.table
#'
#' @param df data.table.
#' @param attr_to_column character. Attribute name to promote to a column.
#' @param dist_stats logical. Is it a distributional statistic?
#' @param aux_data logical. Is it from `aux_data`?
#' @param dattr value or NULL. Pre-fetched attribute value.
#' @param verbose logical.
#'
#' @return data.table with the attribute materialised as a column.
#' @export
attr_to_column <- function(
  df,
  attr_to_column,
  dist_stats = FALSE,
  aux_data = FALSE,
  dattr = NULL,
  verbose = FALSE
) {
  if (is.null(dattr)) {
    dattr <- extract_attr(
      df,
      attr = attr_to_column,
      dist_stats = dist_stats,
      aux_data = aux_data,
      verbose = verbose
    )
  }

  if (isTRUE(dist_stats) && attr_to_column == "dist_stats") {
    cli::cli_abort(
      "Use `dist_stats = TRUE` instead of `attr_to_column = 'dist_stats'`."
    )
  }
  if (isTRUE(aux_data) && attr_to_column == "aux_data") {
    cli::cli_abort(
      "Use `aux_data = TRUE` instead of `attr_to_column = 'aux_data'`."
    )
  }

  if (!aux_data && !dist_stats && is.list(dattr) && length(dattr) > 1L) {
    nm <- names(dattr)
    tm <- dattr$rows
    if (length(tm) > 1L) {
      tm <- c(tm[1L], diff(tm))
    }
    rp <- rep(dattr[[nm[1L]]], times = tm)
    df <- df |> fmutate(temp = rp) |> frename(temp = nm[1L], .nse = FALSE)
  } else if (!aux_data && length(dattr) == 1L) {
    df <- df |>
      fmutate(temp = dattr[[1L]]) |>
      setrename(temp = attr_to_column, .nse = FALSE)
  } else if (!aux_data && dist_stats && is.list(dattr) && length(dattr) > 1L) {
    if (!"reporting_level" %in% names(df)) {
      cli::cli_abort(
        "`reporting_level` must be a column before adding dist stats."
      )
    }
    nm <- names(dattr)
    vals <- unname(unlist(dattr))
    dtemp <- data.table(reporting_level = nm, vals = vals)
    names(dtemp)[names(dtemp) != "reporting_level"] <- attr_to_column
    df <- joyn::left_join(
      x = df,
      y = dtemp,
      by = "reporting_level",
      relationship = "many-to-one",
      reportvar = FALSE,
      verbose = FALSE
    )
  } else if (aux_data) {
    cli::cli_abort("`aux_data` attributes cannot be added as a column.")
  } else {
    cli::cli_abort("No matching condition for `attr_to_column()`.")
  }

  df
}


#' Row-bind lineup data.tables and store their attributes
#'
#' Iterates over `d_list`, optionally materialises selected attributes as
#' columns, then row-binds all elements into a single data.table.  The
#' per-element attribute lists are stored as additional attributes on the
#' combined result.
#'
#' @param d_list list of data.tables. Output of [load_list_refy()].
#' @param attr_to_column character vector. Attributes to promote to columns.
#' @param dist_stats logical vector of the same length as `attr_to_column`.
#' @param aux_data logical vector of the same length as `attr_to_column`.
#'
#' @return data.table.
#' @export
#'
#' @examples
#' \dontrun{
#' input_list <- list(country_code = c("ZAF", "COL"),
#'                    year         = list(c(2020, 2021), c(2015, 2016)))
#' x <- load_list_refy(input_list, path = gls$OUT_LINEUP_DIR_PC) |>
#'   append_refy_dt(attr_to_column = c("reporting_level", "welfare_type"))
#' }
append_refy_dt <- function(
  d_list,
  attr_to_column,
  dist_stats = rep(FALSE, length(attr_to_column)),
  aux_data = rep(FALSE, length(attr_to_column))
) {
  e <- rlang::new_environment()

  d_list <- lapply(d_list, \(x) {
    dattr <- base::attributes(x)
    assign(
      x = paste0(dattr$country_code, dattr$reporting_year, "_attr"),
      value = dattr,
      envir = e
    )

    idx_list <- seq_along(attr_to_column)

    Reduce(
      function(acc, i) {
        attr_to_column(
          acc,
          attr_to_column = attr_to_column[i],
          dist_stats = dist_stats[i],
          aux_data = aux_data[i],
          dattr = dattr
        )
      },
      idx_list,
      init = x
    )
  })

  dt <- rowbind(d_list)
  attrs_extra <- as.list(e)
  attributes(dt) <- c(attributes(dt), attrs_extra)

  dt
}


#' Expand an input list into flat country-year pairs
#'
#' @param input_list list with elements `country_code` (character vector) and
#'   `year` (single value, or list of year vectors with one per country).
#'
#' @return List of `list(country_code, year)`, one element per country-year.
#' @export
#'
#' @examples
#' \dontrun{
#' transform_input(list(country_code = c("ZAF", "COL"),
#'                      year = list(c(2020, 2021), c(2015, 2016))))
#' }
transform_input <- function(input_list) {
  country_codes <- input_list$country_code
  years <- input_list$year

  if (!is.list(years)) {
    years <- lapply(country_codes, function(x) years)
  } else if (length(years) != length(country_codes)) {
    cli::cli_abort(
      "Length of `year` list ({length(years)}) must match length of ",
      "`country_code` ({length(country_codes)})."
    )
  }

  output_list <- lapply(seq_along(country_codes), \(i) {
    lapply(years[[i]], \(y) list(country_code = country_codes[i], year = y))
  })

  unlist(output_list, recursive = FALSE)
}
