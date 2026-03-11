# CMD population weight scaling
# Adapted from CMD_new_methodoly/R/scale_weights.R

#' Load and reshape population aux data for CMD weight scaling
#'
#' Reads `pop.fst` from the `_aux` sub-folder of the vintage output directory,
#' pivots from wide to long format, and returns a tidy data.table suitable
#' for joining against CMD welfare distributions.
#'
#' @param aux_dir character / fs_path. Path to the `_aux` directory
#'   (typically `fs::path(gls$OUT_DIR_PC, gls$vintage_dir, "_aux")`).
#'
#' @return data.table with columns `country_code`, `data_level`,
#'   `reporting_year` (numeric), `reporting_pop`.
#' @family cmd
#' @export
get_pop_to_scale <- function(aux_dir) {
  fst::read_fst(path = fs::path(aux_dir, "pop.fst")) |>
    pivot(ids = c("country_code", "data_level")) |>
    frename(reporting_year = variable, reporting_pop = value) |>
    qDT() |>
    fmutate(reporting_year = as.numeric(as.character(reporting_year)))
}


#' Scale CMD distribution weights to match WDI population totals
#'
#' Compares the sum of weights in each country-year element of `l_cmd` against
#' the corresponding population from `pop`.  Elements whose weight sum differs
#' from the population by more than 1e-5 are rescaled so that
#' `sum(weight) == reporting_pop`.
#'
#' @param l_cmd named list of data.tables. Output of [list_cmd_welfare()].
#' @param pop data.table. Output of [get_pop_to_scale()].
#'
#' @return Named list of rescaled data.tables.
#' @family cmd
#' @export
scale_weights <- function(l_cmd, pop) {
  # Step 1: aggregate weight sums across the list
  dt_weights <- rbindlist(l_cmd, idcol = "id")

  weights_summary <- dt_weights[,
    .(weight_sum = sum(weight)),
    by = .(country_code, reporting_year, reporting_level)
  ]

  # Step 2: join against population
  pop_filtered <- pop[,
    .(country_code, reporting_year, reporting_level = data_level, reporting_pop)
  ]

  wt_check <- joyn::joyn(
    weights_summary,
    pop_filtered,
    by = c("country_code", "reporting_year", "reporting_level"),
    match_type = "1:1",
    keep = "left",
    verbose = FALSE
  )

  wt_check[, scaling_factor := reporting_pop / weight_sum]

  # Step 3: build scaling lookup keyed for fast merge
  scaling_lookup <- wt_check[
    !round(scaling_factor, 5L) == 1,
    .(country_code, reporting_year, reporting_level, scaling_factor)
  ]

  if (nrow(scaling_lookup) == 0L) {
    return(l_cmd)
  }

  setkey(scaling_lookup, country_code, reporting_year, reporting_level)

  lapply(l_cmd, function(x) {
    cn <- funique(x$country_code)
    yr <- funique(x$reporting_year)
    rl <- funique(x$reporting_level)
    sf <- scaling_lookup[
      .(cn, yr, rl),
      scaling_factor,
      nomatch = NULL
    ]
    if (!length(sf)) {
      return(x)
    }
    x[, weight := weight * sf[[1L]]]
    x
  })
}
