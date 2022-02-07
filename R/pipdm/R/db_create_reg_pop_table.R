#' Create regional population table
#'
#' Create an aggregated population table by region.
#'
#' @inheritParams db_create_ref_year_table
#' @param cl_table data.table: A table with country metadata.
#'
#' @return data.table
#' @export
db_create_reg_pop_table <- function(pop_table,
                                    cl_table,
                                    pip_years,
                                    region_code =
                                      c(
                                        "pcn_region_code",
                                        "region_code"
                                      )) {

  # Match argument
  region_code <- match.arg(region_code)

  # Subselect columns
  cl_table$region_code_to_use <- cl_table[[region_code]]
  cl_table <- cl_table[, c("country_code", "region_code_to_use")]

  # Merge POP table w/ WDI meta (left join)
  dt <- data.table::merge.data.table(
    pop_table, cl_table,
    by = "country_code",
    all.x = TRUE
  )

  # Remove territories without regional classification
  # i.e "ESH" "GLP" "GUF" "MTQ" "MYT" "REU"
  dt <- dt[!is.na(region_code_to_use)]

  # Aggregate population by region
  dt <- dt[, .(pop = sum(pop, na.rm = TRUE)),
    by = .(region_code_to_use, year, pop_data_level)
  ]

  # Subset to only include years used by PIP
  dt <- dt[year %in% pip_years]

  # Subset only national rows
  dt <- dt[pop_data_level == "national"]
  dt$pop_data_level <- NULL

  # Add grouping type column
  dt[, grouping_type := "WB"]

  # Change colnames
  data.table::setnames(
    dt, c("year", "pop", "region_code_to_use"),
    c("reporting_year", "reporting_pop", "region_code")
  )

  return(dt)
}
