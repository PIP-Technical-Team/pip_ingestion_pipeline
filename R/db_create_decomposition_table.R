#' Create decomposition table
#'
#' @inheritParams db_create_ref_year_table
#' @return data.table
#' @export
db_create_decomposition_table <- function(dsm_table) {

  pop <- unique(dsm_table$pop_data_level)
  welfare <- unique(dsm_table$welfare_type)

  data.table::data.table(
    variable_code = c(rep('welfare_type', length(welfare)),
                      rep('pop_data_level', length(pop))),
    variable_values = c(welfare, pop)
  )
}

