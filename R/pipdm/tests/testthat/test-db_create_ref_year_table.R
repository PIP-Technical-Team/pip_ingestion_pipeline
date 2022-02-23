skip_if(Sys.getenv("PIPDM_RUN_LOCAL_TESTS") != "TRUE")

dir <- 'Y:/pip_ingestion_pipeline/pc_data/output/estimations/'
dsm_table <- fst::read_fst(sprintf('%ssurvey_means.fst', dir), as.data.table = TRUE)
pfw_table <- pipaux::load_aux("pfw")
gdp_table <- pipaux::load_aux("gdp")
pce_table <- pipaux::load_aux("pce")
pop_table <- pipaux::load_aux("pop")

# Select ARG
dsm_table <- dsm_table[country_code == "ARG"]

# Create refyear table
ref_year_table <- db_create_ref_year_table(
  dsm_table = dsm_table,
  gdp_table = gdp_table,
  pce_table = pce_table,
  pop_table = pop_table,
  pfw_table = pfw_table,
  ref_years = 1981:2019,
  pip_years = 1981:2019
)

test_that("db_create_ref_year_table() does NOT add national survey coverage rows for countries wo/ any national level surveys", {
  expect_true(all(ref_year_table$survey_coverage == "urban"))
})

