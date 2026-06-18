# Uncomment to run targets sequentially on your local machine.
for (proj in c("ppp2021", "ppp2017")) {
  Sys.setenv(TAR_PROJECT = proj)
  # targets::tar_make()
  run_tar()
}

# to run only one PPP year
Sys.setenv(TAR_PROJECT = "ppp2021")
Sys.setenv(TAR_PROJECT = "ppp2017")
run_tar()


# targets::tar_visnetwork()
# Uncomment to run targets in parallel
# on local processes or a Sun Grid Engine cluster.
# targets::tar_make_clustermq(workers = 2L)
# targets::tar_make(callr_function = NULL)

## Uncomment to debug
tar_load_globals()
tar_meta(fields = error, complete_only = TRUE)
