# Uncomment to run targets sequentially on your local machine.
targets::tar_make()#_future()
targets::tar_visnetwork(targets_only = TRUE)

pushover("Done running targets")

# targets::tar_visnetwork()
# Uncomment to run targets in parallel
# on local processes or a Sun Grid Engine cluster.
# targets::tar_make_clustermq(workers = 2L)
# targets::tar_make(callr_function = NULL)