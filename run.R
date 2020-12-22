# Uncomment to run targets sequentially on your local machine.
targets::tar_make()
targets::tar_visnetwork()

# Uncomment to run targets in parallel
# on local processes or a Sun Grid Engine cluster.
# targets::tar_make_clustermq(workers = 2L)