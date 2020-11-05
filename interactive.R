# This script demonstrates how to use the
# r_*() functions described at
# https://docs.ropensci.org/drake/reference/r_make.html

# First, define a script called _drake.R (example provided here).
# Then, try it out.

library(drake)
library(visNetwork) # so the graph renders

r_outdated()

r_make()

# r_outdated()

r_vis_drake_graph(targets_only = TRUE)
# r_vis_drake_graph()
