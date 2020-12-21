# devtools::install_github("PIP-Technical-Team/wbpip")
# devtools::install_github("PIP-Technical-Team/pipdm@improvments")

library(targets)
# This is an example target script.
# Read the tar_script() help file for details.


# Set target-specific options such as packages.
pkgs <- c("data.table", 
          "pipload", 
          "dplyr",
          "purrr",
          "pipdm")

tar_option_set(packages = pkgs,
               imports = pkgs
               )

# Define targets
targets <- list(
  tar_target(data, data.frame(x = sample.int(100), y = sample.int(100))),
  tar_target(summary, summ(data)) # Call your custom functions as needed.
)

# End with a call to tar_pipeline() to wrangle the targets together.
# This target script must return a pipeline object.
tar_pipeline(targets)
