# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# project:       TEsting results
# Author:        Andres Castaneda
# Dependencies:  The World Bank
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Creation Date:    2022-06-03
# Modification Date: 
# Script version:    01
# References:
# 
# 
# Output:             tables
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                   Load Libraries   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(data.table)

remotes::install_github("PIP-technical-team/pipapi@dev")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                   Subfunctions   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                   Set up   ---------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

data_pipeline <-  "//w1wbgencifs01/pip/pip_ingestion_pipeline/pc_data/output-tfs-sync/ITSES-POVERTYSCORE-DATA"
lkups <- pipapi::create_versioned_lkups(data_pipeline)
ctr <- "all"

## survey data
# pip1   <- pipapi::pip (country = ctr, lkup = lkups$versions_paths$`20220504_2017_01_02_INT`)
# pip2   <- pipapi::pip (country = ctr, lkup = lkups$versions_paths$`20220602_2017_01_02_INT`)

pip1   <- pipapi::pip (country = ctr, lkup = lkups$versions_paths$`20220504_2011_02_02_INT`)
pip2   <- pipapi::pip (country = ctr, lkup = lkups$versions_paths$`20220602_2011_02_02_INT`)

waldo::compare(pip1, pip2)



## lineup data
pip1   <- pipapi::pip (country = ctr, 
                       fill_gaps = TRUE,
                       lkup = lkups$versions_paths$`20220504_2011_02_02_INT`)
pip2   <- pipapi::pip (country = ctr, 
                       fill_gaps = TRUE,
                       lkup = lkups$versions_paths$`20220602_2011_02_02_INT`)


waldo::compare(pip1[country_code == "SYR"], pip2[country_code == "SYR"])


waldo::compare(pip1[country_code != "SYR"], pip2[country_code != "SYR"])




# 2017 changes


pip1   <- pipapi::pip (country = ctr, lkup = lkups$versions_paths$`20220504_2017_01_02_INT`)
pip2   <- pipapi::pip (country = ctr, lkup = lkups$versions_paths$`20220602_2017_01_02_INT`)

waldo::compare(pip1, pip2)



waldo::compare(pip1[country_code == "SOM"], pip2[country_code == "SOM"])


waldo::compare(pip1[country_code != "SOM"], pip2[country_code != "SOM"])


# Changes in PROD


pip1   <- pipapi::pip (country = ctr, lkup = lkups$versions_paths$`20220503_2011_02_02_PROD`)
pip2   <- pipapi::pip (country = ctr, lkup = lkups$versions_paths$`20220609_2011_02_02_PROD`)

waldo::compare(pip1, pip2)


cct <- "IND"

waldo::compare(pip1[country_code %in% cct], pip2[country_code %in% cct])


waldo::compare(pip1[!country_code %in% cct], pip2[!country_code %in% cct])



# Changes in PROD and lineupt


pip1   <- pipapi::pip (country = ctr, 
                       fill_gaps = TRUE,
                       lkup = lkups$versions_paths$`20220503_2011_02_02_PROD`)
pip2   <- pipapi::pip (country = ctr, 
                       fill_gaps = TRUE,
                       lkup = lkups$versions_paths$`20220609_2011_02_02_PROD`)

waldo::compare(pip1, pip2)


cct <- "SYR"

waldo::compare(pip1[country_code %in% cct], pip2[country_code %in% cct])


waldo::compare(pip1[!country_code %in% cct], pip2[!country_code %in% cct])








# testing single release


df   <- pipapi::pip (country = ctr, 
                       fill_gaps = TRUE,
                       lkup = lkups$versions_paths$`20220602_2017_01_02_INT`)
