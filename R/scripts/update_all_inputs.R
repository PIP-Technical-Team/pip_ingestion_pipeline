# ---- Step 0: Start up ---- 
message('Start up.')

# Set initial parameters 
options("RStata.StataPath" = "\"C:\\Program Files\\Stata16\\StataMP-64\"")
options('RStata.StataVersion' = 16)
PIP_DATA_DIR <- '//w1wbgencifs01/pip/PIP-Data/_testing/pipdp_testing/' 
PCN_MASTER_DIR <- 'P:/01.PovcalNet/00.Master/02.vintage/'

# Load packages 
# remotes::install_github("PIP-Technical-Team/pipaux@master",
#                         dependencies = FALSE)

library(pipaux)
library(pipload)
library(RStata)

# Load pipeline functions
source('R/_common.R')

# Create folder and file structure (if needed)
create_input_folder_structure(PIP_DATA_DIR)

# Check for missing files 
check_missing_input_files(PIP_DATA_DIR)

message('Data directory set to: ', PIP_DATA_DIR)

  
# ---- Step 1: Update PFW ---- 

# Update or create country_list.fst
pipaux::pip_country_list('update', maindir = PIP_DATA_DIR)

# Update or create pfw.fst  
message('Updating PFW...') 
pipaux::pip_pfw('update', maindir = PIP_DATA_DIR)


# ---- Step 2: Update DLW inventory ---- 

# Notes: 
# 
# If you are running this code from the remote server,
# it will require you to log in. 

message('Updating DLW inventory...') 
cmd <- sprintf('pipdp dlw maindir(%s)', PIP_DATA_DIR)
RStata::stata(cmd)


# ---- Step 3: Update surveys ---- 

# Make sure DLW setup files are up-to-date (needed for 'files' option)
message('Copying setup files to Stata C:/ado/ directory...') 
file.copy(from = 'datalibweb.ado', 
          to = 'C:/ado/personal/datalibweb/datalibweb.ado',
          overwrite = TRUE) 
file.copy(from = 'GMD.do', 
          to = 'C:/ado/personal/datalibwebGMD.do',
          overwrite = TRUE) 

# Update surveys from P-drive (all surveys)
message('Copying grouped data files from P-drive...') 
cmd <- sprintf('pipdp group, countries(all) replace maindir(%s)', 
               PIP_DATA_DIR) 
RStata::stata(cmd)

# Update surveys from DLW (only new surveys)
message('Downloading new surveys from Datalibweb...') 
cmd <- sprintf('pipdp micro, countries(all) files new maindir(%s)',
               PIP_DATA_DIR) 
RStata::stata(cmd)

# ---- Step 4: Update PIP inventory ---- 

message('Updating PIP inventory...') 
pipload::pip_update_inventory(maindir = PIP_DATA_DIR)


# ---- Step 5: Update AUX data ---- 

# Notes 
#
# * Updates to WEO data needs to be manually downloaded. 
# * If NAS special files changes this need to be updated in pipaux. 

message('Updating AUX datasets...') 

# Update CPI
pipaux::update_aux('cpi', 
  maindir = PIP_DATA_DIR) 

# Update PPP
pipaux::update_aux('ppp', 
  maindir = PIP_DATA_DIR) 

# Update GDP
pipaux::update_aux(
  'gdp',
  maddison_action = 'update', 
  weo_action = 'update', 
  maindir = PIP_DATA_DIR)

# Update PCE
pipaux::update_aux('pce',
  maindir = PIP_DATA_DIR) 

# Update POP
pipaux::update_aux(
  'pop',
  src = 'emi', 
  maindir = PIP_DATA_DIR) 

# Update GDM 
pipaux::update_aux(
  'gdm',
  pcndir = PCN_MASTER_DIR, 
  maindir = PIP_DATA_DIR) 


# ---- Step 6: Done ---- 

# Done! 
message('Done. Code is poetry.')


