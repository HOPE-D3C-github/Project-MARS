# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# MARS EMI and EMA metadata analysis pipeline
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# A. Read in raw ema and emi metadata (not battery data)
source("EMI_EMA_metadata/A-read_raw_data.R")
rm(list=ls())

# B. Read in raw battery data
source("EMI_EMA_metadata/B-read_in_battery_data.R")
rm(list=ls())

# C. Process battery data into time periods of battery categories
source("EMI_EMA_metadata/C-processing_battery_data_into_bins.R")
rm(list=ls())

# D. Find withdrawn dates (where applicable)
source("EMI_EMA_metadata/D-get_withdrawn_dates.R")
rm(list=ls())

# E. EMI randomization metadata analysis
source("EMI_EMA_metadata/E-EMI_metadata_analysis.R")
rm(list=ls())

# F. EMA undelivered metadata analysis
source("EMI_EMA_metadata/F-EMA_metadata_analysis.R")
rm(list=ls())

# G. Update datetime variables and prep data for output
source("EMI_EMA_metadata/G-update_dt_vars_and_output_data.R")
rm(list=ls())
