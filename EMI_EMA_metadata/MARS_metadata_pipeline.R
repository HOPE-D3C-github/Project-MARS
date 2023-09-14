# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# MARS EMI and EMA metadata analysis pipeline
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# A. Read in raw ema and emi metadata (not battery data)
source("A-read_raw_data.R")
rm(list=ls())

# B. Read in raw battery data
source("B-read_in_battery_data.R")
rm(list=ls())

# C. Process battery data into time periods of battery categories
source("C-processing_battery_data_into_bins.R")
rm(list=ls())

# D. Find withdrawn dates (where applicable)
source("D-get_withdrawn_dates.R")
rm(list=ls())

# E. EMI randomization metadata analysis
source("E-EMI_metadata_analysis.R")
rm(list=ls())

# F. EMA undelivered metadata analysis
source("F-EMA_metadata_analysis.R")
rm(list=ls())

# G. Extract New Variable Names for Additional Var Codebook
source("G-create_codebook.R")
rm(list=ls())

# H. Update datetime variables and prep data for output
source("H-update_dt_vars_and_output_data.R")
rm(list=ls())
