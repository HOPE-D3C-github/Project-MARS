# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# MARS EMI and EMA metadata analysis pipeline
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# 1. Read in raw ema and emi metadata (not battery data)
source("EMI_EMA_metadata/read_raw_data.R")
rm(list=ls())

# 2. Read in raw battery data
source("EMI_EMA_metadata/read_in_battery_data.R")
rm(list=ls())

# 3. Process battery data into time periods of battery categories
source("EMI_EMA_metadata/processing_battery_data_into_bins.R")
rm(list=ls())

# 4. Find withdrawn dates (where applicable)
source("EMI_EMA_metadata/get_withdrawn_dates.R")
rm(list=ls())

# 5. EMI randomization metadata analysis
source("EMI_EMA_metadata/EMI_metadata_analysis.R")
rm(list=ls())

# 6. EMA undelivered metadata analysis
source("EMI_EMA_metadata/EMA_metadata_analysis.R")
rm(list=ls())

# 7. Update datetime variables and prep data for output
source("EMI_EMA_metadata/update_dt_vars_and_output_data.R")
rm(list=ls())
