library(dplyr)
library(stringr)

# pre_box <- getwd() %>% str_remove("Box.+")
# pre_box <- getwd() %>% str_remove("Documents.+")

pre_box <- getwd() %>% str_remove("OneDrive.+|Documents.+|Box.+")

# Inputs
path_to_input_data_from_MD2K <- paste0(pre_box, "Box/mhealth_data_management/MARS/MARS - data sharing/mars_ema_raw_datashare_20230509")
path_to_input_data_redcap_demogs <- paste0(pre_box, "Box/mhealth_data_management/MARS/MARS - data sharing/mars_redcap_demogs_20230522")
path_to_input_data_RSR_visit <- paste0(pre_box, "Box/mhealth_data_management/MARS/MARS - data sharing/mars_visit_raw_datashare_20230510")
path_to_input_data_from_jamie <- paste0(pre_box, 'Box/MARS Pre-Curated Data/EMA/Input/2023-09-05')
path_to_other_input_data <- paste0(pre_box, 'Box/MARS Pre-Curated Data/EMA/Input')

# Staged
path_to_staged <- paste0(pre_box, 'Box/MARS Pre-Curated Data/EMA/Staged')

# Outputs
path_to_outputs_4_analysis <- paste0(pre_box, 'Box/MARS Curated Data/EMA/Release v1.0.0')
path_to_outputs_4_datamanagers <- paste0(pre_box, 'Box/MARS Curated Data - available upon request/EMA/Release v1.0.0')
