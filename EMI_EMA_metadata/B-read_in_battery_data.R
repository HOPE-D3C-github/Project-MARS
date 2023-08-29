# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Read in each participant's phone battery data, apply initial 
#             data reduction filters, and create a list of participant-level 
#             battery data dataframes           
#     
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(dplyr)
library(lubridate)
library(tidyr)
library(tictoc)
library(stringr)
library(gtools)

source("paths.R")

tic("Main")
# ------------------------------------------------------------------------------
# START read-raw-data steps ####
# Files are:
# "BATTERY--org.md2k.phonesensor--PHONE.json" - metadata
# "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2" - battery data
# -----------------------------------------------------------------------------

# participant's study period dates
study_period_dates <- readRDS(file.path(path_to_input_data_from_jamie, "dat_matched_to_decision_points.rds")) %>% 
  mutate(v1_date_began_unix = as.numeric(v1_date_began_mountain), v1_date_began_plus_nine_unix = as.numeric(v1_date_began_plus_nine_mountain)) %>% 
  select(mars_id, v1_date_began_unix, v1_date_began_plus_nine_unix) %>% filter(!is.na(v1_date_began_unix)) %>% distinct()


# participant's list
ids_pt <- study_period_dates %>% .$mars_id

# # Participant IDs 
# ema_subfolder_names_all <- list.files(path_to_input_data_from_MD2K)
# # remove files (retain only folders) and remove folders for test participants
# subset_indices <- replace_na(!str_detect(ema_subfolder_names_all, "_test_|.csv|.txt"))
# ema_subfolder_names <- ema_subfolder_names_all[subset_indices]
# ids_pt <- mixedsort(ema_subfolder_names)
# remove(ema_subfolder_names_all, subset_indices, ema_subfolder_names)

# -----------------------------------------------------------------------------
# Read battery raw data
# -----------------------------------------------------------------------------

# list_df_filtered <- list()
list_df_raw <- list()

# Specify file of interest
this_file <- "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2"

#start_time <- Sys.time()
for(i in 1: length(ids_pt)){
  this_id <- ids_pt[i]
  print(this_id)
  
  tmp <- try(read.csv(file.path(path_to_input_data_from_MD2K, this_id, this_file), 
                      header = FALSE, 
                      sep = ","))
  if (!inherits(tmp, 'try-error')){ 
    # If it inherits a try-error from attempting to read the battery data file, then move on to the next participant
    # Ran into an error when there are no lines of data in the file ""no lines available in input". this logical allows the process to continue
    df_raw <- tmp
    remove(tmp)
    
    # Seeing an issue where other data is recorded in the battery log. Appears to be phone status data. Writing to a V6 column only when the occurs
    if(ncol(df_raw) == 6){
      # Remove the rows with 6 columns of data (sixth column is not '') and then select the first 5 rows
      df_raw <- df_raw %>% filter(V6 == '') %>% select(colnames(.)[1:5])
    }
    
    if (ncol(df_raw) == 5){   
      # Another issue where the data file didn't have all the 5 columns
      # Seeing another issue where phone status data is written in with the batter data. Identifiable by two ways:
      # 1) 4th column has a value of 'DEBUG'
      #   or
      # 2) Has empty value '' for 3rd column (which is supposed to be battery percent)
      if(ncol(df_raw) > 3){df_raw <- df_raw %>% filter( V4 != 'DEBUG' & V3 != '' )}
      
      df_raw <- df_raw %>%  
        stats::setNames(c("datetime", "unk_1", "battery_percent", "battery_voltage", "unk_2")) %>% 
        select(datetime, battery_percent)
      
      df_raw  <- df_raw %>% mutate(datetime = as.numeric(datetime)/1000L)  # convert from unix with milliseconds to unix with seconds
      
      # Add column to record participant ID
      df_raw <- df_raw %>% 
        mutate(participant_id = this_id) %>% 
        select(participant_id, everything())
      
      #deduplicate entirely duplicated rows
      df_raw <- df_raw[!duplicated(df_raw),]
      
      # Apply time filter for within study period
      v1_date_began_unix_pt_i <- study_period_dates %>% filter(mars_id == this_id) %>% .$v1_date_began_unix
      v1_date_began_plus_nine_unix_pt_i <- study_period_dates %>% filter(mars_id == this_id) %>% .$v1_date_began_plus_nine_unix
      
      
      # df_raw <- df_raw %>%
      #   left_join(y=study_period_dates,
      #             by = c("participant_id"="mars_id"))
      
      df_raw <- df_raw %>% filter(between(datetime, v1_date_began_unix_pt_i, v1_date_began_plus_nine_unix_pt_i))
      
      # df_raw <- df_raw %>% select(-c(v1_date_began_unix, v1_date_began_plus_nine_unix))
      
      # Add participant's data into list of all participants data
      list_df_raw <- append(list_df_raw, list(df_raw))
    }
  } 
}


list_battery_data <- list_df_raw

if(T){save(list_battery_data,
           file = file.path(path_to_staged, "battery_data_mars.RData"))}

toc()
