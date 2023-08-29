# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary: -----
#   * Read in metadata files from MD2k and combine files of the same datastream for all participants 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(dplyr)
library(tidyr)
library(gtools)
library(stringr)
library(lubridate)

source("paths.R")

# Participant IDs 
ema_subfolder_names_all <- list.files(path_to_input_data_from_MD2K)
# remove files (retain only folders) and remove folders for test participants
subset_indices <- replace_na(!str_detect(ema_subfolder_names_all, "_test_|.csv|.txt"))
ema_subfolder_names <- ema_subfolder_names_all[subset_indices]
ids_mars <- mixedsort(ema_subfolder_names)
remove(ema_subfolder_names_all, subset_indices, ema_subfolder_names)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Tailor Report ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
list_df_raw <- list()

# Specify data stream of interest
this_string <- "tailor_report.csv"

for(i in 1:length(ids_mars)){
  this_id <- ids_mars[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_to_input_data_from_MD2K, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    df_raw <- read.csv(file.path(path_to_input_data_from_MD2K, this_id, this_file), 
                       header = TRUE, 
                       sep = ",")  
    # Add column to record participant ID
    df_raw <- df_raw %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}


cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_tailor_report_files <- list_df_raw

tailor_report <- bind_rows(all_tailor_report_files)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# EMI Report ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
list_df_raw <- list()

# Specify data stream of interest
this_string <- "emi_report.csv"

for(i in 1:length(ids_mars)){
  this_id <- ids_mars[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_to_input_data_from_MD2K, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    df_raw <- read.csv(file.path(path_to_input_data_from_MD2K, this_id, this_file), 
                       header = TRUE, 
                       sep = ",")  
    # Add column to record participant ID
    df_raw <- df_raw %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}


cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_emi_report_files <- list_df_raw

emi_report <- bind_rows(all_emi_report_files)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# EMA Report ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
list_df_raw <- list()

# Specify data stream of interest
this_string <- "ema_report.csv"

for(i in 1:length(ids_mars)){
  this_id <- ids_mars[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_to_input_data_from_MD2K, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    df_raw <- read.csv(file.path(path_to_input_data_from_MD2K, this_id, this_file), 
                       header = TRUE, 
                       sep = ",")  
    # Add column to record participant ID
    df_raw <- df_raw %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}


cnt <- lapply(list_df_raw, ncol)
cnt <- unlist(cnt)
# Note that each participant's data frame will have varying number of columns
# due to the way the raw data is structured. Hence, we do not call do.call()
# and leave responses in list form
print(cnt)

all_ema_report_files <- list_df_raw

ema_report <- bind_rows(all_ema_report_files)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Save tailor_report, emi_report, ema_report datasets
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
save(tailor_report, emi_report, ema_report, file = file.path(path_to_staged, "tailor_emi_ema_reports.RData"))



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Other data streams
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Wakeup Info Today Status: ----
#      log of wakeup times. helpful to truncate blocks from previous day if impeding on next day's blocks
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

list_df_raw <- list()

# Specify data stream of interest
this_string <- "WAKEUP_INFO_TODAY--STATUS--org.md2k.scheduler.csv.bz2"

for(i in 1:length(ids_mars)){
  this_id <- ids_mars[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_to_input_data_from_MD2K, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    # Simply using read.csv's default value for sep (the comma: ",") will result in
    # some entries being viewed as corresponding to two cells when they should 
    # correspond to only one cell. This can happen, for example, 
    df_raw <- read.csv(file.path(path_to_input_data_from_MD2K, this_id, this_file), 
                       header = FALSE # STATUS files do not contain column names
                       )  
    # Add column to record participant ID
    df_raw <- as.data.frame(df_raw) %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    # Add df_raw to collection
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}

wakeup_info <- bind_rows(list_df_raw)

save(wakeup_info, file = file.path(path_to_staged, "wakeup_info.RData"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Wakeup Preset Times ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
list_df_raw_day_start <- list()

# Specify data stream of interest
these_strings <- c("WAKEUP--DAY0--org.md2k.studywithema.csv.bz2", "WAKEUP--DAY1--org.md2k.studywithema.csv.bz2", "WAKEUP--DAY2--org.md2k.studywithema.csv.bz2", "WAKEUP--DAY3--org.md2k.studywithema.csv.bz2",
                   "WAKEUP--DAY4--org.md2k.studywithema.csv.bz2", "WAKEUP--DAY5--org.md2k.studywithema.csv.bz2", "WAKEUP--DAY6--org.md2k.studywithema.csv.bz2", "WAKEUP--DAY7--org.md2k.studywithema.csv.bz2",
                   "WAKEUP--DAY8--org.md2k.studywithema.csv.bz2", "WAKEUP--DAY9--org.md2k.studywithema.csv.bz2")

for(i in 1:length(ids_mars)){
  this_id <- ids_mars[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_to_input_data_from_MD2K, this_id))
  for(string_i in these_strings){   # For each participant, iterate once for every day-file
    # Pick out file names related to data stream of interest
    idx <- match(x=string_i, table=all_files)
    # Pick out corresponding files
    this_file <- all_files[idx]
    
    # Read file if it exists for given participant
    if(!is.na(this_file)){
      df_raw <- read.csv(file.path(path_to_input_data_from_MD2K, this_id, this_file),
                         header = FALSE,
                         sep = ",")
      # Add column to record participant ID
      df_raw <- df_raw %>%
        mutate(participant_id = this_id,
               day_int = as.integer(str_remove(str_split(string_i, '--', simplify = T)[1,2], "DAY")) + 1L) %>%
        select(participant_id, day_int, everything())
      
      #deduplicate entirely duplicated rows
      df_raw <- df_raw[!duplicated(df_raw),]
      
      
      list_df_raw_day_start <- append(list_df_raw_day_start, list(df_raw))
      
    }
  }
}


wakeup_times_set <- bind_rows(list_df_raw_day_start)

wakeup_times_set <- wakeup_times_set %>% mutate(time_set_hrts = as_datetime(V1/1000, tz = "America/Denver"), wakeup_time_hrts = as_datetime(V3/1000, tz = "America/Denver"),
                                                olson = case_when(
                                                  V2/(1000*60*60) == -5 ~ "US/Eastern", 
                                                  V2/(1000*60*60) == -6 ~ "US/Central", 
                                                  V2/(1000*60*60) == -7 ~ "US/Mountain", 
                                                  V2/(1000*60*60) == -8 ~ "US/Pacific"
                                                )) %>% select(-c(V1,V2,V3))

save(wakeup_times_set, file = file.path(path_to_staged, "wakeup_times_set.RData"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# System Log file: ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

list_df_raw <- list()

# Specify data stream of interest
this_string <- "SYSTEM_LOG--org.md2k.scheduler.csv.bz2"

for(i in 1:length(ids_mars)){
  this_id <- ids_mars[i]
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_to_input_data_from_MD2K, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    # Simply using read.csv's default value for sep (the comma: ",") will result in
    # some entries being viewed as corresponding to two cells when they should 
    # correspond to only one cell. This can happen, for example, 
    df_raw <- read.csv(file.path(path_to_input_data_from_MD2K, this_id, this_file), 
                       header = FALSE # STATUS files do not contain column names
    )  
    # Add column to record participant ID
    df_raw <- as.data.frame(df_raw) %>% 
      mutate(participant_id = this_id) %>% 
      select(participant_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    # Remove records without a valid timestamp and reformat V1 as a double
    df_raw <- df_raw %>% filter(!is.na(V2)) %>% 
      mutate(V1 = as.double(V1))
    
    # Add df_raw to collection
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}

system_log <- bind_rows(list_df_raw)

save(system_log, file = file.path(path_to_staged, "system_log.RData"))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Conditions file: ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

list_df_raw <- list()

# Specify data stream of interest
this_string <- "CONDITION--org.md2k.scheduler.csv.bz2"

for(i in 1:length(ids_mars)){
  this_id <- ids_mars[i]
  print(this_id)
  
  # List all file names within folder corresponding to this_id
  all_files <- list.files(file.path(path_to_input_data_from_MD2K, this_id))
  # Pick out file names related to data stream of interest
  idx <- match(x=this_string, table=all_files)
  # Pick out corresponding files
  this_file <- all_files[idx]
  
  # Read file if it exists for given participant
  if(!is.na(this_file)){
    # Simply using read.csv's default value for sep (the comma: ",") will result in
    # some entries being viewed as corresponding to two cells when they should 
    # correspond to only one cell. This can happen, for example, 
    df_raw <- read.csv(file.path(path_to_input_data_from_MD2K, this_id, this_file), 
                       header = FALSE # STATUS files do not contain column names
    )  
    # Add column to record participant ID
    df_raw <- as.data.frame(df_raw) %>% 
      mutate(mars_id = this_id) %>% 
      select(mars_id, everything())
    
    #deduplicate entirely duplicated rows
    df_raw <- df_raw[!duplicated(df_raw),]
    
    # Remove records without a valid timestamp and reformat V1 as a double
    df_raw <- df_raw %>% filter(!is.na(V2)) %>% 
      mutate(V1 = as.double(V1))
    
    # Add df_raw to collection
    list_df_raw <- append(list_df_raw, list(df_raw))
  }else{
    next
  }
}

conditions <- bind_rows(list_df_raw)

save(conditions, file = file.path(path_to_staged, "conditions.RData"))
