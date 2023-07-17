# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary:  Combine the participant-level phone battery dataframes and apply binning
#             to create a dataset with start and end time for categories of battery status
#     
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(lubridate)
library(tidyr)
library(purrr)

source("paths.R")

load(file.path(path_to_staged, "battery_data_mars.RData"))

filtered_battery_data <- bind_rows(list_battery_data)

study_period_dates <- readRDS(file.path(path_to_input_data_from_jamie, "dat_matched_to_decision_points.rds")) %>% 
  mutate(v1_date_began_unix = as.numeric(v1_date_began_mountain), v1_date_began_plus_nine_unix = as.numeric(v1_date_began_plus_nine_mountain)) %>% 
  select(mars_id, v1_date_began_unix, v1_date_began_plus_nine_unix) %>% filter(!is.na(v1_date_began_unix)) %>% distinct()


filtered_battery_data <- filtered_battery_data %>%
  filter(datetime > 1) %>% filter(battery_percent <= 100 & battery_percent >= 0) %>% 
  group_by(participant_id) %>%  
  mutate( battery_percent = as.integer(round(as.numeric(battery_percent))),
          datetime_hrts_mtn = as_datetime(as.numeric(datetime), tz = "America/Denver"),
          #datetime_mtn = as.POSIXct(as.numeric(datetime), tz = "UTC", origin="1970-01-01"),
          #datetime_UTC_roundmin = round_date(datetime_UTC, unit = "minute"),
          lag_diff_battery_percent = battery_percent - lag(battery_percent, order_by = datetime),
          lead_diff_battery_percent = lead(battery_percent, order_by = datetime) - battery_percent,
          lag_battery_percent = lag(battery_percent, order_by = datetime),
          lead_battery_percent = lead(battery_percent, order_by = datetime),
          lag_diff_secs = datetime - lag(datetime, order_by = datetime)) %>% 
  ungroup()


filtered_battery_data <- filtered_battery_data %>% 
  mutate(
    bat_bin = case_when(
      lag_diff_secs > 300 ~ "No Battery Data",
      battery_percent <= 100 & battery_percent > 10 ~ "10-100%",
      battery_percent <= 10 & battery_percent >= 0 ~ "0-10%"
    )
  )

filtered_battery_data %>% ungroup() %>% count(bat_bin)


# -------------------------------------------------------
# Use the bat_bin variable to reduce down to time ranges in each category
# -------------------------------------------------------

# Shell to add to with each participant
battery_binned_all <- data.frame(
  participant_id = character(),
  unix_datetime_start = integer(),
  unix_datetime_end = integer(),
  datetime_hrts_mtn_start = as_datetime(character(), tz = "America/Denver"),
  datetime_hrts_mtn_end = as_datetime(character(), tz = "America/Denver"),
  battery_status = character(),
  battery_percent_period_start = integer(),
  battery_percent_period_end = integer(),
  time_duration_minutes = numeric(),
  time_duration_hours = numeric()
) 

for (participant in unique(filtered_battery_data$participant_id)){
  print(participant)
  dat_battery_participant <- filtered_battery_data %>% filter(participant_id == participant)
  
  # Add a lead variable to see if the battery status changed from the current record to the next
  dat_battery_participant <- dat_battery_participant  %>% mutate(bat_bin_lead = lead(bat_bin),
                                                                 bat_bin_lag = lag(bat_bin))
  
  # get participant's study period datetime range
  pt_study_period <- study_period_dates %>% filter(mars_id == participant)
  
  dat_battery_participant <- dat_battery_participant %>% 
    filter(bat_bin != bat_bin_lead | bat_bin != bat_bin_lag | row_number() == 1 | row_number()==nrow(.)) %>% 
    rename(
      unix_datetime_start = datetime,
      datetime_hrts_mtn_start = datetime_hrts_mtn,
      battery_status = bat_bin
    ) %>% 
    mutate(
      unix_datetime_end = lead(unix_datetime_start),
      datetime_hrts_mtn_end = lead(datetime_hrts_mtn_start), 
      .after = datetime_hrts_mtn_start
    ) %>% select(-lag_diff_secs, -lag_diff_battery_percent, -lead_diff_battery_percent, -lead_battery_percent, -battery_status) %>% 
    rename(battery_status = bat_bin_lead) %>% 
    mutate(
      unix_datetime_end = case_when(
        row_number() == nrow(.) ~ pt_study_period$v1_date_began_plus_nine_unix,
        T ~ unix_datetime_end),
      datetime_hrts_mtn_end = case_when(
        row_number() == nrow(.) ~ as_datetime(pt_study_period$v1_date_began_plus_nine_unix, tz = "America/Denver"),
        T ~ datetime_hrts_mtn_end),
      battery_status = case_when(
        row_number() == nrow(.) ~ "No Battery Data",
        T ~ battery_status)
    )
  

  dat_battery_participant <- dat_battery_participant %>% 
    add_row(
      data.frame(
        participant_id = participant, unix_datetime_start = pt_study_period$v1_date_began_unix, battery_percent = NA, datetime_hrts_mtn_start = as_datetime(pt_study_period$v1_date_began_unix, tz = "America/Denver"),
        unix_datetime_end = dat_battery_participant$unix_datetime_start[1], datetime_hrts_mtn_end = dat_battery_participant$datetime_hrts_mtn_start[1],
        lag_battery_percent = NA, battery_status = "No Battery Data", bat_bin_lag = NA), 
      .before = 1
    ) %>% 
    mutate(battery_percent_period_start = battery_percent, battery_percent_period_end = lead(battery_percent)) %>% 
    select(-lag_battery_percent, -bat_bin_lag, -battery_percent)
  
  
  
  # dat_battery_participant <- dat_battery_participant %>% 
  #   add_row(
  #     data.frame(
  #       participant_id = participant, unix_datetime_start = 1262304001, battery_percent = NA, datetime_hrts_mtn_start = as_datetime(1262304001, tz = "America/Denver"),
  #       unix_datetime_end = dat_battery_participant$unix_datetime_start[1], datetime_hrts_mtn_end = dat_battery_participant$datetime_hrts_mtn_start[1],
  #       lag_battery_percent = NA, battery_status = "No Battery Data", bat_bin_lag = NA), 
  #     .before = 1
  #   ) %>% 
  #   mutate(battery_percent_period_start = battery_percent, battery_percent_period_end = lead(battery_percent)) %>% 
  #   select(-lag_battery_percent, -bat_bin_lag, -battery_percent)
  
  # Fixing bug where consecutive same bins are not aggregated
  dat_battery_participant_v2 <- dat_battery_participant %>%
    mutate(lead_same_status = replace_na(lead(battery_status) == battery_status, FALSE),
           lag_same_status = replace_na(lag(battery_status) == battery_status, FALSE))
  
  list_of_rows_to_remove <- c()
  
  for (row_i in 1:nrow(dat_battery_participant_v2)){
    if(dat_battery_participant_v2$lead_same_status[row_i]){
      dat_battery_participant$unix_datetime_end[row_i] <- dat_battery_participant$unix_datetime_end[row_i + 1]
      dat_battery_participant$datetime_hrts_mtn_end[row_i] <- dat_battery_participant$datetime_hrts_mtn_end[row_i + 1]
      dat_battery_participant$battery_percent_period_end[row_i] <- dat_battery_participant$battery_percent_period_end[row_i + 1]
    }
    if(dat_battery_participant_v2$lag_same_status[row_i]){
      list_of_rows_to_remove <- append(list_of_rows_to_remove, row_i)
    }
  }
  
  if(!is.null(list_of_rows_to_remove)){
    dat_battery_participant <- dat_battery_participant[-list_of_rows_to_remove,]
  }
  
  # Add variable for the time duration in minutes spanned by the battery status 
  dat_battery_participant <- dat_battery_participant %>% 
    mutate(
      time_duration_minutes = round(time_length(datetime_hrts_mtn_end - datetime_hrts_mtn_start, "minutes"), digits = 2),
      time_duration_hours = round(time_length(datetime_hrts_mtn_end - datetime_hrts_mtn_start, "hours"), digits = 2)
    )
  
  # Add the participant's data into the dataframe for all participants
  battery_binned_all <- battery_binned_all %>% 
    add_row(dat_battery_participant)
}

battery_binned_all %>% count(battery_status)


battery_binned_all <- battery_binned_all %>% 
    #filter(battery_status == "No Battery Data") %>% 
    mutate(battery_percent_change = battery_percent_period_end - battery_percent_period_start, 
           battery_percent_change_bin = case_when(
             between(battery_percent_change, -2, 2) ~ "No Change",
             battery_percent_change < -2 ~ "Decrease",
             battery_percent_change > 2 ~ "Increase",
             T ~ NA_character_
             )) 

save(battery_binned_all,
     file = file.path(path_to_staged, "battery_data_binned.RData"))
