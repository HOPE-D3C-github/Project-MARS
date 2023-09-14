# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary: ----
#   STEP 0. Split dat_main into two datasets
#   STEP 1. Process and merge metadata with undelivered ema data
#         * 1.1  Prep datasets for merge
#         * 1.2  Merge undelivered dataset with conditions dataset
#         * 1.3  Investigate current summary
#         * 1.4  Merge with Battery data
#   STEP 2. Process the undelivered EMA metadata dataset 
#   STEP 3. Save data
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(dplyr)
library(stringr)
library(tidyr)
library(readr)
library(testthat)
library(lubridate)

source('paths.R')

dat_main <- readRDS(file.path(path_to_staged, "matched_2_dec_pts_summarized_metadata.RDS")) 

load(file.path(path_to_staged, "tailor_emi_ema_reports.RData"))
remove(emi_report)

load(file = file.path(path_to_staged, "conditions.RData"))

load(file.path(path_to_staged, "battery_data_binned.RData")) 

load(file = file.path(path_to_staged, "system_log.RData"))

# Investigating data
if(T){
  dat_main %>% count(status_survey_ema)
  dat_main %>% count(is.na(A), status_survey_ema) #342 EMA that were not delivered but the EMI randomization did occur - need to investigate metadata to determine cause
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 0. Split dat_main into two datasets ----
#   * datasets will be merged later
#   * only need to match ema metadata to the set that had an EMI randomization and no EMA delivered
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dat_undeliverd <- dat_main %>% filter(!is.na(A) & is.na(status_survey_ema))
dat_delivered_or_reconciled <- dat_main %>% anti_join(y = dat_undeliverd)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1. Process and merge metadata with undelivered ema data ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1.1  Prep datasets for merge ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
conditions <- conditions %>% 
  filter(str_detect(V4, "EMA-RANDOM")) %>% 
  filter((V5 == "condition" & V6 == "is_privacy_on()==false && is_driving(now()-time_offset(00:05:00); now())==false") | V5 %in% c("is_driving", "is_privacy_on"))

conditions <- conditions %>% mutate(V3_substr = substring(V3, 1, 19), .after = V3)

conditions_wide <- conditions %>% 
  group_by(mars_id, V3_substr) %>% 
  summarise(
    unixts = V1[1],
    condition_raw = V7[max(which(V5=="condition"))],
    is_driving_raw = V7[max(which(V5=="is_driving"))],
    is_privacy_on_raw = V7[max(which(V5=="is_privacy_on"))]
    ) %>% 
  ungroup() %>% 
  mutate(
    condition = as.logical(as.numeric(condition_raw)),
    is_driving = ifelse(is.na(is_driving_raw), NA_character_, str_split_fixed(is_driving_raw, " ", 2)[,1]), 
    is_privacy_on = as.logical(as.numeric(ifelse(is.na(is_privacy_on_raw), NA_character_, str_split_fixed(is_privacy_on_raw, " ", 2)[,1])))) %>% 
  mutate(
    is_driving = case_when(
      is_driving %in% c("0", "false") ~ FALSE,
      is_driving == "true" ~ TRUE,
      T ~ NA))

conditions_wide %>% count(condition,is_driving, is_privacy_on)

# Add datetime variables
conditions_wide <- conditions_wide %>% mutate(cond_hrts_mountain = as_datetime(unixts/1000, tz = "America/Denver"), .before = unixts)
conditions_wide <- conditions_wide %>% select(-c(V3_substr, condition_raw, is_driving_raw, is_privacy_on_raw))

ema_report <- ema_report %>% rename(mars_id = participant_id) %>% mutate(timestamp_try_mountain = as_datetime(timestamp_try/1000, tz = "America/Denver"), .before = timestamp_try)
  
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1.2  Merge undelivered dataset with conditions dataset ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dat_undeliverd_w_cond <- dat_undeliverd %>% mutate(in_dat = T) %>% 
  left_join(y = conditions_wide %>% mutate(in_cond = T),
            by = join_by(mars_id, between(y$cond_hrts_mountain, x$block_start_mountain_calc, x$block_end_mountain_calc)))

dat_undeliverd_w_cond %>% count(in_dat, in_cond)

dat_undeliverd_w_cond %>% nrow() # 35881

dat_undeliverd_w_cond_v2 <- dat_undeliverd_w_cond %>% 
  group_by(mars_id, study_day_int, block_number) %>% 
  mutate(any_condition_false = if_else(all(is.na(condition)), NA, any(!condition, na.rm = T)), 
         any_is_driving = any(is_driving, na.rm = T),
         any_is_privacy_on = any(is_privacy_on, na.rm = T)
         ) %>% 
  filter(row_number()==n()) %>% 
  select(-c(condition, is_driving, is_privacy_on)) %>% 
  ungroup() %>% 
  mutate(condition_summ = case_when(
    is.na(any_condition_false) ~ "No Condition Data",
    any_is_driving & any_is_privacy_on ~ "Driving and Privacy Mode",
    any_is_driving ~ "Driving",
    any_is_privacy_on ~ "Privacy Mode",
    !any_condition_false ~ "Software Error",
    T ~ NA_character_
  ))

dat_undeliverd_w_cond_v2 %>% count(any_condition_false, any_is_driving, any_is_privacy_on, condition_summ)

dat_undeliverd_w_cond_v2 %>% count(status_survey_ema, condition_summ)

# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # Step 1.3  Merge undelivered dataset + conditions dataset with EMA Report
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# dat_undeliverd_w_cond_emarpt <- dat_undeliverd_w_cond_v2 %>% 
#   left_join(y = ema_report %>% mutate(in_emarpt = T),
#             by = join_by(mars_id, between(y$timestamp_try_mountain, x$block_start_mountain_calc, x$block_end_mountain_calc)))
#   
# dat_undeliverd_w_cond_emarpt %>% count(condition_summ, condition, privacy_off, not_driving)
# # looking at the count above, it is clear that the EMA report does not include any information not included in the conditions log. 
# # Therefore, the EMA report will not be used for determining the undelivered reason


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1.3  Investigate current metadata summary ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dat_undeliverd_w_cond_v2 %>% count(condition_summ)
dat_undeliverd_w_cond_v2 %>% filter(condition_summ == "No Condition Data") %>% count(condition_summ, battery_status_simple) %>% mutate(prop = n/sum(n))

# The overwhelming majority of undelivered EMAs have no conditions data
# 27% of those have some missing battery data in the block
# The conditions data doesn't check for battery percent being over a threshold (like in other studies),
# but we can check if the battery data was missing at the time the EMA should've gone out
# The random EMA goes out 1 hour after the EMI randomization

dat_undeliverd_w_cond_v2 <- dat_undeliverd_w_cond_v2 %>% 
  mutate(ts_ema_expected_mountain = ts_coinflip_mountain + hours(1))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1.4  Merge with Battery data ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

dat_undeliverd_w_cond_bat <- dat_undeliverd_w_cond_v2 %>% 
  left_join(y = battery_binned_all %>% rename(mars_id = participant_id, battery_status_at_ema = battery_status),
            by = join_by(mars_id, between(x$ts_ema_expected_mountain, y$datetime_hrts_mtn_start, y$datetime_hrts_mtn_end)))


dat_undeliverd_w_cond_bat %>% count(condition_summ, battery_status_at_ema)
dat_undeliverd_w_cond_bat %>% filter(condition_summ == 'No Condition Data') %>% count(condition_summ, battery_status_at_ema)

# We do see cases where there was no battery data and no conditions data, but a majority are No Condition Data and sufficient battery

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2. Process the undelivered EMA metadata dataset
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dat_undeliverd_w_cond_bat %>% count(condition_summ, battery_status_at_ema)

dat_undeliverd_v2 <- dat_undeliverd_w_cond_bat %>% 
  mutate(undel_ema_rsn = case_when(
    condition_summ != "No Condition Data" ~ condition_summ,
    battery_status_at_ema == "No Battery Data" ~ "No Battery Data at Time of EMA",
    T ~ "Undetermined"
  ))

dat_undeliverd_v2 %>% count(condition_summ, battery_status_at_ema, undel_ema_rsn)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2. Process the undelivered EMA metadata dataset ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dat_delivered_or_reconciled_v2 <- dat_delivered_or_reconciled %>% mutate(undel_ema_rsn = ifelse(is.na(A), "No EMI Randomization", NA_character_))

dat_delivered_or_reconciled_v2 %>% count(is.na(A), status_survey_ema, undel_ema_rsn)

# select the original columns and "undel_ema_rsn" from the undelivered ema dataset
dat_undeliverd_v2 <- dat_undeliverd_v2 %>% select(all_of(colnames(dat_delivered_or_reconciled_v2)))

dat_main_v2 <- bind_rows(dat_delivered_or_reconciled_v2, dat_undeliverd_v2) %>% arrange(mars_id, study_day_int, block_number)

dat_main_v2 <- dat_main_v2 %>% 
  mutate(status_survey_ema = case_when(
    !is.na(status_survey_ema) ~ status_survey_ema,
    T ~ "undelivered"
  ))

if(F){dat_main_v2 %>% count(!is.na(A), status_survey_ema, undel_ema_rsn)
dat_main_v2 %>% count(undel_ema_rsn)
}

dat_main_v2$undel_ema_rsn <- factor(dat_main_v2$undel_ema_rsn,
                                    levels = c("No EMI Randomization", "No Battery Data at Time of EMA", "Driving and Privacy Mode", 
                                               "Driving", "Privacy Mode", "Software Error", "Undetermined"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3. Save data ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
saveRDS(dat_main_v2,
        file = file.path(path_to_staged, "matched_2_dec_pts_summarized_metadata_plus_EMA.RDS"))

