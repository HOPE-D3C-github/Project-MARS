# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary: ----
#   - STEP 1. Create Date variable for all observations in the decision points dataset
# 
# 
# NOTES:
#   - Counting day of V1 as the first day of EMA, so any predefined blocks that occur before the V1 will be undelivered (most likely) since the phone should not be initialized until V1 visit
#   - Variable "block_bounds_mountain" in "matched_2_dec_pts" contains start and end of the block
#   - Block 0 appears to start 30 minutes after wake up time
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(dplyr)
library(stringr)
library(tidyr)
library(readr)
library(testthat)
library(lubridate)

path_to_inputs <- "C:/Users/u1330076/Box/MARS metadata support for Jamie/Input/Utah Data Sets/Utah Data Sets"
path_to_staged_data <- "C:/Users/u1330076/Box/MARS metadata support for Jamie/Staged"

matched_2_dec_pts <- readRDS(file.path(path_to_inputs, "dat_matched_to_decision_points.rds"))
load(file = file.path(path_to_staged_data, "tailor_emi_ema_reports.RData"))
load(file.path(path_to_staged_data, "wakeup_info.RData")) 
wakeup_info <- wakeup_info %>% mutate(wakeup_time_hrts_mtn = as_datetime(V1/1000, tz = "America/Denver"))
load(file.path(path_to_staged_data, "battery_data_binned.RData"))
load(file = file.path(path_to_staged_data, "wakeup_times_set.RData"))
load(file = file.path(path_to_staged_data, "system_log.RData"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1. Create Date variable for all observations in the decision points dataset ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1A. Create study day integer variable for all observations ----
#   - Some rows have no date information, but all rows have the "decision point" data
#   Math used:
#   - (round down the quotient of ((decision_point minus 1) divided by 6)) then add 1
#   - gives study_day_int in the range of 1 to 10
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if(F){
  matched_2_dec_pts %>% count(is.na(block_bounds_mountain))  # Many rows have no date information
  matched_2_dec_pts %>% count(decision_point) %>% View  # all rows have a decision point which is a running count of blocks from the start: 1-60
}

matched_2_dec_pts_v2 <- matched_2_dec_pts %>% 
  mutate(study_day_int = floor((decision_point - 1)/6) + 1L) 
  
if(F){
  matched_2_dec_pts_v2 %>% count(study_day_int)   # All rows now have a study_day_int
  matched_2_dec_pts_v2 %>% count(study_day_int, decision_point) %>% View  # Study day int and decision point matches expected ordering, 6 blocks per study day
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1B. Create a crosswalk of Study Day Int to Study Date ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cw_date_dayint_pretest <- matched_2_dec_pts %>% group_by(mars_id) %>% filter(!is.na(v1_date_began_mountain)) %>% filter(row_number() == 1) %>% 
  select(mars_id, v1_date_began_mountain, v1_date_began_plus_nine_mountain) %>% 
  slice(rep(1:n(), each = 10)) %>% 
  mutate(study_day_int = row_number(),
         study_date = as_date(v1_date_began_mountain + days(study_day_int - 1)),
         last_date_matches = case_when(
           study_day_int != 10 ~ NA,
           T ~ study_date == as_date(v1_date_began_plus_nine_mountain))
         ) %>% 
  ungroup() %>% 
  rename(v1_date_began_mountain_cw = v1_date_began_mountain, v1_date_began_plus_nine_mountain_cw = v1_date_began_plus_nine_mountain)

test1 <- test_that(desc = 'Does v1_date_began_plus_nine_mountain match with the calculated 10th study day date for all participants?', code = {
  expect_true(all(cw_date_dayint_pretest %>% filter(study_day_int == 10) %>% .$last_date_matches))})

if(test1){
  cw_date_dayint <- cw_date_dayint_pretest %>% select(-last_date_matches)
  remove(cw_date_dayint_pretest)
}

if(F){
  cw_date_dayint %>% count(study_day_int)
  View(cw_date_dayint)
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1C. Get Block 0 start time for each study day int and merge to Crosswalk ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cw_block_0_starttime <- matched_2_dec_pts_v2 %>% group_by(mars_id, study_day_int) %>% filter(!is.na(block0_start_mountain_calculated)) %>% filter(row_number() == 1) %>% 
  select(mars_id, study_day_int, block0_start_mountain_calculated_cw = block0_start_mountain_calculated)

cw_date_dayint <- cw_date_dayint %>% 
  full_join(y = cw_block_0_starttime,
            by = c("mars_id", "study_day_int"))

cw_date_dayint <- cw_date_dayint %>%
  mutate(date_of_block0_start_mountain_calculated = as_date(block0_start_mountain_calculated_cw),
         incorrect_date_on_block0_calculated = date_of_block0_start_mountain_calculated != study_date,
         .after = block0_start_mountain_calculated_cw)


# NOTE: The number below is how many study days did not have any block0_start time info for any of the blocks on that day
matched_2_dec_pts_v2 %>% group_by(mars_id, study_day_int) %>% filter(row_number() == 1) %>% nrow() - matched_2_dec_pts_v2 %>% group_by(mars_id, study_day_int) %>% filter(!is.na(block0_start_mountain_calculated)) %>% filter(row_number() == 1) %>% nrow()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1D. Fill in missing block 0 using the pre-set wakeup times for all days without any block0 start data ----
#     - May need to update this after discussing with Jamie 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

cw_date_dayint_nodaystart <- cw_date_dayint %>% filter(is.na(block0_start_mountain_calculated_cw) | incorrect_date_on_block0_calculated) %>% select(-date_of_block0_start_mountain_calculated)

wakeup_times_set <- wakeup_times_set %>% rename(set_wakeup_time_hrts = wakeup_time_hrts) %>%  mutate(study_date = as_date(set_wakeup_time_hrts))


join_nodaystart_w_setwakeuptimes <- cw_date_dayint_nodaystart %>% 
  left_join(y = wakeup_times_set %>% rename(mars_id = participant_id),
            by = join_by(mars_id, study_date, study_day_int == day_int))

join_nodaystart_w_setwakeuptimes %>% count(is.na(set_wakeup_time_hrts))

join_nodaystart_w_setwakeuptimes <- join_nodaystart_w_setwakeuptimes %>% group_by(mars_id, study_day_int) %>% filter(row_number() == n()) %>% ungroup()
join_nodaystart_w_setwakeuptimes %>% count(is.na(set_wakeup_time_hrts))
join_nodaystart_w_setwakeuptimes %>% nrow()

cw_date_dayint_v2 <- cw_date_dayint %>% 
  left_join(y = join_nodaystart_w_setwakeuptimes %>% select(-time_set_hrts),
            by = join_by(mars_id, v1_date_began_mountain_cw, v1_date_began_plus_nine_mountain_cw, study_day_int, study_date, block0_start_mountain_calculated_cw, incorrect_date_on_block0_calculated))


cw_date_dayint_v2 %>% count(is.na(block0_start_mountain_calculated_cw), is.na(set_wakeup_time_hrts))

# 16 remaining - will use the phone default wakeup info (if available)
join_nodaystart_w_setwakeuptimes %>% count(is.na(set_wakeup_time_hrts))

nodaystart_nosetwakeup_needdefaultwakeup <- join_nodaystart_w_setwakeuptimes %>% filter(is.na(set_wakeup_time_hrts))

wakeup_info <- wakeup_info %>% mutate(study_date = as_date(wakeup_time_hrts_mtn))

join_defaultwakeup <- nodaystart_nosetwakeup_needdefaultwakeup %>% select(-time_set_hrts, -incorrect_date_on_block0_calculated) %>% 
  left_join(y = wakeup_info %>% rename(mars_id = participant_id) %>% select(mars_id, study_date, default_wakeup_time_hrts_mtn = wakeup_time_hrts_mtn),
            by = join_by(mars_id, study_date))


cw_date_dayint_v3 <- cw_date_dayint_v2 %>% 
  left_join(y = join_defaultwakeup,
            by = join_by(mars_id, v1_date_began_mountain_cw, v1_date_began_plus_nine_mountain_cw, study_day_int, study_date, block0_start_mountain_calculated_cw, set_wakeup_time_hrts))

cw_date_dayint_v3 %>% count(is.na(block0_start_mountain_calculated_cw), is.na(set_wakeup_time_hrts), is.na(default_wakeup_time_hrts_mtn))
# Out of 144 without day start data from matched_2_dec dataset, 128 updated with set wakeup time, 3 updated with default wakeup time (no set wakeup time), 
#    and 13 could not be reconciled (no set wakeup nor default wakeup time data)

cw_date_dayint_v3 <- cw_date_dayint_v3 %>% mutate(
  block0_start_mountain_calculated_settime = set_wakeup_time_hrts + minutes(30),
  block0_start_mountain_calculated_deftime = default_wakeup_time_hrts_mtn + minutes(30)
)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1E. Merge Study Day Int <-> Study Date crosswalk to the decision points data ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
matched_2_dec_pts_v3 <- matched_2_dec_pts_v2 %>% 
  full_join(y = cw_date_dayint_v3, 
            by = c("mars_id", "study_day_int")) %>% 
  relocate(
    cluster_id, study_day_int, study_date, block_number, block0_start_mountain_calculated, incorrect_date_on_block0_calculated, block0_start_mountain_calculated_cw, 
    block0_start_mountain_calculated_settime, block0_start_mountain_calculated_deftime, A, .after = mars_id) 
  

matched_2_dec_pts_v3 <- matched_2_dec_pts_v3 %>% 
  mutate(block0_RECALCULATED_start_mountain = case_when(
    incorrect_date_on_block0_calculated & mars_id == 'mars_52' & study_day_int == '3' ~  block0_start_mountain_calculated_cw + days(1),
    T ~ coalesce(block0_start_mountain_calculated, block0_start_mountain_calculated_cw, block0_start_mountain_calculated_settime, block0_start_mountain_calculated_deftime)
    ), .after = block_number)

matched_2_dec_pts_v3 %>% count(is.na(block0_RECALCULATED_start_mountain))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1F. Create date time variables of the block start and end times from "block0_start_mountain_calculated" and block number ----
#   Note: Manually reviewing the "block_bounds_mountain" variable showed that blocks lasted for 2 hours and 20 minutes
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

next_study_day_block0_starttimes <- matched_2_dec_pts_v3 %>% group_by(mars_id, study_day_int) %>% filter(row_number() == 1) %>% ungroup() %>% 
  group_by(mars_id) %>% mutate(next_study_day_block0_start_mtn = lead(block0_RECALCULATED_start_mountain),
                               next_study_day_block0_isNA = is.na(next_study_day_block0_start_mtn),
                               .after = block0_RECALCULATED_start_mountain) %>% ungroup() %>% 
  select(mars_id, study_day_int, next_study_day_block0_start_mtn, next_study_day_block0_isNA)


matched_2_dec_pts_v4 <- matched_2_dec_pts_v3 %>% 
  left_join(y = next_study_day_block0_starttimes,
            by = join_by(mars_id, study_day_int),
            multiple = "all") %>% 
  relocate(next_study_day_block0_start_mtn, next_study_day_block0_isNA, .after = block0_RECALCULATED_start_mountain)
  

matched_2_dec_pts_v5 <- matched_2_dec_pts_v4 %>% 
  mutate(
    block_start_valid = case_when(
      !next_study_day_block0_isNA ~ (block0_RECALCULATED_start_mountain + ((hours(2) + minutes(20))*block_number)) < next_study_day_block0_start_mtn - minutes(30),
      T ~ T),
    block_end_valid = case_when(
      !next_study_day_block0_isNA ~ (block0_RECALCULATED_start_mountain + ((hours(2) + minutes(20))*(block_number + 1L))) < next_study_day_block0_start_mtn - minutes(30),
      T ~ T),
    block_start_hrts = case_when(
      block_start_valid ~ block0_RECALCULATED_start_mountain + ((hours(2) + minutes(20))*block_number)),
    block_end_hrts = case_when(
      block_end_valid ~ block0_RECALCULATED_start_mountain + ((hours(2) + minutes(20))*(block_number + 1L)),
      !block_end_valid & block_start_valid ~ next_study_day_block0_start_mtn), 
    .after = block_number) %>% 
  relocate(A, block_bounds_mountain, .after = block_number)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1G. Create backbone crosswalk of key variables of participant, day, and block to be used when analyzing the metadata ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cw_pt_date_blocks <- matched_2_dec_pts_v5 %>% 
  select(mars_id, study_date, study_day_int, block_number, block_start_hrts, block_end_hrts, A)

cw_pt_date_blocks %>% count(is.na(block_start_hrts), is.na(A))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# END of Step 1. ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
remove(matched_2_dec_pts_v2, matched_2_dec_pts_v3, matched_2_dec_pts_v4)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 2. ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # Update datetime variables in tailor report
# tailor_report <- tailor_report %>% 
#   mutate(human_readable_time_try = as_datetime(timestamp_try/1000, tz = "America/Denver"), 
#          human_readable_time_notification = as_datetime(timestamp_notification/1000, tz = "America/Denver"),
#          human_readable_time_ema_start = as_datetime(timestamp_ema_start/1000, tz = "America/Denver"),
#          human_readable_time_ema_end = as_datetime(timestamp_ema_end/1000, tz = "America/Denver"))
# 
# # Interval Join the tailor report data to the pt/day/block crosswalk
# tailor_w_blocks <- full_join(x = tailor_report %>% mutate(in_tailor_rpt = T),
#           y = cw_pt_date_blocks %>% mutate(in_pt_blocks = T),
#           by = join_by(participant_id == mars_id, between(human_readable_time_try, block_start_hrts, block_end_hrts)))
# 
# tailor_w_blocks %>% count(in_tailor_rpt, in_pt_blocks, A)


# EMI report should be more useful than tailor, because they can still be randomized if they miss/didn't have a tailoring quest
emi_report <- emi_report %>% 
  mutate(human_readable_time_try = as_datetime(timestamp_try/1000, tz = "America/Denver"), 
         human_readable_time_notification = as_datetime(timestamp_notification/1000, tz = "America/Denver"),
         human_readable_time_emi_start = as_datetime(timestamp_emi_start/1000, tz = "America/Denver"))

# Interval Join the emi report data to the pt/day/block crosswalk
emi_w_blocks <- full_join(x = emi_report %>% rename(mars_id = participant_id) %>% mutate(in_emi_rpt = T),
                             y = cw_pt_date_blocks %>% mutate(in_pt_blocks = T),
                             by = join_by(mars_id, between(human_readable_time_try, block_start_hrts, block_end_hrts)))

emi_w_blocks <- emi_w_blocks %>% relocate(all_of(colnames(cw_pt_date_blocks)), .before = everything())


emi_w_blocks %>% count(in_emi_rpt, in_pt_blocks, A)

emi_w_blocks %>% group_by(mars_id, study_date, block_number) %>% filter(row_number() == 1) %>% ungroup() %>% count(in_emi_rpt, in_pt_blocks, A)  # block level

emi_w_blocks %>% count(in_emi_rpt, block_no == block_number)


if(F){emi_w_blocks %>% filter(in_emi_rpt & block_no != block_number)  %>% View()}

matched_2_dec_pts_v5 %>% count(A)

if(F){emi_w_blocks %>% filter(is.na(in_emi_rpt) & in_pt_blocks) %>% View}

emi_w_blocks %>% filter(is.na(A) & in_emi_rpt & in_pt_blocks) %>% group_by(mars_id, study_day_int, block_number) %>% filter(row_number() == 1) %>% ungroup %>% nrow()  # Only 27 of the 1574 blocks without a randomization have 1+ record in the EMI report

emi_w_blocks %>% filter(is.na(A) & in_emi_rpt & in_pt_blocks) %>% group_by(mars_id, study_day_int, block_number) %>% filter(row_number() == n()) %>% ungroup %>% View  


emi_w_blocks %>% filter(is.na(A) & in_pt_blocks) %>% group_by(mars_id, study_day_int, block_number) %>% filter(row_number() == 1) %>% ungroup() %>% count(in_emi_rpt)

# Looking further, it may be that the phone stopped for a given study day after midnight. Need to examine closer

matched_2_dec_pts_v6 <- matched_2_dec_pts_v5 %>% mutate(start_block_date = as_date(block_start_hrts),
                                end_block_date = as_date(block_end_hrts),
                                start_block_different_date = start_block_date != study_date,
                                end_block_different_date = end_block_date != study_date,
                                .after = study_date) 

matched_2_dec_pts_v6 %>% 
  count(end_block_different_date, start_block_different_date, is.na(A))

matched_2_dec_pts_v6 %>% filter(is.na(A)) %>% 
  count(start_block_different_date, end_block_different_date, A)   # most missing randomizations are not a result of blocks on the next day, but some may be ~ 90+87?

if(F){matched_2_dec_pts_v6 %>% filter(is.na(block_start_hrts)) %>% View} # 78 have no day start data. can check log for any data recorded on those days

if(F){matched_2_dec_pts_v6 %>% filter(!start_block_different_date & !end_block_different_date & is.na(A)) %>% View}

# The below code shows that if the condition ever checked out True, then that was the last record for that block. 
emi_w_blocks %>% group_by(mars_id, study_day_int, block_number) %>% 
  mutate(
    condition_log = if_else(condition == 'true', TRUE, FALSE),
    any_condition_true = any(condition_log)) %>% filter(row_number() == n()) %>% 
  ungroup() %>% 
  count(condition_log == any_condition_true)


emi_w_blocks <- emi_w_blocks %>% 
  mutate(
    condition_lgcl = if_else(condition == 'true', TRUE, FALSE),
    privacy_off_lgcl = if_else(privacy_off == 'true', TRUE, FALSE),
    not_driving_lgcl = if_else(not_driving == 'true', TRUE, FALSE)
  )


if(F){emi_w_blocks %>% View}

emi_w_blocks %>% count(condition_lgcl, privacy_off_lgcl, not_driving_lgcl)

emi_w_blocks %>% count(privacy_off_lgcl)  # No records with privacy on

if(F){emi_w_blocks %>% filter(!condition_lgcl, privacy_off_lgcl, not_driving_lgcl) %>% View} # one participant had privacy off and not driving but condition False. 2 blocks, 1 got randomized, 1 did not

emi_w_blocks_v2 <- emi_w_blocks %>% group_by(mars_id, study_day_int, block_number) %>% 
  filter(!is.na(study_date)) %>% 
  mutate(
    n = n(),
    n_condition_FALSE = sum(!condition_lgcl),
    n_condition_TRUE = sum(condition_lgcl),
    n_not_driving_FALSE = sum(!not_driving_lgcl)
  ) %>% 
  filter(row_number() == n()) %>%
  ungroup() %>% 
  select(-in_emi_rpt, -in_pt_blocks)

emi_w_blocks_v2 %>% count(is.na(A), n_condition_TRUE)



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 2B. Look for blocks with no battery data for entire block ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
battery_binned_all <- battery_binned_all %>% rename(mars_id = participant_id,
                              battery_range_start_unix = unix_datetime_start,
                              battery_range_end_unix = unix_datetime_end,
                              battery_range_start_hrts_mtn = datetime_hrts_mtn_start,
                              battery_range_end_hrts_mtn = datetime_hrts_mtn_end,
                              battery_time_in_bin = time_duration_minutes)


battery_w_blocks <- right_join(x = battery_binned_all, y = cw_pt_date_blocks,
            by = join_by(mars_id, overlaps(battery_range_start_hrts_mtn, battery_range_end_hrts_mtn, block_start_hrts, block_end_hrts)))
  

if(F){battery_w_blocks %>% group_by(mars_id, study_day_int, block_number) %>% 
  mutate(multiple_battery_statuses_in_block = n() > 1) %>% 
  ungroup() %>% 
  count(A, battery_status, multiple_battery_statuses_in_block) %>% View}


battery_w_blocks %>% 
  group_by(mars_id, study_day_int, block_number) %>% 
  filter(n() == 1 & battery_status == "No Battery Data") %>% 
  ungroup() %>% 
  count(A)   # 1,200 blocks where the entire block had no battery data and no randomization - 2 cases of no battery data but with a randomization

if(F){battery_w_blocks %>% 
  group_by(mars_id, study_day_int, block_number) %>% 
  filter(n() == 1 & battery_status == "No Battery Data") %>% 
  ungroup() %>% 
  View}

# Compute time in block for battery status
battery_w_blocks_v2 <- battery_w_blocks %>% 
  rowwise() %>% 
  mutate(
    total_block_time = difftime(block_end_hrts, block_start_hrts, units = "mins"),
    time_battery_status_in_block = case_when(
      !is.na(block_start_hrts) ~ difftime( min(battery_range_end_hrts_mtn, block_end_hrts) , max(battery_range_start_hrts_mtn, block_start_hrts), units = "mins"),
      T ~ NA)) %>% 
  ungroup() %>% 
  filter(!is.na(study_date)) # remove battery data that is out of range from the study day/block intervals

remove(battery_w_blocks)


# Group by participant/study day/ block to calculate proportion of time for each of the 3 battery statuses
battery_w_blocks_tall <- battery_w_blocks_v2 %>% 
  group_by(mars_id, study_day_int, block_number, battery_status) %>% 
  mutate(total_time_of_same_battery_status_in_block = sum(time_battery_status_in_block)) %>% 
  filter(row_number() == 1) %>%  # aggregating from within the group_by so can remove non-first rows
  select(-c(battery_range_start_unix, battery_range_end_unix, battery_range_start_hrts_mtn,  # remove vars that are not related to the aggregate (grouped) data
            battery_range_end_hrts_mtn, battery_time_in_bin, time_battery_status_in_block)) %>% 
  ungroup() %>% 
  mutate(percent_of_block_time_in_battery_status = round(as.numeric(total_time_of_same_battery_status_in_block) / as.numeric(total_block_time) * 100, digits = 4))

remove(battery_w_blocks_v2)

# Create a wide version that is one row per participant/day/block 
battery_w_blocks_wide <- battery_w_blocks_tall %>% 
  pivot_wider(
    names_from = battery_status,
    values_from = c(total_time_of_same_battery_status_in_block, percent_of_block_time_in_battery_status)
  )

battery_w_blocks_wide <- battery_w_blocks_wide %>% 
  select(-"total_time_of_same_battery_status_in_block_NA", -"percent_of_block_time_in_battery_status_NA") %>% 
  rename(
    time_no_battery_data = "total_time_of_same_battery_status_in_block_No Battery Data",
    time_low_battery = "total_time_of_same_battery_status_in_block_0-10%",
    time_sufficient_battery = "total_time_of_same_battery_status_in_block_10-100%",
    percent_time_no_battery_data = "percent_of_block_time_in_battery_status_No Battery Data",
    percent_time_low_battery = "percent_of_block_time_in_battery_status_0-10%",
    percent_time_sufficient_battery = "percent_of_block_time_in_battery_status_10-100%"
  )

battery_w_blocks_wide <- battery_w_blocks_wide %>% 
  mutate(entire_block_no_battery_data = percent_time_no_battery_data == 100,
         ninety_plus_perc_block_no_battery_data = percent_time_no_battery_data >= 90)

battery_w_blocks_wide %>% count(entire_block_no_battery_data)

battery_w_blocks_wide %>% filter(entire_block_no_battery_data) %>% count(entire_block_no_battery_data, A) # 1202 out of 1574 non-randomized had entire block with no battery data

battery_w_blocks_wide %>% count(A)

battery_w_blocks_wide %>% filter(!is.na(percent_time_no_battery_data)) %>% count(entire_block_no_battery_data, A) # 53 more non-randomized that had no battery data for part of the block

if(F){battery_w_blocks_wide %>% filter(!is.na(percent_time_no_battery_data)) %>% filter(!entire_block_no_battery_data & is.na(A)) %>% View}

battery_w_blocks_wide %>% count(ninety_plus_perc_block_no_battery_data, entire_block_no_battery_data, A) # non randomized count is close to the randomized count for those with no battery data for 90-99.9% of the block time. Not as clear cut as those with no battery data for the entire block


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Recombine data stream findings with updated "matched_2_dec_pts" data 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
matched_2_dec_pts_v7 <- matched_2_dec_pts_v6 %>% 
  full_join(y = battery_w_blocks_wide,
            by = join_by(mars_id, study_date, study_day_int, block_number, block_start_hrts, block_end_hrts, A)) %>% 
  full_join(y = emi_w_blocks_v2,
            by = join_by(mars_id, study_date, study_day_int, block_number, block_start_hrts, block_end_hrts, A))

if(F){
matched_2_dec_pts_v7 %>% count(A, entire_block_no_battery_data)
matched_2_dec_pts_v7 %>% filter(is.na(A)) %>% count(A, entire_block_no_battery_data) # 1201 with no battery data entire block, 54 partial no battery data in block, 319 with no "no battery data" in block

matched_2_dec_pts_v7 %>% filter(is.na(A)) %>% count(A, entire_block_no_battery_data, condition_lgcl) # EMI data shows up to 9 blocks that *might* have been randomized according to the EMI report - note still some possible issue with crossing past midnight and the phone truncating that day
                                                                                                     # and 15 that were not randomized because conditions were not right for entire time it checked

matched_2_dec_pts_v7 %>% count(A, entire_block_no_battery_data, ninety_plus_perc_block_no_battery_data, condition_lgcl, end_block_different_date) %>% View

matched_2_dec_pts_v7 %>% count(A, is.na(entire_block_no_battery_data), is.na(condition_lgcl))

matched_2_dec_pts_v7 %>% filter(is.na(entire_block_no_battery_data) & is.na(condition_lgcl)) %>%  count(A, is.na(entire_block_no_battery_data), is.na(condition_lgcl), is.na(block0_start_mountain_calculated))

matched_2_dec_pts_v7 %>% filter(is.na(A)) %>% filter(is.na(entire_block_no_battery_data)) %>% count(is.na(percent_time_low_battery), is.na(percent_time_sufficient_battery))

matched_2_dec_pts_v7 %>% filter(is.na(A) & is.na(entire_block_no_battery_data) & !(is.na(percent_time_low_battery) & is.na(percent_time_sufficient_battery))) %>% View

}

# 54 blocks of those have no day start info and couldn't match data into; 265 that could be investigated from those and tried to match data into block intervals - they have low or sufficient battery but no EMI data

# Will look at additional data streams to see if any streams have data 

# conditions file and systemLog file have additional information. initial looking into the 265 showed data gaps in those sources during the blocks in question. Need to read in all the data and match up to see concordance


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Testing - looking at concordance between EMI report and select observations about randomization from the System Log file ("SYSTEM_LOG--org.md2k.scheduler.csv.bz2")
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# sysLog_mars1 <- read_csv(
#   file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_1", 
#                    "SYSTEM_LOG--org.md2k.scheduler.csv.bz2"),
#   col_names = F
# )
# 
# sysLog_mars1_EMI_rands <- sysLog_mars1 %>% filter(str_detect(X6, "random selection=" )) %>% 
#   mutate(mars_id = "mars_1",
#          sysLog_datetime = as_datetime(X1/1000, tz = "America/Denver"),
#          .before = everything())
# 
# 
# test_join_1 <- emi_report %>% filter(participant_id == "mars_1") %>% rename(mars_id = participant_id) %>% mutate(floordt_human_readable_time_try = floor_date(human_readable_time_try), in_emi_rpt = T) %>% 
#   full_join(y = sysLog_mars1_EMI_rands %>% mutate( floordt_sysLog_datetime = floor_date(sysLog_datetime), in_sysLog = T),
#             by = join_by(mars_id, floordt_human_readable_time_try == floordt_sysLog_datetime))
# 
# test_join_1 %>% count(in_emi_rpt, in_sysLog)
# 
# test_join_1 %>% count(in_emi_rpt, in_sysLog, condition)
# 
# # test join 2
# test_join_2 <- emi_report %>% filter(participant_id == "mars_1") %>% rename(mars_id = participant_id) %>% mutate(rounddt_human_readable_time_try = round_date(human_readable_time_try), in_emi_rpt = T) %>% 
#   full_join(y = sysLog_mars1_EMI_rands %>% mutate( rounddt_sysLog_datetime = round_date(sysLog_datetime), in_sysLog = T),
#             by = join_by(mars_id, rounddt_human_readable_time_try == rounddt_sysLog_datetime),
#             keep = T)
# 
# test_join_2 %>% count(in_emi_rpt, in_sysLog)
# test_join_2 %>% count(in_emi_rpt, in_sysLog, condition)
# 
# test_join_2 %>% filter((in_emi_rpt & condition == 'true' & is.na(in_sysLog)) | (is.na(in_emi_rpt) & in_sysLog)) %>% View
# 
# 
# test_join_3 <- full_join(x = sysLog_mars1_EMI_rands %>% mutate( in_sysLog = T),
#                          y = emi_report %>% filter(participant_id == "mars_1") %>% rename(mars_id = participant_id) %>% 
#                            mutate(minus2s_human_readable_time_try = human_readable_time_try - seconds(2),
#                                   plus3s_human_readable_time_try = human_readable_time_try + seconds(3),
#                                   in_emi_rpt = T),
#             by = join_by(mars_id, between(sysLog_datetime, minus2s_human_readable_time_try, plus3s_human_readable_time_try)),
#             keep = T)
# 
# test_join_3 %>% count(in_emi_rpt, in_sysLog)
# test_join_3 %>% count(in_emi_rpt, in_sysLog, condition)
# 
# test_join_3 %>% filter(condition == "true") %>% View
# 
# test_join_3 %>% filter(condition == "true") %>% mutate(random_selection_sysLog = str_remove(X6, 'random selection=')) %>% count(random_selection_sysLog, random_selection)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# System Log file
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

system_log_EMI_rands <- system_log %>% filter(str_detect(V6, "random selection=" )) %>% 
  rename(mars_id = participant_id) %>% 
  mutate(sysLog_datetime = as_datetime(V1/1000, tz = "America/Denver"),
         .before = mars_id)


sytem_log_joined_emi_rpt <- full_join(x = system_log_EMI_rands %>% mutate( in_sysLog = T),
                         y = emi_report %>% rename(mars_id = participant_id) %>% 
                           mutate(minus2s_human_readable_time_try = human_readable_time_try - seconds(2),
                                  plus3s_human_readable_time_try = human_readable_time_try + seconds(3),
                                  in_emi_rpt = T),
                         by = join_by(mars_id, between(sysLog_datetime, minus2s_human_readable_time_try, plus3s_human_readable_time_try)),
                         keep = T)

sytem_log_joined_emi_rpt %>% count(in_emi_rpt, in_sysLog, condition)

if(F){
  sytem_log_joined_emi_rpt %>% filter(condition == 'true') %>% mutate(date = as_date(human_readable_time_try)) %>% 
  group_by(mars_id.y, block_no, date) %>% 
  summarise(n = n()) %>% View

sytem_log_joined_emi_rpt %>% filter(condition == "true") %>% group_by(mars_id.y) %>% count(block_no) %>% View

sytem_log_joined_emi_rpt %>% filter(condition == "true") %>% mutate(random_selection_sysLog = str_remove(V6, 'random selection=')) %>% count(random_selection_sysLog, random_selection)

sytem_log_joined_emi_rpt %>% filter((in_emi_rpt & condition == "true" & is.na(in_sysLog)) | (is.na(in_emi_rpt) & in_sysLog)) %>% View
}

system_log_EMI_rands <- system_log_EMI_rands %>% mutate(random_selection_sysLog = str_remove(V6, 'random selection='))

# Join system log emi rands data to block level backbone
systemlog_emi_blocks <- full_join(x = system_log_EMI_rands %>% mutate(in_syslog = T),
                                  y = cw_pt_date_blocks %>% mutate(in_blocks_cw = T),
                                  by = join_by(mars_id, between("sysLog_datetime", block_start_hrts, block_end_hrts) ))

systemlog_emi_blocks %>% count(in_syslog, in_blocks_cw)

systemlog_emi_blocks %>% count(in_syslog, in_blocks_cw, is.na(A))

systemlog_emi_blocks %>% group_by(mars_id) %>% 
  filter(any(in_blocks_cw)) %>% 
  filter(in_blocks_cw | between(sysLog_datetime, min(block_start_hrts, na.rm = T), max(block_end_hrts, na.rm = T))) %>% 
  ungroup() %>% 
  count(in_syslog, in_blocks_cw, A)



systemlog_emi_blocks %>% filter(in_syslog & in_blocks_cw) %>% count(A, random_selection_sysLog)

systemlog_emi_blocks %>% filter(in_syslog & in_blocks_cw & is.na(A)) %>% View







