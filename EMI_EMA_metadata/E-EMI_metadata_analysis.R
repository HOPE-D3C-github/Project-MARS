# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary: ----
#   STEP 1. Create Date variable for all observations in the decision points dataset
#   STEP 2. Incorporate Other Data Streams
#   STEP 3. Process for Final Summary Indicators and Save Datasets
#   STEP 4. Save Datasets
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(dplyr)
library(stringr)
library(tidyr)
library(readr)
library(testthat)
library(lubridate)

source('paths.R')

matched_2_dec_pts <- readRDS(file.path(path_to_input_data_from_jamie, "dat_matched_to_decision_points.rds")) %>% mutate(block_number = decision_point - (6*(cluster_id - 1)) - 1)
load(file = file.path(path_to_staged, "tailor_emi_ema_reports.RData"))
load(file.path(path_to_staged, "wakeup_info.RData")) 
wakeup_info <- wakeup_info %>% mutate(wakeup_time_hrts_mtn = as_datetime(V1/1000, tz = "America/Denver"))
load(file.path(path_to_staged, "battery_data_binned.RData")) 

battery_binned_all <- battery_binned_all %>% 
  group_by(participant_id) %>% 
  mutate(lag_battery_status = lag(battery_status), lead_battery_status = lead(battery_status)) %>% 
  ungroup()
load(file = file.path(path_to_staged, "wakeup_times_set.RData"))
load(file = file.path(path_to_staged, "system_log.RData"))
pt_withdrawn_date <- readRDS(file = file.path(path_to_staged, "pt_withdrawn_date.RDS"))
load(file = file.path(path_to_staged, "conditions.RData"))

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
# STEP 1C. Implement calculating block start times using Jamie's method ---- 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dat_wakeup <- system_log %>% 
  rename(mars_id = participant_id) %>% 
  # When no wakeup time is set by study staff for a particular day
  # the system reverts back to using a default wakeup time
  # which is at 12:08am US/Mountain (a very peculiar number!)
  mutate(is_default = if_else(grepl(pattern = "type=WAKEUP id=TODAY newValue=Default", x = .data[["V6"]], fixed = TRUE), 1, 0)) %>%
  # When wakeup time is set/changed/adjusted by study staff
  # the system utilizes this wakeup time only for blocks after
  # when the new wakeup time was saved.
  mutate(is_new_value = if_else(grepl(pattern = "type=WAKEUP id=TODAY newValue=", x = .data[["V6"]], fixed = TRUE), 1, 0)) %>%
  filter(is_new_value == 1) %>% 
  mutate(study_date = as_date(mdy_hms(V3)))

dat_wakeup <- dat_wakeup %>% 
  mutate(ts_saved_utc = as_datetime(x = V1/1000, tz = "UTC"))

dat_wakeup <- dat_wakeup %>%
  mutate(olson = case_when(
    V2/(1000*60*60) == -5 ~ "US/Eastern", 
    V2/(1000*60*60) == -6 ~ "US/Central", 
    V2/(1000*60*60) == -7 ~ "US/Mountain", 
    V2/(1000*60*60) == -8 ~ "US/Pacific"
  ))

dat_wakeup[["ts_saved_mountain"]] <- with_tz(time = dat_wakeup[["ts_saved_utc"]], tzone = "US/Mountain")

dat_wakeup <- dat_wakeup %>%
  mutate(string_for_default = substring(V6, first = nchar(V6) - 14, last = nchar(V6))) %>%
  mutate(string_for_default = substring(string_for_default, first = 2, last = nchar(string_for_default)-1)) %>%
  mutate(string_for_default = replace(string_for_default, is_default == 0, NA))

dat_wakeup <- dat_wakeup %>%
  mutate(string_for_adjusted = substring(V6, first = nchar(V6) - 22, last = nchar(V6))) %>%
  mutate(string_for_adjusted = replace(string_for_adjusted, is_default == 1, "01-01-2000 00:00:00"))  # -- this is just a placeholder value

dat_wakeup <- dat_wakeup %>%
  mutate(parsed_default = as_datetime(x = as.numeric(string_for_default)/1000, tz = "UTC")) %>%
  mutate(ts_wakeup_mountain = with_tz(time = parsed_default, tzone = "US/Mountain")) %>%
  mutate(parsed_adjusted_mountain = mdy_hms(string_for_adjusted, tz = "US/Mountain"),
         parsed_adjusted_central = mdy_hms(string_for_adjusted, tz = "US/Central"),
         parsed_adjusted_pacific = mdy_hms(string_for_adjusted, tz = "US/Pacific"),
         parsed_adjusted_eastern = mdy_hms(string_for_adjusted, tz = "US/Eastern")) %>%
  mutate(ts_wakeup_mountain = case_when(
    (is_default == 0) & (olson == "US/Mountain") ~ parsed_adjusted_mountain,
    (is_default == 0) & (olson == "US/Pacific") ~ with_tz(parsed_adjusted_pacific, tzone = "US/Mountain"),
    (is_default == 0) & (olson == "US/Central") ~ with_tz(parsed_adjusted_central, tzone = "US/Mountain"),
    (is_default == 0) & (olson == "US/Eastern") ~ with_tz(parsed_adjusted_eastern, tzone = "US/Mountain"),
    .default = ts_wakeup_mountain
  ))

dat_wakeup <- dat_wakeup %>%
  rename(ts_wakeup_saved_mountain = ts_saved_mountain,
         is_wakeup_default = is_default,
         is_wakeup_adjusted = is_new_value) %>%
  select(mars_id, study_date, olson, ts_wakeup_saved_mountain, is_wakeup_default, is_wakeup_adjusted, ts_wakeup_mountain)

dat_wakeup <- dat_wakeup[!duplicated(dat_wakeup),]

# @TB: 7 days with multiple day start times set - total of 17 set on those 7 days. Only 1 time was the multiple start set time different than the first start set time. mars_53 3/22/2022. Appears to reset to block 0 for the second set time
if(F){dat_wakeup %>% group_by(mars_id, study_date) %>% filter(n()>1) %>% View}

# Interim solution, grab first set time
dat_wakeup_filtered <- dat_wakeup %>% group_by(mars_id, study_date) %>% filter(row_number() == 1)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1D. Merge dat_wakeup filtered with study date cw ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cw_date_dayint_v2 <- cw_date_dayint %>% 
  left_join(y = dat_wakeup_filtered,
            by = join_by(mars_id, study_date))


if(F){cw_date_dayint_v2 %>% count(is.na(ts_wakeup_mountain))}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Some Blocks have no recorded wakeup time (checked system log and there is a gap in data for the days I checked)
# 
# Fill in with Wakeup set times data then use default if wakeup set time is also missing
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

wakeup_times_set <- wakeup_times_set %>% rename(set_wakeup_time_hrts = wakeup_time_hrts) %>%  mutate(study_date = as_date(set_wakeup_time_hrts))

wakeup_times_set_v2 <- wakeup_times_set %>% group_by(participant_id, study_date) %>% filter(row_number()==n()) %>% ungroup() %>% 
  select(participant_id, day_int, study_date, #olson, 
         set_wakeup_time_hrts)

cw_date_dayint_v3 <- cw_date_dayint_v2 %>% 
  left_join(y = wakeup_times_set_v2 %>% rename(mars_id = participant_id),
            by = join_by(mars_id, study_date)) %>%  # %>% filter(mars_id == "mars_106") %>% View
  mutate(olson_calc = olson) %>% 
  fill(olson_calc, .direction ="down") %>% 
  mutate(set_wakeup_time_hrts_mtn = case_when(
    olson_calc == "US/Mountain" ~ set_wakeup_time_hrts,
    olson_calc == "US/Central" ~ set_wakeup_time_hrts - hours(1),
    olson_calc == "US/Pacific" ~ set_wakeup_time_hrts + hours(1)
  )) %>% 
  select(-set_wakeup_time_hrts)
  

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# That reconciled most missing, but some days have no recorded start time either, and must apply the default wakeup time for them
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

cw_date_dayint_v4 <- cw_date_dayint_v3 %>% 
  mutate(wakeup_default_calc = case_when(
    olson_calc == "US/Mountain" ~ as_datetime(paste(study_date, "00:08:00"), tz = "America/Denver"),
    olson_calc == "US/Central" ~ as_datetime(paste(study_date, "00:08:00"), tz = "America/Denver") - hours(1),
    olson_calc == "US/Pacific" ~ as_datetime(paste(study_date, "01:08:00"), tz = "America/Denver"),
    T ~ NA_POSIXct_
  ))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Combine wakeup sources to create one wakeup time per study day
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

cw_date_dayint_v5 <- cw_date_dayint_v4 %>% 
  mutate(study_day_wakeup_time = coalesce(ts_wakeup_mountain, set_wakeup_time_hrts_mtn, wakeup_default_calc)) %>% 
  # mutate(study_day_wakeup_time = case_when(
  #   mars_id == "mars_52" & study_date == as_date('2022-03-23') ~ as_datetime('2022-03-22 00:08:00', tz = 'America/Denver'),
  #   T ~ study_day_wakeup_time)) %>% 
  mutate(block0_RECALC_start_mountain = study_day_wakeup_time + minutes(30)) %>% 
  rename(ts_wakeup_saved_mountain_recalc = ts_wakeup_saved_mountain, ts_wakeup_mountain_recalc = ts_wakeup_mountain) %>% 
  select(-c(day_int, is_wakeup_default, is_wakeup_adjusted, olson))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1E. Merge Study Day Int <-> Study Date crosswalk to the decision points data ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
matched_2_dec_pts_v3 <- matched_2_dec_pts_v2 %>% 
  full_join(y = cw_date_dayint_v5, 
            by = c("mars_id", "study_day_int")) %>% 
  relocate(
    cluster_id, study_day_int, study_date, block_number, #block0_start_mountain_calculated, 
    A, .after = mars_id) 
  
matched_2_dec_pts_v3 %>% count(is.na(block0_RECALC_start_mountain))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1F. Create date time variables of the block start and end times from "block0_start_mountain_calculated" and block number ----
#   Note: Manually reviewing the "block_bounds_mountain" variable showed that blocks lasted for 2 hours and 20 minutes
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

next_study_day_block0_starttimes <- matched_2_dec_pts_v3 %>% group_by(mars_id, study_day_int) %>% filter(row_number() == 1) %>% ungroup() %>% 
  group_by(mars_id) %>% mutate(next_study_day_block0_start_mtn = lead(block0_RECALC_start_mountain),
                               next_study_day_block0_isNA = is.na(next_study_day_block0_start_mtn),
                               .after = block0_RECALC_start_mountain) %>% ungroup() %>% 
  select(mars_id, study_day_int, next_study_day_block0_start_mtn, next_study_day_block0_isNA)


matched_2_dec_pts_v4 <- matched_2_dec_pts_v3 %>% 
  left_join(y = next_study_day_block0_starttimes,
            by = join_by(mars_id, study_day_int),
            multiple = "all") %>% 
  relocate(next_study_day_block0_start_mtn, next_study_day_block0_isNA, .after = block0_RECALC_start_mountain)
  

matched_2_dec_pts_v5 <- matched_2_dec_pts_v4 %>% 
  mutate(
    block_start_valid = case_when(
      !next_study_day_block0_isNA ~ (block0_RECALC_start_mountain + ((hours(2) + minutes(20))*block_number)) < next_study_day_block0_start_mtn - minutes(30),
      T ~ T),
    block_end_valid = case_when(
      !next_study_day_block0_isNA ~ (block0_RECALC_start_mountain + ((hours(2) + minutes(20))*(block_number + 1L))) < next_study_day_block0_start_mtn - minutes(30),
      T ~ T),
    block_start_mountain_calc = case_when(
      block_start_valid ~ block0_RECALC_start_mountain + ((hours(2) + minutes(20))*block_number)),
    block_end_mountain_calc = case_when(
      block_end_valid ~ block0_RECALC_start_mountain + ((hours(2) + minutes(20))*(block_number + 1L)),
      !block_end_valid & block_start_valid ~ next_study_day_block0_start_mtn), 
    .after = block_number) %>% 
  relocate(A, block_bounds_mountain, .after = block_number)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1G. Create backbone crosswalk of key variables of participant, day, and block to be used when analyzing the metadata ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cw_pt_date_blocks <- matched_2_dec_pts_v5 %>% 
  select(mars_id, study_date, study_day_int, block_number, block_start_mountain_calc, block_end_mountain_calc, A)

cw_pt_date_blocks %>% count(is.na(block_start_mountain_calc), is.na(A))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# END of Step 1. ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
remove(matched_2_dec_pts_v2, matched_2_dec_pts_v3, matched_2_dec_pts_v4)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 2. Incorporate Other Data Streams----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # STEP 2A. EMI Conditions - processed from Conditions file ----
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# conditions <- conditions %>% 
#   filter(str_detect(V4, "EMI-RANDOM")) %>% 
#   filter((V5 == "condition" & V6 == "is_privacy_on()==false && is_driving(now()-time_offset(00:05:00); now())==false") | V5 %in% c("is_driving", "is_privacy_on"))
# 
# conditions <- conditions %>% mutate(V3_substr = substring(V3, 1, 19), .after = V3)
# 
# conditions_wide <- conditions %>% 
#   group_by(mars_id, V3_substr) %>% 
#   summarise(
#     unixts = V1[1],
#     condition_raw = V7[max(which(V5=="condition"))],
#     is_driving_raw = V7[max(which(V5=="is_driving"))],
#     is_privacy_on_raw = V7[max(which(V5=="is_privacy_on"))]
#   ) %>% 
#   ungroup() %>% 
#   mutate(
#     condition = as.logical(as.numeric(condition_raw)),
#     is_driving = ifelse(is.na(is_driving_raw), NA_character_, str_split_fixed(is_driving_raw, " ", 2)[,1]), 
#     is_privacy_on = as.logical(as.numeric(ifelse(is.na(is_privacy_on_raw), NA_character_, str_split_fixed(is_privacy_on_raw, " ", 2)[,1])))) %>% 
#   mutate(
#     is_driving = case_when(
#       is_driving %in% c("0", "false") ~ FALSE,
#       is_driving == "true" ~ TRUE,
#       T ~ NA)) %>% 
#   mutate(A = NA_character_)
# 
# # Add datetime variables
# conditions_wide <- conditions_wide %>% mutate(cond_hrts_mountain = as_datetime(unixts/1000, tz = "America/Denver"), .before = unixts)
# conditions_wide <- conditions_wide %>% select(-c(V3_substr, condition_raw, is_driving_raw, is_privacy_on_raw))
# 
# 
# test <- matched_2_dec_pts_v5 %>% 
#   left_join(y = conditions_wide,
#             by = join_by(mars_id, between(y$cond_hrts_mountain, x$block_start_mountain_calc, x$block_end_mountain_calc), A))
# 
# test_2 <- test %>% 
#   group_by(mars_id, study_day_int, block_number) %>% 
#   mutate(any_condition_false = if_else(all(is.na(condition)), NA, any(!condition, na.rm = T)), 
#          any_is_driving = any(is_driving, na.rm = T),
#          any_is_privacy_on = any(is_privacy_on, na.rm = T)
#   ) %>% 
#   filter(row_number()==n()) %>% 
#   select(-c(condition, is_driving, is_privacy_on)) %>% 
#   ungroup() %>% 
#   mutate(condition_summ = case_when(
#     !is.na(A) ~ NA_character_,
#     is.na(any_condition_false) ~ "no condition data",
#     any_is_driving & any_is_privacy_on ~ "driving and privacy mode",
#     any_is_driving ~ "driving",
#     any_is_privacy_on ~ "privacy mode",
#     !any_condition_false ~ "clear",
#     T ~ NA_character_
#   ))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 2A. EMI Report. Processed from MD2K ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# EMI report should be more useful than tailor, because they can still be randomized if they miss/didn't have a tailoring quest
emi_report <- emi_report %>% 
  mutate(human_readable_time_try = as_datetime(timestamp_try/1000, tz = "America/Denver"), 
         human_readable_time_notification = as_datetime(timestamp_notification/1000, tz = "America/Denver"),
         human_readable_time_emi_start = as_datetime(timestamp_emi_start/1000, tz = "America/Denver"))

# Interval Join the emi report data to the pt/day/block crosswalk
emi_w_blocks <- full_join(x = emi_report %>% rename(mars_id = participant_id) %>% mutate(in_emi_rpt = T),
                             y = cw_pt_date_blocks %>% mutate(in_pt_blocks = T),
                             by = join_by(mars_id, between(human_readable_time_try, block_start_mountain_calc, block_end_mountain_calc)))

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

matched_2_dec_pts_v6 <- matched_2_dec_pts_v5 %>% mutate(start_block_date = as_date(block_start_mountain_calc),
                                end_block_date = as_date(block_end_mountain_calc),
                                start_block_different_date = start_block_date != study_date,
                                end_block_different_date = end_block_date != study_date,
                                .after = study_date) 

matched_2_dec_pts_v6 %>% 
  count(end_block_different_date, start_block_different_date, is.na(A))

matched_2_dec_pts_v6 %>% filter(is.na(A)) %>% 
  count(start_block_different_date, end_block_different_date, A)   # most missing randomizations are not a result of blocks on the next day, but some may be ~ 90+87?

if(F){matched_2_dec_pts_v6 %>% filter(is.na(block_start_mountain_calc)) %>% View} # 78 have no day start data. can check log for any data recorded on those days

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
    #n = n(),
    n_emi_rpt_rows = sum(!is.na(condition)),
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
                              battery_time_in_bin_minutes = time_duration_minutes)


battery_w_blocks <- right_join(x = battery_binned_all, y = cw_pt_date_blocks,
            by = join_by(mars_id, overlaps(battery_range_start_hrts_mtn, battery_range_end_hrts_mtn, block_start_mountain_calc, block_end_mountain_calc)))
  

if(F){battery_w_blocks %>% group_by(mars_id, study_day_int, block_number) %>% 
  mutate(multiple_battery_statuses_in_block = n() > 1) %>% 
  ungroup() %>% 
  count(A, battery_status, multiple_battery_statuses_in_block) %>% View}


if(F){battery_w_blocks %>% 
  group_by(mars_id, study_day_int, block_number) %>% 
  filter(n() == 1 & battery_status == "No Battery Data") %>% 
  ungroup() %>% 
  count(A)   # 1,200 blocks where the entire block had no battery data and no randomization - 2 cases of no battery data but with a randomization
}

if(F){battery_w_blocks %>% 
  group_by(mars_id, study_day_int, block_number) %>% 
  filter(n() == 1 & battery_status == "No Battery Data") %>% 
  ungroup() %>% 
  View}

# Compute time in block for battery status
battery_w_blocks_v2 <- battery_w_blocks %>% 
  rowwise() %>% 
  mutate(
    total_block_time = difftime(block_end_mountain_calc, block_start_mountain_calc, units = "mins"),
    time_battery_status_in_block = case_when(
      !is.na(block_start_mountain_calc) ~ difftime( min(battery_range_end_hrts_mtn, block_end_mountain_calc) , max(battery_range_start_hrts_mtn, block_start_mountain_calc), units = "mins"),
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
            battery_range_end_hrts_mtn, #battery_time_in_bin_minutes, 
            time_battery_status_in_block)) %>% 
  ungroup() %>% 
  mutate(percent_of_block_time_in_battery_status = round(as.numeric(total_time_of_same_battery_status_in_block) / as.numeric(total_block_time) * 100, digits = 4)) %>% 
  group_by(mars_id, study_day_int, block_number) %>% 
  mutate(
    battery_percent_change = case_when(n()>1 ~ NA_integer_,
                                       T ~ battery_percent_change),
    battery_percent_period_start = case_when(n()>1 ~ NA_integer_,
                                             T ~ battery_percent_period_start),
    battery_percent_period_end = case_when(n()>1 ~ NA_integer_,
                                             T ~ battery_percent_period_end),
    battery_percent_change = case_when(n()>1 ~ NA_integer_,
                                       T ~ battery_percent_change),
    battery_percent_change_bin = case_when(n()>1 ~ NA_character_,
                                       T ~ battery_percent_change_bin),
    lag_battery_status = case_when(n()>1 ~ NA_character_,
                                   T ~lag_battery_status),
    lead_battery_status = case_when(n()>1 ~ NA_character_,
                                    T ~lead_battery_status)) %>% 
  ungroup()

remove(battery_w_blocks_v2)

# Create a wide version that is one row per participant/day/block 
battery_w_blocks_wide <- battery_w_blocks_tall %>% 
  pivot_wider(
    names_from = battery_status,
    values_from = c(total_time_of_same_battery_status_in_block, percent_of_block_time_in_battery_status, 
                    battery_percent_period_start, battery_percent_period_end, battery_time_in_bin_minutes, time_duration_hours, battery_percent_change, battery_percent_change_bin, lag_battery_status, lead_battery_status
                    )
  )

battery_w_blocks_wide <- battery_w_blocks_wide %>% 
  select(-c("total_time_of_same_battery_status_in_block_NA", "percent_of_block_time_in_battery_status_NA",
            "battery_percent_period_start_0-10%", "battery_percent_period_start_NA", "battery_percent_period_start_10-100%",
            "battery_percent_period_end_10-100%", "battery_percent_period_end_0-10%", "battery_percent_period_end_NA",
            "battery_time_in_bin_minutes_10-100%", "battery_time_in_bin_minutes_0-10%", "battery_time_in_bin_minutes_NA",
            "time_duration_hours_10-100%", "time_duration_hours_0-10%", "time_duration_hours_NA",
            "battery_percent_change_10-100%", "battery_percent_change_0-10%", "battery_percent_change_NA",                                 
            "battery_percent_change_bin_10-100%", "battery_percent_change_bin_0-10%", "battery_percent_change_bin_NA",
            "lag_battery_status_10-100%", "lag_battery_status_0-10%", "lag_battery_status_NA",
            "lead_battery_status_10-100%", "lead_battery_status_0-10%", "lead_battery_status_NA"
            )) %>% 
  rename(
    time_no_battery_data = "total_time_of_same_battery_status_in_block_No Battery Data",
    time_low_battery = "total_time_of_same_battery_status_in_block_0-10%",
    time_sufficient_battery = "total_time_of_same_battery_status_in_block_10-100%",
    percent_time_no_battery_data = "percent_of_block_time_in_battery_status_No Battery Data",
    percent_time_low_battery = "percent_of_block_time_in_battery_status_0-10%",
    percent_time_sufficient_battery = "percent_of_block_time_in_battery_status_10-100%",
    time_duration_hours_no_battery_data = "time_duration_hours_No Battery Data"
  )

battery_w_blocks_wide <- battery_w_blocks_wide %>% 
  mutate(entire_block_no_battery_data = percent_time_no_battery_data == 100,
         ninety_plus_perc_block_no_battery_data = percent_time_no_battery_data >= 90)

if(F){
  battery_w_blocks_wide %>% count(entire_block_no_battery_data)
  
  battery_w_blocks_wide %>% filter(entire_block_no_battery_data) %>% count(entire_block_no_battery_data, A) # 1202 out of 1574 non-randomized had entire block with no battery data
  
  # @TB: looking at battery before and after period of no battery data

  battery_w_blocks_wide %>% filter(entire_block_no_battery_data) %>% count(lag_battery_status, lead_battery_status)

  battery_w_blocks_wide %>% count(A)
  
  battery_w_blocks_wide %>% filter(!is.na(percent_time_no_battery_data)) %>% count(entire_block_no_battery_data, A) # 53 more non-randomized that had no battery data for part of the block
  
  if(F){battery_w_blocks_wide %>% filter(!is.na(percent_time_no_battery_data)) %>% filter(!entire_block_no_battery_data & is.na(A)) %>% View}
  
  battery_w_blocks_wide %>% count(ninety_plus_perc_block_no_battery_data, entire_block_no_battery_data, A) # non randomized count is close to the randomized count for those with no battery data for 90-99.9% of the block time. Not as clear cut as those with no battery data for the entire block
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 2C. Recombine data stream findings with updated "matched_2_dec_pts" data ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
matched_2_dec_pts_v7 <- matched_2_dec_pts_v6 %>% 
  full_join(y = battery_w_blocks_wide,
            by = join_by(mars_id, study_date, study_day_int, block_number, block_start_mountain_calc, block_end_mountain_calc, A)) %>% 
  full_join(y = emi_w_blocks_v2,
            by = join_by(mars_id, study_date, study_day_int, block_number, block_start_mountain_calc, block_end_mountain_calc, A))

if(F){
matched_2_dec_pts_v7 %>% count(A, entire_block_no_battery_data)
matched_2_dec_pts_v7 %>% filter(is.na(A)) %>% count(A, entire_block_no_battery_data) # 1286 with no battery data entire block, 52 partial no battery data in block, 236 with no "no battery data" in block

matched_2_dec_pts_v7 %>% filter(is.na(A)) %>% count(A, entire_block_no_battery_data, condition_lgcl) # EMI data shows up to 4 blocks that *might* have been randomized according to the EMI report - note still some possible issue with crossing past midnight and the phone truncating that day
                                                                                                     # and 15 that were not randomized because conditions were not right for entire time it checked

matched_2_dec_pts_v7 %>% count(A, entire_block_no_battery_data, ninety_plus_perc_block_no_battery_data, condition_lgcl, end_block_different_date) %>% View

matched_2_dec_pts_v7 %>% count(A, is.na(entire_block_no_battery_data), is.na(condition_lgcl))

matched_2_dec_pts_v7 %>% filter(is.na(entire_block_no_battery_data) & is.na(condition_lgcl)) %>%  count(A, is.na(entire_block_no_battery_data), is.na(condition_lgcl), is.na(block0_start_mountain_calculated))

matched_2_dec_pts_v7 %>% filter(is.na(A)) %>% filter(is.na(entire_block_no_battery_data)) %>% count(is.na(percent_time_low_battery), is.na(percent_time_sufficient_battery))

matched_2_dec_pts_v7 %>% filter(is.na(A) & is.na(entire_block_no_battery_data) & !(is.na(percent_time_low_battery) & is.na(percent_time_sufficient_battery))) %>% View

}

# 217 that could be investigated from those and tried to match data into block intervals - they have low or sufficient battery but no EMI data

# Will look at additional data streams to see if any streams have data 

# conditions file and systemLog file have additional information. initial looking into the 265 showed data gaps in those sources during the blocks in question. Need to read in all the data and match up to see concordance


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 2D. System Log file EMI Randomizations ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

system_log_EMI_rands <- system_log %>% filter(str_detect(V6, "random selection=" )) %>% 
  rename(mars_id = participant_id) %>% 
  mutate(system_log_emi_rand_ts_mtn = as_datetime(V1/1000, tz = "America/Denver"),
         .before = mars_id)


sytem_log_joined_emi_rpt <- full_join(x = system_log_EMI_rands %>% mutate( in_sysLog = T),
                         y = emi_report %>% rename(mars_id = participant_id) %>% 
                           mutate(minus2s_human_readable_time_try = human_readable_time_try - seconds(2),
                                  plus3s_human_readable_time_try = human_readable_time_try + seconds(3),
                                  in_emi_rpt = T),
                         by = join_by(mars_id, between(system_log_emi_rand_ts_mtn, minus2s_human_readable_time_try, plus3s_human_readable_time_try)),
                         keep = T)

sytem_log_joined_emi_rpt %>% count(in_sysLog, in_emi_rpt, condition )

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
                                  by = join_by(mars_id, between("system_log_emi_rand_ts_mtn", block_start_mountain_calc, block_end_mountain_calc) ))

systemlog_emi_blocks %>% count(in_syslog, in_blocks_cw)

systemlog_emi_blocks %>% count(in_syslog, in_blocks_cw, is.na(A))

systemlog_emi_blocks %>% group_by(mars_id) %>% 
  filter(any(in_blocks_cw)) %>% 
  filter(in_blocks_cw | between(system_log_emi_rand_ts_mtn, min(block_start_mountain_calc, na.rm = T), max(block_end_mountain_calc, na.rm = T))) %>% 
  ungroup() %>% 
  count(in_syslog, in_blocks_cw, A)

systemlog_emi_blocks %>% filter(in_syslog & in_blocks_cw) %>% count(A, random_selection_sysLog)

if(F){systemlog_emi_blocks %>% filter(in_syslog & in_blocks_cw & is.na(A)) %>% View}


systemlog_emi_blocks <- systemlog_emi_blocks %>% filter(in_blocks_cw) %>% relocate(all_of(colnames(cw_pt_date_blocks)), .before = everything()) %>% select(-c(V1,V2,V3,V4,V5,V6, in_syslog, in_blocks_cw))

systemlog_emi_rand_lookup_table <- tribble(
  ~random_selection_sysLog, ~system_log_emi_randomization,
  "0", "none",
  "1", "none",
  "2", "low_effort",
  "3", "mars")

systemlog_emi_blocks <- systemlog_emi_blocks %>% 
  left_join(y = systemlog_emi_rand_lookup_table,
            by = "random_selection_sysLog")

# Some (5 total) blocks matched to 2 system log emi randomizations. If the randomizations were not the same for both, then keep the one that matches A. Otherwise take the earliest to occur
systemlog_emi_blocks <- systemlog_emi_blocks %>% 
  mutate(syslog_rand_matches_A = system_log_emi_randomization == A) %>% 
  group_by(mars_id, study_day_int, block_number) %>%
  arrange(mars_id, study_day_int, block_number, desc(syslog_rand_matches_A)) %>% 
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  select(-syslog_rand_matches_A)


# Join to main dataset
matched_2_dec_pts_v8 <- matched_2_dec_pts_v7 %>% 
  full_join(y = systemlog_emi_blocks, 
            by = join_by(mars_id, study_date, study_day_int, block_number, block_start_mountain_calc, block_end_mountain_calc, A))

# stats
matched_2_dec_pts_v8 %>% filter(is.na(A)) %>% count(is.na(system_log_emi_randomization), condition_lgcl, entire_block_no_battery_data)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 2E. System Log file looking for any data during the block ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if(F){matched_2_dec_pts_v8 %>% head() %>% View}

system_log_v2 <- system_log %>% rename(mars_id = participant_id) %>% mutate(system_log_hrts_mtn = as_datetime(V1/1000, tz = "America/Denver"))


system_log_joined_2_blocks <- system_log_v2 %>% 
  right_join(y = cw_pt_date_blocks,
             by = join_by(mars_id, between(x$system_log_hrts_mtn, y$block_start_mountain_calc, y$block_end_mountain_calc))) 

block_system_log <-  system_log_joined_2_blocks %>% 
  group_by(mars_id, study_date, study_day_int, block_number, block_start_mountain_calc, block_end_mountain_calc, A) %>% 
  summarise(n_syslog_records = sum(!is.na(system_log_hrts_mtn)),
            any_syslog_records = n_syslog_records > 0,
            ten_plus_syslog_records = n_syslog_records >= 10,
            any_scheduler_error = any(str_detect(V6, "Scheduler Error")),
            any_error = any(str_detect(str_to_lower(V6), "error"))) %>% 
  ungroup()


matched_2_dec_pts_v8 <- matched_2_dec_pts_v8 %>% 
  left_join(y = block_system_log,
            by = join_by(mars_id, study_date, study_day_int, block_number, block_start_mountain_calc, block_end_mountain_calc, A))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 3. Process for Final Summary Indicators ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 3A. Filter out Study Days 1 and 10 ----
#   * Not representative because they begin mid day 1 and end mid day 10
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
matched_2_dec_pts_v9 <- matched_2_dec_pts_v8 %>% filter(!study_day_int %in% c(1,10))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 3B. Add withdrawn info ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
matched_2_dec_pts_v9 <- matched_2_dec_pts_v9 %>% 
  left_join(y = pt_withdrawn_date %>% select(mars_id = Grafana_ID, withdrawn_visit_record = visit_name, withdrawn_date),
            by = "mars_id") %>% 
  relocate(withdrawn_date, .after = study_date) %>% 
  mutate(after_withdraw_date = study_date >= withdrawn_date, .after = withdrawn_date)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 3C. Collapse the information from each data stream into a limited number of key indicators to summarize the information ----
#   - Battery: 
#        * battery_status (refers to amount of No Battery Data)
#        * no_battery_time_hours_categ (binned length of time for the block's no battery data period - only for blocks with no battery data in the entire block)
#        *
#   - EMI Report:
#        * last_condition (last of the recorded condition checks for the given block)
#        * A_emi_report   (EMI randomization [A] from the EMI report)
#   - System Log
#        * Randomizations
#        * Errors
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

emi_report_cw <- tribble(
  ~random_selection,  ~A_emi_report,
  "LOW_INTERVENTION", "low_effort",
  "MARS",             "mars",
  "NO_TRIGGER",       "none")

matched_2_dec_pts_v10 <- matched_2_dec_pts_v9 %>% 
  # Battery
  mutate(
    battery_status = case_when(
      entire_block_no_battery_data ~ "No Data - Entire Block",
      ninety_plus_perc_block_no_battery_data ~ "No Data - 90-99.9% of Block",
      between(percent_time_no_battery_data, 75, 90) ~ "No Data - 75-89.9% of Block",
      between(percent_time_no_battery_data, 50, 75) ~ "No Data - 50-74.9% of Block",
      between(percent_time_no_battery_data, 25, 50) ~ "No Data - 25-49.9% of Block",
      between(percent_time_no_battery_data, 10, 25) ~ "No Data - 10-24.9% of Block",
      between(percent_time_no_battery_data, 0, 10) ~ "No Data - Less than 10% of Block",
      is.na(percent_time_no_battery_data) ~ "No Missing Data"
    )) %>% 
  
  mutate(battery_status_simple = case_when(
    battery_status == "No Data - Entire Block" ~ "No Data - Entire Block",
    battery_status == "No Missing Data" ~ "No Missing Data",
    T ~ "Some Missing Data"
  )) %>% 
  # EMI Report
  mutate(
    #A_emi_report = random_selection,
    last_condition_emi_report = condition_lgcl
  ) %>% 
  # System log EMI Rand
  mutate(
    A_system_log = system_log_emi_randomization
  ) %>% 
  left_join(y = emi_report_cw,
            by = "random_selection") %>% 
  mutate(after_withdraw_date = replace_na(after_withdraw_date, FALSE))


matched_2_dec_pts_v10$battery_status <- factor(matched_2_dec_pts_v10$battery_status, levels = c("No Missing Data", "No Data - Less than 10% of Block", "No Data - 10-24.9% of Block", "No Data - 25-49.9% of Block", "No Data - 50-74.9% of Block",
                                                                                              "No Data - 75-89.9% of Block", "No Data - 90-99.9% of Block", "No Data - Entire Block"))

matched_2_dec_pts_v10 <- matched_2_dec_pts_v10 %>% 
  mutate(no_battery_time_hours_categ = case_when(
    battery_status_simple != "No Data - Entire Block" ~ NA_character_,
    between(time_duration_hours_no_battery_data, 2, 3) ~ "2-3 hours",
    between(time_duration_hours_no_battery_data, 3, 4) ~ "3-4 hours",
    between(time_duration_hours_no_battery_data, 4, 6) ~ "4-6 hours",
    between(time_duration_hours_no_battery_data, 6, 8) ~ "6-8 hours",
    between(time_duration_hours_no_battery_data, 8, 12) ~ "8-12 hours",
    between(time_duration_hours_no_battery_data, 12, 24) ~ "12-24 hours",
    between(time_duration_hours_no_battery_data, 24, 48) ~ "24-48 hours",
    time_duration_hours_no_battery_data > 48 ~ "> 48 hours"))


matched_2_dec_pts_v10$no_battery_time_hours_categ <- factor(matched_2_dec_pts_v10$no_battery_time_hours_categ, levels = c("2-3 hours", "3-4 hours", "4-6 hours", "6-8 hours", 
                                                                                                                          "8-12 hours", "12-24 hours", "24-48 hours", "> 48 hours"))

if(F){matched_2_dec_pts_v10 %>% filter(study_day_int %in% 2:9) %>% filter(is.na(A) & battery_status_simple == "No Data - Entire Block") %>% 
  count(is.na(A), battery_status_simple, no_battery_time_hours_categ) %>% View}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 4. Prep EMI rand summary variable ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# matched_2_dec_pts_v11 <- matched_2_dec_pts_v10 %>% 
#   mutate(
#     emi_non_rand_summary = case_when(
#       !is.na(A) ~ NA_character_,        # no values for blocks with an EMI randomization
#       # all below are for non-randomized EMI
#       after_withdraw_date ~ "After participant withdrew",   # block occurred after the date when the participant withdrew 
#       !block_start_valid ~ "Invalid block start time",    # block could never even begin due to the next day's block 1 starting
#       !last_condition_emi_report ~ "Driving",      # all records of EMI conditions being false coincided with driving - not privacy mode
#       any_error | last_condition_emi_report ~ "Software error",       # log file recorded an error associated to this EMI block or the conditions log indicated conditions were true but EMI rand was not recorded and used
#       !any_syslog_records & battery_status_simple != "No Data - Entire Block" ~ "No log data but sufficient battery",  # No log data during block despite sufficient battery in the phone
#       battery_status_simple == "No Data - Entire Block" ~ "No log data and no battery data",  
#       !block_end_valid ~ "Invalid block end time",
#       T ~ "Undetermined"
#     )
#   )

matched_2_dec_pts_v11 <- matched_2_dec_pts_v10 %>% 
  mutate(
    emi_non_rand_summary = case_when(
      !is.na(A) ~ NA_character_,        # no values for blocks with an EMI randomization
      # all below are for non-randomized EMI
      after_withdraw_date ~ "After participant withdrew",   # block occurred after the date when the participant withdrew 
      !block_start_valid | !block_end_valid ~ "Block 0 on study day was programmed to start after 10am",    # block could never even begin due to the next day's block 1 starting
      !last_condition_emi_report ~ "Driving",      # all records of EMI conditions being false coincided with driving - not privacy mode
      any_error | last_condition_emi_report ~ "Software error",       # log file recorded an error associated to this EMI block or the conditions log indicated conditions were true but EMI rand was not recorded and used
      battery_status_simple == "No Data - Entire Block" ~ "No log data and no battery data",  
      T ~ "Undetermined"
    )
  )

matched_2_dec_pts_v11$emi_non_rand_summary <- factor(matched_2_dec_pts_v11$emi_non_rand_summary, 
                                                     levels = c("After participant withdrew", "Block 0 on study day was programmed to start after 10am", 
                                                                "Driving", "Software error", "No log data and no battery data", "Undetermined"))


if(F){matched_2_dec_pts_v11 %>% count(is.na(A), emi_non_rand_summary)}



if(F){matched_2_dec_pts_v11 %>% filter(is.na(A)) %>% count(emi_non_rand_summary) %>% mutate(percent_n = round(n/sum(n)*100, digits = 1))}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 5. Save Datasets ----
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

saveRDS(matched_2_dec_pts_v11,
        file = file.path(path_to_staged, "matched_2_dec_pts_full_metadata.RDS"))

matched_2_dec_pts_summarized_metadata <- matched_2_dec_pts_v11 %>%
  select(all_of(colnames(matched_2_dec_pts)), study_day_int, study_date, olson_calc, block0_RECALC_start_mountain, block_start_mountain_calc, block_end_mountain_calc, block_start_valid, block_end_valid,
         battery_status, battery_status_simple, last_condition_emi_report, after_withdraw_date, any_error, no_battery_time_hours_categ, emi_non_rand_summary)

saveRDS(matched_2_dec_pts_summarized_metadata,
        file = file.path(path_to_staged, "matched_2_dec_pts_summarized_metadata.RDS"))
