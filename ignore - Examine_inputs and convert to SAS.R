library(dplyr)
library(tidyr)
library(readr)
library(haven)

path_to_inputs <- "C:/Users/borleans/Box/MARS metadata support for Jamie/Input/Utah Data Sets/Utah Data Sets"

list.files(path_to_inputs)

conv_long_ema <- readRDS(file.path(path_to_inputs, "dat_conventional_long_format_ema_responses.rds"))
dim(conv_long_ema)

master_ema_quest <- readRDS(file.path(path_to_inputs, "dat_master_ema_questions.rds"))
dim(master_ema_quest)

matched_2_dec_pts <- readRDS(file.path(path_to_inputs, "dat_matched_to_decision_points.rds"))
dim(matched_2_dec_pts)

matched_2_dec_pts_small <- readRDS(file.path(path_to_inputs, "dat_matched_to_decision_points_small.rds"))
dim(matched_2_dec_pts_small)

v1_visit_dates <- readRDS(file.path(path_to_inputs, "dat_visit_dates_V1_only.rds"))
dim(v1_visit_dates)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

matched_2_dec_pts %>% count(status_survey_ema) %>% mutate(percent_n = scales::percent(round(n/sum(n), digits = 3)))


matched_2_dec_pts %>% count(status_survey_2qs) %>% mutate(percent_n = scales::percent(round(n/sum(n), digits = 3)))

#___________________________________________________________________________________________________________

#
path_to_sas_staged <- "C:/Users/borleans/Box/MARS metadata support for Jamie/Staged/SAS_analysis_data_raw_20230525"

conv_long_ema %>% write_dta(file.path(path_to_sas_staged,'dat_conventional_long_format_ema_responses.dta'))
master_ema_quest %>% write_dta(file.path(path_to_sas_staged,'dat_master_ema_questions.dta'))
matched_2_dec_pts %>% write_dta(file.path(path_to_sas_staged,'dat_matched_to_decision_points.dta'))
matched_2_dec_pts_small %>% write_dta(file.path(path_to_sas_staged,'dat_matched_to_decision_points_small.dta'))
v1_visit_dates %>% write_dta(file.path(path_to_sas_staged,'dat_visit_dates_V1_only.dta'))
