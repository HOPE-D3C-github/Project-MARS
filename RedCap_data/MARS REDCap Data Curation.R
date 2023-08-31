#################################################################################################################################
# Title: MARS REDCap cleaning
# desc: imports data from REDCap, 
#       summarizes surveys into scales, 
#       splits cleaned data into one dataset per event, 
#       creates a codebook and metadata
# relying: Lindsey/Dusti have plans to use data around early September; Jamie will use summary stats for her uMich analysis
# WARNING: Since I don't have access to all mHealth projects, the filepath to import cxwalk will need to be updated (line 36)
# WARNING: The Excel formatting section of the code will take forever to run; just a heads up :)
#################################################################################################################################
# load -------------------------------------------------------------------------
library(tidyverse)
library(redcapAPI)
library(labelled)
library(openxlsx)
library(haven) # export .dta files

# working directory and REDCap connection
mywd <- rstudioapi::getActiveDocumentContext()$path %>% dirname()
pre_box_path <- mywd %>% str_extract(".+Box/")
setwd(mywd)
source(file.path(pre_box_path,'credentials/MARS_redcap_api_key.R'))
con <- redcapConnection("https://hci-redcap.hci.utah.edu/redcap/api/",
                        token=my_api_key[[1]])

# load data
events_raw <- exportEvents(con) %>% as_tibble()
mappings_raw <- exportMappings(con) %>% as_tibble()
fieldnames_raw <- exportFieldNames(con) %>% as_tibble()
meta_raw <- exportMetaData(con) %>% as_tibble()
data_raw <- exportRecordsTyped(con, cast=list(radio = castRaw,
                                              checkbox = castRaw,
                                              yesno = castRaw,
                                              dropdown = castRaw)) %>% as_tibble()
# [TO DO]: add /mhealth_data_management to filepath once added to folder
if(T) {
  cxwalk <- read_rds('../MARS/MARS - data sharing/mars_visit_raw_datashare_20230510/redcap_crosswalk.rds')
} else {
  cxwalk <- read_rds('../mhealth_data_management/MARS/MARS - data sharing/mars_visit_raw_datashare_20230510/redcap_crosswalk.rds')
}
id_check <- read_rds("dat_visit_dates_V1_only.rds")
  
# basic data cleaning ----------------------------------------------------------
## events ----
events_final <- events_raw %>%
  #ignore arm number
  mutate(unique_event_name = str_remove(unique_event_name, "_arm_1")) %>%
  select(event_name, unique_event_name)

## event mapping ----
mappings_final <- mappings_raw %>%
  #ignore arm number
  mutate(unique_event_name = str_remove(unique_event_name, "_arm_1")) %>%
  select(-arm_num)

## fieldnames
fieldnames_final <- fieldnames_raw %>%
  # remove Record ID variable (only appears in metadata, not datasets)
  filter(original_field_name != "record_id")

## metadata ----
# key to add labels to checkbox variables
checkbox_key <- meta_raw %>%
  filter(field_type == "checkbox") %>%
  separate_longer_delim(select_choices_or_calculations, 
                        delim = " | ") %>%
  mutate(select_choices_or_calculations = str_replace(select_choices_or_calculations, ", ", "; ")) %>%
  separate(select_choices_or_calculations, c("choice", "select_choices_or_calculations"), "; ") %>%
  select(field_name, choice, select_choices_or_calculations) %>%
  rename(select_choices = select_choices_or_calculations)

# timestamp variables to add
timestamps <- meta_raw %>%
  select(form_name) %>%
  unique() %>%
  mutate(field_name = paste0(form_name, "_timestamp"),
         field_label = paste("Date subject completed the", form_name, "survey"),
         field_type = "text",
         text_validation_type_or_show_slider_number = "date")

# complete variables to update
completes <- meta_raw %>%
  select(form_name) %>%
  unique() %>%
  mutate(field_name = paste0(form_name, "_complete"),
         field_label = paste("Completion status of the", form_name, "survey"),
         field_type = "radio",
         select_choices_or_calculations = "Complete | Incomplete | Unverified")

meta_clean <- meta_raw %>%
  # remove text blocks
  filter(field_type != "descriptive") %>%
  # remove html syntax from labels
  mutate(field_label = str_remove_all(field_label, "<.*?>")) %>%
  # add response options to checkbox and yesno variables
  right_join(fieldnames_final, join_by(field_name == original_field_name)) %>%
  left_join(checkbox_key, join_by(field_name == field_name, choice_value == choice)) %>%
  mutate(select_choices_or_calculations =
           case_when(
             field_type == "yesno" ~ "0 - No | 1 - Yes",
             field_type == "checkbox" ~ "0 - Unchecked | 1 - Checked",
             .default = select_choices_or_calculations
           )) %>%
  mutate(field_label = if_else(field_type == "checkbox",
                               paste(field_label, select_choices),
                               field_label)) %>%
  # add exported checkbox, complete, and timestamp variables to metadata
  select(-field_name) %>%
  rename(field_name = export_field_name) %>%
  
  mutate(field_name = replace(field_name, field_name == "record_id_80f34c", "record_id")) %>%
  rows_update(completes, by = "field_name") %>%
  full_join(timestamps, by = join_by(form_name, field_name, field_label, field_type, text_validation_type_or_show_slider_number)) %>%
  # standardize NIDA naming conventions
  mutate(field_name = case_match(
    field_name,
    "marijuana_2" ~ "marijuana_1",
    "marijuana_3" ~ "marijuana_2",
    "marijuana_4" ~ "marijuana_3",
    "maijuana_5" ~ "marijuana_4",
    .default = field_name
  )) %>%
  # one row per response option
  separate_longer_delim(select_choices_or_calculations, 
                        delim = " | ") %>%
  mutate(select_choices_or_calculations = str_replace(select_choices_or_calculations, ", ", " - "))

## data ----
data_clean <- cxwalk %>%
  inner_join(data_raw, by=join_by(REDCap_ID == record_id)) %>%
  rename(record_id = REDCap_ID) %>%
  mutate(scr_age = floor(scr_age)) %>%
  labelled::remove_labels() %>%
  rename(marijuana_1 = marijuana_2,
         marijuana_2 = marijuana_3,
         marijuana_3 = marijuana_4,
         marijuana_4 = maijuana_5) %>%
  # ignore arm number
  mutate(redcap_event_name = str_remove(redcap_event_name, " \\(Arm 1: Arm 1\\)")) %>%
  #@brian: check back with this. 
  # add sex to V4 (needed for ARDB summary)
  # add grafana_id and subject_id to all participants
  group_by(record_id) %>%
  fill(c(grafana_id, subject_id, dses1a_1), .direction = "downup") %>%
  ungroup()

# format date variables
for (i in colnames(data_clean)) {
  if (is.POSIXt(data_clean[[i]]) | grepl("_timestamp$", i)) {
    # change timestamps marked "[not completed]" to NA to avoid fail-to-parse warnings
    if(grepl("_timestamp$", i)) {
      data_clean[[i]] <- na_if(data_clean[[i]], "[not completed]")
    }
    # NOTE: assumes yyyy-mm-dd format (should be the case with REDCap API)
    data_clean[[i]] <- as_date(data_clean[[i]])
  }
}

# summary measures for survey scales -------------------------------------------
source("summary_codebook_funs.R")

# keep track of variable changes
recoded_vars <- c()

# INSTRUMENT                                            | ABBR.    | EVENTS
# ------------------------------------------------------|----------|------------
# 1.  Brief Health Literacy                             | BHL      | v1
# 2.  Tobacco History                                   | TH       | v1
# 3.  Heaviness of Smoking Index                        | HSI      | v1       v4
# 4.  Wisconsin Smoking Withdrawal Scale                | WSWS     | v1 v2 v3 v4
# 5.  E-Cigarette Baseline                              | ECIG     | v1
# 6.  Smoking Self-Efficacy                             | SE       | v1 v2 v3 v4
# 7.  Texas Smoking Abstinence Motivation               | TSAM     | v1 v2 v3 v4
# 8.  Patient Health Questionnaire - Alcohol            | PHQA     | v1       v4
# 9.  At-Risk Drinking Behaviors                        | ARDB     | v1       v4
# 10. National Institute on Drug Abuse                  | NIDA     | v1       v4
  ## 10a. Marijuana                                      
  ## 10b. Cocaine
  ## 10c. Prescription Stimulants
  ## 10d. Methamphetamine
  ## 10e. Inhalants
  ## 10f. Sedatives
  ## 10g. Hallucinogens
  ## 10h. Street Opioids
  ## 10i. Prescription Opioids
  ## 10j. Other
# 11. Perception of Poverty                             | PP       | v1       v4
# 12. Financial Strain                                  | FS1      | v1       v4
# 13. Food Security                                     | FS2      | v1       v4
# 14. Subjective Socioeconomic Status Ladders           | SSSL     | v1       v4
# 15. Neighborhood Disorganization                      | ND       | v1       v4
# 16. Adverse Childhood Experience Questionnaire        | ACE      | v1
# 17. MacArthur Lifetime Discrimination                 | MLD      | v1       v4
# 18. Everyday Discrimination Scale                     | EDS      | v1       v4
# 19. Five Facet Mindfulness                            | FFM      | v1
# 20. Mindful Attention Awareness Scale                 | MAAS     | v1       v4
# 21. Shift and Persist                                 | SP       | v1       v4
# 22. Barratt Impulsiveness Scale                       | BIS      | v1
# 23. Social Vigilance                                  | SV       | v1       v4
# 24. Anxiety Index                                     | AI       | v1       v4
# 25. Decentering                                       | DECENTER | v1       v4
# 26. Distress Tolerance Scale                          | DTS      | v1       v4
# 27. Patient Health Questionnaire - Depression         | PHQD     | v1       v4
# 28. Generalized Anxiety Disorder                      | GAD      | v1       v4
# 29. Snaith-Hamilton Pleasure Scale                    | SHAPS    | v1 v2 v3 v4
# 30. Perceived Stress                                  | PS       | v1 v2 v3 v4
# 31. Gratitude Questionnaire                           | GQ       | v1 v2 v3 v4
# 32. Modified Differential Emotions Scale              | MDES     | v1 v2 v3 v4
# 33. Interpersonal Support Evaluation List             | ISEL     | v1       v4
# 34. Social Network Index                              | SNI      | v1       v4
# 35. UCLA Loneliness Scale                             | UCLALS   | v1 v2 v3 v4
# 36. Physical Activity Questionnaire                   | PAQ      | v1       v4
# 37. Sleep                                             | SLEEP    | v1 v2 v3 v4
# 38. Wisconsin Inventory of Smoking Dependence Motives | WISDM    | v1
# 39. Rotterdam Emotional Intelligence Scale            | REIS     | v1       v4
# 40. Need for Cognition Scale                          | NCS      | v1
# 41. Ten-Item Personality Inventory                    | TIPI     | v1
# 42. Self-Regulation Questionnaire                     | SRQ      | v1
# 43. E-Cigarette Follow-Up                             | ECIGFU   |          v4
# 44. Tobacco Abstinence and Compliance with NRT v2     | TAC2     |    v2
# 45. Tobacco Abstinence and Compliance with NRT v3     | TAC3     |       v3
# 46. Tobacco Abstinence and Compliance with NRT v4     | TAC4     |          v4
# 47. Tobacco History v4                                | TH4      |          v4
# 48. Usability v1                                      | US1      | v1
# 49. Usability v3                                      | US3      |       v3

### 1.  BHL ----
# we only have 3 of 16 questions
recode_scale("bhl1_2", 5:1)

data_clean1 <- data_clean %>%
  # reverse code question 2
  mutate(bhl1_2 = 6 - bhl1_2) %>%
  scale_mean(oldvars = grep("bhl1_[1-3]$", colnames(data_clean), value=TRUE),
             newvar = "health_lit",
             newlab = "Higher scores indicate higher health literacy",
             newchoices = "Mean of bhl1_1-bhl1_3",
             prop_nonmiss = 0.7)

### 2.  TH ----
# may be used individually and not combined in scale

if(F) {
  # check for anomalous answers
  lapply(grep("th[2-4]_1$", colnames(data_clean), value=TRUE), 
         function(x) table(data_clean[,x]))
}

data_clean2 <- data_clean1

### 3.  HSI ----
# recode data as follows:
# hsi1_1. How many cigarettes do you typically smoke per day?
  ## 10 or fewer - 0 points
  ## 11-20 - 1 point
  ## 21-30 - 2 points
  ## 31 or more - 3 points
# hsi2_1. On the days that you smoke, how soon after you wake up do you have your first cigarette?
  ## Within 5 minutes - 3 points
  ## 6-30 minutes - 2 points
  ## 31-60 minutes - 1 point
  ## After 60 minutes - 0 points

if(F) {
  # check for anomalous answers
  table(data_clean[,"hsi1_1"])
}

# add recoded variables to codebook
update_codebook(oldvars = "hsi1_1",
                newvar = "rhsi1_1", 
                newlab = "Recoded hsi1_1; scale of number of cigarettes smoked per day",
                newchoices = "0 - 10 or fewer | 1 - 11 to 20 | 2 - 21 to 30 | 3 - 31 or more",
                position = "hsi2_1")
update_codebook(oldvars = "hsi2_1",
                newvar = "rhsi2_1", 
                newlab = "Recoded hsi2_1; how soon after waking up do you smoke your first cigarette?", 
                newchoices = "0 - After 60 minutes | 1 - 31 to 60 minutes after waking up | 2 - 6 to 30 minutes after waking up | 3 - Within 5 minutes of waking up",
                position = "rhsi1_1")
meta_clean <- meta_clean %>%
  rows_update(tibble(field_name = c("rhsi1_1", "rhsi2_1"), field_type = "radio"), by="field_name")

data_clean3 <- data_clean2 %>%
  mutate(rhsi1_1 = as.numeric(cut(hsi1_1, breaks = c(-Inf, 10, 20, 30, Inf),
                                  labels=c("10 or fewer", "11-20", "21-30", "31 or more"))) - 1,
         rhsi2_1 = 4 - hsi2_1, 
         .after = hsi2_1) %>%
  scale_sum(oldvars = c("rhsi1_1", "rhsi2_1"),
            newvar = "Nicotine_dep",
            newlab = "Higher scores indicate higher smoking levels",
            newchoices = "Sum of rhsi1_1 and rhsi2_1")

### 4.  WSWS ----
# unlike BF, the MARS survey only contains questions about craving

data_clean4 <- data_clean3 %>%
  # no recoding for craving questions
  scale_mean(oldvars = grep("wsws_[1-4]$", colnames(data_clean), value=TRUE),
             newvar = "wsws_craving",
             newlab = "Higher scores indicate higher craving levels",
             newchoices = "Mean of wsws_1-wsws_4",
             prop_nonmiss = 0.7)

### 5.  ECIG ----
# may be used individually and not combined in scale

if(F) {
  # check for anomalous answers
  lapply(grep("ecig_[2-4]$", colnames(data_clean), value=TRUE), 
         function(x) table(data_clean[,x]))
}

data_clean5 <- data_clean4

### 6.  SE ----
# subscales
  ## Social/Positive:  se1_1, se1_4, and se1_7
  ## Habitual/Craving: se1_2, se1_5, and se1_8
  ## Negative Affect:  se1_3, se1_6, and se1_9

data_clean6 <- data_clean5 %>%
  # no recoding
  # overall scale
  scale_mean(oldvars = grep("se1_[1-9]$", colnames(data_clean), value=TRUE),
             newvar = "SE_total",
             newlab = "Higher scores indicate greater confidence to not smoke overall",
             newchoices = "Mean of se1_1-se1_9",
             prop_nonmiss = 0.7,
             position="se1_9") %>%
  # negative affect scale
  scale_mean(oldvars = grep("se1_[3,6,9]$", colnames(data_clean), value=TRUE),
             newvar = "se_negaff",
             newlab = "Higher scores indicate greater confidence to not smoke under negative affect",
             newchoices = "Mean of se1_3, se1_6, and se1_9",
             prop_nonmiss = 0.7,
             position="se1_9") %>%
  # habitual/craving scale
  scale_mean(oldvars = grep("se1_[2,5,8]$", colnames(data_clean), value=TRUE),
             newvar = "se_habit",
             newlab = "Higher scores indicate greater confidence to not smoke habitually",
             newchoices = "Mean of se1_2, se1_5, and se1_8",
             prop_nonmiss = 0.7,
             position="se1_9") %>%
  # social/positive scale
  scale_mean(oldvars = grep("se1_[1,4,7]$", colnames(data_clean), value=TRUE),
             newvar = "se_social",
             newlab = "Higher scores indicate greater confidence to not smoke in social situations",
             newchoices = "Mean of se1_1, se1_4, and se1_7",
             prop_nonmiss = 0.7,
             position="se1_9")

### 7.  TSAM ----

data_clean7 <- data_clean6 %>%
  # no recoding
  scale_mean(oldvars = grep("tsam1_[1-5]$", colnames(data_clean), value=TRUE),
             newvar = "TSAM_Total",
             newlab = "Higher scores indicate greater commitment to abstinence",
             newchoices = "Mean of tsam1_1-tsam1_5",
             prop_nonmiss = 0.7)

### 8.  PHQA ----
# phqa1_1 (Do you ever drink alcohol?) if yes, phqa2_1-phqa6_1;

data_clean8 <- data_clean7 %>%
  # phqa1_1 not included in scoring
  mutate(phqa_problem = case_when(
    if_any(grep("phqa[2-6]_1", colnames(data_clean), value=TRUE), ~ .x == 1) ~ 1,
    if_all(grep("phqa[2-6]_1", colnames(data_clean), value=TRUE), ~ .x == 0) ~ 0
  ),
  .after = phqa6_1)

# add phqa_problem to codebook
update_codebook(oldvars = grep("phqa[2-6]_1", colnames(data_clean), value=TRUE),
                newvar = "phqa_problem",
                newlab = "Whether a subject has a drinking problem",
                newchoices = "0 - No (phqa1_1 = \"1\" and all of phqa2_1-phqa6_1 = \"0\") | 1 - Yes (phqa1_1 = \"1\" and at least one of phqa2_1-phqa6_1 = \"1\")",
                position = "phqa6_1")

### 9.  ARDB ----
# NIAAA defines heavy drinking as follows:
  ## For men, consuming more than 4 drinks on any day or more than 14 drinks per week
  ## For women, consuming more than 3 drinks on any day or more than 7 drinks per week
# Gender: 1 = male, 2 = female

if(F) {
  # don't need to worry about dses1a_1 = 3 or 4 when coding heavy drinking
  table(data_clean$dses1a_1) # only Male and Female (no Intersex or Decline to Answer)
  
  lapply(grep("ardb[0-9]_[0-9]", colnames(data_clean), value=TRUE), 
         function(x) table(data_clean[,x]))
}

data_clean9_temp <- data_clean8 %>%
  # rename first two questions to match BF
  rename(binge_drink_male = ardb1_1_male,
         binge_drink_female = ardb1_1_female) %>%
  # number of drinks/week
  scale_sum(oldvars = grep("ardb3_[1-7]", colnames(data_clean), value=TRUE),
            newvar = "num_drinks",
            newlab = "Number of drinks consumed per week",
            newchoices = "Sum of ardb3_1-ardb3_7") %>%
  # ARDB survey uses dses1a_1 for branching logic with Male/Female
  mutate(
    # scale heavy drinking weekly
    ardb_heavy = case_when(
      dses1a_1 == 1 & num_drinks >= 14 ~ 1,
      dses1a_1 == 1 & num_drinks < 14 ~ 0,
      dses1a_1 == 2 & num_drinks >= 7 ~ 1,
      dses1a_1 == 2 & num_drinks < 7 ~ 0
      ),
    # scale heavy drinking on Mondays
    ardb_mon = case_when(
      dses1a_1 == 1 & ardb3_1 >= 4 ~ 1,
      dses1a_1 == 1 & ardb3_1 < 4 ~ 0,
      dses1a_1 == 2 & ardb3_1 >= 3 ~ 1,
      dses1a_1 == 2 & ardb3_1 < 3 ~ 0
    ),
    # scale heavy drinking on Tuesdays
    ardb_tue = case_when(
      dses1a_1 == 1 & ardb3_2 >= 4 ~ 1,
      dses1a_1 == 1 & ardb3_2 < 4 ~ 0,
      dses1a_1 == 2 & ardb3_2 >= 3 ~ 1,
      dses1a_1 == 2 & ardb3_2 < 3 ~ 0
    ),
    # scale heavy drinking on Wednesdays
    ardb_wed = case_when(
      dses1a_1 == 1 & ardb3_3 >= 4 ~ 1,
      dses1a_1 == 1 & ardb3_3 < 4 ~ 0,
      dses1a_1 == 2 & ardb3_3 >= 3 ~ 1,
      dses1a_1 == 2 & ardb3_3 < 3 ~ 0
    ),
    # scale heavy drinking on Thursdays
    ardb_thu = case_when(
      dses1a_1 == 1 & ardb3_4 >= 4 ~ 1,
      dses1a_1 == 1 & ardb3_4 < 4 ~ 0,
      dses1a_1 == 2 & ardb3_4 >= 3 ~ 1,
      dses1a_1 == 2 & ardb3_4 < 3 ~ 0
    ),
    # scale heavy drinking on Fridays
    ardb_fri = case_when(
      dses1a_1 == 1 & ardb3_5 >= 4 ~ 1,
      dses1a_1 == 1 & ardb3_5 < 4 ~ 0,
      dses1a_1 == 2 & ardb3_5 >= 3 ~ 1,
      dses1a_1 == 2 & ardb3_5 < 3 ~ 0
    ),
    # scale heavy drinking on Saturdays
    ardb_sat = case_when(
      dses1a_1 == 1 & ardb3_6 >= 4 ~ 1,
      dses1a_1 == 1 & ardb3_6 < 4 ~ 0,
      dses1a_1 == 2 & ardb3_6 >= 3 ~ 1,
      dses1a_1 == 2 & ardb3_6 < 3 ~ 0
    ),
    # scale heavy drinking on Sundays
    ardb_sun = case_when(
      dses1a_1 == 1 & ardb3_7 >= 4 ~ 1,
      dses1a_1 == 1 & ardb3_7 < 4 ~ 0,
      dses1a_1 == 2 & ardb3_7 >= 3 ~ 1,
      dses1a_1 == 2 & ardb3_7 < 3 ~ 0
    ),
    .after = num_drinks)

# update variable name change in codebook
meta_clean <- meta_clean %>% 
  mutate(field_name = case_match(
    field_name,
    "ardb1_1_male" ~ "binge_drink_male",
    "ardb1_1_female" ~ "binge_drink_female",
    .default = field_name
  ))

# add new variables to codebook
update_codebook(oldvars = c("dses1a_1", "num_drinks"),
                newvar = "ardb_heavy",
                newlab = "Indicates whether subject meets criteria for heavy drinking on an average week",
                position = "num_drinks")

days <- rev(c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))
days_vars <- rev(grep('ardb_[a-z]{3}$', colnames(data_clean9_temp), value=TRUE))
for (i in 1:length(days)) {
  update_codebook(oldvars = c("dses1a_1", grep("ardb3_[0-9]", colnames(data_clean), value=TRUE)[i]),
                  newvar = days_vars[i],
                  newlab = sprintf("Indicates whether subject meets criteria for heavy drinking on an average %s", days[i]),
                  position = "ardb_heavy")
  }

# scale number of heavy drinking days/week
data_clean9 <- data_clean9_temp %>%
  scale_sum(oldvars = grep('ardb_[a-z]{3}$', colnames(data_clean9_temp), value=TRUE),
            newvar = "ardb_heavy_days",
            newlab = "Number of days/week that classify as heavy drinking",
            newchoices = "Sum of ardb_mon-ardb_sun")

### 10. NIDA ----
# one subscale per drug
# recode as follows:
## 1. Frequency of use (`drug`_1):
  ## Never - 0
  ## Once or Twice - 2
  ## Monthly - 3
  ## Weekly - 4
  ## Daily or Almost Daily - 6
## 2. Frequency of desire to use (`drug`_2):
  ## Never - 0
  ## Once or Twice - 3
  ## Monthly - 4
  ## Weekly - 5
  ## Daily or Almost Daily - 6
## 3. Frequency of use causing problems (`drug`_3):
  ## Never - 0
  ## Once or Twice - 4
  ## Monthly - 5
  ## Weekly - 6
  ## Daily or Almost Daily - 7
## 4. Frequency of interference with life due to drug (`drug`_4):
  ## Never - 0
  ## Once or Twice - 5
  ## Monthly - 6
  ## Weekly - 7
  ## Daily or Almost Daily - 8
# 5. Frequency of others expressing concern (nida6_1-nida6_10):
  ## No, never - 0
  ## Yes, but not in the past 3 months - 3
  ## Yes, in the past 3 months - 6
# 6. Frequency of trying and failing to cut down on or stop using drug (nida7_1-nida7-10):
  ## No, never - 0
  ## Yes, but not in the past 3 months - 3
  ## Yes, in the past 3 months - 6

invisible(lapply(grep("nida[6,7]_[0-9]+", colnames(data_clean), value=TRUE), function(x) recode_scale(x, c(0, 3, 6))))

# recode nida6_1-nida7_10
data_clean10 <- data_clean9 %>%
  mutate(across(grep("nida[6,7]_[0-9]+", colnames(data_clean), value=TRUE),
           ~ case_match(
             .x,
             1 ~ 0,
             2 ~ 3,
             3 ~ 6
             )
           )
  )

drugs <- rev(c("marijuana", "cocaine", "stimulants", "methamphetamine", "inhalants", "sedatives", "hallucinogens", "streetopioids", "prescriptopioids", "other"))
for (i in 1:length(drugs)) {
  recode_scale(sprintf("%s_1", drugs[i]), c(0, 2, 3, 4, 6))
  recode_scale(sprintf("%s_2", drugs[i]), c(0, 3, 4, 5, 6))
  recode_scale(sprintf("%s_3", drugs[i]), c(0, 4, 5, 6, 7))
  recode_scale(sprintf("%s_4", drugs[i]), c(0, 5, 6, 7, 8))
  
  data_clean10 <- data_clean10 %>%
    # recode drug-specific variables
    mutate(across(sprintf("%s_1", drugs[i]),
                  ~ case_match(
                    .x,
                    1 ~ 0,
                    2 ~ 2,
                    3 ~ 3,
                    4 ~ 4,
                    5 ~ 6
                  )),
           across(sprintf("%s_2", drugs[i]),
                  ~ case_match(
                    .x,
                    1 ~ 0,
                    2 ~ 3,
                    3 ~ 4,
                    4 ~ 5,
                    5 ~ 6
                  )),
           across(sprintf("%s_3", drugs[i]),
                  ~ case_match(
                    .x,
                    1 ~ 0,
                    2 ~ 4,
                    3 ~ 5,
                    4 ~ 6,
                    5 ~ 7
                  )),
           across(sprintf("%s_4", drugs[i]),
                  ~ case_match(
                    .x,
                    1 ~ 0,
                    2 ~ 5,
                    3 ~ 6,
                    4 ~ 7,
                    5 ~ 8
                  ))) %>%
    # sum all scores for each drug
    scale_sum_nida(oldvars = c(grep(sprintf("^%s_[1-4]", drugs[i]), colnames(data_clean), value=TRUE),
                               grep(sprintf("nida[6,7]_%s$", as.character(11 - i)), colnames(data_clean), value=TRUE)),
                   newvar = sprintf("%s_SI", drugs[i]),
                   newlab = "Higher scores indicate higher substance involvement (SI)",
                   newchoices = sprintf("sum of all %s-related NIDA variables", drugs[i]),
                   position = "nida_other_3")
  
  data_clean10 <- data_clean10 %>%
    # determine risk level based on SI score (`drug`_risk)
    mutate(!! sprintf("%s_risk", drugs[i]) := 
             cut(data_clean10[[sprintf("%s_SI", drugs[i])]],
                 breaks=c(-Inf, 3, 26, Inf),
                 labels=c("Lower Risk", "Moderate Risk", "High Risk")),
           .after = sprintf("%s_SI", drugs[i]))
  
  # add risk level to codebook
  update_codebook(oldvars = sprintf("%s_SI", drugs[i]),
                  newvar = sprintf("%s_risk", drugs[i]),
                  newlab = "Level of risk associated with Substance Involvement (SI) Score",
                  newchoices = "Lower Risk | Moderate Risk | High Risk",
                  position = sprintf("%s_SI", drugs[i]))
  meta_clean <- meta_clean %>%
    rows_update(tibble(field_name = sprintf("%s_risk", drugs[i]), field_type = "radio"), by="field_name")
}

### 11. PP ----
# single question and no recoding needed

data_clean11 <- data_clean10

### 12. FS1 ----
# the scale says to create the sum, but mean was created instead due to missingness

data_clean12 <- data_clean11 %>%
  # no recoding
  scale_mean(oldvars = grep("fs[1-2]_[1-7]$", colnames(data_clean), value=TRUE),
             newvar = "FinancialStrain",
             newlab = "Higher scores indicate greater financial strain",
             newchoices = "Mean of fs1_1-fs1_7 and fs2_1",
             prop_nonmiss = 0.7)

### 13. FS2 ----

invisible(lapply(grep("fs_[1,2]$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 3:1)))

data_clean13 <- data_clean12 %>%
  # reverse code both questions
  mutate(across(grep("fs_[1,2]$", colnames(data_clean), value=TRUE),
            ~ 4 - .x)) %>%
  scale_mean(oldvars = grep("fs_[1,2]$", colnames(data_clean), value=TRUE),
             newvar = "food_security_mean",
             newlab = "Higher scores indicate higher food insecurity",
             newchoices = "Mean of fs_1 and fs_2",
             prop_nonmiss = 0.7)

### 14. SSSL ----

data_clean14 <- data_clean13 %>%
  # no recoding
  scale_mean(oldvars = grep("sssl[1-4]_1$", colnames(data_clean), value=TRUE),
             newvar = "SSSladders",
             newlab = "Higher scores indicate lower socioeconomic status",
             newchoices = "Mean of sssl1_1 and sssl4_1", 
             prop_nonmiss = 0.7)

### 15. ND ----

invisible(lapply(c("nd_1", "nd_2", "nd_4", "nd_5", "nd_7", "nd_8"), function(x) recode_scale(x, 4:1)))

data_clean15 <- data_clean14 %>%
  # reverse code
  mutate(across(c("nd_1", "nd_2", "nd_4", "nd_5", "nd_7", "nd_8"),
                ~ 5 - .x)) %>%
  scale_mean(oldvars = grep("nd_[0-9]$", colnames(data_clean), value=TRUE),
             newvar = "nd_mean",
             newlab = "Higher scores indicate higher neighborhood disorganization",
             newchoices = "Mean of nd_1-nd_8",
             prop_nonmiss = 0.7)

### 16. ACE ----
# subscales
  ## Household Dysfunction:    ace_1-ace_5
  ## Emotional/Physical Abuse: ace_6-ace_8
  ## Sexual Abuse:             ace_9-ace_11

invisible(lapply(grep("ace_[1-4]$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, c(1, 0, 7, 9))))
recode_scale("ace_5", c(1, 0, 8, 7, 9))
invisible(lapply(grep("ace_(([6-9])|(1[0-1]))$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, c(0, 1, 2, 7, 9))))

data_clean16_temp <- data_clean15 %>%
  # recode
  mutate(
    # questions 1-5: change "No" from 2 to 0
    across(grep("ace_[1-5]$", colnames(data_clean), value=TRUE),
            ~ case_match(
              .x,
              2 ~ 0,
              .default = .x
              )
           ),
    # questions 6-10: Make scale 0-2 instead of 1-3
    across(grep("ace_(([6-9])|(1[0-1]))$", colnames(data_clean), value=TRUE),
           ~ case_match(
             .x,
           1 ~ 0,
           2 ~ 1,
           3 ~ 2,
           .default = .x
           ))
    ) %>%
  # create temporary columns for subscales
  mutate(across(grep("ace_[0-9]+", colnames(data_clean), value=TRUE),
                # only take mean of 0-1 or 0-2 scales; treat 7-9 as NA
                ~ if_else(.x %in% c(0,1,2), .x, NA),
                .names = "{.col}_temp"),
         .after = "ace_11")

data_clean16 <- data_clean16_temp %>%
  # scale overall
  scale_mean(oldvars = grep("ace_[0-9]+_temp", colnames(data_clean16_temp), value=TRUE),
             newvar = "ace_overall_mean",
             newlab = "Higher scores indicate higher childhood adverse experiences overall",
             newchoices = "Mean of the 0-1 and 0-2 scales of ace_1-ace_11",
             prop_nonmiss = 0.7,
             position = "ace_11") %>%
  # scale sexual abuse
  scale_mean(oldvars = c("ace_9_temp", "ace_10_temp", "ace_11_temp"),
             newvar = "ace_sa_mean",
             newlab = "Higher scores indicate higher childhood sexual abuse",
             newchoices = "Mean of the ace_9-ace_11 0-2 scale",
             prop_nonmiss = 0.7,
             position = "ace_11") %>%
  # scale emotional/physical abuse
  scale_mean(oldvars = c("ace_6_temp", "ace_7_temp", "ace_8_temp"),
             newvar = "ace_epa_mean",
             newlab = "Higher scores indicate higher childhood emotional/physical abuse",
             newchoices = "Mean of the ace_6-ace_8 0-2 scale",
             prop_nonmiss = 0.7,
             position = "ace_11") %>%
  # scale household dysfunction
  scale_mean(oldvars = grep("ace_[1-5]_temp", colnames(data_clean16_temp), value=TRUE),
             newvar = "ace_hd_mean",
             newlab = "Higher scores indicate higher childhood household dysfunction",
             newchoices = "Mean of the ace_1-ace_5 0-1 scale",
             prop_nonmiss = 0.7,
             position = "ace_11") %>%
  # sum subscale scores
  scale_sum(oldvars = c("ace_hd_mean", "ace_epa_mean", "ace_sa_mean"),
            newvar = "ace_total",
            newlab = "Higher scores indicate higher childhood adverse experiences overall",
            newchoices = "Sum of ace_hd_mean, ace_epa_mean, and ace_sa_mean") %>%
  # remove temporary columns
  select(-grep("ace_[0-9]+_temp", colnames(data_clean16_temp), value=TRUE))

### 17. MLD ----
# count of acute discrimination

if(F) {
  # check for anomalous answers
  lapply(grep("mld[0-9]c", colnames(data_clean), value=TRUE),
         function(x) table(data_clean[,x]))
}

data_clean17 <- data_clean16 %>%
  # no recoding
  scale_sum(oldvars = grep("mld[0-9]_1$", colnames(data_clean), value=TRUE),
            newvar = "mld_total",
            newlab = "Higher scores indicate having experienced more types of acute discrimination",
            newchoices = "Sum of mld1_1-mld9-1",
            position="mld9c_1")

### 18. EDS ----
# NOTE: not equivalent to the MED survey from BF/OT

invisible(lapply(grep("eds_[0-9]$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 5:0)))

data_clean18 <- data_clean17 %>%
  # recode all
  mutate(across(grep("eds_[0-9]$", colnames(data_clean), value=TRUE),
                ~ 6 - .x)) %>%
  scale_sum(oldvars = grep("eds_[0-9]$", colnames(data_clean), value=TRUE),
            newvar = "eds_sum",
            newlab = "Higher scores indicate higher everyday discrimination",
            newchoices = "Sum of eds_1-eds_9")

### 19. FFM ----
# subscales
  # Non-Judging:           ffm1_1-ffm1_8
  # Acting with Awareness: ffm2_1-ffm2_8

invisible(lapply(grep("ffm[1-2]_[1-8]", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 5:1)))

data_clean19 <- data_clean18 %>%
  # reverse code all
  mutate(across(grep("ffm[1-2]_[1-8]", colnames(data_clean), value=TRUE),
                ~ 6 - .x)) %>%
  scale_mean(oldvars = grep("ffm[1-2]_[1-8]", colnames(data_clean), value=TRUE),
             newvar = "FFMQ_total",
             newlab = "Higher scores indicate higher overall mindfulness",
             newchoices = "Mean of ffm1_1-ffm1_8 and ffm2_1-ffm2_8",
             prop_nonmiss = 0.7,
             position = "ffm2_8") %>%
  # scale act with awareness
  scale_mean(oldvars = grep("ffm2_[1-8]", colnames(data_clean), value=TRUE),
             newvar = "ffmq_aware",
             newlab = "Higher scores indicate greater acting with awareness",
             newchoices = "Mean of ffm2_1-ffm2_8",
             prop_nonmiss = 0.7,
             position = "ffm2_8") %>%
  # scale non-judging
  scale_mean(oldvars = grep("ffm1_[1-8]", colnames(data_clean), value=TRUE),
             newvar = "ffmq_nonjudge",
             newlab = "Higher scores indicate greater non-judging of inner experiences",
             newchoices = "Mean of ffm1_1-ffm1_8",
             prop_nonmiss = 0.7,
             position = "ffm2_8")

### 20. MAAS ----

data_clean20 <- data_clean19 %>%
  # no recoding
  scale_mean(oldvars = grep("maas1_[1-5]", colnames(data_clean), value=TRUE),
             newvar = "maas_total",
             newlab = "Higher scores indicate higher mindfulness",
             newchoices = "Mean of maas1_1-maas1_5",
             prop_nonmiss = 0.7)

### 21. SP ----
# subscales
  ## Shift:   sp1_1-sp1_4 and sp2_1-sp2_3
  ## Persist: sp8_1-sp8_5

invisible(lapply(grep("sp8_[1,2,5]$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 4:1)))

data_clean21 <- data_clean20 %>%
  # reverse code questions 8, 9, and 12
  mutate(across(grep("sp8_[1,2,5]$", colnames(data_clean), value=TRUE), 
         ~ 5 - .x)) %>%
  # scale persist
  scale_mean(oldvars = grep("sp8_[0-9]$", colnames(data_clean), value=TRUE),
             newvar = "persist",
             newlab = "Higher scores indicate lower focus on the future",
             newchoices = "Mean of sp8_1-sp8_5",
             prop_nonmiss = 0.7,
             position = "sp8_5") %>%
  # scale shift
  scale_mean(oldvars = grep("sp[1-2]_[0-9]$", colnames(data_clean), value=TRUE),
             newvar = "shift",
             newlab = "Higher scores indicate lower regulation",
             newchoices = "Mean of sp1_1-sp1_4 and sp2_1-sp2_3",
             prop_nonmiss = 0.7,
             position = "sp8_5")

### 22. BIS ----

invisible(lapply(grep("bis1_[1,4,5,6]$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 4:1)))

data_clean22 <- data_clean21 %>%
  # reverse code questions 1, 4, 5, and 6
  mutate(across(grep("bis1_[1,4,5,6]$", colnames(data_clean), value=TRUE),
                ~ 5 - .x)) %>%
  scale_mean(oldvars = grep("bis1_[1-8]$", colnames(data_clean), value=TRUE),
             newvar = "bis_total",
             newlab = "Higher scores indicate higher impulsiveness",
             newchoices = "Mean of bis1_1-bis1_8",
             prop_nonmiss = 0.7)

### 23. SV ----

data_clean23 <- data_clean22 %>%
  # no recoding
  scale_mean(oldvars = grep("sv_[0-9]+$", colnames(data_clean), value=TRUE),
             newvar = "sv_mean",
             newlab = "Higher scores indicate higher social vigilance",
             newchoices = "Mean of sv_1-sv_10",
             prop_nonmiss = 0.7)

### 24. AI ----
# subscales
  ## Social Concerns:    aci_1, aci_6, aci_9, asi_11, asi_13, and asi_17
  ## Cognitive Concerns: aci_2, aci_5, asi_10, asi_14, asi_16, and asi_18
  ## Physical Concerns:  aci_3, aci_4, aci_7, aci_8, asi_12, and asi_15

data_clean24 <- data_clean23 %>%
  # no reverse coding
  # scale overall
  scale_mean(oldvars = grep("a[cs]i_[0-9]+$", colnames(data_clean), value=TRUE),
             newvar = "ai_overall",
             newlab = "Higher scores indicate higher anxiety",
             newchoices = "Mean of aci_1-aci_9 and asi_10-asi_18",
             prop_nonmiss = 0.7,
             position = "asi_18") %>%
  # scale physical concerns
  scale_mean(oldvars = grep("(aci_[3,4,7,8])|(asi_1[2,5])", colnames(data_clean), value=TRUE),
             newvar = "ai_physical",
             newlab = "Higher scores indicate higher anxiety over physical concerns",
             newchoices = "Mean of aci_3, aci_4, aci_7, aci_8, asi_12, and asi_15",
             prop_nonmiss = 0.7,
             position = "asi_18") %>%
  # scale cognitive concerns
  scale_mean(oldvars = grep("(aci_[2,5])|(asi_1[0,4,6,8])", colnames(data_clean), value=TRUE),
             newvar = "ai_cognitive",
             newlab = "Higher scores indicate higher anxiety over cognitive concerns",
             newchoices = "Mean of aci_2, aci_5, asi_10, asi_14, asi_16, and asi_18",
             prop_nonmiss = 0.7,
             position = "asi_18") %>%
  # scale social concerns
  scale_mean(oldvars = grep("(aci_[1,6,9])|(asi_1[1,3,7])", colnames(data_clean), value=TRUE),
             newvar = "ai_social",
             newlab = "Higher scores indicate higher anxiety over social concerns",
             newchoices = "Mean of aci_1, aci_6, aci_9, asi_11, asi_13, and asi_17",
             prop_nonmiss = 0.7,
             position = "asi_18")

### 25. DECENTER ----

invisible(lapply(grep("decenter_[0-9]+$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 5:1)))

data_clean25 <- data_clean24 %>%
  # reverse code all
  mutate(across(grep("decenter_[0-9]+$", colnames(data_clean), value=TRUE),
                ~ 6 - .x)) %>%
  scale_mean(oldvars = grep("decenter_[0-9]+$", colnames(data_clean), value=TRUE),
             newvar = "decenter_mean",
             newlab = "Higher scores indicate greater decentering capabilities",
             newchoices = "Mean of decenter_1-decenter_11",
             prop_nonmiss = 0.7)

### 26. DTS ----
# subscales
  ## Tolerance:  dts1_1, dts1_3, and dts1_5
  ## Absorption: dts1_2, dts1_4, and dts2_7
  ## Appraisal:  dts1_6, dts1_7, and dts2_1-dts2_4
  ## Regulation: dts1_8, dts_2_5, and dts2_6

recode_scale("dts1_6", 5:1)

data_clean26 <- data_clean25 %>%
  # reverse code question 6
  mutate(dts1_6 = 6 - dts1_6) %>%
  # scale overall
  scale_mean(oldvars = grep("dts[1-2]_[1-8]$", colnames(data_clean), value=TRUE),
             newvar = "dts_total",
             newlab = "Higher scores indicate higher distress tolerance overall",
             newchoices = "mean of dts1_1-dts1_8 and dts2_1-dts2_7",
             prop_nonmiss = 0.7) %>%
  # scale regulation
  scale_mean(oldvars = grep("dts((1_8)|(2_[5-6]))$", colnames(data_clean), value=TRUE),
             newvar = "dts_regulation",
             newlab = "Higher scores indicate higher distress regulation",
             newchoices = "Mean of dts1_8, dts_2_5, and dts2_6",
             prop_nonmiss = 0.7) %>%
  # scale appraisal
  scale_mean(oldvars = grep("dts((1_[6,7])|(2_[1-4]))$", colnames(data_clean), value=TRUE),
             newvar = "dts_appraisal",
             newlab = "Higher scores indicate higher distress appraisal",
             newchoices = "Mean of dts1_6, dts1_7, and dts2_1-dts2_4",
             prop_nonmiss = 0.7) %>%
  # scale absorption
  scale_mean(oldvars = grep("dts((1_[2,4])|(2_7))$", colnames(data_clean), value=TRUE),
             newvar = "dts_absorption",
             newlab = "Higher scores indicate higher distress absorption",
             newchoices = "Mean of dts1_2, dts1_4, and dts2_7",
             prop_nonmiss = 0.7) %>%
  # scale tolerance
  scale_mean(oldvars = grep("dts1_[1,3,5]$", colnames(data_clean), value=TRUE),
             newvar = "dts_tolerance",
             newlab = "Higher scores indicate higher distress tolerance",
             newchoices = "Mean of dts1_1, dts1_3, and dts1_5",
             prop_nonmiss = 0.7)

### 27. PHQD ----
# Recode response options (0 to 3) to match Pressler et al. (2011) where sum scores:
  ## < 5 -> no depressive symptoms
  ## 5-9 -> mild 
  ## 10+ moderate depressive symptoms
  ## 15+ high likelihood of depression with medical attention requirement

invisible(lapply(grep("phqd1_[1-8]$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 0:3)))

data_clean27 <- data_clean26 %>%
  # recode all
  mutate(across(grep("phqd1_[1-8]$", colnames(data_clean), value=TRUE),
                ~ .x - 1)) %>%
  scale_sum(oldvars = grep("phqd1_[1-8]$", colnames(data_clean), value=TRUE),
            newvar = "PHQ_depresh",
            newlab = "Higher scores indicate higher levels of depression, with +15 indicating high likelihood of depression with medical attention requirement",
            newchoices = "Sum of phqd1_1-phqd1_8")

### 28. GAD ----
# Item 8 (gad8_1) is not included to compute the scale score. It was used for validation in the original study
# Recode response options from (1-4) to (0 to 3) to get cut-off scores as in Spitzer et al (2006):
  ## 5+: mild levels of anxiety
  ## 10+: moderate levels of anxiety, cut-off point for identifying GAD cases
  ## 15+: severe levels of anxiety

invisible(lapply(grep("gad[1,8]_[1-7]$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 0:3)))

data_clean28 <- data_clean27 %>%
  # recode all variables
  mutate(across(grep("gad[1,8]_[1-7]$", colnames(data_clean), value=TRUE),
                ~ .x - 1)) %>%
  # gad8_1 left out intentionally
  scale_sum(oldvars = grep("gad1_[1-7]$", colnames(data_clean), value=TRUE),
            newvar = "gad_anx",
            newlab = "Higher scores indicate higher anxiety (>=10 indicates GAD)",
            newchoices = "Sum of gad1_1-gad1_7")

### 29. SHAPS ----

invisible(lapply(grep("shaps[1-2]_[1-7]$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 4:1)))

data_clean29 <- data_clean28 %>%
  # reverse code all
  mutate(across(grep("shaps[1-2]_[1-7]$", colnames(data_clean), value=TRUE),
                ~ 5 - .x)) %>%
  scale_mean(oldvars = grep("shaps[1-2]_[1-7]$", colnames(data_clean), value=TRUE),
             newvar = "shaps",
             newlab = "Higher scores indicate less experience with pleasure",
             newchoices = "Mean of shaps1_1-shaps1_7 and shaps2_1-shaps2_7",
             prop_nonmiss = 0.7)

### 30. PS ----

invisible(lapply(c("ps1_2", "ps1_3"), function(x) recode_scale(x, 5:1)))

data_clean30 <- data_clean29 %>%
  # reverse code questions 2 and 3
  mutate(across(c("ps1_2", "ps1_3"),
                ~ 6 - .x)) %>%
  scale_mean(oldvars = grep("^ps1_[1-4]$", colnames(data_clean), value=TRUE),
             newvar = "perceived_stress",
             newlab = "Higher scores indicate higher perceived stress",
             newchoices = "Mean of ps1_1-ps1_4",
             prop_nonmiss = 0.7)

### 31. GQ ----

invisible(lapply(c("gq1_3", "gq1_6"), function(x) recode_scale(x, 7:1)))

data_clean31 <- data_clean30 %>%
  # reverse code questions 3 and 6
  mutate(across(c("gq1_3", "gq1_6"),
                ~ 8 - .x)) %>%
  scale_mean(oldvars = grep("gq1_[1-6]$", colnames(data_clean), value=TRUE),
             newvar = "gratitude",
             newlab = "Higher scores indicate higher gratitude",
             newchoices = "Mean of gq1_1-gq1_6",
             prop_nonmiss = 0.7)

### 32. MDES ----
# subscales
  ## Positive Emotions: mdes_1, mdes_4, mdes_8, mdes_11-mdes_16, and mdes_19
  ## Negative Emotions: mdes_2, mdes_3, mdes_5-mdes_7, mdes_9, mdes_10, mdes_17, mdes_18, and mdes_20

data_clean32 <- data_clean31 %>%
  # no recoding
  # scale positive emotions
  scale_mean(oldvars = c("mdes_1", "mdes_4", "mdes_8", "mdes_11", "mdes_12", "mdes_13", "mdes_14", "mdes_15", "mdes_16", "mdes_19"),
             newvar = "mdes_pos_mean",
             newlab = "Higher scores indicate more intense positive emotions",
             newchoices = "Mean of mdes_1, mdes_4, mdes_8, mdes_11-mdes_16, and mdes_19",
             prop_nonmiss = 0.7,
             position = "mdes_20") %>%
  # scale negative emotions
  scale_mean(oldvars = c("mdes_2", "mdes_3", "mdes_5", "mdes_6", "mdes_7", "mdes_9", "mdes_10", "mdes_17", "mdes_18", "mdes_20"),
             newvar = "mdes_neg_mean",
             newlab = "Higher scores indicate more intense negative emotions",
             newchoices = "Mean of mdes_2, mdes_3, mdes_5-mdes_7, mdes_9, mdes_10, mdes_17, mdes_18, and mdes_20",
             prop_nonmiss = 0.7,
             position = "mdes_20")

### 33. ISEL ----
# subscales:
  ## Belonging: isel1_1, isel1_5, isel2_1, and isel2_3
  ## Appraisal: isel1_2, isel1_4, isel1_6, and isel2_5
  ## Tangible:  isel1_3, isel2_2, isel2_4, and isel2_6

invisible(lapply(c("isel1_1", "isel1_2", "isel2_1", "isel2_2", "isel2_5", "isel2_6"), function(x) recode_scale(x, 4:1)))

data_clean33 <- data_clean32 %>%
  # reverse code questions 1, 2, 7, 8, 11, and 12
  mutate(across(c("isel1_1", "isel1_2", "isel2_1", "isel2_2", "isel2_5", "isel2_6"),
                ~ 5 - .x)) %>%
  # scale overall
  scale_mean(oldvars = grep("isel[1,2]_[1-6]$", colnames(data_clean), value=TRUE),
             newvar = "isel_total",
             newlab = "Higher scores indicate greater interpersonal support",
             newchoices = "Mean of isel1_1-isel1_6 and isel2_1-isel2_6",
             prop_nonmiss = 0.7,
             position="isel2_6") %>%
  # scale tangible
  scale_mean(oldvars = grep("isel((1_3)|(2_[2,4,6]))$", colnames(data_clean), value=TRUE),
             newvar = "isel_tangible",
             newlab = "Higher scores indicate greater tangible interpersonal support",
             newchoices = "Mean of isel1_3, isel2_2, isel2_4, and isel2_6",
             prop_nonmiss = 0.7,
             position="isel2_6") %>%
  # scale appraisal
  scale_mean(oldvars = grep("isel((1_[2,4,6])|(2_5))$", colnames(data_clean), value=TRUE),
             newvar = "isel_appraisal",
             newlab = "Higher scores indicate higher positive appraisal of interpersonal support",
             newchoices = "Mean of isel1_2, isel1_4, isel1_6, and isel2_5",
             prop_nonmiss = 0.7,
             position="isel2_6") %>%
  # scale belonging
  scale_mean(oldvars = grep("isel((1_[1,5])|(2_[1,3]))$", colnames(data_clean), value=TRUE),
             newvar = "isel_belonging",
             newlab = "Higher scores indicate a greater sense of belonging",
             newchoices = "Mean of isel1_1, isel1_5, isel2_1, and isel2_3",
             prop_nonmiss = 0.7,
             position="isel2_6")

### 34. SNI ----
# subscales
  ## number of high-contact roles
  ## number of people in social network
  ## number of embedded networks

data_clean34 <- data_clean33 %>%
  rowwise() %>%
  # count number of social network sources
  mutate(sni_count = if_else(social_network_complete == "Complete",
                             sum(c(
                               # spouse/partner
                               sni_1 == 1,
                               # children
                               sni_2a > 0,
                               # parents
                               sni_3a > 0,
                               # in-laws/partner's parents
                               sni_4a %in% c(1, 2, 3),
                               # other relatives
                               sni_5a > 0,
                               # friends
                               sni_6a > 0,
                               # religion
                               sni_7a > 0,
                               # school
                               sni_8a > 0,
                               # work
                               (sni_9a + sni_9b) > 0,
                               # neighbors
                               sni_10 > 0,
                               # volunteering
                               sni_11a > 0,
                               # other
                               sni_12 == 2
                               ), 
                               na.rm=TRUE),
                             NA),
         .after = sni_12a) %>%
  # sum number of people in social network
  mutate(sni_people = if_else(social_network_complete == "Complete",
                              sum(c(
                                # response = number of people
                                sni_2a, sni_5a, sni_6a, sni_7a, sni_8a, sni_9a, sni_9b, sni_10, sni_11a,
                                # coded responses
                                case_match(sni_1,
                                           1 ~ 1,
                                           .default = 0),
                                case_match(sni_3a,
                                           c(1, 2) ~ 1,
                                           3 ~ 2,
                                           .default = 0),
                                case_match(sni_4a,
                                           c(1, 2) ~ 1,
                                           3 ~ 2,
                                           .default = 0)), 
                                  na.rm=TRUE),
                              NA),
         .after = sni_count) %>%
  # number of embedded networks
  mutate(sni_active = if_else(social_network_complete == "Complete",
                              sum(c(
                                # family
                                (sum(c(sni_1 == 1, sni_2a > 0, sni_3a > 0, sni_4a %in% c(1, 2, 3), sni_5a > 0), na.rm=TRUE) >= 3) & 
                                  (sum(c(case_match(sni_1, 1 ~ 1, .default = 0), sni_2a, case_match(sni_3a, c(1, 2) ~ 1, 3 ~ 2,  .default = 0), case_match(sni_4a, c(1, 2) ~ 1, 3 ~ 2, .default = 0), sni_5a), na.rm=TRUE) >= 4),
                                # friends
                                sni_6a >= 4,
                                # religion
                                sni_7a >= 4,
                                # school
                                sni_8a >= 4,
                                # work
                                (sni_9a + sni_9b) >= 4,
                                # neighbors
                                sni_10 >= 4,
                                # volunteering
                                sni_11a >= 4
                              ), na.rm=TRUE),
                              NA),
         .after = sni_people) %>%
  ungroup()

update_codebook(oldvars = NA,
                newvar = "sni_count",
                newlab = "Number of high-contact roles in a subject's social network (network diversity)",
                position = "sni_12a")

update_codebook(oldvars = NA,
                newvar = "sni_people",
                newlab = "Number of people in a subject's social network",
                position = "sni_count")

update_codebook(oldvars = NA,
                newvar = "sni_active",
                newlab = "Number of active, embedded roles in a subject's social network",
                position = "sni_people")

### 35. UCLALS ----
# original source suggests using a 0-3 scale rather than 1-4

invisible(lapply(grep("uclals_[1-6]$", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 0:3)))

data_clean35 <- data_clean34 %>%
  # recode
  mutate(across(grep("uclals_[1-6]$", colnames(data_clean), value=TRUE),
                ~ .x - 1)) %>%
  scale_mean(oldvars = grep("uclals_[1-6]$", colnames(data_clean), value=TRUE),
             newvar = "uclals_mean",
             newlab = "Higher scores indicate a higher sense of loneliness",
             newchoices = "Mean of uclals_1-uclals_6",
             prop_nonmiss = 0.7)

### 36. PAQ [? - B] ----
# [TO DO] have Brian, Tony, someone, review this code
source("paq_clean.R")

data_clean36 <- data_clean35 %>%
  select(-grep("paq_[0-9]+", colnames(data_clean35), value=TRUE)) %>%
  left_join(paq_cleaning(data_clean35),
            join_by(record_id, redcap_event_name)) %>%
  relocate(starts_with("paq"), .after = globalphysical_timestamp)


### 37. SLEEP ----
# one numeric value; no scale necessary

if(F) {
  # check for anomalous answers
  table(data_clean$sleep)
}

data_clean37 <- data_clean36

### 38. WISDM ----
# subscales
  ## Automaticity:    wisdm1_1, wisdm1_5, wisdm1_6, wisdm2_1, and wisdm2_7   
  ## Loss of Control: wisdm1_2, wisdm1_7, wisdm1_9, and wisdm2_5
  ## Tolerance:       wisdm1_3, wisdm2_2, wisdm2_4, wisdm2_6, and wisdm2_8
  ## Craving:         wisdm1_4, wisdm1_8, wisdm1_10, and wisdm2_3

data_clean38 <- data_clean37 %>%
    # no recoding
    # scale overall
    scale_mean(oldvars = grep("wisdm[1,2]_[1-9]0?$", colnames(data_clean), value=TRUE),
               newvar = "wisdm_total",
               newlab = "Higher scores indicate higher overall dependence",
               newchoices = "Mean of wisdm1_1-wisdm1_10 and wisdm2_1-wisdm2_8",
               prop_nonmiss = 0.7,
               position = "wisdm2_8") %>%
    # scale craving
    scale_mean(oldvars = grep("wisdm((1_[4,8])|(1_10)|(2_3))$", colnames(data_clean), value=TRUE),
               newvar = "wisdm_craving",
               newlab = "Higher scores indicate higher dependence due to craving",
               newchoices = "Mean of wisdm1_4, wisdm1_8, wisdm1_10, and wisdm2_3",
               prop_nonmiss = 0.7,
               position = "wisdm2_8") %>%
    # scale tolerance
    scale_mean(oldvars = grep("wisdm((1_3)|(2_[2,4,6,8]))$", colnames(data_clean), value=TRUE),
               newvar = "wisdm_tolerance",
               newlab = "Higher scores indicate higher dependence due to tolerance",
               newchoices = "Mean of wisdm1_3, wisdm2_2, wisdm2_4, wisdm2_6, wisdm2_8",
               prop_nonmiss = 0.7,
               position = "wisdm2_8") %>%
    # scale loss of control
    scale_mean(oldvars = grep("wisdm((1_[2,7,9])|(2_5))$", colnames(data_clean), value=TRUE),
               newvar = "wisdm_losscontrol",
               newlab = "Higher scores indicate higher dependence due to loss of control",
               newchoices = "Mean of wisdm1_2, wisdm1_7, wisdm1_9, wisdm2_5",
               prop_nonmiss = 0.7,
               position = "wisdm2_8") %>%
    # scale automaticity
    scale_mean(oldvars = grep("wisdm((1_[1,5,6])|(2_[1,7]))$", colnames(data_clean), value=TRUE),
               newvar = "wisdm_automaticity",
               newlab = "Higher scores indicate higher dependence due to automaticity",
               newchoices = "Mean of wisdm1_1, wisdm1_5, wisdm1_6, wisdm2_1, wisdm2_7",
               prop_nonmiss = 0.7,
               position = "wisdm2_8") 

### 39. REIS ----
# subscales
  ## Self-Focused Emotion Appraisal:   reis_1-reis_7
  ## Other-Focused Emotion Appraisal:  reis_8-reis_14
  ## Self-Focused Emotion Regulation:  reis_15-reis_21
  ## Other-Focused Emotion Regulation: reis_22-reis_28

data_clean39 <- data_clean38 %>%
  # no recoding
  # scale overall
  scale_mean(oldvars = grep("reis_[0-9]+$", colnames(data_clean), value=TRUE),
             newvar = "reis_overall_mean",
             newlab = "Higher scores indicate higher overall emotional intelligence",
             newchoices = "Mean of reis_1-reis_28",
             prop_nonmiss = 0.7,
             position = "reis_28") %>%
  # scale other-focused emotion regulation
  scale_mean(oldvars = grep("reis_2[2-8]$", colnames(data_clean), value=TRUE),
             newvar = "reis_other_regulate_mean",
             newlab = "Higher scores indicate higher emotional intelligence with other-focused emotion regulation",
             newchoices = "Mean of reis_22-reis_28",
             prop_nonmiss = 0.7,
             position = "reis_28") %>%
  # scale self-focused emotion regulation
  scale_mean(oldvars = grep("reis_((1[5-9])|(2[0-1]))$", colnames(data_clean), value=TRUE),
             newvar = "reis_self_regulate_mean",
             newlab = "Higher scores indicate higher emotional intelligence with self-focused emotion regulation",
             newchoices = "Mean of reis_15-reis_21",
             prop_nonmiss = 0.7,
             position = "reis_28") %>%
  # scale other-focused emotion appraisal
  scale_mean(oldvars = grep("reis_([8-9]|(1[0-4]))$", colnames(data_clean), value=TRUE),
             newvar = "reis_other_appraise_mean",
             newlab = "Higher scores indicate higher emotional intelligence with other-focused emotion appraisal",
             newchoices = "Mean of reis_8-reis_14",
             prop_nonmiss = 0.7,
             position = "reis_28") %>%
  # scale self-focused emotion appraisal
  scale_mean(oldvars = grep("reis_[1-7]$", colnames(data_clean), value=TRUE),
             newvar = "reis_self_appraise_mean",
             newlab = "Higher scores indicate higher emotional intelligence with self-focused emotion appraisal",
             newchoices = "Mean of reis_1-reis_7",
             prop_nonmiss = 0.7,
             position = "reis_28")


### 40. NCS ----

invisible(lapply(c("ncs_3", "ncs_4", "ncs_5", "ncs_7", "ncs_8", "ncs_9", "ncs_12", "ncs_16", "ncs_17"), function(x) recode_scale(x, 5:1)))

data_clean40 <- data_clean39 %>%
  # reverse code
  mutate(across(c("ncs_3", "ncs_4", "ncs_5", "ncs_7", "ncs_8", "ncs_9", "ncs_12", "ncs_16", "ncs_17"),
                ~ 6 - .x)) %>%
  scale_mean(oldvars = grep("ncs_[0-9]+$", colnames(data_clean), value=TRUE),
             newvar = "ncs_mean",
             newlab = "Higher scores indicate a higher tendency to engage in and enjoy thinking",
             newchoices = "Mean of ncs_1-ncs_18",
             prop_nonmiss = 0.7)

### 41. TIPI ----
# subscales
  ## Extraversion:            tipi_1 and tipi_6
  ## Agreeableness:           tipi_2 and tipi_7
  ## Conscientiousness:       tipi_3 and tipi_8
  ## Emotional Stability:     tipi_4 and tipi_9
  ## Openness to Experiences: tipi_5 and tipi_10

invisible(lapply(grep("tipi_[2,4,6,8,10]", colnames(data_clean), value=TRUE), function(x) recode_scale(x, 7:1)))

data_clean41 <- data_clean40 %>%
  # reverse code questions 2, 4, 6, 8, and 10
  mutate(across(grep("tipi_[2,4,6,8,10]", colnames(data_clean), value=TRUE),
         ~ 8 - .x)) %>%
  # scale openness to experiences
  scale_mean(oldvars = c("tipi_5", "tipi_10"),
             newvar = "tipi_O_mean",
             newlab = "Higher scores indicate higher openness to experiences",
             newchoices = "Mean of tipi_5 and tipi_10",
             prop_nonmiss = 0.7,
             position="tipi_10") %>%
  # scale emotional stability
  scale_mean(oldvars = c("tipi_4", "tipi_9"),
             newvar = "tipi_S_mean",
             newlab = "Higher scores indicate higher emotional stability",
             newchoices = "Mean of tipi_4 and tipi_9",
             prop_nonmiss = 0.7,
             position="tipi_10") %>%
  # scale conscientiousness
  scale_mean(oldvars = c("tipi_3", "tipi_8"),
             newvar = "tipi_C_mean",
             newlab = "Higher scores indicate higher conscientiousness",
             newchoices = "Mean of tipi_3 and tipi_8",
             prop_nonmiss = 0.7,
             position="tipi_10") %>%
  # scale agreeableness
  scale_mean(oldvars = c("tipi_2", "tipi_7"),
             newvar = "tipi_A_mean",
             newlab = "Higher scores indicate higher agreeableness",
             newchoices = "Mean of tipi_2 and tipi_7",
             prop_nonmiss = 0.7,
             position="tipi_10") %>%
  # scale extraversion
  scale_mean(oldvars = c("tipi_1", "tipi_6"),
             newvar = "tipi_E_mean",
             newlab = "Higher scores indicate higher extraversion",
             newchoices = "Mean of tipi_1 and tipi_6",
             prop_nonmiss = 0.7,
             position="tipi_10")

### 42. SRQ ----
# full survey has subscales, but only an overall scale will be calculated for the brief version

invisible(lapply(c("srq_1", "srq_2", "srq_4"), function(x) recode_scale(x, 5:1)))

data_clean42 <- data_clean41 %>%
  # reverse coding
  mutate(across(c("srq_1", "srq_2", "srq_4"),
                ~ 6 - .x)) %>%
  scale_mean(oldvars = grep("srq_[1-6]$", colnames(data_clean), value=TRUE),
             newvar = "srq_mean",
             newlab = "Higher scores indicate higher self-regulation abilities",
             newchoices = "Mean of srq_1-srq_6",
             prop_nonmiss = 0.7)

### 43. ECIGFU ----
# may be used individually and not combined in scale
if(F) {
  # check for anomalous answers
  lapply(grep("ecigfu_[1-3]$", colnames(data_clean), value=TRUE), 
         function(x) table(data_clean[,x]))
}

data_clean43 <- data_clean42

### 44. TAC2 ----
# may be used individually and not combined in scale

data_clean44 <- data_clean43

### 45. TAC3 ----
# may be used individually and not combined in scale
if(F) {
  # check for anomalous answers
  table(data_clean[,"tac_v3_2"])
}

data_clean45 <- data_clean44

### 46. TAC4 ----
# may be used individually and not combined in scale

data_clean46 <- data_clean45

### 47. TH4 ----
# may be used individually and not combined in scale

if(F) {
  # check for anomalous answers
  table(data_clean[,"th4_1_v5"])
}

data_clean47 <- data_clean46

### 48. US1 ----
# may be used individually and not combined in scale

data_clean48 <- data_clean47

### 49. US3 ----
# may be used individually and not combined in scale

data_clean49 <- data_clean48

# final data cleaning ----------------------------------------------------------
# identifiers
identifiers <- c(
  "dob", "dod", "first_name", "last_name", 
  "street", "street2", "city", "state", "zipcode", 
  "scr_mailstreet", "scr_mailstreet2", "scr_mailcity", "scr_mailstate", "scr_mailzip", 
  "phone_number", "other_phone_number", "email", "note",
  "scr_verifyinfo"
)

# variables with skip logic
skip_vars <- meta_clean %>%
  filter(!is.na(branching_logic) & 
           !(field_name %in% identifiers) &
           # remove Date variables
           !grepl("datetime", text_validation_type_or_show_slider_number)) %>%
  select(field_name, branching_logic) %>%
  unique()

## metadata ----

meta_final <- meta_clean %>%
  filter(!(field_name %in% identifiers)) %>%
  # add class of each field
  mutate(field_class = case_when(
    (field_type == "text" & grepl("date", text_validation_type_or_show_slider_number)) ~ "Date",
    (field_type %in% c("calc", "summary")) | ((field_type == "text" & text_validation_type_or_show_slider_number %in% c("number", "time", "integer") | grepl("#|number", field_note, ignore.case=TRUE))) ~ "numeric",
    field_type %in% c("checkbox", "radio", "dropdown", "yesno") ~ "categorical",
    # all other text and notes can fit into categorical
    .default = "categorical"
  ))

# add row for NA responses before each variable
for (i in unique(meta_final[["field_name"]])) {
    position <- head(which(meta_final$field_name == i), 1)
    
    meta_final <- meta_final %>%
      add_row(meta_final[rep(position, 1),-grep("select", colnames(meta_final))],
              select_choices_or_calculations = "NA - Missing",
              .before = position)
}

## data ----
data_final <- data_clean49 %>%
  # NA to -99 when question is skipped
  add_skip(skip_vars) %>%
  select(-c(SubjectID, redcap_survey_identifier, any_of(identifiers))) %>%
  # make sure numeric classes are actually numeric
  mutate(across(meta_final %>% filter(field_class == "numeric") %>% pull(field_name),
                ~ suppressWarnings(as.numeric(.x))))

# output -----------------------------------------------------
## setup ----
# variable names for each event
event_mapping <- events_final %>%
  full_join(mappings_final, by = "unique_event_name") %>%
  full_join(meta_final %>% select(form_name, field_name, field_label), by = join_by(form == form_name),
            relationship = "many-to-many") %>%
  unique()

# list of id's for each event
id_mapping <- data_final %>% 
  left_join(cxwalk, by = join_by(record_id == REDCap_ID)) %>%
  # finish matching up Grafana ID's
  left_join(id_check %>% mutate(rsr_id = as.character(rsr_id)), 
            by = join_by(subject_id == rsr_id)) %>%
  mutate(grafana_id = coalesce(grafana_id, mars_id)) %>%
  select(record_id, subject_id, grafana_id) %>%
  unique()
  
# initialize codebook with event mapping worksheet
codebook <- createWorkbook()
addWorksheet(codebook, "Event Mapping")
event_mapping %>%
  select(-unique_event_name) %>%
  rename(Event = event_name,
         Survey = form,
         `Variable Name` = field_name,
         Label = field_label) %>%
  writeDataTable(wb = codebook, sheet = 1, tableStyle = "TableStyleLight1")

datasets_list <- list()
metadata_list <- list()

## loop through events ----
for (i in 1:nrow(events_final)) {
  ### curate event-specific data ----
  var_list <- c("subject_id", "record_id", "grafana_id", event_mapping[event_mapping$event_name == events_final[[i, 1]],][["field_name"]])
  
  dataset <- data_final %>%
    filter(redcap_event_name == events_final[[i, 1]]) %>%
    select(all_of(var_list)) %>%
    right_join(cxwalk, by = join_by(record_id == REDCap_ID)) %>%
    select(-SubjectID) %>%
    # add in missing subject and grafana ID's
    mutate(across(c(subject_id, grafana_id),
           ~ coalesce(.x, id_mapping$.x)))
  
  datasets_list[[i]] <- dataset
  write_csv(dataset, paste0(events_final[[i, 2]], ".csv"))
  write_dta(dataset, paste0(events_final[[i, 2]], ".dta"))
  write_rds(dataset, paste0(events_final[[i, 2]], ".rds"))

  ### add worksheet to codebook ----
  code_temp <- meta_final %>%
    filter(field_name %in% var_list) %>%
    codebook_summary(dataset) %>%
    rename(`Variable Name` = field_name,
           Label = field_label,
           `Skip Logic` = branching_logic,
           Type = field_class,
           Values = select_choices_or_calculations,
           `Required Field` = required_field) %>%
    select(`Variable Name`, Label, `Skip Logic`, `Required Field`, Type, Range, Mean, Values, Frequency, Percent)
  
  metadata_list[[i]] <- code_temp
  addWorksheet(codebook, events_final[i, 1])
  writeData(codebook, i+1, x = code_temp)
  
  # QC check
  # put metadata in order of data column names
  data_vars <- sort(colnames(datasets_list[[i]]))
  meta_vars <- sort(unique(code_temp$`Variable Name`))
  
  if(!setequal(data_vars, meta_vars)) {stop("Not all variables in the dataset appear in metadata")}
}

# datasets to R environment
names(datasets_list) <- events_final$unique_event_name
list2env(datasets_list, envir = globalenv())

## style and export codebook ----
header_cells <- createStyle(textDecoration = "bold", fgFill = "#D4D4D4", halign = "center")
body_cells <- createStyle(valign = "center")
wrap_cells <- createStyle(wrapText = TRUE)
center_cells <- createStyle(halign = "center")
border_cells <- createStyle(border = "bottom")

# styling for all sheets
invisible(lapply(1:(nrow(events_final)+1), 
       function(x) { 
         freezePane(codebook, x, firstRow = TRUE)
         showGridLines(codebook, x, showGridLines = FALSE)
       }
))

# styling for event mapping sheet
setColWidths(codebook, sheet = 1, cols = 1:4, widths = c(20, 20, 30, 75))
addStyle(codebook, sheet = 1, wrap_cells, rows = 1:nrow(event_mapping), cols = 4, gridExpand = TRUE, stack = TRUE)
addStyle(codebook, sheet = 1, body_cells, rows = 2:nrow(event_mapping), cols = 1:4, gridExpand = TRUE, stack = TRUE)

# styling for dataset-specific sheets
invisible(lapply(2:(nrow(events_final)+1), 
       function(x) { 
         setColWidths(codebook, x, 1:length(metadata_list[[x-1]]), 
                      widths = c(30, 75, 25, 12, 12, 12, 12, 50, 12, 12))
         addStyle(codebook, x, header_cells,
                  rows = 1,
                  cols = 1:length(metadata_list[[x-1]]),
                  gridExpand = TRUE,
                  stack = TRUE)
         addStyle(codebook, x, body_cells,
                  rows = 2:nrow(metadata_list[[x-1]]),
                  cols = 1:length(metadata_list[[x-1]]),
                  gridExpand = TRUE,
                  stack = TRUE)
         addStyle(codebook, x, wrap_cells,
                  rows = 2:nrow(metadata_list[[x-1]]),
                  cols = 1:length(metadata_list[[x-1]]),
                  gridExpand = TRUE,
                  stack = TRUE)
         addStyle(codebook, x, center_cells,
                  rows = 2:nrow(metadata_list[[x-1]]),
                  cols = 5:8,
                  gridExpand = TRUE,
                  stack = TRUE)
         }
       ))

# merge rows
for (i in 1:length(datasets_list)) {
  row_merges <- lapply(colnames(datasets_list[[i]]), function(x) which(metadata_list[[i]]$`Variable Name` == x) + 1)
  for (j in row_merges[lapply(row_merges, is_empty) == FALSE]) {
    for (k in c(1:7)) {
      mergeCells(codebook,
               sheet = i + 1,
               cols = k,
               rows = j)
      addStyle(codebook,
               sheet = i + 1,
               style = border_cells,
               cols = 1:length(metadata_list[[i]]),
               rows = max(j),
               gridExpand = TRUE,
               stack = TRUE)
    }
  }
}

saveWorkbook(codebook, "codebook.xlsx", overwrite=TRUE)

###@ellen: this is really really fantastic work. you're programming at a very high level right of the bat and you have a lot of things very automated. nice work!!!

# documentation ----------------------------------------------------------------
# [TO DO]: after done ironing out, reach out to Tony about adding to Git/renv project

# value labels in SAS ----------------------------------------------------------

###create value labels in SAS code -- optional thing to circle back to.