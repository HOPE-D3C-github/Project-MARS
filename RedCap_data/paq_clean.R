#################################################################################################################################
# Title: GPAQ questionnaire cleaning
# Desc: this function goes through the steps and tables in the WHO analysis guide
#################################################################################################################################

source("summary_codebook_funs.R")

# domains
  ## activity at work:          paq_1-paq_6
  ## travel to and from places: paq_7-paq_9
  ## recreational activities:   paq_10-paq_15
# subdomains
  ## vigorous work:             paq_1-paq_3
  ## moderate work:             paq_4-paq_6
  ## travel:                    paq_7-paq_9
  ## vigorous recreation:       paq_10-paq_12
  ## moderate recreation:       paq_13-paq_15
  ## sitting:                   paq_16

# Metabolic Equivalents (METs): express intensity of physical activities

paq_cleaning <- function(dataset) {
  
  # data cleaning (pp. 9-13) ---------------------------------------------------
  ## reformat columns ----
  paq_clean <- dataset %>%
    # as.numeric results in fraction of day spent on activity
    mutate(across(c("paq_3", "paq_6", "paq_9", "paq_12", "paq_15"),
                  ~ as.numeric(.x)*24*60)) %>%
    mutate(paq_16 = if_else(paq_16 <= 24, 60*paq_16, NA))
  
  ## remove unusable cases from analysis ----
  # GPAQ data are cleaned as a whole
  paq_use <- paq_clean %>%
    # no rows that are fully NA
    filter(!if_all(grep("paq_[0-9]+", colnames(dataset), value=TRUE),
                   ~ is.na(.x))) %>%
    # max 16 hours (960 minutes)/day per activity
    filter(if_all(c(paq_3, paq_6, paq_9, paq_12, paq_15),
                  ~ (.x <= 960 | is.na(.x)))) %>%
    # max 7 days/week per activity
    filter(if_all(c(paq_2, paq_5, paq_8, paq_11, paq_14),
                  ~ (.x <= 7 | is.na(.x)))) %>%
    rowwise() %>%
    # no inconsistencies in answers
    mutate(count_correct = sum(
      c(
        # case 1. if reported 'No', both days and time spent must equal 0
        (paq_1 == 0 & paq_2 == 0 & paq_3 == 0),
        (paq_4 == 0 & paq_5 == 0 & paq_6 == 0),
        (paq_7 == 0 & paq_8 == 0 & paq_9 == 0),
        (paq_10 == 0 & paq_11 == 0 & paq_12 == 0),
        (paq_13 == 0 & paq_14 == 0 & paq_15 == 0),
        # case 2. if reported 'Yes', both days and time spent must be > 0
        (paq_1 == 1 & paq_2 > 0 & paq_3 > 0),
        (paq_4 == 1 & paq_5 > 0 & paq_6 > 0),
        (paq_7 == 1 & paq_8 > 0 & paq_9 > 0),
        (paq_10 == 1 & paq_11 > 0 & paq_12 > 0),
        (paq_13 == 1 & paq_14 > 0 & paq_15 > 0),
        # case 3. reported NA to all of an activity (given at least one activity was fully/correctly filled out)
        (is.na(paq_1) & is.na(paq_2) & is.na(paq_3)),
        (is.na(paq_4) & is.na(paq_5) & is.na(paq_6)),
        (is.na(paq_7) & is.na(paq_8) & is.na(paq_9)),
        (is.na(paq_10) & is.na(paq_11) & is.na(paq_12)),
        (is.na(paq_13) & is.na(paq_14) & is.na(paq_15))
      ), na.rm=TRUE
    )
    ) %>%
    ungroup() %>%
    # subject must have all 5 subdomains fall into one of the categories
    filter(count_correct == 5) %>%
    select(record_id, redcap_event_name, grep("paq_[0-9]+", colnames(data_clean), value=TRUE))
  
  # summary scale calculations -------------------------------------------------
  ## calculate time/week spent on each subdomain activity ----
  paq_vars <- head(grep("paq_[0-9]+", colnames(dataset), value=TRUE), -1)
  for (i in seq(1, 15, 3)) {
    wk_var <- paste0(paq_vars[i+2], "_WK")
    met_var <- paste0(paq_vars[i+2], "_MET")
    
    # minutes per week
    paq_use <- paq_use %>%
      mutate({{wk_var}} := paq_use[[paq_vars[i+1]]]*paq_use[[paq_vars[i+2]]],
             .after = paq_vars[i+2])
    update_codebook(oldvars = paq_vars[c(i+1,i+2)],
                    newvar = paste0(paq_vars[i+2], "_WK"),
                    newlab = "Minutes spent on activity",
                    newchoices = paste0("Product of ", paq_vars[i+1], " and ", paq_vars[i+2]),
                    paq_vars[i+2]
    )
    
    # MET values: multiply minutes/week by MET conversion value
    # moderate: multiply by 4
    if(i %in% c(4, 7, 13)) {
      paq_use <- paq_use %>%
        mutate({{met_var}} := paq_use[[wk_var]]*4,
               .after = which(colnames(paq_use)==wk_var))
      update_codebook(oldvars = wk_var,
                      newvar = paste0(paq_vars[i+2], "_MET"),
                      newlab = "MET value for moderate activity",
                      newchoices = paste0("Product of ", wk_var, " and 4"),
                      position = wk_var
                      )
    } 
    # vigorous: multiply by 8
    else if (i %in% c(1, 10)) {
      paq_use <- paq_use %>%
        mutate({{met_var}} := paq_use[[wk_var]]*8,
               .after = which(colnames(paq_use)==wk_var))
      update_codebook(oldvars = wk_var,
                      newvar = paste0(paq_vars[i+2], "_MET"),
                      newlab = "MET value for moderate activity",
                      newchoices = paste0("Product of ", wk_var, " and 8"),
                      position = wk_var
      )
    }
  }
  
  ## Whether meets WHO physical activity recs (p. 15-16) ----
  paq_recs <- paq_use %>%
    scale_sum(oldvars = grep("MET", colnames(paq_use), value=TRUE),
              newvar = "paq_total_MET",
              newlab = "MET value of all activity per week",
              newchoices = "Sum of paq_3_MET, paq_6_MET, paq_9_MET, paq_12_MET, and paq_15_MET",
              position = "paq_16") %>%
    mutate(paq_meet_req = if_else(paq_total_MET >= 600, 1, 0))
  
  update_codebook(oldvars = "paq_total_MET", 
                  newvar = "paq_meet_req",
                  newlab = "Indicates whether a subject meets WHO recommendations for weekly physical activity for health based on MET values (>= 600)",
                  newchoices = "0 - No | 1 - Yes",
                  position = "paq_total_MET")
  
  ## Total time spent on physical activity (p. 17) ----
  paq_total <- paq_recs %>%
    # time spent/week
    scale_sum(oldvars = grep("WK", colnames(paq_recs), value=TRUE),
              newvar = "paq_total_week",
              newlab = "Indicates minutes spent on physical activity per week",
              newchoices = "Sum of paq_3_WK, paq_6_WK, paq_9_WK, paq_12_WK, and paq_15_WK",
              position = "paq_meet_req") %>%
    # time spent/day
    mutate(paq_total_day = round(paq_total_week/7))
  
  update_codebook(oldvars = "paq_total_week", 
                  newvar = "paq_total_day",
                  newlab = "Indicates minutes spent on physical activity per day",
                  newchoices = "paq_total_week divided by 7",
                  position = "paq_total_week")
  
  ## Time spent on setting-specific physical activities (p. 18) ----
  paq_setting <- paq_total %>%
    scale_sum(oldvars = c("paq_12_WK", "paq_15_WK"),
              newvar = "paq_rec_day",
              newlab = "Represents average minutes spent on recreation-related physical activities per day",
              newchoices = "Sum of paq_12_WK and paq_15_WK divided by 7",
              position = "paq_total_day") %>%
    scale_sum(oldvars = c("paq_9_WK"),
              newvar = "paq_travel_day",
              newlab = "Represents average minutes spent on travel-related physical activities per day",
              newchoices = "paq_9_WK divided by 7",
              position = "paq_total_day") %>%
    scale_sum(oldvars = c("paq_3_WK", "paq_6_WK"),
              newvar = "paq_work_day",
              newlab = "Represents average minutes spent on work-related physical activities per day",
              newchoices = "Sum of paq_3_WK and paq_6_WK divided by 7",
              position = "paq_total_day") %>%
    mutate(across(c(paq_work_day, paq_travel_day, paq_rec_day),
                  ~ round(.x/7)))
  
  ## Whether engaged in setting-specific physical activities (p. 19) ----
  paq_engage <- paq_setting %>%
    mutate(paq_engaged_work = if_else(paq_1 == 1 | paq_4 == 1, 1, 0),
           paq_engaged_travel = if_else(paq_7 == 1, 1, 0),
           paq_engaged_rec = if_else(paq_10 == 1 | paq_13 == 1, 1, 0))
  update_codebook(oldvars = c("paq_10", "paq_13"),
                  newvar = "paq_engaged_rec",
                  newlab = "Whether a subject engages in recreational physical activity during the week",
                  newchoices = "0 - No | 1 - Yes",
                  position = "paq_rec_day")
  update_codebook(oldvars = "paq_7",
                  newvar = "paq_engaged_travel",
                  newlab = "Whether a subject engages in travel-related physical activity during the week",
                  newchoices = "0 - No | 1 - Yes",
                  position = "paq_rec_day")
  update_codebook(oldvars = c("paq_1", "paq_4"),
                  newvar = "paq_engaged_work",
                  newlab = "Whether a subject engages in work-related physical activity during the week",
                  newchoices = "0 - No | 1 - Yes",
                  position = "paq_rec_day")
  
  ## Percentage of total physical activity from each setting per week (p. 20) ----
  paq_percent <- paq_engage %>%
    rowwise() %>%
    mutate(paq_pct_work = round(sum(c(paq_3_WK, paq_6_WK))/paq_total_week*100, 2),
           paq_pct_travel = round(paq_9_WK/paq_total_week*100, 2),
           paq_pct_rec = round(sum(c(paq_12_WK, paq_15_WK))/paq_total_week*100, 2)) %>%
    ungroup()
  
  update_codebook(oldvars = c("paq_12_WK", "paq_15_WK", "paq_total_week"),
                  newvar = "paq_pct_rec",
                  newlab = "Percent of all activity from recreational physical activity during the week",
                  newchoices = "Sum of paq_12_WK and paq_15_WK divided by paq_total_week",
                  position = "paq_engaged_rec")
  update_codebook(oldvars = c("paq_9_WK", "paq_total_week"),
                  newvar = "paq_pct_travel",
                  newlab = "Percent of all activity from travel-related activity during the week",
                  newchoices = "paq_9_WK divided by paq_total_week",
                  position = "paq_engaged_rec")
  update_codebook(oldvars = c("paq_3_WK", "paq_6_WK", "paq_total_week"),
                  newvar = "paq_pct_work",
                  newlab = "Percent of all activity from work-related activity during the week",
                  newchoices = "Sum of paq_3_WK and paq_6_WK divided by paq_total_week",
                  position = "paq_engaged_rec")
  
  ## Whether engaged in vigorous physical activity (p. 21) ----
  paq_final <- paq_percent %>%
    mutate(paq_engaged_vigorous = if_else(paq_1 == 1 | paq_10 == 1, 1, 0))
  
  update_codebook(oldvars = c("paq_1", "paq_10"),
                  newvar = "paq_engaged_vigorous",
                  newlab = "Whether a subject engages in vigorous physical activity during the week",
                  newchoices = "0 - No | 1 - Yes",
                  position = "paq_pct_rec")
  
  return(paq_final)
}