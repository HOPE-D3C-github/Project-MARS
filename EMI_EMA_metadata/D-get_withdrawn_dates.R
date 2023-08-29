library(tidyverse)
library(dplyr)
library(lubridate)

source("paths.R")

event <- readRDS(file.path(path_to_input_data_RSR_visit, "event_log.rds"))

chk_v1 <- readRDS(file.path(path_to_input_data_RSR_visit, "Visit1_DataChecklist.rds"))
chk_v2 <- readRDS(file.path(path_to_input_data_RSR_visit, "Visit2_DataChecklist.rds"))
chk_v3 <- readRDS(file.path(path_to_input_data_RSR_visit, "Visit3_DataChecklist.rds"))
chk_v4 <- readRDS(file.path(path_to_input_data_RSR_visit, "Visit4_DataChecklist.rds"))

chk_visit_statuses <- chk_v1 %>% select(SubjectID, Grafana_ID, Visit1_Status) %>% 
  full_join(chk_v2 %>% select(SubjectID, Visit2_Status),
            by = "SubjectID") %>% 
  full_join(chk_v3 %>% select(SubjectID, Visit3_Status),
            by = "SubjectID") %>% 
  full_join(chk_v4 %>% select(SubjectID, Visit4_Status),
            by = "SubjectID")

chk_visit_statuses_withdrawn <- chk_visit_statuses %>% rowwise() %>% filter(Visit1_Status == "Drop/Withdrawn" | Visit2_Status == "Drop/Withdrawn" |Visit3_Status == "Drop/Withdrawn" | Visit4_Status == "Drop/Withdrawn")

chk_visit_statuses_withdrawn_tall <- chk_visit_statuses_withdrawn %>% pivot_longer(cols = ends_with("Status"), names_to = "visit_name", values_to = "visit_status")

chk_visit_statuses_withdrawn_pt <- chk_visit_statuses_withdrawn_tall %>% group_by(SubjectID) %>% filter(visit_status == "Drop/Withdrawn") %>% filter(row_number() == 1) %>% ungroup() %>% 
  mutate(visit_number_withdrawn = str_remove(visit_name, "_Status")) %>% 
  mutate(EventType = case_when(
    visit_number_withdrawn == "Visit3" ~ "V3 No show",
    visit_number_withdrawn == "Visit4" ~ "V4 No show"
  ))

event_v2 <- event %>% filter(Study == "MARS") %>% select(SubjectID, EventType, EndDate)

pt_withdrawn_date <- chk_visit_statuses_withdrawn_pt %>% 
  left_join(event_v2,
            by = c("SubjectID", "EventType")) %>% 
  rename(EventType_noshow = EventType, EndDate_noshow = EndDate) %>% 
  mutate(EventType = "18 Participant withdrawn") %>% 
  left_join(event_v2,
            by = c("SubjectID", "EventType")) %>% 
  mutate(EndDate_manualwithdraw = case_when(
    SubjectID == "176811" ~ as_datetime("2022-09-20 14:56:07")
  )) %>% 
  mutate(withdrawn_date = as_date(coalesce(EndDate_noshow, EndDate, EndDate_manualwithdraw))) %>% 
  filter(!is.na(withdrawn_date)) %>% 
  select(SubjectID, Grafana_ID, visit_name, withdrawn_date)

saveRDS(pt_withdrawn_date,
        file = file.path(path_to_staged, "pt_withdrawn_date.RDS"))
