library(dplyr)
library(readr)
library(lubridate)
library(DBI)
library(tidyverse)
library(dbplyr)
library(collateral)
library(bit64)

source('paths.R')
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Load Data
chk_v1 <- readRDS(file.path(path_to_input_data_RSR_visit, "Visit1_DataChecklist.rds"))
redcap_dat <- readRDS(file.path(path_to_input_data_redcap_demogs, "redcap_data.rds"))

nudge_dat_all <- read_csv(file.path(path_to_other_input_data, "nudge_dat_from_RC_20230627.csv"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Pre-process nudge data
nudge_dat <- nudge_dat_all %>% 
  rowwise() %>% 
  mutate(any_ema_3to9_non_compliant = any("Not Compliant" %in% c(ema_response_3_v2, ema_response_4_v2, ema_response_5_v2, ema_response_6_v2, ema_response_7_v2, 
                                                            ema_response_8_v2, ema_response_9_v2)), .after = ema_response_9_v2) %>% 
  mutate(day3_date = as_date(start_nudge_date3)) %>% 
  filter(any_ema_3to9_non_compliant) %>% 
  select(SubjectID = subject_id, record_id, day3_date, any_ema_3to9_non_compliant, 
         nudge_3_v2, nudge_4_v2, nudge_5_v2, nudge_6_v2, nudge_7_v2, nudge_8_v2, nudge_9_v2) %>% 
  ungroup()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Connect to DB and grab event log
sort(unique(odbc::odbcListDrivers()[[1]]))
con <- dbConnect(odbc::odbc(),
                 driver = "ODBC Driver 17 for SQL Server",
                 server ="HCI-DBStage",
                 database = "ProdReadOnly",
                 Trusted_Connection = "yes")

my_tables <- tbl(con, in_schema("information_schema", "tables")) %>% as_tibble
my_tables$TABLE_NAME

my_readtable <- function(mytable){
  tbl(con, in_schema("dbo", mytable)) %>% as_tibble
}
RSR.tables.names <- my_tables$TABLE_NAME
RSR.tables.safely <- map_safely(RSR.tables.names,my_readtable)

tally_errors(RSR.tables.safely)
# tally_warnings(RSR.tables.safely)
RSR.tables <- map(RSR.tables.safely,~pluck(.,'result'))
names(RSR.tables) <- RSR.tables.names

event <- RSR.tables$Event %>% filter(Study == 'MARS')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 
cw_ids <- chk_v1 %>% select(SubjectID, Grafana_ID)

# Nudge tracker was not implemented for all pts. Event log equipment notes 
eventtypes_tokeep <- sort(unique(event$EventType))[c(15:18, 47, 57)]
# print(eventtypes_tokeep)
# "19 Equipment-early checkin"  "20 Equipment-issue reported" "21 Equipment-issue resolved" "22 Equipment-lost"           "V2 Equipment-checked"        "V3 Equipment-checked"  
event_equipment_notes <- event %>% filter(EventType %in% eventtypes_tokeep) %>% arrange(SubjectID, StartDate) %>% filter(!is.na(Note)) %>% 
  select(SubjectID, event_date = StartDate, EventType, Note) %>% 
  left_join(y = cw_ids, by = "SubjectID") %>% relocate(Grafana_ID, .after = SubjectID) %>% 
  filter(!is.na(Grafana_ID))

# Prep event log data to be merged with nugde data
event <- event %>%
  full_join(y = nudge_dat %>% select(SubjectID, day3_date),
             by = 'SubjectID')

event <- event %>% 
  mutate(day9_date = day3_date + days(6)) %>% 
  filter(between(as_date(StartDate), day3_date, day9_date)) %>% 
  filter(!is.na(Note)) %>% 
  filter(!str_detect(EventType, 'Scheduled|Gift cards|Quest|Calls')) %>%  #filter pertinent event types
  relocate(SubjectID, .before = everything()) %>% 
  select(-c(EventID, Study, EditPersonID, PerformPersonID, LogPersonID, EndDate, LastEditDate, IsDone, MRAuthID, IdImage,  day3_date, day9_date)) %>% 
  rename(event_datetime = StartDate) %>% 
  mutate(event_date = as_date(event_datetime),.before = event_datetime)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Convert nudge data to tall and prep to merge with pertinent event log records
nudge_dat_tall <- nudge_dat %>% 
  pivot_longer(cols = starts_with('nudge_'),
               names_to = "nudge_date_txt",
               values_to = "nudge_result",
               values_drop_na = TRUE) # filter out days without a nudge record

nudge_dat_tall <- nudge_dat_tall %>% 
  filter(nudge_result != "Not Needed") %>% # filter out days that dont need a nudge 
  mutate(nudge_day_int = as.integer(substr(nudge_date_txt, 7,7)),
         nudge_date = day3_date + days(nudge_day_int - 3),
         .after = day3_date
         ) %>% 
  select(-c(day3_date, any_ema_3to9_non_compliant, nudge_date_txt))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Merge tall nudge data and event log
combined_nudge_event <- nudge_dat_tall %>% 
  left_join(y = event, suffix = c("_nudge", "_event"),
            by = c("SubjectID", "nudge_date" = "event_date"),
            multiple = "all")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Merge grafana id from v1 data checklist
combined_nudge_event <- combined_nudge_event %>% 
  left_join(y = cw_ids,
            by = "SubjectID")

combined_nudge_event <- combined_nudge_event %>% 
  select(SubjectID, Grafana_ID, record_id, nudge_date, nudge_day_int, nudge_result, event_datetime, EventType, CallResult, EventNote = Note)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Save data to view in excel
write_csv(combined_nudge_event,
          file.path(path_to_other_input_data, "combined_nudge_event_dat.csv"))

write_csv(event_equipment_notes,
          file.path(path_to_other_input_data, "event_equipment_notes.csv"))
