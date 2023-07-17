


library(readr)
library(dplyr)
library(lubridate)
library(DBI)
library(tidyverse)
library(dbplyr)
library(collateral)
library(bit64)

path_to_inputs <- "C:/Users/u1330076/Box/MARS metadata support for Jamie/Input/Utah Data Sets/Utah Data Sets"
path_to_staged_data <- "C:/Users/u1330076/Box/MARS metadata support for Jamie/Staged"


chk <- read_csv(file.path("C:/Users/u1330076/Box/MARS metadata support for Jamie/Input", "MARS Data Checklist Dataset.csv"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

chk_visit_statuses <- chk %>% select(SubjectID, Grafana_ID, Visit1_Status, Visit2_Status, Visit3_Status, Visit4_Status)

chk_visit_statuses_withdrawn <- chk_visit_statuses %>% rowwise() %>% filter(Visit1_Status == "Drop/Withdrawn" | Visit2_Status == "Drop/Withdrawn" |Visit3_Status == "Drop/Withdrawn" | Visit4_Status == "Drop/Withdrawn")

chk_visit_statuses_withdrawn_tall <- chk_visit_statuses_withdrawn %>% pivot_longer(cols = ends_with("Status"), names_to = "visit_name", values_to = "visit_status")

chk_visit_statuses_withdrawn_pt <- chk_visit_statuses_withdrawn_tall %>% group_by(SubjectID) %>% filter(visit_status == "Drop/Withdrawn") %>% filter(row_number() == 1) %>% ungroup() %>% 
  mutate(visit_number_withdrawn = str_remove(visit_name, "_Status")) %>% 
  mutate(EventType = case_when(
    visit_number_withdrawn == "Visit3" ~ "V3 No show",
    visit_number_withdrawn == "Visit4" ~ "V4 No show"
  ))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

list2env(RSR.tables, envir = environment())

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

event_v2 <- Event %>% filter(Study == "MARS") %>% select(SubjectID, EventType, EndDate) 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
        file = file.path(path_to_staged_data, "pt_withdrawn_date.RDS"))


