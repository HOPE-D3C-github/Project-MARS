library(tidyverse)
library(lubridate)
library(redcapAPI)
library(labelled)
library(rlang)

con <- redcapConnection("https://hci-redcap.hci.utah.edu/redcap/api/",token=readLines('../../../my_credentials/redcap_apikey_mars.txt'))
# meta <- exportMetaData(con) %>% as_tibble
# mars_instruments <- exportInstruments(con)
# mars_fieldnames <- exportFieldNames(con)
mars_raw <- exportRecords(con, factors = F,labels = T) %>% as_tibble
# mars_labelled <- exportRecords(con,labels = T) %>% as_tibble
# meta_2 <- meta %>% 
#   left_join(mars_fieldnames, by = c("field_name"="original_field_name")) %>% #add on exported field name -- one to many match
#   left_join(mars_instruments, by = c("form_name"="instrument_name")) %>% #grab long instrument names, as they have some info we want to parse
#   filter(!field_type %in% c("descriptive","file")) 
# meta_3 <- tibble(export_field_name = names(mars_raw)) %>% 
#   full_join(meta_2, by = "export_field_name") %>% 
#   filter((!is.na(form_name)|str_detect(export_field_name,'^redcap_repeat|_complete$'))) %>% 
#   mutate(form_name = if_else(str_detect(export_field_name,"_complete$"),str_remove(export_field_name,"_complete"),form_name))
# 


# ----------------------------------

# nudge_field_names <- append("record_id", meta %>% filter(form_name == "nudge_tracking_v2") %>% .$field_name)

nudge_data <- exportRecords(con, factors = T,labels = T, events = "nudge_tracking_arm_1") %>% 
  select(
    where(
      ~!all(is.na(.x))
    )
  ) %>% as_tibble() %>% 
  left_join(y = mars_raw %>% select(record_id, subject_id) %>% filter(!is.na(subject_id)) %>% distinct(),
            by = "record_id") %>% 
  relocate(subject_id, grafana_id, .after = record_id)

remove(mars_raw)

# ----------------------------------

write_csv(nudge_data, 
          file = "../Input/nudge_dat_from_RC_20230627.csv")

