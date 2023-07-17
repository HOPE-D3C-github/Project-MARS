
library(dplyr)
library(readr)


emareport_mars1 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_1", 
                   "ema_report.csv"),
  col_names = T
)

emireport_mars1 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_1", 
                   "emi_report.csv"),
  col_names = T
)

tailorreport_mars1 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_1", 
                   "tailor_report.csv"),
  col_names = T
)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

cond_mars_101 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_101", "CONDITION--org.md2k.scheduler.csv.bz2"),
  col_names = F
  )

battery_mars1 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_1", 
                   "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2"),
  col_names = F
)

acttype_mars1 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_1", 
                   "ACTIVITY_TYPE--org.md2k.phonesensor--PHONE.csv.bz2"),
  col_names = F
)

# logMARS_mars101 <- read_csv(
#   file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_101", 
#                    "LOG--org.md2k.mars.csv.bz2"),
#   col_names = F
# )

sysLog_mars1 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_1", 
                   "SYSTEM_LOG--org.md2k.scheduler.csv.bz2"),
  col_names = F
)

sysLog_mars100 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_100", 
                   "SYSTEM_LOG--org.md2k.scheduler.csv.bz2"),
  col_names = F
)

sysLog_mars101 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_101", 
                   "SYSTEM_LOG--org.md2k.scheduler.csv.bz2"),
  col_names = F
)

privacy_mars1 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_1", 
                   "PRIVACY--org.md2k.datakit--PHONE.csv.bz2"),
  col_names = F
)

intervLog_mars1 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_1", 
                   "INTERVENTION_LOG--org.md2k.loweffortintervention.csv.bz2"),
  col_names = F
)


wakeupinfotoday_mars55 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_55", 
                   "WAKEUP_INFO_TODAY--STATUS--org.md2k.scheduler.csv.bz2"),
  col_names = F
)

wakeupinfotoday_mars55 %>% mutate(as_datetime(X1/1000, tz = "America/Denver")) %>% View


phonebattery_mars10 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_10", 
                   "BATTERY--org.md2k.phonesensor--PHONE.csv.bz2"),
  col_names = F
)

phonebattery_mars10 <- phonebattery_mars10 %>% mutate(time_hrts_mtn = as_datetime(X1/1000, tz = "America/Denver"), date = as_date(time_hrts_mtn))

inv_dates <- phonebattery_mars10 %>% filter(between(date, as_date("2021-06-19"), as_date("2021-06-21"))) 

inv_dates %>% count(date)


wakeup_day4_mars10 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_10", 
                   "WAKEUP--DAY4--org.md2k.studywithema.csv.bz2"),
  col_names = F
)

wakeup_day4_mars10 <- wakeup_day4_mars10 %>% mutate(X1_hrts = as_datetime(X1/1000, tz = "America/Denver"), X3_hrts = as_datetime(X3/1000, tz = "America/Denver"))


wakeup_day6_mars10 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_10", 
                   "WAKEUP--DAY6--org.md2k.studywithema.csv.bz2"),
  col_names = F
)

wakeup_day6_mars10 <- wakeup_day4_mars10 %>% mutate(X1_hrts = as_datetime(X1/1000, tz = "America/Denver"), X3_hrts = as_datetime(X3/1000, tz = "America/Denver"))



emi_sched_22 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_22", 
                   "EMI--RANDOM--org.md2k.scheduler.csv.bz2"),
  col_names = F
)

emi_sched_22 %>% mutate(datetime = as_datetime(X1/1000, tz = "America/Denver")) %>% View



emi_status_22 <- read_csv(
  file = file.path("C:/Users/u1330076/Box/Affective Science and U01 - Shared with Michigan and Memphis/MD2K Data - MARS-AffectiveScience/U01 MARS/CerebralCortexUpload_Updated 07012021/mars_22", 
                   "EMI_RANDOM--STATUS--org.md2k.scheduler.csv.bz2"),
  col_names = F
)

emi_status_22 %>% mutate(datetime = as_datetime(X1/1000, tz = "America/Denver")) %>% View







