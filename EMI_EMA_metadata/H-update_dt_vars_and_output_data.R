# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary: ----
#    * Update datetime variables from "America/Denver" to the local time as experienced by the participant
#    * Save data in multiple file types to be used for analysis
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
library(dplyr)
library(purrr)
library(haven)
library(lubridate)

source('paths.R')

ema_questions <- readRDS(file.path(path_to_input_data_from_jamie, 'dat_master_ema_questions.rds')) # does not contain date time vars
ema_response_options <- readRDS(file.path(path_to_input_data_from_jamie, "dat_master_ema_response_options.rds")) # does not contain date time vars
matched_2_dec_pts_summ_metadata <- readRDS(file.path(path_to_staged, 'matched_2_dec_pts_summarized_metadata_plus_EMA.RDS'))  # contains date time vars
all_v1_visit_dates <- readRDS(file.path(path_to_input_data_from_jamie, "dat_visit_dates.rds")) # does not contain date time vars
v1_visit_dates <- readRDS(file.path(path_to_input_data_from_jamie, 'dat_visit_dates_V1_only.rds')) # does not contain date time vars
mars_ids_excluded <- data.frame(mars_id = readRDS(file.path(path_to_input_data_from_jamie, "mars_ids_excluded_from_all_analytic_datasets.rds"))) # does not contain date time vars
codebook <- readRDS(file.path(path_to_staged, "add_var_codebook.RDS"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Update datetime variables from Mountain to Local
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if(F){ #use to initialize for testing function
  dat <- ema_responses
  var_i <- 5
  dat %>% colnames
}

convrt_mtn_dt_to_local <- function(dat, timezone){
  original_col_names <- colnames(dat)
  final_col_names <- c()
  for (var_i in 1:ncol(dat)){
    col_name_i <- original_col_names[var_i]
    final_col_names <- append(final_col_names, col_name_i)
    if (str_detect(col_name_i, '_mountain') & !col_name_i %in% c("block_bounds_mountain", "randomization_bounds_mountain", "delivery_bounds_mountain")){
      new_col_name_i <- str_replace(col_name_i, '_mountain', '_local')
      final_col_names <- append(final_col_names, new_col_name_i)
      dat <- dat %>% mutate('{new_col_name_i}' := map2(!!rlang::sym(col_name_i), timezone,
                                                       ~with_tz(.x, .y) %>% force_tz(., "UTC")) %>% unlist %>% as_datetime(., origin = lubridate::origin))
    }
  }
  dat <- dat %>% select(all_of(final_col_names))
  return(dat)
}

matched_2_dec_pts_summ_metadata_local <- convrt_mtn_dt_to_local(matched_2_dec_pts_summ_metadata, matched_2_dec_pts_summ_metadata$olson_calc) # Need to bring forward olson from my code, since jamie's has gaps

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Output datasets
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
saveRDS(all_v1_visit_dates, 
        file.path(path_to_outputs_4_analysis, "all_v1_visit_dates.RDS"))
write_dta(all_v1_visit_dates, 
          file.path(path_to_outputs_4_analysis, "all_v1_visit_dates.DTA"))

saveRDS(ema_questions, 
        file.path(path_to_outputs_4_analysis, "ema_questions.RDS"))
write_dta(ema_questions, 
          file.path(path_to_outputs_4_analysis, "ema_questions.DTA"))

saveRDS(ema_response_options, 
        file.path(path_to_outputs_4_analysis, "ema_response_options.RDS"))
write_dta(ema_response_options, 
          file.path(path_to_outputs_4_analysis, "ema_response_options.DTA"))

saveRDS(mars_ids_excluded, 
        file.path(path_to_outputs_4_analysis, "mars_ids_excluded.RDS"))
write_dta(mars_ids_excluded, 
          file.path(path_to_outputs_4_analysis, "mars_ids_excluded.DTA"))

saveRDS(matched_2_dec_pts_summ_metadata_local, 
        file.path(path_to_outputs_4_analysis, "matched_2_dec_pts_summ_metadata_local.RDS"))
write_dta(matched_2_dec_pts_summ_metadata_local, 
          file.path(path_to_outputs_4_analysis, "matched_2_dec_pts_summ_metadata_local.DTA"))

saveRDS(v1_visit_dates, 
        file.path(path_to_outputs_4_analysis, "v1_visit_dates.RDS"))
write_dta(v1_visit_dates, 
          file.path(path_to_outputs_4_analysis, "v1_visit_dates.DTA"))

saveRDS(codebook, 
        file.path(path_to_outputs_4_analysis, "codebook_4_add_vars.RDS"))
write_dta(codebook, 
          file.path(path_to_outputs_4_analysis, "codebook_4_add_vars.DTA"))
