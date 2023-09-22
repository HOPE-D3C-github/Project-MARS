library(dplyr)
library(readr)
library(lubridate)
library(testthat)

source('paths.R')

matched_2_dec_pts_original <- readRDS(file.path(path_to_input_data_from_jamie, "dat_matched_to_decision_points.rds"))
matched_2_dec_pts_summ_metadata <- readRDS(file.path(path_to_staged, 'matched_2_dec_pts_summarized_metadata_plus_EMA.RDS'))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 1. Create Codebook Template for Manual Updates
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
(additional_vars <- matched_2_dec_pts_summ_metadata %>% select(!colnames(matched_2_dec_pts_original)) %>% colnames())

codebook_template <- data.frame(variable_name = additional_vars)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 2. Save Codebook Template
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
write_csv(codebook_template,
          file = file.path(path_to_staged, "add_var_codebook_template.csv"))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 3. 
# 
# Manual Updates done outside of code - If new variables are added, then this 
#   step requires re-doing
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# STEP 4. Load Codebook for Read-in
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
codebook_unvalidated <- read_csv(file.path(path_to_other_input_data, "add_var_codebook_4_read_in.csv"))

test1 <- test_that("Codebook contains the correct variable names. Update the read_in file if test fails.", {
  expect_equal(
    object = codebook_unvalidated$variable_name,
    expected = additional_vars
  )})

if(test1){
  codebook_validated <- codebook_unvalidated
}

saveRDS(codebook_validated,
        file = file.path(path_to_staged, "add_var_codebook.RDS"))
