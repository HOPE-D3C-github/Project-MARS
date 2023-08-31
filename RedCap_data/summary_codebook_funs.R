#################################################################################################################################
# Title: Summary Scale and Codebook Functions
# Desc: this script contains functions for creating summary scales and updating the codebook for REDCap data
#################################################################################################################################
library(quest) # conditional rowSums

# scale summaries --------------------------------------------------------------
## mean ----
scale_mean <- function(dataset, oldvars, newvar, newlab, newchoices=NA, 
                       prop_nonmiss, position = tail(oldvars,1)) {
  # average across variables for each subject
  dataset <- dataset %>%
    mutate({{newvar}} := quest::rowMeans_if(dataset[,oldvars],
                                            ov.min=prop_nonmiss),
           .after = all_of(position))
  
  # add to codebook
  update_codebook(oldvars, newvar, newlab, newchoices, position)
  return(dataset)
}

## sum ----
scale_sum <- function(dataset, oldvars, newvar, newlab, newchoices=NA, 
                      position = tail(oldvars,1)) {
  # add across variables for each subject
  dataset <- dataset %>%
    mutate({{newvar}} := rowSums(dataset[,oldvars],
                                 na.rm = FALSE), # NA unless all questions are answered
           .after = all_of(position))
  
  # add to codebook
  update_codebook(oldvars, newvar, newlab, newchoices, position)
  return(dataset)
}

## sum for NIDA variables ----
scale_sum_nida <- function(dataset, oldvars, newvar, newlab, newchoices=NA, 
                           position = tail(oldvars,1)) {
  # add across variables for each subject
  dataset <- dataset %>%
    mutate(!!newvar := rowSums(dataset[,oldvars],
                               na.rm = FALSE), # NA unless all questions are answered
           .after = all_of(position))
  
  # add to codebook
  update_codebook(oldvars, newvar, newlab, newchoices, position)
  return(dataset)
}

# update codebook --------------------------------------------------------------
## add new variable to codebook ----
update_codebook <- function(oldvars, newvar, newlab, newchoices=NA, position) {
  if(!(newvar %in% meta_clean[["field_name"]])) {
    # place new row right after contributing variables
    meta_position <- tail(which(meta_clean$field_name == position), 1)

    # branching logic based on contributing variables
    newbranch <- na.omit(unique(meta_clean[which(meta_clean$field_name %in% oldvars),][["branching_logic"]]))

    meta_clean <<- meta_clean %>%
      add_row(field_name = newvar,
              # form name matches that of contributing variables
              form_name = meta_clean[[meta_position, "form_name"]],
              field_type = "summary",
              field_label = newlab,
              select_choices_or_calculations = newchoices,
              branching_logic = paste(newbranch, collapse = " and "),
              .after = meta_position) %>%
      mutate(branching_logic = if_else(branching_logic == "", NA, branching_logic)) %>%
      separate_longer_delim(select_choices_or_calculations, 
                            delim = " | ")
  }
}

## recode existing variable in codebook ----
recode_scale <- function(recode_var, new_num) {
  if (!(recode_var %in% recoded_vars)) {
    # pull descriptions without numbers from codebook
    to_change <- meta_clean[meta_clean$field_name == recode_var,][["select_choices_or_calculations"]]
    to_change <- sub(".*-\\s", "", to_change)
    
    # add new numbers to description
    new_scale <- paste(as.character(new_num), "-", to_change)
    # put in numerical order
    new_scale <- new_scale[order(new_scale, sort(new_num))]
    
    meta_clean[meta_clean$field_name == recode_var,][["select_choices_or_calculations"]] <<- new_scale
    recoded_vars <<- append(recoded_vars, recode_var)
  }
}

# output prep ------------------------------------------------------------------
## add skip logic to cleaned dataset ----
add_skip <- function(dataset, skip_vars) {
  for (i in 1:nrow(skip_vars)) {
    skip_logic <- skip_vars[[i, 2]]
    # remove brackets, etc, around variable names and conditions
    skip_logic <- str_replace_all(skip_logic, "\\)|\\'|\\[|\\]", "")
    # parentheses appear for checkbox variables; change to match export variable name
    skip_logic <- str_replace_all(skip_logic, "\\(", "___")
    # add in correct R logical syntax
    skip_logic <- str_replace_all(skip_logic, " = ", " == ")
    skip_logic <- str_replace_all(skip_logic, "and", "&")
    skip_logic <- str_replace_all(skip_logic, "or", "|")
    
    
    skip_class <- paste0("as.", class(dataset[[skip_vars[[i, 1]]]]), "(-99)")
    
    dataset <- dataset %>%
      mutate(!!skip_vars[[i, 1]] := if_else(eval(parse(text=skip_logic)),
                                            !!as.name(skip_vars[[i, 1]]),
                                            eval(parse(text=skip_class))))
    
    # add row for -99 to codebook
    position <- tail(which(meta_final$field_name == skip_vars[[i, 1]]), 1)
    
    meta_final <<- meta_final %>%
      add_row(meta_final[rep(position, 1), -grep("select", colnames(meta_final))],
              select_choices_or_calculations = "-99 - Skipped",
              .after = position)
  }
  return(dataset)
}

## create codebook summaries for each dataset ----
codebook_summary <- function(code_temp, dataset) {
  ### take frequencies/percents of response options ----
  value_vars <- code_temp %>%
    filter(field_type %in% c("checkbox", "dropdown", "radio", "yesno")) %>%
    select(field_name) %>%
    unique() %>%
    pull(field_name)
  
  # safety net: if there aren't any frequency variables, create empty dataframe
  if (length(value_vars) == 0) {
    value_freqs <- tibble(field_name = character(0), choice = character(0), Frequency = numeric(0), Percent = numeric(0))
  } else {
    value_freqs <- dataset %>%
      select(all_of(value_vars)) %>%
      map(function(x) table(x, exclude = NULL)) %>%
      unlist() %>%
      enframe() %>%
      rename(Frequency = value) %>%
      left_join(dataset %>%
                  select(all_of(value_vars)) %>%
                  map(function(x) table(x, exclude = NULL) %>% prop.table()*100) %>%
                  unlist() %>%
                  enframe() %>%
                  rename(Percent = value) %>%
                  mutate(Percent = round(Percent, 2)),
                by = "name") %>%
      separate(name, c("field_name", "choice"), sep = "\\.")
  }

  ### take frequencies/percents of variables that don't have value options, but do have potential for NA and/or -99 ----
  nonvalue_vars <- code_temp %>%
    filter(!(field_name %in% value_vars)) %>%
    select(field_name) %>%
    unique() %>%
    pull(field_name)
  
  if (length(nonvalue_vars) == 0) {
    nonvalue_freqs <- tibble(field_name = character(0), choice = character(0), Frequency = numeric(0), Percent = numeric(0))
  } else {
    # temporary recoding: NA = NA, Skipped = "-99", Answered = "-98"
    nonvalue_recode <- dataset %>%
      select(all_of(nonvalue_vars)) %>%
      mutate(across(everything(), as.character)) %>%
      mutate(across(everything(), ~ if_else(!is.na(.x) & .x != "-99", "-98", .x)))
    
    nonvalue_freqs <- nonvalue_recode %>%
      map(function(x) table(x, exclude = NULL)) %>%
      unlist() %>%
      enframe() %>%
      rename(Frequency = value) %>%
      left_join(nonvalue_recode %>%
                  map(function(x) table(x, exclude = NULL) %>% prop.table()*100) %>%
                  unlist() %>%
                  enframe() %>%
                  rename(Percent = value) %>%
                  mutate(Percent = round(Percent, 2)),
                by = "name") %>%
      separate(name, c("field_name", "choice"), sep = "\\.")
  }
  
  ### take range of subject-entered numbers or dates
  range_vars <- code_temp %>%
    filter((field_class %in% c("integer", "numeric", "Date") & field_type == "text") | field_type == "summary") %>%
    select(field_name) %>%
    unique() %>%
    pull(field_name)
  
  if (length(range_vars) == 0) {
    ranges <- tibble(key = character(0), value = character(0))
  } else {
    ranges <- dataset %>%
      select(all_of(range_vars)) %>%
      # ignore NA and -99
      mutate(across(everything(), ~ if_else(.x == -99, NA, .x))) %>%
      # suppress warnings - allow for Inf/-Inf bounds to range
      mutate(across(everything(), ~ paste(as.character(suppressWarnings(min(.x, na.rm=TRUE))), 
                                          "to", 
                                          as.character(suppressWarnings(max(.x, na.rm=TRUE)))))) %>%
      unique() %>%
      gather() %>%
      mutate(value = if_else(value == "Inf to -Inf", "All values missing", value))
  }
  
  ### take mean of subject-entered numbers ----
  mean_vars <- code_temp %>%
    filter((field_class %in% c("integer", "numeric") & field_type == "text") | field_type == "summary") %>%
    select(field_name) %>%
    unique() %>%
    pull(field_name)
  
  if (length(mean_vars) == 0) {
    means <- tibble(name = character(0), value = numeric(0))
  } else {
    means <- dataset %>%
      select(all_of(mean_vars)) %>%
      # ignore NA and -99
      mutate(across(everything(), ~ replace(.x, .x == -99, NA))) %>%
      colMeans(na.rm = TRUE) %>%
      enframe() %>%
      mutate(value = round(value, 2))
  }

  codebook_final <- code_temp %>%
    # temporary recoding
    mutate(choice = str_match(select_choices_or_calculations, "(-?[0-9a-zA-Z]+)")[,2]) %>%
    mutate(choice = replace_na(choice, "-98")) %>%
    # add frequencies and percents
    left_join(value_freqs, by = c("field_name", "choice")) %>%
    left_join(nonvalue_freqs, by = c("field_name", "choice")) %>%
    mutate(Frequency = coalesce(Frequency.x, Frequency.y),
           Percent = coalesce(Percent.x, Percent.y)) %>%
    # replace NA with zero (option wasn't in dataset) or remaining number of rows (only NA vs. not NA)
    group_by(field_name) %>%
    mutate(Frequency = replace_na(Frequency, nrow(dataset) - sum(Frequency, na.rm=TRUE)),
           Percent = replace_na(Percent, (nrow(dataset) - sum(Frequency, na.rm=TRUE))/nrow(dataset))) %>%
    ungroup() %>%
    # add means/ranges
    left_join(means, join_by(field_name == name)) %>%
    left_join(ranges, join_by(field_name == key)) %>%
    rename(Mean = value.x, Range = value.y) %>%
    # replace NA (-98) with description
    mutate(select_choices_or_calculations = case_when(
      is.na(select_choices_or_calculations) & field_class %in% c("numeric", "integer", "Date") ~ "Range",
      is.na(select_choices_or_calculations) & field_class == "character" ~ "Written response supplied",
      .default = select_choices_or_calculations)
    )
}
