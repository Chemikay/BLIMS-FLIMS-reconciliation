library(readxl)
library(janitor)
library(here)
library(dplyr)
library(tidyverse)
library(data.table)
library(writexl)

#Pull in all data except lab file
lab_file <- "BLIMS - FLIMS Transfer (1).xlsx"

all_files <- list.files(pattern='*.xlsx', recursive = TRUE)
wdl_files <- setdiff(all_files, lab_file)
wdl.list <- lapply(wdl_files, read_excel)

#combine the datasets
wdl1 <- rbindlist(wdl.list, fill = TRUE)

wdl1 <- clean_names(wdl1)

#read in crosswalk from lab
lab_list <- read_excel("BLIMS - FLIMS Transfer (1).xlsx")

lab_list <- clean_names(lab_list)

names(wdl1)
names(lab_list)

#remove unnecessary columns from both dataframes

wdl2 <- select(wdl1, data_owner, data_status, long_station_name, short_station_name, station_number, description, sample_code)

lab_list1 <- select(lab_list, station_name, error_occurred_skipped, flims_sample_i_ds, blims_flims_complete, key,
                    awaiting_field_spreadsheet, blims_sample_i_ds, requires_lab_update)

#rename columns in lab's list to match what's in wdl, adjust those in wdl for ease of use

view(lab_list1$station_name)
  #lab's ID of the station is not the official short/long/number for the station submitted by field group, so will have to deal with this later to match up

lab_list2 <- lab_list1 %>%
  rename('station_lab' = 'station_name')
    
wdl3 <- wdl2 %>%
  rename(
    'data_status_wdl' = 'data_status',
    'long_station_wdl' = 'long_station_name',
    'short_station_wdl' = 'short_station_name',
    'station_num_wdl' = 'station_number',
    'wdl_sample_id' = 'sample_code')

#prepare to combine

str(wdl3)
str(lab_list2)

#wdl dataframe has data broken down to analyte level unlike lab dataframe, so need to select just row for each, remove by selecting the first one

wdl4 <- wdl3 %>%  distinct(wdl_sample_id, data_owner,data_status_wdl, long_station_wdl,short_station_wdl,station_num_wdl,description)

# Check if there are any duplicates using just wdl_sample_id as unique identifier. shouldn't be any
wdl4 %>% count(wdl_sample_id) %>% filter(n > 1)
#two cases

#take a closer look
dups <- wdl4 %>%
  count(wdl_sample_id) %>%
  filter(n>1) %>%
  select(-n) %>%
  left_join(wdl4, by = join_by(wdl_sample_id))
#these are NA columns for the sample ID and can be removed

wdl5 <- wdl4 %>% 
  drop_na(wdl_sample_id)

  
lab_list2 <- mutate(lab_list2, 
                  error_occurred_skipped = as.character(error_occurred_skipped),
                  blims_flims_complete = as.character(blims_flims_complete),
                  key = as.character(key))

#remove rows without any sample IDs
lab_list3 <- filter(lab_list2, blims_sample_i_ds != "NA" | flims_sample_i_ds != "NA") 
#there are no rows where NA values in all three of these columns


# Check if there are any duplicates using just blims_sample_i_ds as unique identifier, shouldn't be any
lab_list3 %>% count(blims_sample_i_ds) %>% filter(n > 1)
 #all just text descriptions entries so fine to leave

# Check if there are any duplicates using just flims_sample_i_ds as unique identifier, shouldn't be any
lab_list3 %>% count(flims_sample_i_ds) %>% filter(n > 1)
#one sample is found as dup - EF0825B0073, other is one identified as cancelled, rest are NA. 
#flims_sample_i_ds EF0825B0073 has two different blims_i_ds: EF0824B00028 (shows up as one wdl_sample_id) and EF0824B00041 (shows up as one wdl_sample_id))
##Flag to come back to

## looking to see which IDs on WDL are flims IDs rather than blims IDs

#wdl sample IDs that are reflected in blims sample IDs
wdl_in_blims <- wdl5 %>% filter(wdl_sample_id %in% lab_list3$blims_sample_i_ds)
#706 total samples

#wdl sample IDs not reflected in blims sample IDs
wdl_not_in_blims <- wdl5 %>% filter(!(wdl_sample_id %in% lab_list3$blims_sample_i_ds))
#316 total samples

#double check if any of those IDs not reflected in blims sample IDs are captured in flims IDs
wdl_not_in_blims_but_flims <-wdl_not_in_blims %>% filter(wdl_sample_id %in% lab_list3$flims_sample_i_ds)
#46 total samples

#wdl sample IDs reflected in the flims sample IDs
wdl_in_flims <- wdl5 %>% filter(wdl_sample_id %in% lab_list3$flims_sample_i_ds)
#46 total samples

#are the sample IDs in 'wdl_not_in_blims_but_flims' and 'wdl_in_flims' the same?
check1 <- wdl_not_in_blims_but_flims %>% filter(wdl_sample_id %in% wdl_in_flims$wdl_sample_id)
#46 total samples so yes, these are the exact same sample IDs.

#double check if any of those flims IDs from lab list happen to also be the blims sample IDs
wdl_in_flims_in_blims <- wdl_in_flims %>% filter(wdl_sample_id %in% lab_list3$blims_sample_i_ds)
#0 total samples

#save file of output
write_xlsx(wdl_not_in_blims_but_flims, "C:/Users/krein/Documents/wdl_not_in_blims_but_flims.xlsx")


