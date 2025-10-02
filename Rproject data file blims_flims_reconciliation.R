library(readxl)
library(janitor)
library(here)
library(dplyr)

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

# Check if there are any duplicates using just blims_sample_i_ds as unique identifier, shouldn't be any
lab_list2 %>% count(blims_sample_i_ds) %>% filter(n > 1)
 #all just text descriptions or NA entries so fine to leave

# Check if there are any duplicates using just flims_sample_i_ds as unique identifier, shouldn't be any
lab_list2 %>% count(flims_sample_i_ds) %>% filter(n > 1)
#one sample is found as dup - EF0825B0073, other is one identified as cancelled, rest are NA. 
#flims_sample_i_ds EF0825B0073 has two different blims_i_ds: EF0824B00028 (shows up as one wdl_sample_id) and EF0824B00041 (shows up as one wdl_sample_id))
##Flag to come back to


wdl6 <- wdl5 %>% filter(wdl_sample_id %in% lab_list2$blims_sample_i_ds)

#capture in new subset which wdl IDs are NOT in the blims sample ids column and 

print(wdl5)


#combine dataframes
data_bound <- bind_rows(wdl5, lab_list2)

match(data_bound$wdl_sample_id, data_bound$blims_sample_i_ds)


data_bound %>% summarize(across(everything(), ~sum(is.na(.x))))
#there are NA values and likely some of the rows with NA in all of the columns for sample ids 

names(data_bound)

data_bound1 <- filter(data_bound, wdl_sample_id == "NA" & blims_sample_i_ds == "NA" & flims_sample_i_ds== "NA") 
#there are no rows where NA values in all three of these columns


#look for where sample IDs match between the lab list and wdl
data_bound %>%
  count(wdl_sample_id, blims_sample_i_ds) %>% filter(n>1)
#n = 306

#pivot wider to align the sample IDs 

#data_bound_wide <- data_bound %>%
 # pivot_wider(id_cols = c(blims_sample_i_ds, 
                         # names_from = ))


