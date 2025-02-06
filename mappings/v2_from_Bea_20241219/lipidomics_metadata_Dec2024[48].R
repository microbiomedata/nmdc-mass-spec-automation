library(tidyverse)
library(jsonlite)
library(readxl)

setwd("C:/Users/melu925/OneDrive - PNNL/Documents/NMDC/Katherine_lipidomics_metadata")

#################### API functions
get_first_page_results <- function(collection, filter, max_page_size, fields) {
  og_url <- paste0(
    'https://api.microbiomedata.org/nmdcschema/', 
    collection, '?&filter=', filter, '&max_page_size=', 
    max_page_size, '&projection=', fields
  )
  
  response <- jsonlite::fromJSON(URLencode(og_url, repeated = TRUE))
  return(response)
}

get_next_results <- function(collection, filter_text, max_page_size, fields) {
  initial_data <- get_first_page_results(collection, filter_text, max_page_size, fields)
  results_df <- initial_data$resources
  
  if (!is.null(initial_data$next_page_token)) {
    next_page_token <- initial_data$next_page_token
    
    while (TRUE) {
      url <- paste0('https://api.microbiomedata.org/nmdcschema/', collection, 
                    '?&filter=', filter_text, '&max_page_size=', max_page_size, 
                    '&page_token=', next_page_token, '&projection=', fields)
      
      response <- jsonlite::fromJSON(URLencode(url, repeated = TRUE))
      results_df <- results_df %>% bind_rows(response$resources)
      next_page_token <- response$next_page_token
      
      if (is.null(next_page_token)) {
        break
      }
    }
  }
  
  return(results_df)
}

get_results_by_id <- function(collection, match_id_field, id_list, fields, max_id = 50) {
  # collection: the name of the collection to query
  # match_id_field: the field in the new collection to match to the id_list
  # id_list: a list of ids to filter on
  # fields: a list of fields to return
  # max_id: the maximum number of ids to include in a single query
  
  # If id_list is longer than max_id, split it into chunks of max_id
  if (length(id_list) > max_id) {
    id_list <- split(id_list, ceiling(seq_along(id_list)/max_id))
  } else {
    id_list <- list(id_list)
  }
  
  output <- list()
  for (i in 1:length(id_list)) {
    # Cast as a character vector and add double quotes around each ID
    mongo_id_string <- as.character(id_list[[i]]) %>%
      paste0('"', ., '"') %>%
      paste(collapse = ', ')
    
    # Create the filter string
    filter = paste0('{"', match_id_field, '": {"$in": [', mongo_id_string, ']}}')
    
    # Get the data
    output[[i]] = get_next_results(
      collection = collection,
      filter = filter,
      max_page_size = max_id*3, #assumes that there are no more than 3 records per query
      fields = fields
    )
  }
  output_df <- bind_rows(output)
}

#################### Assemble biosample ID info (Stegen, Brodie)

# read in JSON from Katherine/Alicia
biosamples_to_find <- fromJSON("20231109.lipidomics_nmdc.json")$resources %>%
  mutate(across(where(is.list), as.character)) %>%
  flatten()

# this JSON is pre-reIDing datageneration records for lipids
# find the new biosample IDs by searching for biosamples with has_input among the alternate identifiers
biosamples <- get_results_by_id(
  collection = "biosample_set",
  match_id_field = "alternative_identifiers",
  id_list = biosamples_to_find$has_input,
  fields = "id,name,alternative_identifiers,gold_biosample_identifiers,igsn_biosample_identifiers,emsl_biosample_identifiers,associated_studies"
  ) %>%
  bind_rows(get_results_by_id(
    collection = "biosample_set",
    match_id_field = "gold_biosample_identifiers",
    id_list = biosamples_to_find$has_input,
    fields = "id,name,alternative_identifiers,gold_biosample_identifiers,igsn_biosample_identifiers,emsl_biosample_identifiers,associated_studies"
  )) %>%
  bind_rows(get_results_by_id(
    collection = "biosample_set",
    match_id_field = "igsn_biosample_identifiers",
    id_list = biosamples_to_find$has_input,
    fields = "id,name,alternative_identifiers,gold_biosample_identifiers,igsn_biosample_identifiers,emsl_biosample_identifiers,associated_studies"
  )) %>%
  bind_rows(get_results_by_id(
    collection = "biosample_set",
    match_id_field = "emsl_biosample_identifiers",
    id_list = biosamples_to_find$has_input,
    fields = "id,name,alternative_identifiers,gold_biosample_identifiers,igsn_biosample_identifiers,emsl_biosample_identifiers,associated_studies"
  )) %>%
  distinct() %>%
  mutate(across(where(is.list), as.character))


# collapse the HELPFUL identifiers found in query into one column for joining
biosamples <- biosamples %>%
  
  # cleanup - it has a bunch of "NULL" as a string rather than NULL values
  mutate(across(everything(), ~na_if(.x, "NULL"))) %>%
  
  # prioritize emsl identifier, then igsn identifier, then gold identifier
  mutate(ids_for_joining = case_when(
    !is.na(emsl_biosample_identifiers) ~ emsl_biosample_identifiers,
    !is.na(igsn_biosample_identifiers) ~ igsn_biosample_identifiers,
    !is.na(gold_biosample_identifiers) ~ gold_biosample_identifiers,
    .default = "FIX ME")) %>%
  rename(biosample_id = id)

# join with json info to connect nmdc biosample IDs to dataset names (lipidomics datagen names)
joined <- biosamples_to_find %>%
  # join with just the information we care about
  left_join(select(biosamples, biosample_id, ids_for_joining, associated_studies),
            by = join_by(has_input == ids_for_joining))


#################### Add Blanchard biosamples

# Read in mapping file from Excel and clean up to just lipid samples
blanchard_mapping_cleaned <- read_xlsx("EMSL_49483_Blanchard_DataMapping_2024-11-27.xlsx") %>%
  rename_all(~ make.names(.)) %>%
  select(id, matches("lipid")) %>%
  na.omit() %>%
  pivot_longer(cols = matches("lipid"), names_to = "mode", values_to = "dataset_name") %>%
  select(-mode) %>%
  # rename columns to match joined dataframe
  rename(biosample_id = id, 
         name = dataset_name) %>%
  mutate(associated_studies = "nmdc:sty-11-8ws97026")

joined <- bind_rows(joined, blanchard_mapping_cleaned)


#################### Get other info from DMS (instrument, run dates)

# This feels really hacky but here goes
# Print out the list of dataset names you want the information for into the R console

print(paste0("'", paste(joined$name, collapse="', '"), "'"), quote = FALSE, row.names = FALSE)

# Copypaste that into the following SQL query
# SELECT
#   public.t_dataset.dataset_id,
#   public.t_dataset.dataset,
#   public.t_dataset.acq_time_start,
#   public.t_dataset.acq_time_end,
#   public.t_instrument_name.instrument
# FROM 
#   public.t_dataset 
#   INNER JOIN public.t_instrument_name ON public.t_dataset.instrument_id = public.t_instrument_name.instrument_id
# WHERE 
#   public.t_dataset.dataset IN (PRINTOUT GOES HERE)

# save that output as a CSV to read in here
dms_query_results <- read_csv("dms_lipidomics_query_output.csv")

joined <- left_join(joined, dms_query_results, by = join_by(name == dataset))


#################### Write out correctly formatted tables

data.frame(
  "Biosample ID" = joined$biosample_id,
  "Associated Studies" = joined$associated_studies,
  "Processing Type" = "",
  "Raw Data File" = paste0(joined$name, ".raw"),
  "Raw Data Object Alt ID" = "",
  "Proessed Data Directory" = "",
  "mass spec configuration name" = ifelse(str_detect(joined$name, "_POS_"), 
                                          "EMSL lipidomics DDA mass spectrometry method, positive",
                                          "EMSL lipidomics DDA mass spectrometry method, negative"),
  "lc config name" = "EMSL LC method for non-polar metabolites",
  "instrument used" = joined$instrument,
  "processing instutition" = "EMSL",
  "instrument analysis start date" = joined$acq_time_start,
  "instrument analysis end date" = joined$acq_time_end,
  "execution resrouce" = "EMSL-RZR",
  check.names = FALSE
  ) %>%
  split(f = as.factor(.$`Associated Studies`)) %>%
  map2(.x = ., .y = names(.), ~write_csv(x = .x, file = paste0(gsub(":", "_", .y), ".csv")))

