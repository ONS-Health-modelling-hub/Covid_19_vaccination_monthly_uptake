#--------------
# Load packages
#--------------

library(sparklyr)
library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(writexl)

#----------------------------
# Set up the spark connection
#----------------------------

config <- spark_config()
config$spark.executor.memory <- "30g"
config$spark.yarn.executor.memoryOverhead <- "3g"
config$spark.executor.cores <- 6
config$spark.dynamicAllocation.enabled <- "true"
config$spark.dynamicAllocation.maxExecutors <- 15
config$spark.sql.shuffle.partitions <- 300
config$spark.shuffle.service.enabled <- "true"

sc <- spark_connect(master = "yarn-client",
                    app_name = "monthly_vacc_rates",
                    config = config)

#-------------
# User options
#-------------

# Set user-defined parameters
run_year <- 2023
run_month <- 1
min_age <- 18
max_age <- 1000
dataset_version <- ""

# Optional running tasks
run_data_checks <- TRUE
run_linkage <- TRUE
run_counts <- TRUE
run_disclosure_checks <- TRUE

if (min_age == 18){
  run_age_specific <- TRUE
} else if (min_age == 50){
  run_age_specific <- FALSE
} else {stop("Non-standard minimum age given")}

#--------------
# Set variables
#--------------

temp_date <- paste(run_year, ifelse(run_month < 10, paste0("0", run_month), run_month), "01", sep = "-") 
period_end <- paste(run_year, ifelse(run_month < 10, paste0("0", run_month), run_month), days_in_month(as.Date(temp_date)), sep = "-")

#---------------
# Set file paths
#---------------

# Set folder path
uptake_folder <- ""
project_folder <- paste0(uptake_folder, "")
current_month_folder <- paste0(project_folder, run_year, "_", ifelse(run_month < 10, paste0("0", run_month), run_month), "/")
code_folder <- paste0(current_month_folder, "")
outputs_folder <- paste0(current_month_folder, "")
intermediates_folder <- paste0(outputs_folder, "")

for (folder in c(current_month_folder, code_folder, outputs_folder, intermediates_folder)){
  if (!dir.exists(folder)){
    dir.create(folder)  
  }  
}

#---------------
# Load functions
#---------------

functions_folder <- paste0(project_folder, "")
functions_list <- paste(functions_folder, list.files(functions_folder), sep="/")
for(i in 1:length(functions_list)) {source(functions_list[i])}

#----------
# Load data
#----------

# Linked vaccination dataset
# Select only the required variables to optimise the runtime.
df <- sdf_sql(sc, paste0("SELECT ",
                         "cen_pr_flag, present_in_census, present_in_gpes, uresindpuk11_census, rgn_derived, ", 
                         "vaccine_number_1_vaccination_date_vacc, vaccine_number_2_vaccination_date_vacc, ",
                         "vaccine_number_3_vaccination_date_vacc, ",
                         "dob_census, dod_deaths, dor_deaths, ",
                         "dodyr_deaths, dodmt_deaths, sex_census, ethpuk11_census, relpuk11_census, cob_census, ",
                         "mainlangprf11_census, age_at_rollout_years_vacc, vaccine_number_1_gender_vacc, ",
                         "hlqpuk11_census, disability_census, care_home_pr19, nsshuk11_census, tenhuk11_census, ",
                         "rural_urban_derived, lsoa_derived ",
                         "FROM ", "", dataset_version))

# Processing to convert vaccine date to date format:
df <- df %>%
  rename(first_vacc_date = vaccine_number_1_vaccination_date_vacc,
         second_vacc_date = vaccine_number_2_vaccination_date_vacc,
         third_vacc_date = vaccine_number_3_vaccination_date_vacc,
         age_when_eligible_years = age_at_rollout_years_vacc,
         gender_vacc = vaccine_number_1_gender_vacc,
         dor = dor_deaths,
         dodyr = dodyr_deaths,
         dodmt = dodmt_deaths,
         dod = dod_deaths,
         dob = dob_census) %>%
  mutate(first_vacc_date = to_date(first_vacc_date, "yyyyMMdd"),
         second_vacc_date = to_date(second_vacc_date, "yyyyMMdd"),
         third_vacc_date = to_date(third_vacc_date, "yyyyMMdd"))

# Index of Multiple Deprivation (IMD)
df_imd <- spark_read_csv(sc, name = "IMD", path = "", header = TRUE, delimiter = ",")

# NSPL
df_nspl <- sdf_sql(sc, "SELECT * FROM ", "")

# LA names lookup
df_la_names <- spark_read_csv(sc, name = "df_la_names", path = "", header = TRUE, delimiter = ",") %>%
  select(ltla22cd, utla22cd, la_name = utla22nm) %>%
  sdf_distinct()
# la_name is the name of the upper-tier local authority - e.g. "Suffolk", rather than "West Suffolk"

# Join NSPL to LA names
df_la_lookup <- left_join(df_nspl, df_la_names, by = c("laua" = "ltla22cd"))

# European standard population
df_esp <- as.data.frame(spark_read_csv(sc, name = "ESP", path = "", header = TRUE, delimiter = ","))

# Poisson distribution
df_poisson <- as.data.frame(spark_read_csv(sc,name = "Poisson", path = "", header = TRUE, delimiter = ","))

#------------
# Data checks
#------------

if (run_data_checks == TRUE) {

  first_date_of_year <- paste0(run_year, "-01-01")
  
  # Run checks to determine period_end and age filter
  check_deaths(df)
  check_latest_vaccinations(df, death_date = period_end, age_date = NA, min_age = NA)
  check_age_rates(df, vacc_date = NA, death_date = period_end, age_date = period_end, min_age = NA)
  check_missing_data(df, death_date = period_end, age_date = NA, min_age = NA)

  # Run checks with period_end and age filters
  check_deaths(df)
  check_latest_vaccinations(df, death_date = period_end, age_date = period_end, min_age = min_age)
  check_age_rates(df, vacc_date = period_end, death_date = period_end, age_date = period_end, min_age = min_age)
  check_missing_data(df, death_date = period_end, age_date = period_end, min_age = min_age)
  
}

#-------------------------
# Data linkage information
#-------------------------

if (run_linkage == TRUE) {

  get_linkage_information(df, vacc_date = period_end, death_date = period_end, vaccine_number = "first", min_age = min_age)

}

#---------------
# Filter dataset
#---------------

# Static filters
df <- df %>%
  # Keep only those who are present in census & GPES
  filter(cen_pr_flag == 1 & present_in_census == 1 & present_in_gpes == 1) %>%
  # Keep only those who were Usual Residents in Census 2011
  filter(uresindpuk11_census == 1) %>%
  # Remove those not living in England
  mutate(country = substr(rgn_derived, 1, 1)) %>%
  filter(country == "E") %>%
  # Remove those that received either a first or second dose prior to 8th December - likely involved in trials or erroneous data
  filter((is.na(first_vacc_date)) | (first_vacc_date >= "2020-12-08")) %>%
  filter((is.na(second_vacc_date)) | (second_vacc_date >= "2020-12-08")) %>%
  filter((is.na(third_vacc_date)) | (third_vacc_date >= "2021-09-16"))

#--------------------------
# Create derived variables
#--------------------------

df <- create_static_variables(df, df_imd, df_nspl)

#------------------------------------------------------------
# Group small local authorities with a nearby local authority
#------------------------------------------------------------

df <- df %>% 
  mutate(la_derived = ifelse(la_derived == "Isles of Scilly", "Cornwall", la_derived),
         la_derived = ifelse(la_derived == "City of London", "Hackney", la_derived))

#--------------------
# Alter ESP age bands
#--------------------

df_esp <- df_esp %>%
  select(age_group_esp = agegroup, ESP) %>%
  filter(!(age_group_esp %in% c("<1", "1-4", "5-9", "10-14"))) %>%
  mutate(ESP = ifelse(age_group_esp=="15-19", ESP*0.4, ESP)) %>%
  mutate(age_group_esp = ifelse(age_group_esp %in% c("15-19", "20-24"), "18-24", age_group_esp)) %>%
  group_by(age_group_esp) %>%
  summarise(ESP = sum(ESP, na.rm=TRUE)) %>%
  ungroup()    

#-----------------------------------------------------------
# Add year and month variables to the dataset in tidy format
#-----------------------------------------------------------

# Dynamically create year variables
years <- paste0("year_", 2020:run_year)

for (i in 1:length(years)) {
  df <- df %>%
    mutate(!!sym(years[i]) := 1)
}

df <- df %>%
  pivot_longer(cols = all_of(years),
               names_to = "year",
               values_to = "dummy_year")

# Months can be defined as static from 1 to 12
df <- df %>%
  mutate(month_01 = 1, month_02 = 1, month_03 = 1, month_04 = 1, month_05 = 1, month_06 = 1,
         month_07 = 1, month_08 = 1, month_09 = 1, month_10 = 1, month_11 = 1, month_12 = 1) %>%
  pivot_longer(cols = c("month_01", "month_02", "month_03", "month_04", "month_05", "month_06", 
                        "month_07", "month_08", "month_09", "month_10", "month_11", "month_12"),
               names_to = "month",
               values_to = "dummy_month") 

df <- df %>%
  select(-dummy_year, -dummy_month) %>%
  mutate(year = substr(year, 6, 10),
         month = substr(month, 7, 8)) %>%
  mutate(month_numeric = as.numeric(month)) %>%
  filter(!(year == "2020" & month != "12") & !(year == run_year & month_numeric > run_month))

#-----------
# Get rates
#-----------

#---------------------
# Age group and Region
#---------------------

group_vars <- c("age_group", "age_group_esp", "region")
exposures <- c("sex", "disability", "ethnicity", "religion", "country_of_birth", "language", "imd_quintile", "imd_quintile_region", "nssec_agg", "education", "tenure", "rural_urban")

if (run_counts == TRUE) {

  df_exposures <- purrr::map_dfr(exposures,
                                 function(exp){
                                   df_exp <- get_monthly_counts(df, min_age=min_age, exposure=exp, group_vars=group_vars) %>%
                                   return(df_exp)
                                 })
  
  df_exposures <- df_exposures %>%
    filter(!(year == "2020" & vaccine_number != "first") &
           !(year == "2021" & !month %in% c("11", "12") & !vaccine_number %in% c("first", "second")))
  
  write.csv(df_exposures, paste0(intermediates_folder, "rates_exposures_", paste(group_vars, collapse = "_"), "_", min_age, "+.csv"), row.names=FALSE)    

}

#--------------------------
# Read in intermediate file
#--------------------------

df_intermediate <- read.csv(paste0(intermediates_folder, "rates_exposures_", paste(group_vars, collapse = "_"), "_", min_age, "+.csv"), stringsAsFactors=FALSE)

#----------
# Get rates
#----------

#-----------------
# Age standardised
#-----------------

# England, overall, all ages
df1 <- df_intermediate %>%
  filter(exposure == "sex") %>%
  group_by(vaccine_number, year, month, age_group_esp) %>%
  summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
            population = sum(population, na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
df1 <- get_asmrs(df1, df_esp, df_poisson, min_age = min_age, max_age = max_age, group_vars = c("vaccine_number", "year", "month")) %>%
  mutate(exposure = "Total",
         group = "Total",
         SubCategory = "England")

# England, by exposure, all ages
df2 <- df_intermediate %>%
  filter(exposure != "imd_quintile_region") %>%
  group_by(vaccine_number, year, month, exposure, group, age_group_esp) %>%
  summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
            population = sum(population, na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
df2 <- get_asmrs(df2, df_esp, df_poisson, min_age = min_age, max_age = max_age, group_vars = c("vaccine_number", "year", "month", "exposure", "group")) %>%
  mutate(SubCategory = "England")

# Regions, overall, all ages
df3 <- df_intermediate %>%
  filter(exposure == "sex") %>%
  group_by(vaccine_number, year, month, region, age_group_esp) %>%
  summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
            population = sum(population, na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
df3 <- get_asmrs(df3, df_esp, df_poisson, min_age = min_age, max_age = max_age, group_vars = c("vaccine_number", "year", "month", "region")) %>%
  mutate(exposure = "Total",
         group = "Total") %>%
  rename(SubCategory = region)

# Regions, by exposure, all ages
df4 <- df_intermediate %>%
  filter(exposure != "imd_quintile") %>%
  group_by(vaccine_number, year, month, exposure, group, region, age_group_esp) %>%
  summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
            population = sum(population, na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
df4 <- get_asmrs(df4, df_esp, df_poisson, min_age = min_age, max_age = max_age, group_vars = c("vaccine_number", "year", "month", "exposure", "group", "region")) %>%
  rename(SubCategory = region)

#-------------
# Age specific
#-------------

if (run_age_specific == TRUE) {
  
  # England, overall, by age group
  df5 <- df_intermediate %>%
    filter(exposure == "sex") %>%
    group_by(vaccine_number, year, month, age_group) %>%
    summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
              population = sum(population, na.rm=TRUE)) %>%
    ungroup() %>%
    mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
  df5 <- get_crude_rates(df5, df_poisson, group_vars = c("vaccine_number", "year", "month", "age_group")) %>%
    mutate(exposure = "Total",
           group = "Total") %>%
    rename(SubCategory = age_group)

  # England, by exposure, by age group
  df6 <- df_intermediate %>%
    filter(exposure != "imd_quintile_region") %>%
    group_by(vaccine_number, year, month, exposure, group, age_group) %>%
    summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
              population = sum(population, na.rm=TRUE)) %>%
    ungroup() %>%
    mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
  df6 <- get_crude_rates(df6, df_poisson, group_vars = c("vaccine_number", "year", "month", "exposure", "group", "age_group")) %>%
    rename(SubCategory = age_group)

  # Regions, overall, by age group
  df7 <- df_intermediate %>%
    filter(exposure == "sex") %>%
    group_by(vaccine_number, year, month, region, age_group) %>%
    summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
              population = sum(population, na.rm=TRUE)) %>%
    ungroup() %>%
    mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
  df7 <- get_crude_rates(df7, df_poisson, group_vars = c("vaccine_number", "year", "month", "region", "age_group")) %>%
    mutate(exposure = "age_group") %>%
    rename(SubCategory = region,
           group = age_group)


  df_agg_region <- bind_rows(df1, df2, df3, df4, df5, df6, df7) 

} else {
  
  df_agg_region <- bind_rows(df1, df2, df3, df4)
   
}

# Add sex column and translate embedded sex rows into new column
df_agg_region <- df_agg_region %>%
  mutate(Sex = case_when(exposure == "sex" ~ group,
                         exposure != "sex" ~ "Persons",
                         is.na(exposure) ~ "Persons")) %>%
  mutate(group = case_when(exposure == "sex" ~ "Total",
                           exposure != "sex" ~ group)) %>%
  mutate(exposure = case_when(exposure == "sex" ~ "Total",
                              exposure != "sex" ~ exposure))

# Write to Intermediates folder 
write.csv(df_agg_region, paste0(intermediates_folder, "df_agg_region", "_", min_age, "+.csv"), row.names=FALSE)

#------------------------------
# Age group and local authority
#------------------------------

# Split the exposures to be able to execute the code without running out of memory
# Ethnicity, religion and rural_urban are excluded due to disclosure issues
group_vars <- c("age_group", "age_group_esp", "la_derived")
exposures1 <- c("sex", "disability", "country_of_birth", "language")

# Only include tenure for 18+ due to disclosure issues for 50+
if (min_age == 18) {
  exposures2 <- c("imd_quintile_la", "nssec_agg", "education", "tenure")
} else if (min_age == 50) {
  exposures2 <- c("imd_quintile_la", "nssec_agg", "education")
} else { 
  stop("Non-standard minimum age given") 
}

# Start time period for local authority breakdown from April 2021 onwards due to disclosure issues 
df_subset <- df %>%
  filter(year != "2020" & !(year == "2021" & month %in% c("01", "02", "03")))

# Put counts and rates calculations into a function to run each group of exposures consecutively
calculate_la_counts_rates <- function(exposures) {

  if (run_counts == TRUE) {

    df_exposures <- purrr::map_dfr(exposures,
                                   function(exp){
                                     df_exp <- get_monthly_counts(df_subset, min_age=min_age, exposure=exp, group_vars=group_vars) %>%
                                     return(df_exp)
                                   })

    df_exposures <- df_exposures %>%
      filter(!(year == "2020" & vaccine_number != "first") &
             !(year == "2021" & !month %in% c("11", "12") & !vaccine_number %in% c("first", "second")))

    write.csv(df_exposures, paste0(intermediates_folder, "rates_exposures_", paste(group_vars, collapse = "_"), "_", paste(exposures, collapse = "_"), "_", min_age, "+.csv"), row.names=FALSE)    

  }

  #--------------------------
  # Read in intermediate file
  #--------------------------

  df_intermediate_la <- read.csv(paste0(intermediates_folder, "rates_exposures_", paste(group_vars, collapse = "_"), "_", paste(exposures, collapse = "_"), "_", min_age, "+.csv"), stringsAsFactors=FALSE)

  #----------
  # Get rates
  #----------

  #-----------------
  # Age standardised
  #-----------------

  # Local authorities, overall, all ages

  if ("sex" %in% exposures) { 

    df1_la <- df_intermediate_la %>%
      filter(exposure == "sex") %>%
      group_by(vaccine_number, year, month, la_derived, age_group_esp) %>%
      summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
                population = sum(population, na.rm=TRUE)) %>%
      ungroup() %>%
      mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
    df1_la <- get_asmrs(df1_la, df_esp, df_poisson, min_age = min_age, max_age = max_age, group_vars = c("vaccine_number", "year", "month", "la_derived")) %>%
      mutate(exposure = "Total",
             group = "Total") %>%
      rename(SubCategory = la_derived)

  }

  # Local authorities, by exposure, all ages
  df2_la <- df_intermediate_la %>%
    group_by(vaccine_number, year, month, exposure, group, la_derived, age_group_esp) %>%
    summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
              population = sum(population, na.rm=TRUE)) %>%
    ungroup() %>%
    mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))  
  df2_la <- get_asmrs(df2_la, df_esp, df_poisson, min_age = min_age, max_age = max_age, group_vars = c("vaccine_number", "year", "month", "exposure", "group", "la_derived")) %>%
    rename(SubCategory = la_derived)

  #-------------
  # Age specific
  #-------------

  if ("sex" %in% exposures) { 

    # Removed age specific filters
    # Local authorities, overall, by age group
    df3_la <- df_intermediate_la %>%
      filter(exposure == "sex") %>%
      group_by(vaccine_number, year, month, la_derived, age_group) %>%
      summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
                population = sum(population, na.rm=TRUE)) %>%
      ungroup() %>%
      mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
    df3_la <- get_crude_rates(df3_la, df_poisson, group_vars = c("vaccine_number", "year", "month", "la_derived", "age_group")) %>%
      mutate(exposure = "age_group") %>%
      rename(SubCategory = la_derived,
             group = age_group)

    df_agg_la <- bind_rows(df1_la, df2_la, df3_la)

  } else {

    df_agg_la <- df2_la

  }

  # Add sex column and translate embedded sex rows into new column
  df_agg_la <- df_agg_la %>%
    mutate(Sex = case_when(exposure == "sex" ~ group,
                           exposure != "sex" ~ "Persons",
                           is.na(exposure) ~ "Persons")) %>%
    mutate(group = case_when(exposure == "sex" ~ "Total",
                             exposure != "sex" ~ group)) %>%
    mutate(exposure = case_when(exposure == "sex" ~ "Total",
                                exposure != "sex" ~ exposure))

  # Write to Intermediates folder 
  write.csv(df_agg_la, paste0(intermediates_folder, "df_agg_la_", paste(exposures, collapse = "_"), "_", min_age, "+.csv"), row.names=FALSE)

}

calculate_la_counts_rates(exposures1)
calculate_la_counts_rates(exposures2)


#----
# Sex
#----

group_vars <- c("age_group_esp", "sex")
exposures <- c("disability", "ethnicity", "religion", "country_of_birth", "language", "imd_quintile", "nssec_agg", "education", "tenure", "rural_urban")

if (run_counts == TRUE) {

  df_exposures <- purrr::map_dfr(exposures,
                                 function(exp){
                                   df_exp <- get_monthly_counts(df, min_age=min_age, exposure=exp, group_vars=group_vars) %>%
                                   return(df_exp)
                                 })
  
  df_exposures <- df_exposures %>%
    filter(!(year == "2020" & vaccine_number != "first") &
           !(year == "2021" & !month %in% c("11", "12") & !vaccine_number %in% c("first", "second")))
  
  write.csv(df_exposures, paste0(intermediates_folder, "rates_exposures_", paste(group_vars, collapse = "_"), "_", min_age, "+.csv"), row.names=FALSE)    

}

#--------------------------
# Read in intermediate file
#--------------------------

df_intermediate_sex <- read.csv(paste0(intermediates_folder, "rates_exposures_", paste(group_vars, collapse = "_"), "_", min_age, "+.csv"), stringsAsFactors=FALSE)

#----------
# Get rates
#----------

#-----------------
# Age standardised
#-----------------

# England, by exposure, all ages, by sex
df_agg_sex <- df_intermediate_sex %>%
  group_by(vaccine_number, year, month, exposure, group, sex, age_group_esp) %>%
  summarise(vaccinated = sum(vaccinated, na.rm=TRUE),
            population = sum(population, na.rm=TRUE)) %>%
  ungroup() %>%
  mutate(vaccinated = ifelse(vaccine_number == "first", population - vaccinated, vaccinated))
df_agg_sex <- get_asmrs(df_agg_sex, df_esp, df_poisson, min_age = min_age, max_age = max_age, group_vars = c("vaccine_number", "year", "month", "exposure", "group", "sex")) %>%
  mutate(SubCategory = "England") %>%
  rename(Sex = sex)

# Write to Intermediates folder 
write.csv(df_agg_sex, paste0(intermediates_folder, "df_agg_sex", "_", min_age, "+.csv"), row.names=FALSE)

#--------------------------------------------------
# Combine region, LA and sex tables into full table
#--------------------------------------------------

df_agg_region <- read.csv(paste0(intermediates_folder, "df_agg_region", "_", min_age, "+.csv"), stringsAsFactors = FALSE)
df_agg_la1 <- read.csv(paste0(intermediates_folder, "df_agg_la_", paste(exposures1, collapse = "_"), "_", min_age, "+.csv"), stringsAsFactors = FALSE)
df_agg_la2 <- read.csv(paste0(intermediates_folder, "df_agg_la_", paste(exposures2, collapse = "_"), "_", min_age, "+.csv"), stringsAsFactors = FALSE)
df_sex <- read.csv(paste0(intermediates_folder, "df_agg_sex", "_", min_age, "+.csv"), stringsAsFactors = FALSE)

df_agg <- bind_rows(df_agg_region, df_agg_la1, df_agg_la2, df_sex)
  
#------------------
# Disclosure checks
#------------------

if (run_disclosure_checks == TRUE) {

  disclosure_checks(df_agg, limit=10)

}

#-------------------------------------------------------------
# WARNING: Manual suppression to deal with secondary disclosure
#-------------------------------------------------------------

if (min_age == 18) {

  df_agg <- df_agg %>%
    mutate(disclosive = case_when(vaccinated < 10 ~ 1,
                                  population - vaccinated < 10 ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="religion" & group=="Jewish" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="religion" & group=="Jewish" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="religion" & group=="Buddhist" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="religion" & group=="Buddhist" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="religion" & group=="Jewish" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="religion" & group=="Jewish" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="religion" & group=="Buddhist" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="religion" & group=="Buddhist" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="first" & year==2020 & month==12 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="first" & year==2020 & month==12 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="first" & year==2020 & month==12 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="first" & year==2020 & month==12 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="East Midlands" ~ 1,
                                  TRUE ~ 0))
  
} else if (min_age == 50){

  df_agg <- df_agg %>%
    mutate(disclosive = case_when(vaccinated < 10 ~ 1,
                                  population - vaccinated < 10 ~ 1,
                                  Sex == "Persons" & exposure=="nssec_agg" & group=="Not classified" & SubCategory=="Rutland" ~ 1,
                                  Sex == "Persons" & exposure=="nssec_agg" & group=="Not classified" & SubCategory=="Derby" ~ 1,
                                  Sex == "Persons" & exposure=="nssec_agg" & group=="8 Never worked and long-term unemployed" & SubCategory=="Rutland" ~ 1,
                                  Sex == "Persons" & exposure=="nssec_agg" & group=="8 Never worked and long-term unemployed" & SubCategory=="Derby" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==8 & Sex == "Persons" & exposure=="language" & SubCategory=="Rutland" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==8 & Sex == "Persons" & exposure=="language" & SubCategory=="Derby" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="South West" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="South West" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="South West" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="South West" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="religion" & group=="Jewish" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="religion" & group=="Jewish" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="religion" & group=="Buddhist" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="religion" & group=="Buddhist" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="religion" & group=="Jewish" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="religion" & group=="Jewish" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="religion" & group=="Buddhist" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==2 & Sex == "Persons" & exposure=="religion" & group=="Buddhist" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="tenure" & group=="Not classified" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="tenure" & group=="Not classified" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="tenure" & group=="Other" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="second" & year==2021 & month==1 & Sex == "Persons" & exposure=="tenure" & group=="Other" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="first" & year==2020 & month==12 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="first" & year==2020 & month==12 & Sex == "Persons" & exposure=="ethnicity" & group=="Black Caribbean" & SubCategory=="East Midlands" ~ 1,
                                  vaccine_number=="first" & year==2020 & month==12 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="North East" ~ 1,
                                  vaccine_number=="first" & year==2020 & month==12 & Sex == "Persons" & exposure=="ethnicity" & group=="Bangladeshi" & SubCategory=="East Midlands" ~ 1,
                                  TRUE ~ 0))
  
} else {stop("Non-standard minimum age given")}

df_agg <- df_agg %>%
  mutate(vaccinated = ifelse(disclosive == 1, NA_integer_, vaccinated),
         crude_rate = ifelse(disclosive == 1, NA_real_, crude_rate),
         crude_lower_ci = ifelse(disclosive == 1, NA_real_, crude_lower_ci),
         crude_upper_ci = ifelse(disclosive == 1, NA_real_, crude_upper_ci),
         age_standardised_rate = ifelse(disclosive == 1, NA_real_, age_standardised_rate),
         age_standardised_lower_ci = ifelse(disclosive == 1, NA_real_, age_standardised_lower_ci),
         age_standardised_upper_ci = ifelse(disclosive == 1, NA_real_, age_standardised_upper_ci)) %>%
  select(!disclosive)

#-----------------------------------
# Re-structure data to longer format
#-----------------------------------

df_agg <- df_agg %>%
  pivot_longer(starts_with("crude"), names_to = "stat_type_crude", names_prefix = "crude_", values_to = "value_crude") %>%
  pivot_longer(starts_with("age_standardised"), names_to = "stat_type_age_standardised", names_prefix = "age_standardised_", values_to = "value_age_standardised") %>%
  filter(stat_type_crude == stat_type_age_standardised) %>%
  select(!stat_type_age_standardised) %>%
  rename(stat_type = stat_type_crude) %>%
  pivot_longer(starts_with("value"), names_to = "rate_type", names_prefix = "value_", values_to = "value") %>%
  pivot_wider(id_cols = c(vaccine_number, year, month, population, vaccinated, exposure, group, SubCategory, Sex, rate_type),
              names_from = stat_type, values_from = value) %>%
  filter(rate_type == "crude" | (rate_type == "age_standardised" & (exposure != "age_group" | is.na(exposure)) & !SubCategory %in% c("18-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80+"))) %>%
  select(vaccine_number, rate_type, year, month, exposure, group, SubCategory, Sex, population, vaccinated, rate, lower_ci, upper_ci)

#-----------------------
# Format aggregated file
#-----------------------

run_month_string <- case_when(run_month==1 ~ "January",
                              run_month==2 ~ "February",
                              run_month==3 ~ "March",
                              run_month==4 ~ "April",
                              run_month==5 ~ "May",
                              run_month==6 ~ "June",
                              run_month==7 ~ "July",
                              run_month==8 ~ "August",
                              run_month==9 ~ "September",
                              run_month==10 ~ "October",
                              run_month==11 ~ "November",
                              run_month==12 ~ "December")

df_agg <- df_agg %>%
  mutate(rate_type = gsub("_", "-", rate_type),
         month = case_when(month==1 ~ "Jan",
                           month==2 ~ "Feb",
                           month==3 ~ "Mar",
                           month==4 ~ "Apr",
                           month==5 ~ "May",
                           month==6 ~ "Jun",
                           month==7 ~ "Jul",
                           month==8 ~ "Aug",
                           month==9 ~ "Sep",
                           month==10 ~ "Oct",
                           month==11 ~ "Nov",
                           month==12 ~ "Dec"),
         exposure = case_when(exposure=="sex" ~ "Sex",
                              exposure=="age_group" ~ "Age group",
                              exposure=="disability" ~ "Disability status",
                              exposure=="ethnicity" ~ "Ethnic group",
                              exposure=="religion" ~ "Religion",
                              exposure=="country_of_birth" ~ "Country of birth",
                              exposure=="language" ~ "English language proficiency",
                              exposure=="imd_quintile" ~ "Deprivation quintile",
                              exposure=="imd_quintile_la" ~ "Local authority deprivation quintile",
                              exposure=="imd_quintile_region" ~ "Regional deprivation quintile",
                              exposure=="nssec_agg" ~ "National Statistics Socio-economic classification",
                              exposure=="education" ~ "Educational attainment",
                              exposure=="tenure" ~ "Household tenure",
                              exposure=="rural_urban" ~ "Rural-urban",
                              TRUE ~ exposure),
         general_footnote = paste0("1. Figures based on vaccinations administered between 08 December 2020 and ", substr(period_end, 9, 10), " ", run_month_string, " ", run_year, " for residents in England who could be linked to the 2011 Census and General Practice Extraction Service (GPES) Data for Pandemic Planning and Research (GDPPR).\n",
                                   "2. Therefore, these data only cover a subset of the population and should not be used to represent the total number of vaccinations. ",
                                   "For this reason, these data may also differ from the administrative data on vaccinations published by NHS England weekly, which cover all vaccinations given to individuals who have an NHS number and are currently alive in the resident population."),
         age_footnote = ifelse(!is.na(exposure) & exposure == "Educational attainment" & min_age < 30,
                               "3. These data only cover adults aged 30+",
                               paste0("3. These data only cover adults aged ", min_age, "+")),
         exposure_footnote = case_when(exposure %in% c("Rural-Urban") ~ paste("4. The", tolower(exposure), "information of an individual was derived from their resident address in the General Practice Extraction Service (GPES) data for pandemic planning and research. If this was not available, it was derived from the individual's resident address in the 2011 Census"),
                                       exposure %in% c("Deprivation quintile", "Regional deprivation quintile", "Local authority deprivation quintile") ~ "4. Index of Multiple Deprivation quintile was based on the English Index of Multiple Deprivation (IMD), version 2019. It was derived using the individual's resident address in the General Practice Extraction Service (GPES) Data for Pandemic Planning and Research. If this was not available, it was derived from the individual's resident address in the 2011 Census",
                                       exposure == "Household tenure" ~ paste("4.", exposure, "was derived from the 2011 Census. Not classified includes people living in communal establishments at the time of the 2011 Census."),
                                       exposure == "National Statistics Socio-economic classification" ~ paste("4.", exposure, "was derived from the 2011 Census. Not classified includes full-time students, people with insufficient occupation information and people in communal establishments at the time of the 2011 Census."),
                                       is.na(exposure) ~ "",
                                       TRUE ~ paste("4.", exposure, "was derived from the 2011 Census")),
         la_footnote = "5. For statistical disclosure control purposes, Cornwall (E06000052) includes Isles of Scilly (E06000053), Hackney (E09000012) includes City of London (E09000001), and the time period starts from April 2021 for all local authority breakdowns.") %>%
  mutate(Theme = "Vaccinations",
         IndDropdown = case_when(vaccine_number=="first" ~ paste0("Cumulative ", rate_type, " percentage of people aged ", min_age, "+ who have not received a vaccination"),
                                 vaccine_number=="second" ~ paste0("Cumulative ", rate_type, " percentage of people aged ", min_age, "+ who have received two vaccinations"),
                                 vaccine_number=="third" ~ paste0("Cumulative ", rate_type, " percentage of people aged ", min_age, "+ who have received three vaccinations")),
         ChartTitle = ifelse(is.na(exposure), IndDropdown, paste0(IndDropdown, ", by ", exposure)),
         TimePeriod = paste0(month, " ", year),
         TimePeriodLabel = TimePeriod,
         yAxisLabel = case_when(vaccine_number=="first" ~ paste0(toupper(substr(rate_type, 1, 1)), substr(rate_type, 2, nchar(rate_type)), " percentage of people aged ", min_age, "+ who have not received a vaccination (%)"),
                                vaccine_number=="second" ~ paste0(toupper(substr(rate_type, 1, 1)), substr(rate_type, 2, nchar(rate_type)), " percentage of people aged ", min_age, "+ who have received two vaccinations (%)"),
                                vaccine_number=="third" ~ paste0(toupper(substr(rate_type, 1, 1)), substr(rate_type, 2, nchar(rate_type)), " percentage of people aged ", min_age, "+ who have received three vaccinations (%)")),
         ValueNote = "NA",
         Footnote = paste0(general_footnote, "\n", age_footnote, "\n", exposure_footnote, "\n", la_footnote),
         Source = "NHS England National Immunisation Management System, NHS Digital General Practice Extraction Service, and Office for National Statistics mortality data and 2011 Census") %>%
  select(Theme,
         ChartTitle,
         IndDropdown,
         TimePeriod,
         TimePeriodLabel,
         CategoryType = exposure,
         Category = group,
         SubCategory,
         Sex,
         Count = vaccinated,
         Denominator = population,
         Value = rate,
         LCI = lower_ci,
         UCI = upper_ci,
         yAxisLabel,
         ValueNote,
         Footnote,
         Source)

#------------------
# Save main dataset
#------------------

df_dataset <- df_agg %>%
  select(Theme, ChartTitle, IndDropdown, TimePeriodLabel, CategoryType, Category, SubCategory, Sex, Count, Denominator, Value, LCI, UCI)

# Split the dataset by TimePeriodLabel for export due to its large size
df_split <- split(df_dataset, list(df_dataset$TimePeriodLabel))

# Create an ordered vector of months and years to output data chronologically
years  <- paste0(2020:run_year)
months <- 1:12
months_years <- crossing(months, years) %>%
  filter(!(years == 2020 & months != 12) & !(years == run_year & months > run_month)) %>%
  arrange(years, months) %>%
  mutate(months = case_when(months==1 ~ "Jan",
                            months==2 ~ "Feb",
                            months==3 ~ "Mar",
                            months==4 ~ "Apr",
                            months==5 ~ "May",
                            months==6 ~ "Jun",
                            months==7 ~ "Jul",
                            months==8 ~ "Aug",
                            months==9 ~ "Sep",
                            months==10 ~ "Oct",
                            months==11 ~ "Nov",
                            months==12 ~ "Dec")) %>%
  mutate(month_year = paste(months, years)) %>%
  select(month_year) %>%
  pull()

df_split <- df_split[months_years]

# Write to xlsx with one sheet for each month of data
write_xlsx(
  df_split,
  paste0(outputs_folder, "monthly_vaccination_rates_", run_year, "_", ifelse(run_month < 10, paste0("0", run_month), run_month), "_", min_age, "+.xlsx")
)

#---------------------
# Save metadata lookup
#---------------------

df_metadata <- df_agg %>%
  select(ChartTitle, yAxisLabel, Footnote, Source) %>%
  distinct()

write.csv(df_metadata, paste0(outputs_folder, "vaccination_metadata_lookup_", run_year, "_", ifelse(run_month < 10, paste0("0", run_month), run_month), "_", min_age, "+.csv"), row.names=FALSE)



