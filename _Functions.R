#------------#
# Check data #
#------------#

check_deaths <- function(df_vacc){
  
  # Check latest death registrations and occurrences

  cat("Death registrations by month")
  df_vacc %>%
    mutate(doryr = as.numeric(substr(dor,1,4)),
           dormt = as.numeric(substr(dor,5,6))) %>%
    group_by(doryr, dormt) %>%
    summarise(n = n()) %>%
    ungroup() %>%
    collect() %>%
    arrange(doryr, dormt) %>%
    print()
  cat("Death registrations by day")
  df_dor_trend <- df_vacc %>%  
    filter(!is.na(dor) & dor >= "2021-01-01") %>%
    group_by(dor) %>%
    summarise(n = n()) %>%
    ungroup() %>%
    collect() %>%
    arrange(dor)
  df_dor_trend %>% print()
  gg <- ggplot(df_dor_trend, aes(x = dor, y = n)) + 
    geom_col()
  print(gg)
  
  cat("Death occurrences by month")
  df_vacc %>%
    mutate(dodyr = as.numeric(dodyr),
           dodmt = as.numeric(dodmt)) %>%
    group_by(dodyr, dodmt) %>%
    summarise(n = n()) %>%
    ungroup() %>%
    collect() %>%
    arrange(dodyr, dodmt) %>%
    print()
  cat("Death occurrences by day")
  df_dod_trend <- df_vacc %>%
    filter(!is.na(dod) & dod >= "2021-01-01") %>%
    group_by(dod) %>%
    summarise(n = n()) %>%
    ungroup() %>%
    collect() %>%
    arrange(dod)
  df_dod_trend %>% print()
  gg <- ggplot(df_dod_trend, aes(x = dod, y = n)) + 
    geom_col()
  print(gg)

}

check_latest_vaccinations <- function(df_vacc, death_date, age_date = NA, min_age = NA, max_age = NA){

  df_vacc <- df_vacc %>%
    # Keep only those who are present in census & GPES
    filter(present_in_gpes == 1, cen_pr_flag == 1, present_in_census == 1) %>%
    # Remove those not living in England
    mutate(country = substr(rgn_derived, 1, 1)) %>%
    filter(country == "E")
  
  if (!is.na(death_date)){
    df_vacc <- df_vacc %>%
      # Keep only people alive by the cut-off date
      filter(is.na(dod) | dod >= death_date)
  }
  
  # Calculate age
  df_vacc <- df_vacc %>%
    mutate(age = datediff(age_date, dob)/365.25)

  # Remove those outside age range if applicable
  if (!is.na(min_age) & is.na(max_age)){
    df_vacc <- df_vacc %>%
      filter(age >= min_age)
  } else if (is.na(min_age) & !is.na(max_age)){
    df_vacc <- df_vacc %>%
      filter(age < max_age)    
  } else if (!is.na(min_age) & !is.na(max_age)){
    df_vacc <- df_vacc %>%
      filter(age >= min_age & age < max_age)    
  } # else no filtering

  # Check latest vaccination date and counts
  cat("First dose vaccinations by day")
  df_vacc_trend <- df_vacc %>% 
    mutate(first_vacc_date = to_date(first_vacc_date)) %>%
    group_by(first_vacc_date) %>%
    summarise(n = n()) %>%
    ungroup() %>%
    collect() %>%
    arrange(first_vacc_date) %>%
    mutate(n_cum = cumsum(n))
  print(df_vacc_trend)
  gg <- ggplot(df_vacc_trend %>% filter(!is.na(first_vacc_date) & first_vacc_date>="2020-12-08"), aes(x = first_vacc_date, y = n)) + 
    geom_col()
  print(gg)

  cat("Second dose vaccinations by day")
  df_vacc_trend <- df_vacc %>% 
    mutate(second_vacc_date = to_date(second_vacc_date)) %>%
    group_by(second_vacc_date) %>%
    summarise(n = n()) %>%
    ungroup() %>%
    collect() %>%
    arrange(second_vacc_date) %>%
    mutate(n_cum = cumsum(n))
  print(df_vacc_trend)
  gg<- ggplot(df_vacc_trend %>% filter(!is.na(second_vacc_date) & second_vacc_date>="2020-12-08"), aes(x = second_vacc_date, y = n)) + 
    geom_col()
  print(gg)

}

check_age_rates <- function(df_vacc, vacc_date = NA, death_date, age_date, min_age = NA, max_age = NA){

  df_vacc <- df_vacc %>%
    # Keep only those who are present in census & GPES
    filter(present_in_gpes == 1, cen_pr_flag == 1, present_in_census == 1) %>%
    # Remove those not living in England
    mutate(country = substr(rgn_derived, 1, 1)) %>%
    filter(country == "E") %>%
    # Keep only people alive by the cut-off date
    filter(is.na(dod) | dod >= death_date)
  
  df_vacc <- df_vacc %>%
    mutate(age = datediff(age_date, dob)/365.25,
           age_group = case_when(age < 18 ~ "<18",
                                 (age >= 18 & age < 30) ~ "18-29",
                                 (age >= 30 & age < 40) ~ "30-39",
                                 (age >= 40 & age < 50) ~ "40-49",
                                 (age >= 50 & age < 60) ~ "50-59",
                                 (age >= 60 & age < 70) ~ "60-69",
                                 (age >= 70 & age < 80) ~ "70-79",
                                 (age >= 80 & age < 90) ~ "80-89",
                                 age >= 90 ~ "90+"))

  # Remove those outside age range if applicable
  if (!is.na(min_age) & is.na(max_age)){
    df_vacc <- df_vacc %>%
      filter(age >= min_age)
  } else if (is.na(min_age) & !is.na(max_age)){
    df_vacc <- df_vacc %>%
      filter(age < max_age)    
  } else if (!is.na(min_age) & !is.na(max_age)){
    df_vacc <- df_vacc %>%
      filter(age >= min_age & age < max_age)    
  } # else no filtering

  if (!is.na(vacc_date)){
    df_vacc <- df_vacc %>%
      mutate(first_vacc = case_when(is.na(first_vacc_date) ~ 0,
                                    first_vacc_date <= vacc_date ~ 1,
                                    first_vacc_date > vacc_date ~ 0),
             second_vacc = case_when(is.na(second_vacc_date) ~ 0,
                                     second_vacc_date <= vacc_date ~ 1,
                                     second_vacc_date > vacc_date ~ 0))
  } else {
    df_vacc <- df_vacc %>%
      mutate(first_vacc = ifelse(is.na(first_vacc_date), 0, 1),
             second_vacc = ifelse(is.na(second_vacc_date), 0, 1))
  }
  
  
  cat("First dose rates by age group")
  df_vacc %>%
    group_by(age_group) %>%
    summarise(n = n(),
              rate = mean(first_vacc, na.rm = TRUE)) %>%
    ungroup() %>%
    collect() %>%
    arrange(age_group) %>%
    print()

  cat("Second dose rates by age group")
  df_vacc %>%
    group_by(age_group) %>%
    summarise(n = n(),
              rate = mean(second_vacc, na.rm = TRUE)) %>%
    ungroup() %>%
    collect() %>%
    arrange(age_group) %>%
    print()

}

check_missing_data <- function(df_vacc, death_date, age_date = NA, min_age = NA, max_age = NA){

  df_vacc <- df_vacc %>%
    # Keep only those who are present in census & GPES
    filter(present_in_gpes == 1, cen_pr_flag == 1, present_in_census == 1) %>%
    # Remove those not living in England
    mutate(country = substr(rgn_derived, 1, 1)) %>%
    filter(country == "E") %>%
    # Keep only people alive by the cut-off date
    filter(is.na(dod) | dod>=death_date)
  
  # Calculate age
  df_vacc <- df_vacc %>%
    mutate(age = datediff(age_date, dob)/365.25)

  # Remove those outside age range if applicable
  if (!is.na(min_age) & is.na(max_age)){
    df_vacc <- df_vacc %>%
      filter(age >= min_age)
  } else if (is.na(min_age) & !is.na(max_age)){
    df_vacc <- df_vacc %>%
      filter(age < max_age)    
  } else if (!is.na(min_age) & !is.na(max_age)){
    df_vacc <- df_vacc %>%
      filter(age >= min_age & age < max_age)    
  } # else no filtering
  
  # Check for missing values
  cat("Missing values")
  df_vacc %>%
    mutate_all(is.na) %>%
    mutate_all(as.numeric) %>%
    summarise_all(sum, na.rm=TRUE) %>%
    collect() %>%
    pivot_longer(everything(), names_to="variable", values_to="missing") %>%
    arrange(missing, variable) %>%
    print()
  
}

#--------------------#
# Check data linkage #
#--------------------#

get_linkage_information <- function(df, vacc_date, death_date, vaccine_number, min_age = NA, max_age = NA){

  df_linkage <- df %>%
    filter(is.na(dod) | dod >= death_date) %>%
    mutate(vaccinated = case_when(is.na(.data[[paste0(vaccine_number, "_vacc_date")]]) ~ 0,
                                  .data[[paste0(vaccine_number, "_vacc_date")]] <= vacc_date ~ 1,
                                  .data[[paste0(vaccine_number, "_vacc_date")]] > vacc_date ~ 0)) %>%
    filter(vaccinated == 1) %>%
    mutate(age_group = case_when(age_when_eligible_years < 18 ~ "<18",
                                 (age_when_eligible_years >= 18 & age_when_eligible_years < 30) ~ "18-29",
                                 (age_when_eligible_years >= 30 & age_when_eligible_years < 40) ~ "30-39",
                                 (age_when_eligible_years >= 40 & age_when_eligible_years < 50) ~ "40-49",
                                 (age_when_eligible_years >= 50 & age_when_eligible_years < 60) ~ "50-59",
                                 (age_when_eligible_years >= 60 & age_when_eligible_years < 70) ~ "60-69",
                                 (age_when_eligible_years >= 70 & age_when_eligible_years < 80) ~ "70-79",
                                 (age_when_eligible_years >= 80 & age_when_eligible_years < 90) ~ "80-89",
                                 age_when_eligible_years >= 90 ~ "90+"),
           sex = ifelse(is.na(gender_vacc), "Unknown", gender_vacc),
           country = substr(rgn_derived, 1, 1)) %>%
    mutate(linked = ifelse((present_in_gpes == 1) & (cen_pr_flag == 1) & (present_in_census == 1) & (country == "E"),
                           "Linked", "Not linked"))

  # Remove those outside age range if applicable
  if (!is.na(min_age) & is.na(max_age)){
    df_linkage <- df_linkage %>%
      filter(age_when_eligible_years >= min_age)
  } else if (is.na(min_age) & !is.na(max_age)){
    df_linkage <- df_linkage %>%
      filter(age_when_eligible_years < max_age)    
  } else if (!is.na(min_age) & !is.na(max_age)){
    df_linkage <- df_linkage %>%
      filter(age_when_eligible_years >= min_age & age_when_eligible_years < max_age)    
  } # else no filtering
  
  print("Proportion of vaccinations that link")
  df_linkage %>%
    count(linked) %>%
    collect() %>%
    mutate(proportion = n / sum(n, na.rm=TRUE) * 100) %>%
    print()

  print("Proportion of vaccinations that link by age group")
  df_linkage %>%
    count(linked, age_group) %>%
    collect() %>%
    group_by(age_group) %>%
    mutate(proportion = n / sum(n, na.rm=TRUE) * 100) %>%
    ungroup() %>%
    pivot_wider(id_cols = age_group, names_from = linked, values_from = c(n, proportion)) %>%
    arrange(age_group) %>%
    print()

  print("Proportion of vaccinations that link by sex")
  df_linkage %>%
    count(linked, sex) %>%
    collect() %>%
    group_by(sex) %>%
    mutate(proportion = n / sum(n, na.rm=TRUE) * 100) %>%
    ungroup() %>%
    pivot_wider(id_cols = sex, names_from = linked, values_from = c(n, proportion)) %>%
    arrange(sex) %>%
    print()

  print("Average age in linked and unlinked vaccinations")
  df_linkage %>%
    group_by(linked) %>%
    summarise(mean_age = mean(age_when_eligible_years, na.rm=TRUE)) %>%
    ungroup() %>%
    collect() %>%
    print()

  print("Age distribution of linked and unlinked vaccinations")
  df_linkage %>%
    group_by(linked, age_group) %>%
    summarise(n = n()) %>%
    ungroup() %>%
    collect() %>%
    group_by(linked) %>%
    mutate(proportion = n / sum(n, na.rm=TRUE) * 100) %>%
    ungroup() %>%
    pivot_wider(id_cols = age_group, names_from = linked, values_from = c(n, proportion)) %>%
    arrange(age_group) %>%
    print()

  print("Sex distribution of linked and unlinked vaccinations")
  df_linkage %>%
    group_by(linked, sex) %>%
    summarise(n = n()) %>%
    ungroup() %>%
    collect() %>%
    group_by(linked) %>%
    mutate(proportion = n / sum(n, na.rm=TRUE) * 100) %>%
    ungroup() %>%
    pivot_wider(id_cols = sex, names_from = linked, values_from = c(n, proportion)) %>%
    arrange(sex) %>%
    print()

}

#-------------------------#
# Create static variables #
#-------------------------#

create_static_variables <- function(df, df_imd, df_nspl){
  
  # Create socio-demographic variables
  df <- df %>%
    mutate(sex = ifelse(sex_census == "1", "Male", "Female"),
           ethpuk11_census = as.numeric(ethpuk11_census),
           ethnicity = case_when(ethpuk11_census == 1 ~ "White British", 
                                 ethpuk11_census %in% 2:4 ~ "White other",
                                 ethpuk11_census %in% 5:8 ~ "Mixed",
                                 ethpuk11_census == 9 ~ "Indian",
                                 ethpuk11_census == 10 ~ "Pakistani",
                                 ethpuk11_census == 11 ~ "Bangladeshi",
                                 ethpuk11_census == 12 ~ "Chinese",
                                 ethpuk11_census == 14 ~ "Black African",
                                 ethpuk11_census == 15 ~ "Black Caribbean",
                                 ethpuk11_census > 15 | ethpuk11_census == 13 ~  "Other"),
           religion = case_when(relpuk11_census == 1 ~ "No religion", 
                                relpuk11_census == 2 ~ "Christian",
                                relpuk11_census == 3 ~ "Buddhist",
                                relpuk11_census == 4 ~ "Hindu",
                                relpuk11_census == 5 ~ "Jewish",
                                relpuk11_census == 6 ~ "Muslim",
                                relpuk11_census == 7 ~ "Sikh",
                                relpuk11_census == 8 ~ "Other Religion",
                                relpuk11_census == 9 ~ "Religion Not Stated"),
           # Codes refer to England, Northern Ireland, Scotland, Wales, GB and UK respectively
           # Excludes Isle of Man and Channgel Islands
           country_of_birth = case_when(cob_census %in% c("921", "922", "923", "924", "925", "926") ~ "UK",
                                        !(cob_census %in% c("921", "922", "923", "924", "925", "926")) ~ "Non-UK"),
           language = case_when(mainlangprf11_census == "1" ~ "Main language",
                                mainlangprf11_census %in% c("2", "3", "4", "5") ~ "Not main language",
                                mainlangprf11_census == "X" ~ "Not classified"),
           education = case_when(hlqpuk11_census == "10" ~ "No qualification",
                                 hlqpuk11_census == "11" ~ "Level 1",
                                 hlqpuk11_census == "12" ~ "Level 2",
                                 hlqpuk11_census == "13" ~ "Apprenticeship",
                                 hlqpuk11_census == "14" ~ "Level 3",
                                 hlqpuk11_census == "15" ~ "Level 4+",
                                 hlqpuk11_census == "16" ~ "Other",
                                 hlqpuk11_census == "XX" ~ "Not classified"),
           disability = case_when(disability_census == "1" ~ "Limited a lot",
                                  disability_census == "2" ~ "Limited a little",
                                  disability_census == "3" ~  "Not Limited"),
           care_home = ifelse(!is.na(care_home_pr19), "Yes", "No"),
           nsshuk11_census = as.numeric(nsshuk11_census),
           nssec_agg = case_when(nsshuk11_census >= 1 &  nsshuk11_census < 4 ~ "1 Higher managerial, administrative and professional occupations",
                                 nsshuk11_census >= 4 &  nsshuk11_census < 7 ~ "2 Lower managerial, administrative and professional occupations",
                                 nsshuk11_census >= 7 &  nsshuk11_census < 8 ~ "3 Intermediate occupations",
                                 nsshuk11_census >= 8 &  nsshuk11_census < 10 ~ "4 Small employers and own account workers",
                                 nsshuk11_census >= 10 &  nsshuk11_census < 12 ~ "5 Lower supervisory and technical occupations",
                                 nsshuk11_census >= 12 &  nsshuk11_census < 13 ~ "6 Semi-routine occupations",
                                 nsshuk11_census >= 13 &  nsshuk11_census < 14 ~ "7 Routine occupations",
                                 nsshuk11_census >= 14 &  nsshuk11_census < 15 ~ "8 Never worked and long-term unemployed",
                                 (nsshuk11_census >= 15 & nsshuk11_census <= 17) | is.na(nsshuk11_census) ~ "Not classified"),
           # Other codes refer to Shared ownership and Living rent free respectively
           tenure = case_when(tenhuk11_census %in% c("0", "1") ~ "Owned",
                              tenhuk11_census %in% c("3", "4") ~ "Social rented",
                              tenhuk11_census %in% c("5", "6", "7", "8") ~ "Private rented",
                              tenhuk11_census %in% c("2", "9") ~ "Other",
                              is.na(tenhuk11_census) ~ "Not classified"),
           rural_urban = case_when(rural_urban_derived %in% c("A1", "B1", "C1", "A2", "B2", "C2") ~ "Urban",
                                   rural_urban_derived %in% c("D1", "E1", "F1", "D2", "E2", "F2") ~ "Rural"),
           region = case_when(rgn_derived=="E12000001" ~ "North East",
                              rgn_derived=="E12000002" ~ "North West",
                              rgn_derived=="E12000003" ~ "Yorkshire and The Humber",
                              rgn_derived=="E12000004" ~ "East Midlands",
                              rgn_derived=="E12000005" ~ "West Midlands",
                              rgn_derived=="E12000006" ~ "East of England",
                              rgn_derived=="E12000007" ~ "London",
                              rgn_derived=="E12000008" ~ "South East",
                              rgn_derived=="E12000009" ~ "South West"))
  
  # Join IMD quintiles onto the dataframe
  df_imd_quintile <- df_imd %>%
    select(lsoa_derived = LSOA11CD, imd_quintile = IMD_QUINTILE)
  
  df <- left_join(df, df_imd_quintile, by = "lsoa_derived")
  
  # Derive local authority
  
  df_nspl_la <- df_la_lookup %>%
    select(lsoa11, la_code = laua, la_name) %>%
    sdf_distinct()
  
  # Derive local authority IMD

  df_imd_la <- df_imd %>%
    select(lsoa_derived = LSOA11CD, imd_rank = IMD_RANK)
  
  df_imd_la <- left_join(df_imd_la, df_nspl_la, by = c("lsoa_derived" = "lsoa11"))
  
  df_quintiles_la <- df_imd_la %>%
    group_by(la_code) %>%
    summarise(imd_rank_p0 = percentile(imd_rank, 0),
              imd_rank_p20 = percentile(imd_rank, 0.2),
              imd_rank_p40 = percentile(imd_rank, 0.4),
              imd_rank_p60 = percentile(imd_rank, 0.6),
              imd_rank_p80 = percentile(imd_rank, 0.8),
              imd_rank_p100 = percentile(imd_rank, 1)) %>%
    ungroup()
  
  df_imd_la <- left_join(df_imd_la, df_quintiles_la, by = "la_code") %>%
    mutate(imd_quintile_la = case_when(imd_rank <= imd_rank_p20 ~ "1",
                                       imd_rank > imd_rank_p20 & imd_rank <= imd_rank_p40 ~ "2",
                                       imd_rank > imd_rank_p40 & imd_rank <= imd_rank_p60 ~ "3",
                                       imd_rank > imd_rank_p60 & imd_rank <= imd_rank_p80 ~ "4",
                                       imd_rank > imd_rank_p80 ~ "5")) %>%
    select(la_code, la_name, lsoa_derived, imd_quintile_la)
  
  # Join local authority codes, names and IMD quintiles onto the dataframe
  df <- left_join(df, df_imd_la, by = "lsoa_derived")
  
  # Create new la_derived variable
  df <- mutate(df, la_derived = la_name)

  # Derive regional IMD
  
  df_nspl_rgn <- df_nspl %>%
    select(lsoa11, rgn) %>%
    sdf_distinct()
  
  df_imd_rgn <- df_imd %>%
    select(lsoa_derived = LSOA11CD, imd_rank = IMD_RANK)
  
  df_imd_rgn <- left_join(df_imd_rgn, df_nspl_rgn, by = c("lsoa_derived" = "lsoa11"))
  
  df_quintiles_rgn <- df_imd_rgn %>%
    group_by(rgn) %>%
    summarise(imd_rank_p0 = percentile(imd_rank, 0),
              imd_rank_p20 = percentile(imd_rank, 0.2),
              imd_rank_p40 = percentile(imd_rank, 0.4),
              imd_rank_p60 = percentile(imd_rank, 0.6),
              imd_rank_p80 = percentile(imd_rank, 0.8),
              imd_rank_p100 = percentile(imd_rank, 1)) %>%
    ungroup()
  
  df_imd_rgn <- left_join(df_imd_rgn, df_quintiles_rgn, by = "rgn") %>%
    mutate(imd_quintile_region = case_when(imd_rank <= imd_rank_p20 ~ "1",
                                           imd_rank > imd_rank_p20 & imd_rank <= imd_rank_p40 ~ "2",
                                           imd_rank > imd_rank_p40 & imd_rank <= imd_rank_p60 ~ "3",
                                           imd_rank > imd_rank_p60 & imd_rank <= imd_rank_p80 ~ "4",
                                           imd_rank > imd_rank_p80 ~ "5")) %>%
  select(lsoa_derived, imd_quintile_region)
  
  # Join regional IMD quintiles onto the dataframe
  df <- left_join(df, df_imd_rgn, by = "lsoa_derived")
  
}

#--------------------#
# Get monthly counts #
#--------------------#

get_monthly_counts <- function(df, min_age, exposure, group_vars){
  
  if (exposure == "education" & min_age < 30){
    min_age <- 30
  }

  df <- df %>%
    mutate(month_start = paste(year, month, "01", sep = "-"),
           month_end = last_day(as.Date(month_start)),
           age = datediff(month_end, dob)/365.25,
           age_group = case_when(age < 18 ~ "<18",
                                 (age >= 18 & age < 30) ~ "18-29",
                                 (age >= 30 & age < 40) ~ "30-39",
                                 (age >= 40 & age < 50) ~ "40-49",
                                 (age >= 50 & age < 60) ~ "50-59",
                                 (age >= 60 & age < 70) ~ "60-69",
                                 (age >= 70 & age < 80) ~ "70-79",
                                 age >= 80 ~ "80+"),
           age_group_esp = case_when(age < 18 ~ "<18",
                                     (age >= 18 & age < 25) ~ "18-24",
                                     (age >= 25 & age < 30) ~ "25-29",
                                     (age >= 30 & age < 35) ~ "30-34",
                                     (age >= 35 & age < 40) ~ "35-39",
                                     (age >= 40 & age < 45) ~ "40-44",
                                     (age >= 45 & age < 50) ~ "45-49",
                                     (age >= 50 & age < 55) ~ "50-54",
                                     (age >= 55 & age < 60) ~ "55-59",
                                     (age >= 60 & age < 65) ~ "60-64",
                                     (age >= 65 & age < 70) ~ "65-69",
                                     (age >= 70 & age < 75) ~ "70-74",
                                     (age >= 75 & age < 80) ~ "75-79",
                                     (age >= 80 & age < 85) ~ "80-84",
                                     (age >= 85 & age < 90) ~ "85-89",
                                     age >= 90 ~ "90+"),
           vaccine_first = ifelse((!is.na(first_vacc_date)) & (first_vacc_date <= month_end), 1, 0),
           vaccine_second = ifelse((!is.na(second_vacc_date)) & (second_vacc_date <= month_end), 1, 0),
           vaccine_third = ifelse((!is.na(third_vacc_date)) & (third_vacc_date <= month_end), 1, 0))
  
  # Dynamic filters
  df <- df %>%
    # Keep only people alive after/at the end of the current month
    filter((is.na(dod)) | (dod > month_end)) %>%
    # Remove those under the cut-off age
    filter(age >= min_age)
  
  df <- df %>%
    pivot_longer(cols = c(vaccine_first, vaccine_second, vaccine_third), names_to = "vaccine_number", names_prefix = "vaccine_", values_to = "vaccine")
  
  df <- df %>%
    group_by(across(all_of(!!c("vaccine_number", "year", "month", group_vars, exposure)))) %>%
    summarise(population = n(),
              vaccinated = sum(vaccine, na.rm = TRUE)) %>%
    ungroup() %>%
    collect() %>%
    mutate(exposure = exposure,
           year = year,
           month = month) %>%
    rename(group = !!exposure) %>%
    mutate(group = as.character(group)) %>%
    arrange(across(all_of(!!c("vaccine_number", "year", "month", "exposure", group_vars, "group")))) %>%
    select(vaccine_number, year, month, exposure, all_of(group_vars), group, population, vaccinated)
  
  cat(paste0("- ", exposure, "\n"))
  
  return(df)
}

#-----------#
# Get rates #
#-----------#

get_crude_rates <- function(df, df_poisson, group_vars){
  
  # group_vars : list of socio-demographic variables to calculate ASMRs by. E.g c("ethnicity", "country_of_birth"). List must be of at least length 1.

  df_rates <- df %>%
    mutate(vaccinated = round(vaccinated / 5) * 5,
           population = round(population / 5) * 5) %>%
    group_by(across(all_of(!!group_vars))) %>%
    summarise(across(c(population, vaccinated), sum, na.rm=TRUE)) %>%
    ungroup() %>%
    mutate(crude_rate = vaccinated / population) %>%
    mutate(crude_variance = crude_rate * (1-crude_rate),
           crude_standard_error = sqrt(crude_variance / population)) %>%
    mutate(crude_lower_ci = crude_rate - (1.96 * crude_standard_error),
           crude_upper_ci = crude_rate + (1.96 * crude_standard_error)) %>%
    mutate(across(.cols = c(crude_rate, crude_lower_ci, crude_upper_ci), .fns = ~ .x * 100)) %>%
    mutate(crude_lower_ci = ifelse(crude_lower_ci < 0, 0, crude_lower_ci),
           crude_upper_ci = ifelse(crude_upper_ci > 100, 100, crude_upper_ci))

  df_rates <- df_rates %>%
    select(all_of(group_vars), population, vaccinated, crude_rate, crude_lower_ci, crude_upper_ci) %>%
    mutate(age_standardised_rate = NA_real_,
           age_standardised_lower_ci = NA_real_,
           age_standardised_upper_ci = NA_real_)
  
  return(df_rates)
  
}

get_asmrs <- function(df, df_esp, df_poisson, min_age, max_age, group_vars){
  
  # group_vars : list of socio-demographic variables to calculate ASMRs by. E.g c("ethnicity", "country_of_birth"). List must be of at least length 1.

  # Augment ESPs to fit age groups

  df_rates <- left_join(df, df_esp, by = "age_group_esp") %>%
    # Filter age bands that are below min age or above max age
    mutate(age_group_lower = unlist(purrr::map(strsplit(age_group_esp, "-", fixed=TRUE), 1)),
           age_group_lower = gsub("+", "", age_group_lower, fixed=TRUE),
           age_group_lower = gsub("<", "", age_group_lower, fixed=TRUE),
           age_group_lower = as.integer(age_group_lower),
           age_group_lower = ifelse(age_group_esp == "<1", 0, age_group_lower),
           age_group_upper = unlist(purrr::map(strsplit(age_group_esp, "-", fixed=TRUE), last)),
           age_group_upper = gsub("+", "", age_group_upper, fixed=TRUE),
           age_group_upper = gsub("<", "", age_group_upper, fixed=TRUE),
           age_group_upper = ifelse(age_group_esp == "<1", as.integer(age_group_upper), as.integer(age_group_upper) + 1),
           age_group_upper = ifelse(age_group_esp == "90+", 91, age_group_upper)) %>%
    filter(age_group_lower >= min_age & age_group_upper <= max_age) %>%
    select(!c(age_group_lower, age_group_upper))

  df_rates <- df_rates %>%
    mutate(vaccinated = round(vaccinated / 5) * 5,
           population = round(population / 5) * 5) %>%
    mutate(rate_per_100000 = vaccinated / population * 100000,
           variance = ((rate_per_100000^2) / vaccinated) * (ESP^2),
           standardised_rate = rate_per_100000 * ESP)
  if (sum(is.na(df_rates$ESP)) != 0){
    stop("Missing population weights. Check the labels of the age groups")
  }

  df_rates <- df_rates %>%
    group_by(across(all_of(!!group_vars))) %>%
    summarise(across(c(population, vaccinated, standardised_rate, variance, ESP), sum, na.rm=TRUE)) %>%
    ungroup() %>%
    left_join(., df_poisson, by = c("vaccinated" = "Deaths")) %>%
    mutate(crude_rate = vaccinated / population) %>%
    mutate(crude_variance = crude_rate * (1-crude_rate),
           crude_standard_error = sqrt(crude_variance / population)) %>%
    mutate(crude_lower_ci = crude_rate - (1.96 * crude_standard_error),
           crude_upper_ci = crude_rate + (1.96 * crude_standard_error)) %>%  
    mutate(age_standardised_rate = (standardised_rate / ESP),
           age_standardised_variance = variance / (ESP^2),
           age_standardised_standard_error = sqrt(age_standardised_variance)) %>%
    mutate(age_standardised_lower_ci = ifelse(vaccinated >= 100 | vaccinated==0,
                                              age_standardised_rate - (1.96 * age_standardised_standard_error),
                                              age_standardised_rate + (L * vaccinated - vaccinated) * ((age_standardised_variance/vaccinated)^0.5)),
           age_standardised_upper_ci = ifelse(vaccinated >= 100 | vaccinated==0,
                                              age_standardised_rate + (1.96 * age_standardised_standard_error),
                                              age_standardised_rate + (U * vaccinated - vaccinated) * ((age_standardised_variance/vaccinated)^0.5))) %>%
    mutate(across(.cols = c(crude_rate, crude_lower_ci, crude_upper_ci), .fns = ~ .x * 100)) %>%
    mutate(across(.cols = c(age_standardised_rate, age_standardised_lower_ci, age_standardised_upper_ci), .fns = ~ .x / 1000)) %>%
    mutate(crude_lower_ci = ifelse(crude_lower_ci < 0, 0, crude_lower_ci),
           crude_upper_ci = ifelse(crude_upper_ci > 100, 100, crude_upper_ci),
           age_standardised_lower_ci = ifelse(age_standardised_lower_ci < 0, 0, age_standardised_lower_ci),
           age_standardised_upper_ci = ifelse(age_standardised_upper_ci > 100, 100, age_standardised_upper_ci))

  df_rates <- df_rates %>%
    select(all_of(group_vars), population, vaccinated, crude_rate, crude_lower_ci, crude_upper_ci, age_standardised_rate, age_standardised_lower_ci, age_standardised_upper_ci)
  
  return(df_rates)
  
}

#----------#
# Rounding #
#----------#

round2 <- function(x, n = 0) {
  # x is variable to be rounded
  # n : integer : number of decimal places
  posneg = sign(x)
  z = abs(x)*10^n
  z = z + 0.5
  z = trunc(z)
  z = z/10^n
  z * posneg
}

#-------------------#
# Disclosure checks #
#-------------------#

disclosure_checks <- function(df, limit){

  cat("Number of rows\n")
  df %>%
    count() %>% 
    print()

  cat(paste0("Vaccinations below ", limit, "\n"))
  # Low counts
  df %>%
    filter(vaccinated < limit) %>%
    count() %>% 
    print()

  cat(paste0("Vaccinations below ", limit, " by month\n"))
  df %>%
    filter(vaccinated < limit) %>%
    count(year, month) %>% 
    print()

  cat(paste0("Vaccinations below ", limit, "\n"))
  df %>%
    filter(vaccinated < limit) %>%
    arrange(vaccine_number, year, month, exposure, group, SubCategory) %>%
    print()

  cat(paste0("Non-vaccinations below ", limit, "\n"))
  # High counts
  df %>%
    mutate(not_vaccinated = population - vaccinated) %>%
    filter(not_vaccinated < limit) %>%
    count() %>% 
    print()

  cat(paste0("Non-vaccinations below ", limit, " by month\n"))
  df %>%
    mutate(not_vaccinated = population - vaccinated) %>%
    filter(not_vaccinated < limit) %>%
    count(year, month) %>% 
    print()

  cat(paste0("Non-vaccinations below ", limit, "\n"))
  df %>%
    mutate(not_vaccinated = population - vaccinated) %>%
    filter(not_vaccinated < limit) %>%
    arrange(vaccine_number, year, month, exposure, group, SubCategory) %>%
    print()

}
