# INSTALL PACKAGES IF THEY ARE NOT INSTALLED YET
# install.packages("bigrquery")
# install.packages("DBI")
# install.packages("dplyr")

# useful github page: https://github.com/r-dbi/bigrquery

# load packages
library(bigrquery)
library(DBI)
library(dplyr)

rm(list = ls())

# connect to project 
bq_auth()
bq_projects()

# set project_id and dataset_id for connection
project_id <- "yhcr-prd-bradfor-bia-core"
dataset_id <- "CB_2707_HUP_FDM"

con <- dbConnect(
  bigrquery::bigquery(),
  project = project_id,
  dataset = dataset_id,
  billing = project_id
)

# inspect accessible big query tables
dbListTables(con)

# check whether data contains unique person_id i.e. one person per row

sql <- "
  SELECT 
    'person' AS table_name, COUNT(*) AS total_rows,
    COUNT(DISTINCT person_id) AS unique_person_ids
  FROM 
    `yhcr-prd-bradfor-bia-core.CB_2707_HUP_FDM_NO_DFE.person`
  
  UNION ALL
  
  SELECT 
    'personLSOA' AS table_name, COUNT(*) AS total_rows,
    COUNT(DISTINCT person_id) AS unique_person_ids
  FROM 
    `yhcr-prd-bradfor-bia-core.CB_2707_HUP_FDM_NO_DFE.personLSOA`
  
  UNION ALL
  
  SELECT 
    'tbl_srcode_1' AS table_name, COUNT(*) AS total_rows,
    COUNT(DISTINCT person_id) AS unique_person_ids
  FROM 
    `yhcr-prd-bradfor-bia-core.CB_2707_HUP_FDM_NO_DFE.tbl_srcode_1`;
"

dbGetQuery(con, sql)

# query to link tables: person, personLSOA, tbl_srcode_1
query_link <- "
  SELECT
    p.person_id,
    p.race_source_value,
    p.gender_source_value,
    p.year_of_birth,
    p.ethnicity_source_value,
    p.death_datetime,
    p.birth_datetime,
    l.LSOA,
    l.ward,
    s.CTV3Code, 
    s.CTV3Text,
    s.DateEventRecorded,
    s.tbl_SRCode_start_date,
    s.tbl_SRCode_end_date,
  FROM
    `yhcr-prd-bradfor-bia-core.CB_2707_HUP_FDM_NO_DFE.person` AS p
  LEFT JOIN
    `yhcr-prd-bradfor-bia-core.CB_2707_HUP_FDM_NO_DFE.personLSOA` AS l
  ON
    p.person_id = l.person_id
  LEFT JOIN
    `yhcr-prd-bradfor-bia-core.CB_2707_HUP_FDM_NO_DFE.tbl_srcode_1` AS s
  ON
    p.person_id = s.person_id
  WHERE
    p.death_datetime IS NULL OR p.death_datetime > TIMESTAMP('2022-09-26')
"

# send query to BigQuery
tb <- bq_project_query("yhcr-prd-bradfor-bia-core", query_link)

# download temp results (saved in tb) to a data frame (first 10000 rows)
tb_df_1 <- bq_table_download(tb, n_max = 10000)

# inspect the data - long format (multiple rows for one individual)
head(tb_df_1)

# inspect gender
table(tb_df_1$gender_source_value)

tb_df_1$gender_source_value[tb_df_1$gender_source_value == "Female"] <- "F"
tb_df_1$gender_source_value[tb_df_1$gender_source_value == "Male"] <- "M"
tb_df_1$gender_source_value[tb_df_1$gender_source_value == 2] <- NA
tb_df_1$gender_source_value[tb_df_1$gender_source_value == 1] <- NA

# inspect ethinicity
table(tb_df_1$ethnicity_source_value)


# count the number of LSOA included in the data sample
length(unique(tb_df_1$LSOA))
length(unique(tb_df_1$person_id))

# one row per person for characteristics summary 
tb_unique <- tb_df_1 %>%
  select(person_id, 
         gender_source_value, 
         ethnicity_source_value, 
         LSOA,
         birth_datetime,
         death_datetime) %>%
  distinct(person_id, .keep_all = TRUE)

dim(tb_unique)
length(unique(tb_df_1$person_id))

# generate age using 2018-01-01 as baseline
library(lubridate)
tb_unique <- tb_unique %>%
  mutate(
    birth_date = as_date(birth_datetime),
    age_2018 = if_else(
      birth_date <= ymd("2018-01-01"),
      as.numeric(interval(birth_date, ymd("2018-01-01")) / years(1)),
      NA_real_ # born after 2018-01-01
    )
  )

summary(tb_unique$age_2018)

# number of population less than 5 years old 
tb_unique %>%
  filter(age_2018 < 5) %>%
  summarise(count = n())
# number of population more than 65 years old 
tb_unique %>%
  filter(age_2018 >= 65) %>%
  summarise(count = n())

# summarise characteristics by LSOA
library(dplyr)
lsoa_totals <- tb_unique %>%
  group_by(LSOA) %>%
  summarise(n_total = n(), .groups = "drop")

gender_summary <- tb_unique %>%
  group_by(LSOA, gender_source_value) %>%
  summarise(n = n(), .groups = "drop") %>%
  left_join(lsoa_totals, by = "LSOA") %>%
  mutate(prop = n / n_total)

avg_gender_prop <- gender_summary %>%
  group_by(gender_source_value) %>%
  summarise(avg_prop = mean(prop, na.rm = TRUE), .groups = "drop")

ethnicity_summary <- tb_unique %>%
  group_by(LSOA, ethnicity_source_value) %>%
  summarise(n = n(), .groups = "drop") %>%
  left_join(lsoa_totals, by = "LSOA") %>%
  mutate(prop = n / n_total)

# results <- dbGetQuery(con, query_link, n = 10)



