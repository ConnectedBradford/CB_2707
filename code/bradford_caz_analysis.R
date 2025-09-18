# Script for difference-in-differences analysis of clean air zone (CAZ) impact in Bradford

# Date. Thursday, September 11th 2025

## ----------------------------
## 0. R-package preparation
## ----------------------------
# Install packages that typically don't require compilation
install.packages(c("dplyr", "lubridate", "ggplot2", "tidyr", "tidyverse", "scales"))

# Then try these (some might need compilation but usually have binaries)
install.packages(c("fixest", "kableExtra", "broom"))

# For bigrquery and DBI (database packages)
install.packages(c("DBI", "bigrquery"))


library(bigrquery)
library(DBI)
library(dplyr)
library(lubridate)
library(ggplot2)
library(fixest)
library(tidyr)
library(kableExtra)
library(broom)
library(scales)
library(tidyverse)

## ----------------------------
## 1. Data Preparation
## ----------------------------

# Establish connection to SQL Server
sql_conn <- dbConnect(odbc::odbc(),
                      driver = "SQL Server",
                      server = "BHTS-CONNECTYO3",
                      database = "CB_2707_HUP",
                      Trusted_Connection = "True")

# Query data from a table
query <- "SELECT TOP 10 * FROM [dbo].[person]"

# Store query result in a dataframe
data <- dbGetQuery(sql_conn, query)

# Print dataframe to console
print(data)

#Exploring the tables in the dataset CB_2707_HUB
tables <- dbListTables(sql_conn)
print(tables)

#List columns for a specific table
columns <- dbListFields(sql_conn, "person")
print(columns)

#Get summary statistics for a table
query_summary <- "SELECT COUNT(*) as total_rows FROM [dbo].[person]"
total_rows <- dbGetQuery(sql_conn, query_summary)
print(total_rows)

#Tabulate sex
query_tabulate <- "SELECT gender_source_value, COUNT(*) as count
                  FROM [dbo].[person]
                  GROUP BY gender_source_value
                  ORDER BY count DESC"
gender_table <- dbGetQuery(sql_conn, query_tabulate)
print(gender_table)

#Tabulate race
query_tabulate <- "SELECT race_source_value, COUNT(*) as count
                  FROM [dbo].[person]
                  GROUP BY race_source_value
                  ORDER BY count DESC"
race_table <- dbGetQuery(sql_conn, query_tabulate)
print(race_table)

#Tabulate location
query_tabulate <- "SELECT location_id, COUNT(*) as count
                  FROM [dbo].[person]
                  GROUP BY location_id
                  ORDER BY count DESC"
location_table <- dbGetQuery(sql_conn, query_tabulate)
print(location_table)

#Tabulate care site
query_tabulate <- "SELECT care_site_id, COUNT(*) as count
                  FROM [dbo].[person]
                  GROUP BY care_site_id
                  ORDER BY count DESC"
care_site_table <- dbGetQuery(sql_conn, query_tabulate)
print(care_site_table)

#Tabulate provider
query_tabulate <- "SELECT provider_id, COUNT(*) as count
                  FROM [dbo].[person]
                  GROUP BY provider_id
                  ORDER BY count DESC"
provider_table <- dbGetQuery(sql_conn, query_tabulate)
print(provider_table)

#Calculate age
query_age <- "SELECT
              year_of_birth,
              month_of_birth,
              day_of_birth,
              CASE
              WHEN year_of_birth IS NOT NULL AND
                    month_of_birth IS NOT NULL AND
                    day_of_birth IS NOT NULL
                    THEN DATEDIFF(year,
                    CAST(CAST(year_of_birth AS VARCHAR) +'-'+
                    CAST(month_of_birth AS VARCHAR) +'-'+
                    CAST(day_of_birth AS VARCHAR) AS DATE),
                    GETDATE())
                    ELSE NULL
                    END as age
                    FROM[dbo].[person]"

age_data <- dbGetQuery(sql_conn, query_age)
print(head(age_data))


###I TRIED TO SAVE THE DATASET TO THE PC. THE CODE IS BELOW
#Export the entire person table to CSV
person_data <- dbGetQuery(sql_conn, "SELECT*FROM [dbo].[person]")
write.csv(person_data, "CB_2707_HUB_person_table_csv", row.names = FALSE)
# Get list of all tables in the database
tables <- dbGetQuery(sql_conn, 
                    "SELECT TABLE_NAME 
                     FROM INFORMATION_SCHEMA.TABLES 
                     WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'")

# Export all tables
for (table in tables$TABLE_NAME) {
  query <- paste0("SELECT * FROM [dbo].[", table, "]")
  table_data <- dbGetQuery(sql_conn, query)
  write.csv(table_data, paste0("CB_2707_HUB_", table, ".csv"), row.names = FALSE)
  print(paste("Exported", table, "-", nrow(table_data), "rows"))
}

# Get list of all tables in the database
tables <- dbGetQuery(sql_conn, 
                     "SELECT TABLE_NAME 
                     FROM INFORMATION_SCHEMA.TABLES 
                     WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'")

# Export all tables
for (table in tables$TABLE_NAME) {
  query <- paste0("SELECT * FROM [dbo].[", table, "]")
  table_data <- dbGetQuery(sql_conn, query)
  write.csv(table_data, paste0("CB_2707_HUB_", table, ".csv"), row.names = FALSE)
  print(paste("Exported", table, "-", nrow(table_data), "rows"))
}


# Close the connection
dbDisconnect(sql_conn)
