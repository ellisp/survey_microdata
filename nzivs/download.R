library(data.table)
library(RPostgres)
library(stringr)
library(dplyr)

# TODO - re-write so instead of dropping the database table then writing a new one,
# it writes a new one with a new name, then just changes the names, so there's less
# downtime during refreshes

url <- "http://www.mbie.govt.nz/info-services/sectors-industries/tourism/tourism-research-data/ivs/documents-image-library/vw_IVS.zip"
download.file(url, destfile = "ivs.zip")

source("https://github.com/nz-mbie/mbie-r-package-public/raw/master/pkg/R/CountryManip.R") # for CountryGroup
source("https://github.com/nz-mbie/mbie-r-package-public/raw/master/pkg/R/NZTourism.R")    # for rename.levels


unzip("ivs.zip")

# all the CSV files:
csvs <- list.files("IVS", full.names = TRUE)
# the names of the views, stripped of path etc
views <- gsub("^IVS/", "", gsub("\\.csv$", "", csvs))

con <- dbConnect(RPostgres::Postgres(), dbname = "ivs")

dbListTables(con)

mh <- fread("IVS/vw_IVSSurveyMainHeader.csv") %>%
  select(SurveyResponseID, PopulationWeight)

for(j in 1:length(csvs)){
  this_csv <- fread(csvs[j])
  
  # add in some extra columns needed for use in PowerBI
  if(views[j] == "vw_IVSSurveyMainHeader"){
    this_csv$weight_x_spend <- with(this_csv, PopulationWeight * WeightedSpend)
    this_csv$weight_x_los <- with(this_csv, PopulationWeight * LengthOfStay)
    this_csv$country_group <- CountryGroup(this_csv$CORNextYr)
  }
  
  if(views[j] == "vw_IVSSatisfactionRatings"){
    this_csv <- this_csv %>%
      mutate(satisfaction_rating_num = as.numeric(
        str_extract(this_csv$SatisfactionRating ,"[0-9]+"))) %>%
      left_join(mh, by = "SurveyResponseID") %>%
      mutate(weight_x_satisfaction = PopulationWeight * satisfaction_rating_num) %>%
      select(-PopulationWeight)
  }
  
  # name of the table to create in the database. postgresql gets freaked out by mixed case
  # so best to just use lower case
  this_table <- tolower(views[j])
  names(this_csv) <- tolower(names(this_csv))
  
  # Some of the views in MBIE TRED have illegal names (spaces and minus
  # signs) and we replace this with underscores - so for these small 
  # number of tables the MySQL version will differ from that in MBIE.
  this_table <- gsub(" - ", "_", this_table, fixed = TRUE)
  this_table <- gsub(" ", "_", this_table, fixed = TRUE)
  
  message(paste("Writing", this_table))
  dbWriteTable(conn = con, 
               name = this_table,
               value = this_csv,
               row.names = FALSE, 
               overwrite = TRUE)
  
  
  # Index the database table we just made.
  # If SurveyResponseID uniquely identifies rows, make it the primary key.
  # Otherwise it should still be an index, and the table will not have a 
  # primary key (this is not good database practice, but does the job for
  # these relatively small datasets)
  if(length(unique(this_csv$SurveyResponseID)) == nrow(this_csv)){
    message (paste("Using SurveyResponseID as primary key on", this_table))
    indexing_sql <- paste("ALTER TABLE", this_table, 
                          "ADD PRIMARY KEY(SurveyResponseID)")
  } else {
    if("SurveyResponseID" %in% names(this_csv)){
      message(paste("Adding a primary key to", this_table))
      dbSendQuery(con, paste("ALTER TABLE", this_table, "ADD pk_column INT AUTO_INCREMENT PRIMARY KEY;"))
      
      message(paste("Adding a SurveyResponseID index to", this_table))
      indexing_sql <- paste("CREATE INDEX SurveyResponseID ON", 
                            this_table,
                            "(SurveyResponseID)")
    } else {
      next()
    }
    
  }
  
  dbSendQuery(con, indexing_sql)
  
}

dbDisconnect(con)
rm(con)
