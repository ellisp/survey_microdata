library(dplyr)
library(tidyr)
library(readr)
library(RPostgres)

# imports, clean up, and save to database the data from
# http://www.stats.govt.nz/tools_and_services/microdata-access/nzis-2011-cart-surf.aspx

url <- "http://archive.stats.govt.nz/~/media/Statistics/services/microdata-access/nzis11-cart-surf/nzis11-cart-surf.csv"
nzis <- read_csv(url)


#-----------------fact tables------------------
# Create a main table with a primary key

f_mainheader <- nzis %>%
  mutate(survey_id = 1:nrow(nzis))

# we need to normalise the multiple ethnicities, currently concatenated into a single variable
cat("max number of ethnicities is", max(nchar(nzis$ethnicity)), "\n")

f_ethnicity <- f_mainheader %>%
  select(ethnicity, survey_id) %>%
  mutate(First = substring(ethnicity, 1, 1),
         Second = substring(ethnicity, 2, 2)) %>%
  select(-ethnicity) %>%
  gather(ethnicity_type, ethnicity_id, -survey_id) %>%
  filter(ethnicity_id != "") 

# drop the original messy ethnicity variable and tidy up names on main header
f_mainheader <- f_mainheader %>%
  select(-ethnicity) %>%
  rename(region_id = lgr,
         sex_id = sex,
         agegrp_id = agegrp,
         qualification_id = qualification,
         occupation_id = occupation)

#-----------------dimension tables------------------
# all drawn from the data dictionary available at the first link given above
d_sex <- data_frame(sex_id = 1:2, sex = c("male", "female"))

d_agegrp <- data_frame(
  agegrp_id = seq(from = 15, to = 65)) %>%
  mutate(agegrp = ifelse(agegrp_id == 65, "65+", paste0(agegrp_id, "-", agegrp_id + 4)))

d_ethnicity <- data_frame(ethnicity_id = c(1,2,3,4,5,6,9),
                          ethnicity = c(
                            "European",
                            "Maori",
                            "Pacific Peoples",
                            "Asian",
                            "Middle Eastern/Latin American/African",
                            "Other Ethnicity",
                            "Residual Categories"))


d_occupation <- data_frame(occupation_id = 1:10,
                           occupation = c(
                             "Managers",
                             "Professionals",
                             "Technicians and Trades Workers",
                             "Community and Personal Service Workers",
                             "Clerical and Adminsitrative Workers",
                             "Sales Workers",
                             "Machinery Operators and Drivers",
                             "Labourers",
                             "Residual Categories",
                             "No occupation"                          
                           ))


d_qualification <- data_frame(qualification_id = 1:5,
                              qualification = c(
                                "None",
                                "School",
                                "Vocational/Trade",
                                "Bachelor or Higher",
                                "Other"
                              ))

d_region <- data_frame(region_id =1:12,
                       region = c("Northland", "Auckland", "Waikato", "Bay of Plenty", "Gisborne / Hawke's Bay",
                                  "Taranaki", "Manawatu-Wanganui", "Wellington", 
                                  "Nelson/Tasman/Marlborough/West Coast", "Canterbury", "Otago", "Southland"))


#====================load up to database=====================
con <- dbConnect(RPostgres::Postgres(), dbname = "nzis2011")


dbSendQuery(con, "DROP TABLE IF EXISTS f_mainheader")
dbWriteTable(con, "f_mainheader", f_mainheader, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS f_ethnicity")
dbWriteTable(con, "f_ethnicity", f_ethnicity, row.names = FALSE)


# dimension tables
dbSendQuery(con, "DROP TABLE IF EXISTS d_sex")
dbSendQuery(con, "DROP TABLE IF EXISTS d_agegrp")
dbSendQuery(con, "DROP TABLE IF EXISTS d_ethnicity")
dbSendQuery(con, "DROP TABLE IF EXISTS d_occupation")
dbSendQuery(con, "DROP TABLE IF EXISTS d_qualification")
dbSendQuery(con, "DROP TABLE IF EXISTS d_region")



dbWriteTable(con, "d_sex", d_sex, row.names = FALSE)
dbWriteTable(con, "d_agegrp", d_agegrp, row.names = FALSE)
dbWriteTable(con, "d_ethnicity", d_ethnicity, row.names = FALSE)
dbWriteTable(con, "d_occupation", d_occupation, row.names = FALSE)
dbWriteTable(con, "d_qualification", d_qualification, row.names = FALSE)
dbWriteTable(con, "d_region", d_region, row.names = FALSE)

#----------------indexing----------------------

dbSendQuery(con, "ALTER TABLE f_mainheader ADD PRIMARY KEY(survey_id)")

dbSendQuery(con, "ALTER TABLE d_sex ADD PRIMARY KEY(sex_id)")
dbSendQuery(con, "ALTER TABLE d_agegrp ADD PRIMARY KEY(agegrp_id)")
dbSendQuery(con, "ALTER TABLE d_ethnicity ADD PRIMARY KEY(ethnicity_id)")
dbSendQuery(con, "ALTER TABLE d_occupation ADD PRIMARY KEY(occupation_id)")
dbSendQuery(con, "ALTER TABLE d_qualification ADD PRIMARY KEY(qualification_id)")
dbSendQuery(con, "ALTER TABLE d_region ADD PRIMARY KEY(region_id)")

#---------------create an analysis-ready view-------------------

dbSendQuery(con, "drop view if exists vw_mainheader")

sql1 <-
  "CREATE VIEW vw_mainheader AS 
   SELECT survey_id, sex, agegrp, occupation, qualification, region, hours, income 
   FROM
      f_mainheader a   JOIN
      d_sex b          on a.sex_id = b.sex_id JOIN
      d_agegrp c       on a.agegrp_id = c.agegrp_id JOIN
      d_occupation e   on a.occupation_id = e.occupation_id JOIN
      d_qualification f on a.qualification_id = f.qualification_id JOIN
      d_region g       on a.region_id = g.region_id"

dbSendQuery(con, sql1)

dbDisconnect(con)
