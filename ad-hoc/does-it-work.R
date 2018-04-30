con <- dbConnect(RPostgres::Postgres(), dbname = "ivs")

dbGetQuery(con, "select  * from vw_IVSSurveyMainHeader limit 10")

dbDisconnect(con)
