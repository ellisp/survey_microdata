

sql <- "create schema private;"
dbSendQuery(con, sql)

sql <- "SELECT * INTO private.vw_ivssurveymainheader FROM public.vw_IVSSurveyMainHeader"
dbSendQuery(con, sql)

dbGetQuery(con, "select * from private.vw_IVSSurveyMainHeader limit 5")
dbListTables(con)
