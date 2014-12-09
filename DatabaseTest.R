require(RPostgreSQL)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="postgres", user="postgres", password="usgs", host="localhost")

# get 10 rows
rs <- dbSendQuery(con,"select * from data limit 10")

#fetch(rs)


rs <- dbSendQuery(con,"select distinct seriesid from data")

rs2 <- dbSendQuery(con, "")



SELECT 
data.ts,
data.seriesid,
data.value  
FROM 
public.data
WHERE 
data.ts >= '2014-12-04' AND 
data.ts < '2014-12-05'
LIMIT 10;
