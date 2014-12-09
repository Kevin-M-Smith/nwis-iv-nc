#!/usr/bin/Rscript
require(RPostgreSQL)
require(dataRetrieval)

download.site.file <- function(state){
  url <- paste("http://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=",
               state,
               "&period=P5W&siteOutput=expanded&hasDataTypeCd=iv",
               sep = "")

  active <- importRDB1(url)

  #pathToFile <- tempfile()
  #download.file(url, pathToFile)
  #active <- importRDB(pathToFile)
}


for (i in 1:length(state.abb)){
  state.active <- download.site.file(state.abb[i])
  if(i == 1){
    active <- state.active
  } else {
    active <- rbind(active, state.active)
  }
}

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "postgres", user="postgres", host="localhost", password="usgs")

dbWriteTable(con, "active.sites", active)

dbDisconnect(con)
dbUnloadDriver(drv)
