#!/usr/bin/Rscript
cat("===========================================\n")
cat("		NETCDF BUILD-ALL					\n")
cat("===========================================\n")
cat("Loading libraries...\n")
require(RPostgreSQL)
require(doParallel)

#################################
#	Connecting To Database
#################################
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "postgres", user="postgres", host="localhost", password="usgs")
cat("===========================================\n")
cat("Connecting to database...\n")

#################################
#	Connect To Cluster
#################################
cat("===========================================\n")
cat("Setting up cluster...\n")
cl <- makePSOCKcluster(2, outfile = "")
registerDoParallel(cl)
cc <- clusterEvalQ(cl, {
	require(RPostgreSQL)
	require(doParallel)
	require(plyr)
	require(reshape2)
	require(lubridate)
	require(ncdf4)	
  require(data.table)
	drv2 <- dbDriver("PostgreSQL")
	con2 <- dbConnect(drv2, dbname = "postgres", user = "postgres", host = "localhost", password = "usgs")
	source("netcdf4.dt.R")
	})

###########################################
#	Create Table of Active Sites
###########################################
cat("===========================================\n")
cat("Building NetCDF Files...\n")

t <- dbGetQuery(con, "select min(ts)::date, max(ts)::date from data;")
dates <- seq(t$min, t$max, 1)
pairs <- data.frame(startDate = format(dates[-length(dates)]), endDate = format(dates[-1]), stringsAsFactors = FALSE)

pb <- txtProgressBar(min = 1, max = nrow(pairs), style = 3)

cc <- foreach(i = 1) %dopar% { build.ncdf(pairs[i,1], pairs[i,2]) }

cc <- foreach(i = 2:nrow(pairs)) %dopar% { 
		build.ncdf(pairs[i,1], pairs[i,2]) 
		if(i%%5 == 0){
			setTxtProgressBar(pb, i)
		}
	}

setTxtProgressBar(pb, nrow(pairs))

cat("\nCleaning up connections...\n")
cc <- clusterEvalQ(cl, {
        dbDisconnect(con2)
        dbUnloadDriver(drv2)
})
cc <- stopCluster(cl)

cat("===========================================\n")
cat("	NetCDF conversion complete. 		\n")
cat("===========================================\n")
