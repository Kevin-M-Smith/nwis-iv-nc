#!/usr/bin/Rscript
cat("===========================================\n")
cat("	BOOTSTRAPPER : TEST			\n")
cat("===========================================\n")
cat("Loading libraries...\n")
require(RPostgreSQL)
require(RCurl)
require(XML)
require(doParallel)

cat("===========================================\n")
cat("Connecting to database...\n")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "postgres", user="postgres", host="localhost", password="usgs")
sites <- dbGetQuery(con, "SELECT site_no from activesites;")
cc <- dbDisconnect(con)
cc <- dbUnloadDriver(drv)

cat("===========================================\n")
cat("Setting up cluster...\n")
cl <- makePSOCKcluster(detectCores(), outfile = "")
registerDoParallel(cl)

cc <- clusterEvalQ(cl, {
        require(RPostgreSQL)
        require(RCurl)
        require(XML)
        drv2 <- dbDriver("PostgreSQL")
        con2 <- dbConnect(drv2, dbname = "postgres", user="postgres", host="localhost", password="usgs")
        source("bootstrapFn.R")
})

cat("===========================================\n")
cat(paste("Bootstrapping",
	10, "sites...\n"))
	
cc <- foreach(i = 1) %dopar% { 
	bootstrap(sites[i,1])
 }

cc <- foreach(i = 2:10) %dopar% { 
	result = tryCatch({
		bootstrap(sites[i,1]) 
	}, warning = function(w) {
	}, error = function(e) {
		cat(paste("Site:",
		sites[i,1],
		"at index",
		i,
		"failed:",
		e)
	    )
	}, finally = {
	})
}


cat("===========================================\n")
cat("Cleaning up connections...\n")
cc <- clusterEvalQ(cl, {
        dbDisconnect(con2)
        dbUnloadDriver(drv2)
})
cc <- stopCluster(cl)

cat("===========================================\n")
cat("	Bootstrapping  test complete.			\n") 
cat("===========================================\n")
