
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
	50, "sites...\n"))
	
cc <- foreach(i = 51) %dopar% { 
	bootstrap(sites[i,1])
 }

cc <- foreach(i = 52:500) %dopar% { 
	result = tryCatch({
		bootstrap(sites[i,1], delay = runif(1, 0.1, 0.6)) 
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
