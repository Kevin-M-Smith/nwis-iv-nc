bootstrap <- function(site){ 
  
  #drv2 <- dbDriver("PostgreSQL") 
  #con2 <- dbConnect(drv2, dbname = "postgres", user="postgres", host="localhost", password="usgs") 
 
  g = basicTextGatherer() 
  url = "http://waterservices.usgs.gov/nwis/iv/?format=waterml,1.1&period=P5W&parameterCd=00060,00065,00045,00095,00010,00062,00054,00036,00035&sites=" 
  url = paste(url, site, sep = "")  
  xml = curlPerform(url = url, writefunction = g$update, httpheader = c(AcceptEncoding="gzip,deflate")) 

  doc <- xmlTreeParse(g$value(), getDTD = FALSE, useInternalNodes = TRUE) 
  doc <- xmlRoot(doc) 
   
  vars <- xpathApply(doc, "//ns1:timeSeries") 
  now <- format(Sys.time(), "%FT%T%z") 

  valid <- function(x){
  	if(x == "P") 0 else 1
  }

  for (i in 1:length(vars)){ 
    parent <- xmlDoc(vars[[i]]) 
    parent <- xmlRoot(parent) 
    parentName <- unlist(xpathApply(parent, "//ns1:timeSeries/@name")) 
    sensors <- xpathApply(parent, "//ns1:values") 
    parameter <- xpathApply(parent, "//ns1:variableCode", xmlValue)
    familyName <- paste(unlist(strsplit(parentName, ":", fixed = TRUE))[-3], collapse = ":")
    for (j in 1:length(sensors)){ 
      child <- xmlDoc(sensors[[j]]) 
      child <- xmlRoot(child) 
      childName <- unlist(xpathApply(child, "//ns1:method/@methodID")) 
       
      childName <- formatC(strtoi(childName), width = 5, format = "d", flag = "0")  
      res <- data.frame( 
        unlist(xpathApply(child, "//@dateTime")), 
        paste(parentName, ":", childName, sep = ""), 
        paste(familyName, ":", childName, sep = ""),
        unlist(xpathApply(child, "//ns1:value", xmlValue)),
        parameter, 
        unlist(lapply(xpathApply(child, "//@qualifiers"), valid)), 
        now, 
        now 
      ) 
       
      colnames(res) <- c("ts", "seriesid", "value", "validated", "imported", "updated") 
      dbWriteTable(con2, "data", res, append = TRUE, row.names = FALSE, overwrite = FALSE) 
    } 
  } 
   
  #dbDisconnect(con2) 
  #dbUnloadDriver(drv2) 
} 
