#!/usr/bin/Rscript

pkgs = c("RPostgreSQL",
	"XML",
	"RCurl",
	"dataRetrieval",
	"doParallel",
	"lubridate",
	"ncdf4")

install.packages(pkgs, repos = "http://cran.us.r-project.org", type = "source")
