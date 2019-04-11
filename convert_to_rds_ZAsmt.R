rm(list=ls())
library(data.table)

removeNABlank <- function(df) {
  col_means <- colMeans(is.na(df))
  col_means <- col_means[col_means>0.3]
  keep_names <- names(df)[! names(df) %in% names(col_means)]
  df <- subset(df,select=keep_names)
  col_means <- colMeans(df=="")
  col_means <- col_means[col_means>0.3]
  keep_names <- names(df)[! names(df) %in% names(col_means)]
  return(subset(df,select=keep_names))
}

# use Layout.xlsx for column names

folder = 'C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/36/ZAsmt'
files = list.files(path = folder, pattern = '*',full.names = FALSE)

variable_names <- read.csv(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/variable_names_ZAsmt.csv",stringsAsFactors = FALSE)
variable_names$file <- tolower(sapply(variable_names$file,function(x) paste(substr(x,3,nchar(x)),".txt",sep="")))

options(warn = -1)

for(file in files) {
  
  print(file)

  
  tryCatch(
    {
        # temp <- read.dta(file=paste(folder,"/",file,sep=""))
        temp <- fread(file=paste(folder,"/",file,sep=""),sep="|")
        names(temp) <- variable_names[variable_names$file==file,]$columnname
        # names(temp) <- variable_names[variable_names$file==paste(substr(file,1,4),".txt",sep=""),]$columnname
        
        # temp <- removeNABlank(removeNABlank)
        
        saveRDS(temp,file=paste(folder,"/",substr(file,1,nchar(file)-4),".rds",sep=""))
        # saveRDS(temp,file=paste(folder,"/",file,".rds",sep=""))
        rm(temp)
        gc()
        
        
    },error=function(cond) {
      print(paste("Error",file))
    })
}


options(warn = 0)


# 
# # Run following only for Historical files ---------------------------------
# 
# folder = 'C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/Historical/17'
# files = list.files(path = folder, pattern = '.rds',full.names = FALSE)
# 
# for(file in files) {
#   print(file)
#   temp <-readRDS(file=paste(folder,"/",file,sep=""))
#   temp[,c("BKFSPID","BatchID","SubEdition","PropertyAddressMatchcode","PropertyAddressCarrierRoute","PropertyAddressGeoCodeMatchCode",
#           "PropertyAddressLatitude","PropertyAddressLongitude","PropertyAddressCensusTractAndBlock","LoadID","LegalSecTwnRngMer",
#           "FIPS","State","County","ExtractDate","Edition","ZVendorStndCode","UnformattedAssessorParcelNumber","ParcelSequenceNumber",
#           "PropertyHouseNumber","PropertyStreetName","PropertyStreetSuffix","PropertyFullStreetAddress",
#           "PropertyCity","PropertyState","PropertyZip","PropertyZip4")] <- NULL
#   temp <- removeNABlank(temp)
#   saveRDS(temp,file=paste(folder,"/",file,sep=""))
# }
# 
# 
# 
# files <- list.files(path=folder,pattern=".*rds",full.names = TRUE)
# propertyinfo <- lapply(files,function(x) readRDS(x))
# propertyinfo <- rbindlist(propertyinfo)
# saveRDS(propertyinfo,file=paste(folder,"/main.rds",sep=""))