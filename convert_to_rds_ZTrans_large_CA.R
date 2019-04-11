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

folder = 'C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/36/ZTrans'

variable_names <- read.csv(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/variable_names_ZTrans.csv",stringsAsFactors = FALSE)
variable_names$file <- tolower(sapply(variable_names$file,function(x) paste(substr(x,3,nchar(x)),".txt",sep="")))

options(warn = -1)



# main files --------------------------------------------------------------


files = list.files(path = folder, pattern = '^main',full.names = FALSE)

trans_main_or <- readRDS(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/41/ZTrans/main.rds")
trans_main_or[,c("KeyedDate","KeyerID","SubVendorStndCode","MatchStndCode","BatchID","ZVendorStndCode","SourceChkSum","BKFSPID","LoadID")] <- NULL
trans_main_or$SalesPriceAmount <- ifelse(is.na(trans_main_or$SalesPriceAmount),-1,trans_main_or$SalesPriceAmount)
trans_main_or <- removeNABlank(trans_main_or)
trans_main_or <- names(trans_main_or)

for(file in files) {
  
  print(file)

  
  tryCatch(
    {
        temp <- fread(file=paste(folder,"/",file,sep=""),sep="|")
        names(temp) <- variable_names[variable_names$file=="main.txt",]$columnname
        
        temp <- temp[,trans_main_or,with=FALSE]
        
        saveRDS(temp,file=paste(folder,"/",file,".rds",sep=""))  
        rm(temp)
        gc()
        
        
    },error=function(cond) {
      print(paste("Error",file))
    })
}

files <- list.files(path=folder,pattern="^main.*rds$",full.names = TRUE)
main <- lapply(files,function(x) readRDS(x))
main <- rbindlist(main)
saveRDS(main,file=paste(folder,"/main.rds",sep=""))
rm(main)
gc()




# propertyinfo ------------------------------------------------------------

files = list.files(path = folder, pattern = '^propertyinfo',full.names = FALSE)

trans_main_or <- readRDS(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/41/ZTrans/propertyinfo.rds")
trans_main_or[,c("PropertyZip4","LegalLot","LegalSubdivisionName","PropertySequenceNumber","PropertyAddressMatchcode",
                  "PropertyAddressCarrierRoute","PropertyAddressGeoCodeMatchCode","FIPS","LoadID","BKFSPID","AssessmentRecordMatchFlag",
                  "BatchID")] <- NULL
trans_main_or <- removeNABlank(trans_main_or)
trans_main_or <- names(trans_main_or)

for(file in files) {
  
  print(file)
  
  
  tryCatch(
    {
      temp <- fread(file=paste(folder,"/",file,sep=""),sep="|")
      names(temp) <- variable_names[variable_names$file=="propertyinfo.txt",]$columnname
      
      temp <- temp[,trans_main_or,with=FALSE]
      
      saveRDS(temp,file=paste(folder,"/",file,".rds",sep=""))  
      rm(temp)
      gc()
      
      
    },error=function(cond) {
      print(paste("Error",file))
    })
}

files <- list.files(path=folder,pattern="^propertyinfo.*rds$",full.names = TRUE)
propertyinfo <- lapply(files,function(x) readRDS(x))
propertyinfo <- rbindlist(propertyinfo)
saveRDS(propertyinfo,file=paste(folder,"/propertyinfo.rds",sep=""))




# historical_main ---------------------------------------------------------

folder = 'C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/Historical/36'

variable_names <- read.csv(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/variable_names_ZAsmt.csv",stringsAsFactors = FALSE)
variable_names$file <- tolower(sapply(variable_names$file,function(x) paste(substr(x,3,nchar(x)),".txt",sep="")))


trans_main_or <- c("ImportParcelID","TaxAmount","TaxYear")

files = list.files(path = folder, pattern = '^main',full.names = FALSE)

for(file in files) {
  
  print(file)
  
  
  tryCatch(
    {
      temp <- fread(file=paste(folder,"/",file,sep=""),sep="|")
      names(temp) <- variable_names[variable_names$file=="main.txt",]$columnname
      
      temp <- temp[,trans_main_or,with=FALSE]
      
      saveRDS(temp,file=paste(folder,"/",file,".rds",sep=""))  
      rm(temp)
      gc()
      
      
    },error=function(cond) {
      print(paste("Error",file))
    })
}

files <- list.files(path=folder,pattern="*.rds",full.names = TRUE)
main <- lapply(files,function(x) readRDS(x))
main <- rbindlist(main)
saveRDS(main,file=paste(folder,"/main.rds",sep=""))



options(warn = 0)
