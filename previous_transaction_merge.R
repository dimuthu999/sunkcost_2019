rm(list=ls())
library(data.table)
library(plyr)
library(dplyr)

state_name ="New York"
state_code = "NY"
state_fips = "36"
min_sale_price = 0

selected_deed_types <- c("WRDE","DEED","SPWD","VLDE","GRDE","QCDE","BSDE","PRDE")

merge_previous_trans <- function(it,df) {
  
  if(it==1) {
    df[,success_prev:=0]
    df[,TransId_prev:=NA]
    df[,success_iteration:=NA]
    df[,INTR:=0]
  }

  setkey(df,ImportParcelID,DocumentDate)
  
  
  df <- trans_merge[df,roll=TRUE]
  
  df[,DocumentTypeStndCode:=ifelse(df$DocumentTypeStndCode=="WRDE","AAAA",
                                   ifelse(df$DocumentTypeStndCode=="MTGE","AAAB",
                                          ifelse(df$DocumentTypeStndCode=="INTR","AAAC",df$DocumentTypeStndCode)))]
  df <- df[order(df$ImportParcelID,df$DocumentTypeStndCode)]
  df <- df[!duplicated(df$ImportParcelID)]
  df[,DocumentTypeStndCode:=ifelse(df$DocumentTypeStndCode=="AAAA","WRDE",
                                   ifelse(df$DocumentTypeStndCode=="AAAB","MTGE",
                                          ifelse(df$DocumentTypeStndCode=="AAAC","INTR",df$DocumentTypeStndCode)))]
  
  df[,success:=ifelse(df$DocumentTypeStndCode=="WRDE" & df$SalesPriceAmount>0 & df$success_prev==0,1,0)]
  df[,success_prev:=ifelse(df$success_prev==1 | df$success==1,1,0)]
  df <- df[df$success_prev==1 | !(df$DocumentTypeStndCode=="WRDE" & df$SalesPriceAmount==0)]
  df <- df[df$success_prev==1 | df$DocumentTypeStndCode %in% c("WRDE","MTGE","INTR")]
  df[,INTR:=ifelse(df$INTR==1 | (df$DocumentTypeStndCode=="INTR" & df$success_prev==0),1,0)]
  df[,DocumentDate:=merge_date-1]
  
  df[,TransId_prev:=ifelse(df$success==1,df$TransId,df$TransId_prev)]
  df[,success_iteration:=ifelse(df$success==1,it,df$success_iteration)]
  
  df <- plyr::rename(df,replace=c(
    "TransId"=paste("TransId_it",it,sep=""),
    "SalesPriceAmount"=paste("SalesPriceAmount_it",it,sep=""),
    "DocumentTypeStndCode"=paste("DocumentTypeStndCode_it",it,sep=""),
    "success"=paste("success_it",it,sep=""),
    "merge_date"=paste("merge_date_it",it,sep="")))
    
  return(df)
}

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





trans_main <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZTrans/main.rds",sep=""))

trans_property<- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZTrans/propertyinfo.rds",sep=""))
dup_transid <- trans_property[duplicated(trans_property$TransId)]$TransId
trans_property_no_dups <- trans_property[!trans_property$TransId %in% dup_transid]
trans_property__dups <- trans_property[trans_property$TransId %in% dup_transid]
trans_property__dups <- trans_property__dups[!duplicated(trans_property__dups[,c("TransId","PropertyFullStreetAddress")])]
trans_property__dups <- trans_property__dups[!is.na(trans_property__dups$PropertyZip)]
dup_transid <- trans_property__dups[duplicated(trans_property__dups$TransId)]$TransId
trans_property__dups <- trans_property__dups[!trans_property__dups$TransId %in% dup_transid]
trans_property <- rbind(trans_property_no_dups,trans_property__dups)
trans_property$ImportParcelID <- ifelse(is.na(trans_property$ImportParcelID),-1,trans_property$ImportParcelID)

rm(list=c("trans_property_no_dups","trans_property__dups"))

trans_main$SalesPriceAmount <- ifelse(is.na(trans_main$SalesPriceAmount),-1,trans_main$SalesPriceAmount)

# trans_main <- trans_main[trans_main$TransId %in% trans_property$TransId,]
gc()
trans_main[,c("KeyedDate","KeyerID","SubVendorStndCode","MatchStndCode","BatchID","ZVendorStndCode","SourceChkSum")] <- NULL

trans_main <- removeNABlank(trans_main)
gc()
trans_property[,c("PropertyZip4","LegalLot","LegalSubdivisionName","PropertySequenceNumber","PropertyAddressMatchcode",
                  "PropertyAddressCarrierRoute","PropertyAddressGeoCodeMatchCode","FIPS","LoadID","BKFSPID","AssessmentRecordMatchFlag",
                  "BatchID")] <- NULL
trans_property <- removeNABlank(trans_property)
gc()
trans_main <- merge(trans_main,trans_property,by="TransId")
rm(trans_property)
trans_main <- trans_main[trans_main$ImportParcelID>0]

trans_main[,DocumentDate:=as.Date(trans_main$DocumentDate)]
trans_main[,DocumentYear:=as.integer(format(trans_main$DocumentDate,"%Y"))]

trans_main[,DocumentTypeStndCode:=ifelse(trans_main$DocumentTypeStndCode %in% selected_deed_types,"WRDE",trans_main$DocumentTypeStndCode)]
gc()


trans_9616 <- trans_main[trans_main$DocumentYear %in% c(1996:2016) & trans_main$DocumentTypeStndCode=="WRDE" & trans_main$SalesPriceAmount>min_sale_price]
trans_9616 <- trans_9616[,c("TransId","DocumentDate","SalesPriceAmount","DocumentTypeStndCode","ImportParcelID")]
names(trans_9616) <- c("TransId_sale","DocumentDate","SalesPriceAmount_sale","DocumentTypeStndCode_sale","ImportParcelID")
trans_9616 <- trans_9616[!is.na(trans_9616$ImportParcelID)]
trans_9616[,Sale_date:=DocumentDate]
trans_9616[,DocumentDate:=DocumentDate-1]


trans_merge <- trans_main[,c("TransId","DocumentDate","SalesPriceAmount","DocumentTypeStndCode","ImportParcelID")]
trans_merge[,merge_date:=DocumentDate]
trans_merge <- trans_merge[!is.na(trans_main$ImportParcelID)]
setkey(trans_merge,ImportParcelID,DocumentDate)

merged_data <- merge_previous_trans(1,trans_9616)
merged_data <- merge_previous_trans(2,merged_data)
merged_data <- merge_previous_trans(3,merged_data)
merged_data <- merge_previous_trans(4,merged_data)
merged_data <- merge_previous_trans(5,merged_data)
merged_data <- merge_previous_trans(6,merged_data)
merged_data <- merge_previous_trans(7,merged_data)
merged_data <- merge_previous_trans(8,merged_data)


merged_data <- merged_data[,c("TransId_sale","ImportParcelID","SalesPriceAmount_sale","Sale_date","DocumentTypeStndCode_sale",
                              "TransId_prev","success_iteration","INTR")]

dup_trans_prev <- merged_data[duplicated(merged_data$TransId_prev)]$TransId_prev

merged_data <- merged_data[!merged_data$TransId_prev %in% dup_trans_prev]
merged_data <- merged_data[!duplicated(merged_data$TransId_prev)]

trans_merge[,c("ImportParcelID","merge_date"):=list(NULL)]
names(trans_merge) <- c("TransId_prev","Purchase_date","Purchase_price","DocumentTypeStndCode_purchase")

dup_trans_prev <- trans_merge[duplicated(trans_merge$TransId_prev)]$TransId_prev

merged_data <- merge(merged_data,trans_merge,by="TransId_prev")
merged_data[,Sale_month:=as.Date(paste(substr(merged_data$Sale_date,1,7),"-01",sep=""))]
merged_data[,Sale_year:=as.numeric(format(merged_data$Sale_date,"%Y"))]
merged_data[,Purchase_month:=as.Date(paste(substr(merged_data$Purchase_date,1,7),"-01",sep=""))]
merged_data[,Purchase_year:=as.numeric(format(merged_data$Purchase_date,"%Y"))]

merged_data <- merged_data[merged_data$Purchase_year>=1990]

  asmt_main <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/main.rds",sep=""))
  asmt_main <- data.table(asmt_main)
  asmt_main <- asmt_main[asmt_main$ImportParcelID %in% unique(merged_data$ImportParcelID)]
  asmt_main <- asmt_main[,c("RowID","ImportParcelID","FIPS","PropertyFullStreetAddress","PropertyCity","PropertyZip","TaxAmount","TaxYear",
                            "LotSizeSquareFeet","PropertyAddressLatitude","PropertyAddressLongitude","PropertyAddressCensusTractAndBlock")]
  names(asmt_main) <- c("RowID","ImportParcelID","FIPS","StreetAddress","City","zip","TaxAmount","TaxYear",
                        "Lot_sqft","Latitude","Longitude","CensusTractAndBlock")

merged_data <- merge(merged_data,asmt_main,by="ImportParcelID")


  asmt_building <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/building.rds",sep=""))
  asmt_building <- asmt_building[asmt_building$RowID %in% unique(merged_data$RowID)]
  asmt_building <- asmt_building[,c("RowID","NoOfUnits","OccupancyStatusStndCode","PropertyCountyLandUseCode",
                                    "PropertyLandUseStndCode","YearBuilt","NoOfStories","TotalBedrooms",
                                    "TotalCalculatedBathCount")]
  names(asmt_building) <- c("RowID","NoOfUnits","OccupancyStatus","CountyLandUseCode","LandUseStndCode",
                            "YearBuilt","NoOfStories","Bedrooms","Bathrooms")

merged_data <- merge(merged_data,asmt_building,by="RowID")

  asmt_buildingarea <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/buildingareas.rds",sep="")) 
  asmt_buildingarea <- asmt_buildingarea[asmt_buildingarea$RowID %in% unique(merged_data$RowID)]
  asmt_buildingarea <- asmt_buildingarea[asmt_buildingarea$BuildingAreaStndCode=="BAL"]
  asmt_buildingarea <- asmt_buildingarea[,c("RowID","BuildingAreaSqFt")]

merged_data <- merge(merged_data,asmt_buildingarea,by="RowID",all.x=TRUE)

  zhvi <- read.csv(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/Zip_Zhvi_AllHomes.csv",stringsAsFactors = FALSE)
  zhvi <- zhvi[zhvi$State %in% c(state_code),]
  zhvi[,c("RegionID","City","State","Metro","CountyName","SizeRank")] <- NULL
  zhvi <- melt(zhvi,id.vars = c("RegionName"))
  zhvi['month'] <- sapply(zhvi$variable,function(x) as.Date(paste(gsub("\\.","-",substr(x,2,8)),"-01",sep="")))
  zhvi$month <- as.Date(zhvi$month,origin = "1970-01-01")
  zhvi$variable <- NULL
  names(zhvi) <- c("zip","HPI_purchase","Purchase_month")
  
merged_data <- merge(merged_data,zhvi,by=c("zip","Purchase_month"))

  names(zhvi) <- c("zip","HPI_sale","Sale_month")
  
merged_data <- merge(merged_data,zhvi,by=c("zip","Sale_month"))

merged_data[,HPI_inflation:=merged_data$HPI_sale/merged_data$HPI_purchase]
merged_data[,Adj_purch_price:=merged_data$Purchase_price*merged_data$HPI_inflation]
merged_data[,Sale_premium:=merged_data$SalesPriceAmount_sale/merged_data$Adj_purch_price]
merged_data[,Ownership_years:=merged_data$Sale_year-merged_data$Purchase_year]
merged_data[,Sale_Purchase_year:=paste(merged_data$Sale_year,merged_data$Purchase_year)]


if(!state_fips %in% c("32","04")) {
  historical_asmt <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/Historical/",state_fips,"/main.rds",sep=""))
  historical_asmt <- historical_asmt[,c("ImportParcelID","TaxAmount","TaxYear")]
  historical_asmt <- historical_asmt[historical_asmt$ImportParcelID %in% unique(merged_data$ImportParcelID),]
  # historical_asmt[,TaxYear:=paste("year",historical_asmt$TaxYear,sep="")]
  historical_asmt <- historical_asmt[!is.na(historical_asmt$TaxAmount)]
  historical_asmt <- historical_asmt[!duplicated(historical_asmt[,c("ImportParcelID","TaxYear")]),]
  names(historical_asmt) <- c("ImportParcelID","Tax_prev","Sale_year")
  historical_asmt <- historical_asmt[!is.na(historical_asmt$Tax_prev)]
  historical_asmt[,Sale_year:=historical_asmt$Sale_year+1]
  
  merged_data <- merge(merged_data,historical_asmt,by=c("ImportParcelID","Sale_year"),all.x = TRUE)
}

if(state_fips %in% c("32","04")) {
  merged_data[,Tax_prev:=NA]
}

merged_data[,Tax_prev:=ifelse(merged_data$Sale_year==2016 & merged_data$TaxYear==2015,merged_data$TaxAmount,merged_data$Tax_prev)]
merged_data[,Tax_prev:=ifelse(merged_data$Sale_year==2015 & merged_data$TaxYear==2014,merged_data$TaxAmount,merged_data$Tax_prev)]
merged_data[,Tax_prev:=ifelse(merged_data$Sale_year==2014 & merged_data$TaxYear==2013,merged_data$TaxAmount,merged_data$Tax_prev)]
merged_data[,Tax_prev:=ifelse(merged_data$Sale_year==2013 & merged_data$TaxYear==2012,merged_data$TaxAmount,merged_data$Tax_prev)]
merged_data[,Tax_prev:=ifelse(merged_data$Sale_year==2012 & merged_data$TaxYear==2011,merged_data$TaxAmount,merged_data$Tax_prev)]
merged_data[,Tax_prev:=ifelse(merged_data$Sale_year==2011 & merged_data$TaxYear==2010,merged_data$TaxAmount,merged_data$Tax_prev)]
merged_data[,Tax_rate:=merged_data$Tax_prev/merged_data$Adj_purch_price]
merged_data[,Purchase_week:=as.numeric(strftime(merged_data$Purchase_date,format="%V"))+1]


library(rgdal)
library(sp)
library(rgeos)
library(ggplot2)

tracts <- readOGR(paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/cb_2017_",state_fips,"_tract_500k",sep=""),paste("cb_2017_",state_fips,"_tract_500k",sep=""))
tracts_df <- fortify(tracts,region="GEOID")


dat <- data.frame(merged_data[,c("ImportParcelID","Longitude","Latitude")])
dat <- dat[!is.na(dat$Longitude),]
dat <- dat[!duplicated(dat$ImportParcelID),]




dimnames(dat)[[1]] <- dat$ImportParcelID
dat$ImportParcelID <- NULL

points <- SpatialPoints(dat)
proj4string(points) <- proj4string(tracts)

df <- over(points, tracts)
df['ImportParcelID'] <- row.names(df)
row.names(df) <- NULL
df <- df[,c("ImportParcelID","GEOID")]
df$ImportParcelID <- as.integer(df$ImportParcelID)
df$GEOID <- as.character(df$GEOID)


merged_data <- merge(merged_data,df,by="ImportParcelID",all.x=TRUE)

saveRDS(merged_data,file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/perv_transactions/",state_code,"_prev_transaction_merged.rds",sep=""))
