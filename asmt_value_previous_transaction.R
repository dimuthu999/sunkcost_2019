rm(list=ls())
library(data.table)
library(plyr)

state_name ="Florida"
state_code = "FL"
state_fips = "12"
min_sale_price = 0

selected_deed_types <- c("WRDE","DEED","SPWD","VLDE","GRDE","QCDE","BSDE","PRDE")


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



asmt_value <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/value.rds",sep=""))
asmt_value <- asmt_value[asmt_value$AssessmentYear %in% 2014:2016,c("RowID","AssessmentYear","TotalAssessedValue")]


asmt_main <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/main.rds",sep=""))
asmt_main <- data.table(asmt_main)
asmt_main <- asmt_main[,c("RowID","ImportParcelID","FIPS","PropertyFullStreetAddress","PropertyCity","PropertyZip","TaxAmount","TaxYear",
                          "LotSizeSquareFeet","PropertyAddressLatitude","PropertyAddressLongitude","PropertyAddressCensusTractAndBlock")]
names(asmt_main) <- c("RowID","ImportParcelID","FIPS","StreetAddress","City","zip","TaxAmount","TaxYear",
                      "Lot_sqft","Latitude","Longitude","CensusTractAndBlock")


asmt_value <- merge(asmt_value,asmt_main,by="RowID")

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
gc()
trans_main$SalesPriceAmount <- ifelse(is.na(trans_main$SalesPriceAmount),-1,trans_main$SalesPriceAmount)

# trans_main <- trans_main[trans_main$TransId %in% trans_property$TransId,]
gc()
trans_main[,c("KeyedDate","KeyerID","SubVendorStndCode","MatchStndCode","BatchID","ZVendorStndCode","SourceChkSum")] <- NULL

trans_main <- removeNABlank(trans_main)

trans_property[,c("PropertyZip4","LegalLot","LegalSubdivisionName","PropertySequenceNumber","PropertyAddressMatchcode",
                  "PropertyAddressCarrierRoute","PropertyAddressGeoCodeMatchCode","FIPS","LoadID","BKFSPID","AssessmentRecordMatchFlag",
                  "BatchID")] <- NULL
trans_property <- removeNABlank(trans_property)
gc()
trans_main <- merge(trans_main,trans_property,by="TransId")
rm(trans_property)
gc()
trans_main <- trans_main[trans_main$ImportParcelID>0]

trans_main[,DocumentDate:=as.Date(trans_main$DocumentDate)]
trans_main[,DocumentYear:=as.integer(format(trans_main$DocumentDate,"%Y"))]

trans_main[,DocumentTypeStndCode:=ifelse(trans_main$DocumentTypeStndCode %in% selected_deed_types,"AAAA",trans_main$DocumentTypeStndCode)]
gc()

trans_main <- trans_main[!trans_main$DocumentTypeStndCode %in% c("MTGE","INTR")]
trans_main <- trans_main[order(trans_main$ImportParcelID,-trans_main$DocumentDate,trans_main$DocumentTypeStndCode)]
trans_main <- trans_main[!duplicated(trans_main$ImportParcelID)]
trans_main <- trans_main[trans_main$DocumentTypeStndCode == "AAAA" & trans_main$SalesPriceAmount>0]
trans_main <- trans_main[trans_main$DocumentYear>=1996 & trans_main$DocumentYear<=2013]


asmt_value <- merge(asmt_value,trans_main, by="ImportParcelID")


asmt_value[,Purchase_month:=as.Date(paste(substr(asmt_value$DocumentDate,1,7),"-01",sep=""))]
asmt_value[,Purchase_year:=as.numeric(format(asmt_value$DocumentDate,"%Y"))]

merged_data<-asmt_value[,c("ImportParcelID","RowID","AssessmentYear","TotalAssessedValue","StreetAddress","City","zip",
                          "TaxAmount","TaxYear","Lot_sqft","Latitude","Longitude","TransId","State",
                          "SalesPriceAmount","LoanAmount","Purchase_month","Purchase_year")]
rm(asmt_main)
rm(asmt_value)
  
  asmt_building <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/building.rds",sep=""))
  asmt_building <- asmt_building[asmt_building$RowID %in% unique(merged_data$RowID)]
  asmt_building <- asmt_building[,c("RowID","NoOfUnits","OccupancyStatusStndCode","PropertyCountyLandUseCode",
                                    "PropertyLandUseStndCode","YearBuilt","NoOfStories","TotalBedrooms",
                                    "TotalCalculatedBathCount")]
  names(asmt_building) <- c("RowID","NoOfUnits","OccupancyStatus","CountyLandUseCode","LandUseStndCode",
                            "YearBuilt","NoOfStories","Bedrooms","Bathrooms")

merged_data <- merge(merged_data,asmt_building,by="RowID")
rm(asmt_building)
  
  asmt_buildingarea <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/buildingareas.rds",sep="")) 
  asmt_buildingarea <- asmt_buildingarea[asmt_buildingarea$RowID %in% unique(merged_data$RowID)]
  asmt_buildingarea <- asmt_buildingarea[asmt_buildingarea$BuildingAreaStndCode=="BAL"]
  asmt_buildingarea <- asmt_buildingarea[,c("RowID","BuildingAreaSqFt")]

merged_data <- merge(merged_data,asmt_buildingarea,by="RowID",all.x=TRUE)
rm(asmt_buildingarea)
  
  zhvi <- read.csv(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/Zip_Zhvi_AllHomes.csv",stringsAsFactors = FALSE)
  zhvi <- zhvi[zhvi$State %in% c(state_code),]
  zhvi[,c("RegionID","City","State","Metro","CountyName","SizeRank")] <- NULL
  zhvi <- melt(zhvi,id.vars = c("RegionName"))
  zhvi['month'] <- sapply(zhvi$variable,function(x) as.Date(paste(gsub("\\.","-",substr(x,2,8)),"-01",sep="")))
  zhvi$month <- as.Date(zhvi$month,origin = "1970-01-01")
  zhvi$variable <- NULL
  names(zhvi) <- c("zip","HPI_purchase","Purchase_month")
  
merged_data <- merge(merged_data,zhvi,by=c("zip","Purchase_month"))

  names(zhvi) <- c("zip","HPI_asmt","Asmt_month")
merged_data[,Asmt_month:=as.Date(paste(merged_data$AssessmentYear,"-01-01",sep=""))]
merged_data <- merge(merged_data,zhvi,by=c("zip","Asmt_month"))

merged_data[,HPI_inflation:=merged_data$HPI_asmt/merged_data$HPI_purchase]
merged_data[,Adj_purch_price:=merged_data$SalesPriceAmount*merged_data$HPI_inflation]
merged_data[,Ownership_years:=merged_data$AssessmentYear-merged_data$Purchase_year]



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

saveRDS(merged_data,file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/asmt_value_hist/",state_code,"_asmt_value_hist.rds",sep=""))
