rm(list=ls())
library(data.table)
library(plyr)
library(stringr)

state_name="New York"
state_code="NY"
state_fips = "36"

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


files <- list.files(path=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/truliadata/",state_code,sep=""),pattern="*.rds",full.names = TRUE)
listprice <- lapply(files,function(x) readRDS(x))
listprice <- rbindlist(listprice)
listprice$V2 <- NULL
listprice$address <- tolower(str_squish(listprice$address))

if(state_code=="NY") {
  listprice[,List_date:=as.Date("2019-01-01")]
}


asmt_main <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/main.rds",sep=""))
asmt_main <- data.table(asmt_main)

asmt_main[,full_address:= tolower(paste(asmt_main$PropertyFullStreetAddress, asmt_main$PropertyCity,asmt_main$PropertyState))]

ImportParcelIDs <- unique(asmt_main[asmt_main$full_address %in% listprice$address]$ImportParcelID)

asmt_main <- asmt_main[asmt_main$ImportParcelID %in% ImportParcelIDs,]

asmt_main <- asmt_main[,c("RowID","ImportParcelID","FIPS","PropertyFullStreetAddress","PropertyCity","PropertyZip","TaxAmount","TaxYear",
                          "LotSizeSquareFeet","PropertyAddressLatitude","PropertyAddressLongitude","PropertyAddressCensusTractAndBlock","full_address")]
names(asmt_main) <- c("RowID","ImportParcelID","FIPS","StreetAddress","City","zip","TaxAmount","TaxYear",
                      "Lot_sqft","Latitude","Longitude","CensusTractAndBlock","full_address")


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


trans_property <- trans_property[trans_property$ImportParcelID %in% ImportParcelIDs]
trans_property[,c("PropertyZip4","LegalLot","LegalSubdivisionName","PropertySequenceNumber","PropertyAddressMatchcode",
                  "PropertyAddressCarrierRoute","PropertyAddressGeoCodeMatchCode","FIPS","LoadID","BKFSPID","AssessmentRecordMatchFlag",
                  "BatchID")] <- NULL
# trans_property <- removeNABlank(trans_property)


trans_main <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZTrans/main.rds",sep=""))
trans_main <- trans_main[trans_main$TransId %in% unique(trans_property$TransId)]
trans_main$SalesPriceAmount <- ifelse(is.na(trans_main$SalesPriceAmount),-1,trans_main$SalesPriceAmount)

# trans_main <- trans_main[trans_main$TransId %in% trans_property$TransId,]
gc()
trans_main[,c("KeyedDate","KeyerID","SubVendorStndCode","MatchStndCode","BatchID","ZVendorStndCode","SourceChkSum")] <- NULL

# trans_main <- removeNABlank(trans_main)



trans_main <- merge(trans_main,trans_property,by="TransId")
rm(trans_property)
trans_main <- trans_main[trans_main$ImportParcelID>0]

trans_main[,DocumentDate:=as.Date(trans_main$DocumentDate)]
trans_main[,DocumentYear:=as.integer(format(trans_main$DocumentDate,"%Y"))]


trans_main <- trans_main[!trans_main$DocumentTypeStndCode %in% c("INTR","MTGE")]

trans_main[,DocumentTypeStndCode:=ifelse(trans_main$DocumentTypeStndCode %in% selected_deed_types,"AAAA",trans_main$DocumentTypeStndCode)]
trans_main <- trans_main[order(trans_main$ImportParcelID,-trans_main$DocumentDate,trans_main$DocumentTypeStndCode)]
trans_main <- trans_main[!duplicated(trans_main$ImportParcelID)]

trans_main <- trans_main[trans_main$DocumentTypeStndCode %in% c("AAAA")]


trans_main <- merge(trans_main,asmt_main,by="ImportParcelID")
listprice$zip <- NULL
listprice$state <- NULL
trans_main <- merge(trans_main,listprice,by.x = "full_address",by.y="address")




asmt_building <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/building.rds",sep=""))
asmt_building <- asmt_building[asmt_building$RowID %in% unique(trans_main$RowID)]
asmt_building <- asmt_building[,c("RowID","NoOfUnits","OccupancyStatusStndCode","PropertyCountyLandUseCode",
                                  "PropertyLandUseStndCode","YearBuilt","NoOfStories","TotalBedrooms",
                                  "TotalCalculatedBathCount")]
names(asmt_building) <- c("RowID","NoOfUnits","OccupancyStatus","CountyLandUseCode","LandUseStndCode",
                          "YearBuilt","NoOfStories","Bedrooms","Bathrooms")

trans_main <- merge(trans_main,asmt_building,by="RowID")


asmt_buildingarea <- readRDS(file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/",state_fips,"/ZAsmt/buildingareas.rds",sep="")) 
asmt_buildingarea <- asmt_buildingarea[asmt_buildingarea$RowID %in% unique(trans_main$RowID)]
asmt_buildingarea <- asmt_buildingarea[asmt_buildingarea$BuildingAreaStndCode=="BAL"]
asmt_buildingarea <- asmt_buildingarea[,c("RowID","BuildingAreaSqFt")]

trans_main <- merge(trans_main,asmt_buildingarea,by="RowID",all.x = TRUE)
trans_main <- trans_main[!duplicated(trans_main$ImportParcelID)]

  zhvi <- read.csv(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/Zip_Zhvi_AllHomes.csv",stringsAsFactors = FALSE)
  zhvi <- zhvi[zhvi$State %in% c(state_code),]
  zhvi[,c("RegionID","City","State","Metro","CountyName","SizeRank")] <- NULL
  zhvi <- melt(zhvi,id.vars = c("RegionName"))
  zhvi['month'] <- sapply(zhvi$variable,function(x) as.Date(paste(gsub("\\.","-",substr(x,2,8)),"-01",sep="")))
  zhvi$month <- as.Date(zhvi$month,origin = "1970-01-01")
  zhvi$variable <- NULL
  names(zhvi) <- c("zip","HPI_purchase","Purchase_month")

trans_main <- trans_main[!is.na(trans_main$DocumentDate)]
trans_main[,Purchase_month:=as.Date(paste(substr(trans_main$DocumentDate,1,7),"-01",sep=""))]
trans_main <- merge(trans_main,zhvi,by=c("zip","Purchase_month"))

names(zhvi) <- c("zip","HPI_list","List_date")
trans_main <- merge(trans_main,zhvi,by=c("zip","List_date"))

trans_main[,List_year:=as.integer(format(trans_main$List_date,"%Y"))]
trans_main[,Purchase_year:=as.integer(format(trans_main$Purchase_month,"%Y"))]
trans_main[,HPI_inflation:=trans_main$HPI_list/trans_main$HPI_purchase]
trans_main[,Adj_purch_price:=trans_main$SalesPriceAmount*trans_main$HPI_inflation]
trans_main[,List_premium:=trans_main$list_price/trans_main$Adj_purch_price]
trans_main[,Ownership_years:=trans_main$List_year-trans_main$Purchase_year]

library(rgdal)
library(sp)
library(rgeos)
library(ggplot2)

tracts <- readOGR(paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/cb_2017_",state_fips,"_tract_500k",sep=""),paste("cb_2017_",state_fips,"_tract_500k",sep=""))
tracts_df <- fortify(tracts,region="GEOID")


dat <- data.frame(trans_main[,c("ImportParcelID","Longitude","Latitude")])
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


trans_main <- merge(trans_main,df,by="ImportParcelID",all.x=TRUE)
saveRDS(trans_main,file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/list_data/",state_code,"_listing_data.rds",sep=""))
