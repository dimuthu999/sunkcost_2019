rm(list=ls())
library(reshape2)


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


  
  state_code="OR"
  state_fips="41"
  # Trulia Data -------------------------------------------------------------
  
  trulia <- readRDS("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/truliadata/trulia_20190319.rds")
  trulia <- trulia[trulia$state==state_code,]
  trulia$address <- tolower(paste(trulia$address,trulia$zip))
  
  # Oregon ------------------------------------------------------------------
  
  state = paste(state_fips,"/",sep="")
  
  path = "C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/"
  
  asmt_main <- readRDS(file=paste(path,state,"ZAsmt/main.rds",sep=""))
  
  asmt_main['full_address'] <- tolower(paste(asmt_main$PropertyFullStreetAddress, asmt_main$PropertyCity,asmt_main$PropertyState,asmt_main$PropertyZip))
  
  ImportParcelIDs <- unique(asmt_main[asmt_main$full_address %in% trulia$address,]$ImportParcelID)
  
  asmt_main <- asmt_main[asmt_main$ImportParcelID %in% ImportParcelIDs,]
  
  
  trans_property<- readRDS(file=paste(path,state,"ZTrans/propertyinfo.rds",sep=""))
  trans_property <- trans_property[trans_property$ImportParcelID %in% ImportParcelIDs,]
  
  
  trans_main <- readRDS(file=paste(path,state,"ZTrans/main.rds",sep=""))
  trans_main$SalesPriceAmount <- ifelse(is.na(trans_main$SalesPriceAmount),-1,trans_main$SalesPriceAmount)
  
  trans_main <- trans_main[trans_main$TransId %in% trans_property$TransId,]
  
  trans_main[,c("KeyedDate","KeyerID","SubVendorStndCode","MatchStndCode","BatchID","ZVendorStndCode","SourceChkSum")] <- NULL
  
  trans_main <- removeNABlank(trans_main)
  
  trans_property[,c("PropertyZip4","LegalLot","LegalSubdivisionName","PropertySequenceNumber","PropertyAddressMatchcode",
                    "PropertyAddressCarrierRoute","PropertyAddressGeoCodeMatchCode","FIPS","LoadID","BKFSPID","AssessmentRecordMatchFlag",
                    "BatchID")] <- NULL
  trans_property <- removeNABlank(trans_property)
  
  trans_main <- merge(trans_main,trans_property,by="TransId")
  rm(trans_property)
  
  trans_main <- trans_main[trans_main$SalesPriceAmount>0,]
  
  trans_main$DocumentTypeStndCode <- ifelse(trans_main$DocumentTypeStndCode=="WRDE","AAAA",trans_main$DocumentTypeStndCode)
  
  trans_main['DocumentMonth'] <- as.numeric(as.Date(paste(substr(trans_main$DocumentDate,1,7),"-01",sep="")))
  trans_main$DocumentDate <- as.Date(trans_main$DocumentDate)
  
  trans_main <- trans_main[order(trans_main$ImportParcelID,-trans_main$DocumentMonth,trans_main$DocumentTypeStndCode),]
  trans_main <- trans_main[!duplicated(trans_main$ImportParcelID),]
  
  trans_main$DocumentTypeStndCode <- ifelse(trans_main$DocumentTypeStndCode=="AAAA","WRDE",trans_main$DocumentTypeStndCode)
  
  asmt_main <- asmt_main[asmt_main$ImportParcelID %in% unique(trans_main$ImportParcelID),]
  RowIDs <- unique(asmt_main$RowID)
  
  asmt_main[,c("FIPS","State","County","Edition","ZVendorStndCode","UnformattedAssessorParcelNumber","ParcelSequenceNumber",
               "PropertyHouseNumber","PropertyStreetPreDirectional","PropertyStreetName","PropertyStreetSuffix","PropertyState","PropertyZip4",
               "TaxRateCodeArea","LegalLot","LegalSubdivisionName","LegalSecTwnRngMer","LegalDescription","LegalNeighborhoodSourceCode",
               "LotSizeAcres","LoadID","PropertyAddressMatchcode","PropertyAddressCarrierRoute","PropertyAddressGeoCodeMatchCode","SubEdition",
               "BatchID","SourceChkSum","PropertyAddressLatitude","PropertyAddressLongitude","PropertyAddressCensusTractAndBlock","BKFSPID")] <- NULL
  asmt_main <- removeNABlank(asmt_main)
  
  
  trans_main <-merge(trans_main,asmt_main,by="ImportParcelID")
  
  
  asmt_building <- readRDS(file=paste(path,state,"ZAsmt/building.rds",sep=""))
  asmt_building <- asmt_building[asmt_building$RowID %in% RowIDs,]
  asmt_building[,c("LoadID","FIPS","BatchID")] <- NULL
  asmt_building <- removeNABlank(asmt_building)
  
  trans_main <- merge(trans_main,asmt_building,by="RowID")
  
  historical_asmt <- readRDS(file=paste(path,"Historical/",state,"ZAsmt/main.rds",sep=""))
  historical_asmt <- historical_asmt[historical_asmt$ImportParcelID %in% unique(trans_main$ImportParcelID),]
  historical_asmt <- historical_asmt[,c("ImportParcelID","TaxAmount","TaxYear")]
  historical_asmt$TaxYear <- paste("year",historical_asmt$TaxYear,sep="")
  historical_asmt <- historical_asmt[!duplicated(historical_asmt[,c("ImportParcelID","TaxYear")]),]
  historical_asmt <- dcast(historical_asmt,ImportParcelID~TaxYear,value.var = "TaxAmount")
  historical_asmt[,c("year0","yearNA")] <- NULL
  
  trans_main <- merge(trans_main,historical_asmt,by="ImportParcelID")
  trans_main['purchased_year'] <- as.integer(format(trans_main$DocumentDate,"%Y"))
  
  
  trans_main <- trans_main[trans_main$purchased_year<=2014 & trans_main$purchased_year>=1997,]
  
  trans_main$year2014 <- ifelse(is.na(trans_main$year2014),trans_main$year2013*1.03,trans_main$year2014)
  trans_main['year2016'] <- trans_main$TaxAmount
  trans_main$year2015 <- ifelse(is.na(trans_main$year2015),(trans_main$year2014+trans_main$year2016)/2,trans_main$year2015)
  trans_main['year2017'] <- trans_main$year2016 * 1.06
  trans_main['year2018'] <- trans_main$year2016 * 1.06
  
  trans_main['property_tax_1418'] <- trans_main$year2014+trans_main$year2015+trans_main$year2016+trans_main$year2017+trans_main$year2018
  
  trans_main <- merge(trans_main,trulia[,c("address","zip","sqft","n_baths","n_beds","list_price")],by.x="full_address", by.y = "address")
  
  saveRDS(trans_main,file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztrax_processed_",state_code,".rds",sep=""))

