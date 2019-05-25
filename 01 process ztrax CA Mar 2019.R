rm(list=ls())
library(reshape2)
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


  
  state_code="CA"
  state_fips="06"
  # Zillow Data -------------------------------------------------------------
  
  zillow <- readRDS("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ca_2016_data_Mar2019.rds")
  zillow['address'] <- paste(zillow$street,zillow$city,"CA",zillow$zip)
  zillow$address <- tolower(paste(zillow$address))
  zillow <- zillow[!duplicated(zillow$link),]
 
  
  # CA ------------------------------------------------------------------
  
  state = paste(state_fips,"/",sep="")
  
  path = "C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/"
  
  asmt_main <- readRDS(file=paste(path,state,"ZAsmt/main.rds",sep=""))
  
  asmt_main[,full_address:= tolower(paste(asmt_main$PropertyFullStreetAddress, asmt_main$PropertyCity,asmt_main$PropertyState,asmt_main$PropertyZip))]
  
  ImportParcelIDs <- unique(asmt_main[asmt_main$full_address %in% zillow$address,]$ImportParcelID)
  
  asmt_main <- asmt_main[asmt_main$ImportParcelID %in% ImportParcelIDs,]
  
  
  # asmt_value <- readRDS(file=paste(path,state,"ZAsmt/value.rds",sep=""))
  
  trans_property<- readRDS(file=paste(path,state,"ZTrans/propertyinfo.rds",sep=""))
  trans_property <- trans_property[trans_property$ImportParcelID %in% ImportParcelIDs,]
  
  
  trans_main <- readRDS(file=paste(path,state,"ZTrans/main.rds",sep=""))
  trans_main <- trans_main[trans_main$TransId %in% trans_property$TransId,]
  
  trans_main <- merge(trans_main,trans_property,by="TransId")
  rm(trans_property)
  
  # trans_main <- trans_main[trans_main$SalesPriceAmount>0,]
  temp <- asmt_main[,c("full_address","ImportParcelID")]
  temp <- temp[!duplicated(temp)]
  zillow <- merge(zillow,temp,by.x = "address",by.y = "full_address")
  
  
  zillow_merge <- zillow[,c("link","listed_date","listing_amount","sale_date","sales_price","purchased_date","purchased_amount","ImportParcelID")]
  zillow_merge <- data.table(zillow_merge)
  zillow_merge <- zillow_merge[!duplicated(zillow_merge$link)]
  zillow_merge[,DocumentDate:=listed_date+1]

  trans_merge <- trans_main[,c("TransId","DocumentDate","SalesPriceAmount","DocumentTypeStndCode","ImportParcelID")]
  trans_merge[,merge_date:=DocumentDate]
  trans_merge <- trans_merge[!is.na(trans_main$ImportParcelID)]
  names(trans_merge) <- c("TransId_it1","DocumentDate","SalesPriceAmount_it1","DocumentTypeStndCode_it1","ImportParcelID","merge_date_it1")
  
  
  setkey(trans_merge,ImportParcelID,DocumentDate)
  setkey(zillow_merge,ImportParcelID,DocumentDate)
  
  merged_data <- trans_merge[zillow_merge,roll=-Inf]
  merged_data[,INTR_it1:=ifelse(merged_data$DocumentTypeStndCode_it1=="INTR",1,0)]
  merged_data[,success_it1:=ifelse(merged_data$DocumentTypeStndCode_it1=="GRDE",1,0)]
  merged_data <- merged_data[merged_data$success_it1==1 | is.na(merged_data$DocumentTypeStndCode_it1)| !is.na(merged_data$sales_price) | merged_data$DocumentTypeStndCode_it1 %in% c("GRDE","MTGE","INTR")]
  merged_data[,DocumentDate:=merge_date_it1+1]
  
  names(trans_merge) <- c("TransId_it2","DocumentDate","SalesPriceAmount_it2","DocumentTypeStndCode_it2","ImportParcelID","merge_date_it2")
  setkey(trans_merge,ImportParcelID,DocumentDate)
  setkey(merged_data,ImportParcelID,DocumentDate)
  
  merged_data <- trans_merge[merged_data,roll=-Inf]
  merged_data <- merged_data[!duplicated(merged_data$link)]
  merged_data[,INTR_it2:=ifelse(merged_data$DocumentTypeStndCode_it2=="INTR",1,0)]
  merged_data[,success_it2:=ifelse(merged_data$DocumentTypeStndCode_it2=="GRDE" | merged_data$success_it1==1,1,0)]
  merged_data <- merged_data[merged_data$success_it2==1 | is.na(merged_data$DocumentTypeStndCode_it1)| !is.na(merged_data$sales_price) | merged_data$DocumentTypeStndCode_it2 %in% c("GRDE","MTGE","INTR")]

  merged_data[,TransId_next:= ifelse(merged_data$success_it1==1,merged_data$TransId_it1,
                                     ifelse(merged_data$success_it2==1,merged_data$TransId_it2,NA))]
  
  merged_data[,INTR:= ifelse(merged_data$success_it1==1,0,
                                     ifelse(merged_data$success_it2==1 & merged_data$INTR_it1==1,1,0))]
  
  sales_data <- merged_data[,c("link","listed_date","listing_amount","sale_date","sales_price","purchased_date","purchased_amount","ImportParcelID","TransId_next","INTR")]
  sales_data <- sales_data[!duplicated(sales_data)]
  sales_data <- sales_data[!duplicated(sales_data$link)]
  
  
  
  
  zillow_merge <- zillow[,c("link","listed_date","listing_amount","sale_date","sales_price","purchased_date","purchased_amount","ImportParcelID")]
  zillow_merge <- data.table(zillow_merge)
  zillow_merge <- zillow_merge[!duplicated(zillow_merge$link)]
  zillow_merge[,DocumentDate:=listed_date-1]
  
  trans_merge <- trans_main[,c("TransId","DocumentDate","SalesPriceAmount","DocumentTypeStndCode","ImportParcelID")]
  trans_merge[,merge_date:=DocumentDate]
  trans_merge <- trans_merge[!is.na(trans_main$ImportParcelID)]
  names(trans_merge) <- c("TransId_it1","DocumentDate","SalesPriceAmount_it1","DocumentTypeStndCode_it1","ImportParcelID","merge_date_it1")
  
  setkey(trans_merge,ImportParcelID,DocumentDate)
  setkey(zillow_merge,ImportParcelID,DocumentDate)
  
  
  merged_data <- trans_merge[zillow_merge,roll=TRUE]
  merged_data$DocumentTypeStndCode_it1 <- ifelse(merged_data$DocumentTypeStndCode_it1=="GRDE","AAAA",merged_data$DocumentTypeStndCode_it1)
  merged_data <- merged_data[order(merged_data$ImportParcelID,merged_data$DocumentTypeStndCode_it1),]
  merged_data <- merged_data[!duplicated(merged_data$link)]
  merged_data$DocumentTypeStndCode_it1 <- ifelse(merged_data$DocumentTypeStndCode_it1=="AAAA","GRDE",merged_data$DocumentTypeStndCode_it1)
  merged_data[,INTR_it1:=ifelse(merged_data$DocumentTypeStndCode_it1=="INTR" ,1,0)]
  merged_data[,success_it1:=ifelse(merged_data$DocumentTypeStndCode_it1=="GRDE" ,1,0)]
  merged_data <- merged_data[is.na(merged_data$DocumentTypeStndCode_it1) | merged_data$DocumentTypeStndCode_it1 %in% c("GRDE","MTGE","INTR")]
  merged_data[,DocumentDate:=merge_date_it1-1]
  
  names(trans_merge) <- c("TransId_it2","DocumentDate","SalesPriceAmount_it2","DocumentTypeStndCode_it2","ImportParcelID","merge_date_it2")
  setkey(trans_merge,ImportParcelID,DocumentDate)
  setkey(merged_data,ImportParcelID,DocumentDate)
  
  merged_data <- trans_merge[merged_data,roll=TRUE]
  merged_data$DocumentTypeStndCode_it2 <- ifelse(merged_data$DocumentTypeStndCode_it2=="GRDE","AAAA",merged_data$DocumentTypeStndCode_it2)
  merged_data <- merged_data[order(merged_data$ImportParcelID,merged_data$DocumentTypeStndCode_it2),]
  merged_data <- merged_data[!duplicated(merged_data$link)]
  merged_data$DocumentTypeStndCode_it2 <- ifelse(merged_data$DocumentTypeStndCode_it2=="AAAA","GRDE",merged_data$DocumentTypeStndCode_it2)
  merged_data[,INTR_it2:=ifelse(merged_data$DocumentTypeStndCode_it2=="INTR" ,1,0)]
  merged_data[,success_it2:=ifelse(merged_data$success_it1==1 | (merged_data$DocumentTypeStndCode_it2=="GRDE" )>0,1,0)]
  merged_data <- merged_data[is.na(merged_data$DocumentTypeStndCode_it1) | merged_data$success_it2==1 | merged_data$DocumentTypeStndCode_it2 %in% c("GRDE","MTGE","INTR")]
  merged_data[,DocumentDate:=merge_date_it2-1]
  
  
  names(trans_merge) <- c("TransId_it3","DocumentDate","SalesPriceAmount_it3","DocumentTypeStndCode_it3","ImportParcelID","merge_date_it3")
  setkey(trans_merge,ImportParcelID,DocumentDate)
  setkey(merged_data,ImportParcelID,DocumentDate)
  
  merged_data <- trans_merge[merged_data,roll=TRUE]
  merged_data$DocumentTypeStndCode_it3 <- ifelse(merged_data$DocumentTypeStndCode_it3=="GRDE","AAAA",merged_data$DocumentTypeStndCode_it3)
  merged_data <- merged_data[order(merged_data$ImportParcelID,merged_data$DocumentTypeStndCode_it3),]
  merged_data <- merged_data[!duplicated(merged_data$link)]
  merged_data$DocumentTypeStndCode_it3 <- ifelse(merged_data$DocumentTypeStndCode_it3=="AAAA","GRDE",merged_data$DocumentTypeStndCode_it3)
  merged_data[,INTR_it3:=ifelse(merged_data$DocumentTypeStndCode_it3=="INTR" ,1,0)]
  merged_data[,success_it3:=ifelse(merged_data$success_it1==1 | merged_data$success_it2==1 |(merged_data$DocumentTypeStndCode_it3=="GRDE" )>0,1,0)]
  merged_data <- merged_data[is.na(merged_data$DocumentTypeStndCode_it1) | merged_data$success_it2==1 | merged_data$success_it3== 1| merged_data$DocumentTypeStndCode_it3 %in% c("GRDE","MTGE","INTR")]
  merged_data[,DocumentDate:=merge_date_it3-1]
  
  names(trans_merge) <- c("TransId_it4","DocumentDate","SalesPriceAmount_it4","DocumentTypeStndCode_it4","ImportParcelID","merge_date_it4")
  setkey(trans_merge,ImportParcelID,DocumentDate)
  setkey(merged_data,ImportParcelID,DocumentDate)
  
  merged_data <- trans_merge[merged_data,roll=TRUE]
  merged_data$DocumentTypeStndCode_it4 <- ifelse(merged_data$DocumentTypeStndCode_it4=="GRDE","AAAA",merged_data$DocumentTypeStndCode_it4)
  merged_data <- merged_data[order(merged_data$ImportParcelID,merged_data$DocumentTypeStndCode_it4),]
  merged_data <- merged_data[!duplicated(merged_data$link)]
  merged_data$DocumentTypeStndCode_it4 <- ifelse(merged_data$DocumentTypeStndCode_it4=="AAAA","GRDE",merged_data$DocumentTypeStndCode_it4)
  merged_data[,INTR_it4:=ifelse(merged_data$DocumentTypeStndCode_it4=="INTR" ,1,0)]
  merged_data[,success_it4:=ifelse(merged_data$success_it3==1  |(merged_data$DocumentTypeStndCode_it4=="GRDE")>0,1,0)]
  merged_data <- merged_data[is.na(merged_data$DocumentTypeStndCode_it1) | merged_data$success_it4==1 | merged_data$DocumentTypeStndCode_it4 %in% c("GRDE","MTGE","INTR")]
  merged_data[,DocumentDate:=merge_date_it4-1]
  
  
  names(trans_merge) <- c("TransId_it5","DocumentDate","SalesPriceAmount_it5","DocumentTypeStndCode_it5","ImportParcelID","merge_date_it5")
  setkey(trans_merge,ImportParcelID,DocumentDate)
  setkey(merged_data,ImportParcelID,DocumentDate)
  
  merged_data <- trans_merge[merged_data,roll=TRUE]
  merged_data$DocumentTypeStndCode_it5 <- ifelse(merged_data$DocumentTypeStndCode_it5=="GRDE","AAAA",merged_data$DocumentTypeStndCode_it5)
  merged_data <- merged_data[order(merged_data$ImportParcelID,merged_data$DocumentTypeStndCode_it5),]
  merged_data <- merged_data[!duplicated(merged_data$link)]
  merged_data$DocumentTypeStndCode_it5 <- ifelse(merged_data$DocumentTypeStndCode_it5=="AAAA","GRDE",merged_data$DocumentTypeStndCode_it5)
  merged_data[,INTR_it5:=ifelse(merged_data$DocumentTypeStndCode_it5=="INTR" ,1,0)]
  merged_data[,success_it5:=ifelse(merged_data$success_it4==1  |(merged_data$DocumentTypeStndCode_it5=="GRDE")>0,1,0)]
  merged_data <- merged_data[is.na(merged_data$DocumentTypeStndCode_it1) | merged_data$success_it5==1 | merged_data$DocumentTypeStndCode_it5 %in% c("GRDE","MTGE","INTR")]
  
  
  merged_data[,TransId_prev:= ifelse(merged_data$success_it1==1,merged_data$TransId_it1,
                                     ifelse(merged_data$success_it2==1,merged_data$TransId_it2,
                                            ifelse(merged_data$success_it3==1,merged_data$TransId_it3,
                                                   ifelse(merged_data$success_it4,merged_data$TransId_it4,merged_data$TransId_it5))))]
  
  merged_data[,INTR:= ifelse(merged_data$success_it1==1,0,
                                     ifelse(merged_data$success_it2==1 & merged_data$INTR_it1==1,1,
                                            ifelse(merged_data$success_it3==1 & (merged_data$INTR_it1==1 | merged_data$INTR_it2==1),1,
                                                   ifelse(merged_data$success_it4==1 & (merged_data$INTR_it1==1 | merged_data$INTR_it2==1 | merged_data$INTR_it3==1),1,
                                                          ifelse(merged_data$success_it5==5 & (merged_data$INTR_it1==1 | merged_data$INTR_it2==1 | merged_data$INTR_it3==1 | merged_data$INTR_it4==1),1,0))))) ]
  
  
  purchase_data <- merged_data[,c("link","TransId_prev","INTR")]
  purchase_data <- purchase_data[!duplicated(purchase_data)]
  purchase_data <- purchase_data[!duplicated(purchase_data$link)]
  
  
  
  
  zillow_ztrax <- merge(sales_data,purchase_data,by="link")
  
  
  trans_merge <- trans_main[,c("TransId","DocumentDate","SalesPriceAmount","DocumentTypeStndCode","ImportParcelID","LoanAmount")]
  names(trans_merge) <- c("TransId_prev","DocumentDate_prev","SalesPriceAmount_prev","DocumentTypeStndCode_prev","ImportParcelID_prev","LoanAmount_prev")
  
  zillow_ztrax <- merge(zillow_ztrax,trans_merge,by="TransId_prev",all.x = TRUE)
  
  zillow_ztrax[,SalesPriceAmount_prev:=ifelse(is.na(zillow_ztrax$SalesPriceAmount_prev) | zillow_ztrax$SalesPriceAmount_prev==0,zillow_ztrax$purchased_amount,zillow_ztrax$SalesPriceAmount_prev)]
  zillow_ztrax[,DocumentDate_prev:=ifelse(is.na(zillow_ztrax$DocumentDate_prev),zillow_ztrax$purchased_date,zillow_ztrax$DocumentDate_prev)]
  
  
  # names(trans_merge) <- c("TransId_next","DocumentDate_next","SalesPriceAmount_next","DocumentTypeStndCode_next","ImportParcelID_next")
  # zillow_ztrax <- merge(zillow_ztrax,trans_merge,by="TransId_next",all.x = TRUE)
  # 
  # zillow_ztrax[,SalesPriceAmount_next:=ifelse(is.na(zillow_ztrax$SalesPriceAmount_next) | zillow_ztrax$SalesPriceAmount_next==0,zillow_ztrax$sales_price,zillow_ztrax$SalesPriceAmount_prev)]
  # zillow_ztrax[,DocumentDate_next:=ifelse(is.na(zillow_ztrax$DocumentDate_next),zillow_ztrax$sale_date,zillow_ztrax$DocumentDate_next)]
  # 
  
  zillow_ztrax <- zillow_ztrax[!is.na(zillow_ztrax$listing_amount)  & !is.na(zillow_ztrax$SalesPriceAmount_prev)]
  zillow_ztrax <- zillow_ztrax[!duplicated(zillow_ztrax$link)]
  zillow_ztrax[,c("purchased_date","purchased_amount","ImportParcelID_prev","ImportParcelID_next"):=list(NULL)]

  
  zillow_ztrax <- merge(zillow_ztrax,zillow[,c("link","address","zip","beds","baths","sqft","avg_school_rating","avg_school_distance","walk_score","proptax_2010","proptax_2011","proptax_2012","proptax_2013","proptax_2014","proptax_2015","street","city","assesment_2015","assesment_2014","assesment_2013","assesment_2012","assesment_2011","assesment_2010","assesment_2009","assesment_2008","assesment_2007","assesment_2006","assesment_2005","assesment_2004")],by="link")
  zillow_ztrax <- zillow_ztrax[!duplicated(zillow_ztrax$link)]
  
  
  
  
  
  
  asmt_main <- asmt_main[asmt_main$ImportParcelID %in% unique(zillow_ztrax$ImportParcelID),]
  RowIDs <- unique(asmt_main$RowID)
  
  asmt_main[,c("FIPS","State","County","Edition","ZVendorStndCode","UnformattedAssessorParcelNumber","ParcelSequenceNumber",
               "PropertyHouseNumber","PropertyStreetPreDirectional","PropertyStreetName","PropertyStreetSuffix","PropertyState","PropertyZip4",
               "TaxRateCodeArea","LegalLot","LegalSubdivisionName","LegalSecTwnRngMer","LegalDescription","LegalNeighborhoodSourceCode",
               "LotSizeAcres","LoadID","PropertyAddressMatchcode","PropertyAddressCarrierRoute","PropertyAddressGeoCodeMatchCode","SubEdition",
               "BatchID","SourceChkSum","BKFSPID")] <- NULL
  asmt_main[,c("ValueCertDate","ExtractDate","AssessorParcelNumber","PropertyFullStreetAddress"
               ,"PropertyCity","PropertyZip","full_address"):=list(NULL)]
  asmt_main <- removeNABlank(asmt_main)
  
  
  zillow_ztrax <-merge(zillow_ztrax,asmt_main,by="ImportParcelID",all.x = TRUE)
  
  
  
  asmt_building <- readRDS(file=paste(path,state,"ZAsmt/building.rds",sep=""))
  asmt_building <- asmt_building[asmt_building$RowID %in% RowIDs,]
  asmt_building[,c("LoadID","FIPS","BatchID")] <- NULL
  asmt_building <- removeNABlank(asmt_building)
  
  zillow_ztrax <- merge(zillow_ztrax,asmt_building,by="RowID",all.x = TRUE)
  
  
  
  historical_asmt <- readRDS(file=paste(path,"Historical/",state,"main.rds",sep=""))
  historical_asmt <- historical_asmt[historical_asmt$ImportParcelID %in% unique(zillow_ztrax$ImportParcelID),]
  historical_asmt <- historical_asmt[,c("ImportParcelID","TaxAmount","TaxYear")]
  historical_asmt <- historical_asmt[!is.na(historical_asmt$TaxAmount)]
  historical_asmt$TaxYear <- paste("year",historical_asmt$TaxYear,sep="")
  historical_asmt <- historical_asmt[!duplicated(historical_asmt[,c("ImportParcelID","TaxYear")]),]
  historical_asmt <- dcast(historical_asmt,ImportParcelID~TaxYear,value.var = "TaxAmount")
  historical_asmt[,c("year0","yearNA")] <- NULL
  
  historical_asmt <- historical_asmt[,c("ImportParcelID","year2014","year2015")]
  
  zillow_ztrax <-merge(zillow_ztrax,historical_asmt,by="ImportParcelID",all.x = TRUE)
  
  
  zillow_ztrax[,year2015:=ifelse(is.na(zillow_ztrax$year2015) & zillow_ztrax$TaxYear==2015,zillow_ztrax$TaxAmount,zillow_ztrax$year2015)]
  zillow_ztrax[,year2015:=ifelse(is.na(zillow_ztrax$year2015),zillow_ztrax$proptax_2015,zillow_ztrax$year2015)]
  zillow_ztrax[,year2016:=ifelse(zillow_ztrax$TaxYear==2016,zillow_ztrax$TaxAmount,NA)]
  zillow_ztrax[,year2014:=ifelse(is.na(zillow_ztrax$year2014),zillow_ztrax$proptax_2014,zillow_ztrax$year2014)]
  
  zillow_ztrax[,PurchaseMonth:=as.Date(zillow_ztrax$DocumentDate_prev,origin = "1970-01-01")]
  zillow_ztrax[,PurchaseMonth:=as.Date(paste(substr(zillow_ztrax$PurchaseMonth,1,7),"-01",sep=""))]
  
  zillow_ztrax[,ListingMonth:=as.Date(paste(substr(zillow_ztrax$listed_date,1,7),"-01",sep=""))]
  
  zhvi <- read.csv(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/Zip_Zhvi_AllHomes.csv",stringsAsFactors = FALSE)
  zhvi <- zhvi[zhvi$State %in% c("CA"),]
  zhvi[,c("RegionID","City","State","Metro","CountyName","SizeRank")] <- NULL
  zhvi <- melt(zhvi,id.vars = c("RegionName"))
  zhvi['month'] <- sapply(zhvi$variable,function(x) as.Date(paste(gsub("\\.","-",substr(x,2,8)),"-01",sep="")))
  zhvi$month <- as.Date(zhvi$month,origin = "1970-01-01")
  zhvi$variable <- NULL
  names(zhvi) <- c("zip","purchase_hpi","PurchaseMonth")
  
  zillow_ztrax <-merge(zillow_ztrax,zhvi,by=c("zip","PurchaseMonth"))
  
  names(zhvi) <- c("zip","listing_hpi","ListingMonth")
  
  zillow_ztrax <-merge(zillow_ztrax,zhvi,by=c("zip","ListingMonth"))
  
  zillow_ztrax[,hpi_inflation:=zillow_ztrax$listing_hpi/zillow_ztrax$purchase_hpi]
  zillow_ztrax[,adj_purch_price:=zillow_ztrax$SalesPriceAmount_prev*zillow_ztrax$hpi_inflation]
  zillow_ztrax <- zillow_ztrax[zillow_ztrax$adj_purch_price>0]
  zillow_ztrax[,list_premium:=zillow_ztrax$listing_amount/zillow_ztrax$adj_purch_price]
  
  zillow_ztrax <- zillow_ztrax[zillow_ztrax$list_premium >=quantile(zillow_ztrax$list_premium,0.01) & zillow_ztrax$list_premium<=quantile(zillow_ztrax$list_premium,0.99)]
  
  zillow_ztrax[,ListingYear:=as.integer(format(zillow_ztrax$ListingMonth,"%Y"))]
  zillow_ztrax[,PurchaseYear:=as.integer(format(zillow_ztrax$PurchaseMonth,"%Y"))]
  
  zillow_ztrax[,ownership_years:=zillow_ztrax$ListingYear-zillow_ztrax$PurchaseYear]
  zillow_ztrax[,house_age:=zillow_ztrax$ListingYear-zillow_ztrax$YearBuilt]
  
  zillow_ztrax[,prop_tax_prev_year:= ifelse(zillow_ztrax$ListingYear==2014,zillow_ztrax$proptax_2013,
                                            ifelse(zillow_ztrax$ListingYear==2015,zillow_ztrax$year2014,
                                                   ifelse(zillow_ztrax$ListingYear==2016,zillow_ztrax$year2015,
                                                          ifelse(zillow_ztrax$ListingYear==2017,zillow_ztrax$year2016,NA))))]
  
  zillow_ztrax[,prop_tax_rate:=zillow_ztrax$prop_tax_prev_year/zillow_ztrax$adj_purch_price]
  zillow_ztrax[,LTV_prev:=zillow_ztrax$LoanAmount_prev/zillow_ztrax$SalesPriceAmount_prev]
  
  zillow_ztrax <- zillow_ztrax[zillow_ztrax$prop_tax_rate > 0 & zillow_ztrax$prop_tax_rate<=quantile(zillow_ztrax$prop_tax_rate,0.99,na.rm = TRUE)]
  
  
  zillow_ztrax[,address_geo:=paste(zillow_ztrax$street,zillow_ztrax$city,"CA",zillow_ztrax$zip,sep=", ")]
  zillow_ztrax <- zillow_ztrax[!duplicated(zillow_ztrax$address_geo),]
  
  # zillow_ztrax[,state:="CA"]
  # zillow_ztrax[,id:=1:nrow(zillow_ztrax)]
  # 
  # for(i in seq(0,nrow(zillow_ztrax),by = 10000)) {
  #   print(i)
  #   write.table(zillow_ztrax[(i+1):(i+10000),c("id","street","city","state","zip")],file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/address_geo_",i,".csv",sep=""),
  #             col.names = FALSE,row.names = FALSE,quote = FALSE,sep=",")
  # }
  
  
  geo_results <- read.csv("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/GeocodeResults.csv",stringsAsFactors = FALSE)
  geo_results <- geo_results[!duplicated(geo_results$INPUT.ADDRESS),c("INPUT.ADDRESS","COUNTY.CODE","TRACT.CODE","INTERPOLATED.LONGITUDE.AND.LATITUDE")]
  names(geo_results) <- c("address_geo","county","tract","geo_cords")
  library(stringr)
  geo_results <- cbind(geo_results,str_split_fixed(geo_results$geo_cords,",",2))
  names(geo_results) <- c("address_geo","county","tract","geo_cords","long","lat")
  geo_results$lat <- as.numeric(as.character(geo_results$lat))
  geo_results$long <- as.numeric(as.character(geo_results$long))
  geo_results$geo_cords <- NULL
  
  zillow_ztrax <- merge(zillow_ztrax,geo_results,by="address_geo",all.x=TRUE)
  
  zillow_ztrax[,long:=ifelse(is.na(zillow_ztrax$long),zillow_ztrax$PropertyAddressLongitude,zillow_ztrax$long)]
  zillow_ztrax[,lat:=ifelse(is.na(zillow_ztrax$lat),zillow_ztrax$PropertyAddressLatitude,zillow_ztrax$lat)]
  
  
  library(rgdal)
  library(sp)
  library(rgeos)
  library(ggplot2)
  
  tracts <- readOGR("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/cb_2017_06_tract_500k","cb_2017_06_tract_500k")
  tracts_df <- fortify(tracts,region="GEOID")
  
  
  dat <- data.frame(zillow_ztrax[,c("ImportParcelID","long","lat")])
  dat <- dat[!is.na(dat$lat),]
  
  
  

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
  
  
  zillow_ztrax <- merge(zillow_ztrax,df,by="ImportParcelID",all.x=TRUE)
  
  
  library(tidycensus)
  census_api_key("06b232797e7854aad802d7c7d8673c337fe1b29a")
  year=2016
  
  variables <- c("B19013_001E","B01002_001E","B01003_001E","B07013_003E")
  names <- c("GEOID","medianhouseholdincome","medianage","totalpopulation","renters")
  var_remove <-  gsub("E","M",variables)
  
  acs <- get_acs(geography = "tract",state = "06", year = 2015,output = "wide", variables = variables)
  acs[, var_remove] <- NULL
  acs$NAME <- NULL
  names(acs) <- names
  
  zillow_ztrax <- merge(zillow_ztrax,acs,by="GEOID",all.x=TRUE)
  
  
  trans_merge <- trans_main[,c("TransId","LoanAmount")]
  names(trans_merge) <- c("TransId_next","LoanAmount_next")
  
  
  zillow_ztrax <- merge(zillow_ztrax,trans_merge,by="TransId_next",all.x=TRUE)
  
  
  redfin <- read.csv("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/home_sales_redfin.csv")
  redfin$month <- as.Date(redfin$month,origin="1900-01-01")
  redfin <- data.table(redfin)
  redfin <- redfin[redfin$month<="2016-01-01"]
  redfin <- redfin[,.(median_home_sales=median(home_sales)),by=list(zip)]
  redfin[,low_activiy:=ifelse(redfin$median_home_sales<quantile(redfin$median_home_sales,0.7),1,0)]
  
  zillow_ztrax <- merge(zillow_ztrax,redfin,by="zip",all.x=TRUE)
  
  list_price_qs <- zillow_ztrax[,.(
                                    listing_q1=quantile(listing_amount,0.25),
                                    listing_q2=quantile(listing_amount,0.5),
                                    listing_q3=quantile(listing_amount,0.75)),
                                by=list(zip)]
  
  
  zillow_ztrax <- merge(zillow_ztrax,list_price_qs,by="zip",all.x=TRUE)
  
  zillow_ztrax[,ownership_cohort:= ntile(zillow_ztrax$ownership_years,10)]
  
  
  inflation_factors <- data.frame(1996:2017,c(1.0111,1.02,1.02,1.01853,rep(1.02,4),1.01867,rep(1.02,5),0.99763,1.00753,1.02,1.02,1.00454,1.01998,1.01525,1.02))
  names(inflation_factors) <- c("year","inflation_factor")
  
  inflation_matrix <- matrix(NA,ncol = 4,nrow=length(1996:2017),dimnames = list(1996:2017,2014:2017))
  
  for(list_year in dimnames(inflation_matrix)[[2]]) {
    for(purch_year in dimnames(inflation_matrix)[[1]]) {
      inf_fac <- 1
      for(i in (as.numeric(purch_year)+1):(as.numeric(list_year)-1)) {
        print(i)
        if(as.numeric(list_year)>as.numeric(purch_year)+2) {
          inf_fac <- inf_fac * inflation_factors[inflation_factors$year==i,]$inflation_factor
        }
      }
      inflation_matrix[purch_year,list_year]=inf_fac
      # inflation_matrix[paste("'",purch_year,"'",sep=""),paste("'",list_year,"'",sep="")]=inf_fac
    }
  }
  
  inflation_matrix <- data.frame(inflation_matrix)
  inflation_matrix['purchase_year'] <- row.names(inflation_matrix)
  inflation_matrix <- melt(inflation_matrix,id.vars = c("purchase_year"))
  inflation_matrix['purchase_list_year'] <- paste(inflation_matrix$purchase_year,substr(inflation_matrix$variable,2,5)) 
  inflation_matrix[,c("purchase_year","variable")]<-NULL
  names(inflation_matrix) <- c("inflation_factor_1","purchase_list_year")
  
  zillow_ztrax[,purchase_list_year:=paste(zillow_ztrax$PurchaseYear,zillow_ztrax$ListingYear)]
  
  zillow_ztrax <- merge(zillow_ztrax,inflation_matrix,by="purchase_list_year",all.x=TRUE)
  
  zillow_ztrax[,asmt_prev_year_calc:=zillow_ztrax$SalesPriceAmount_prev*zillow_ztrax$inflation_factor_1]
  
  zillow_ztrax[,asmt_prev_year:= ifelse(zillow_ztrax$ListingYear==2014,zillow_ztrax$assesment_2013,
                                            ifelse(zillow_ztrax$ListingYear==2015,zillow_ztrax$assesment_2014,
                                                   ifelse(zillow_ztrax$ListingYear==2016,zillow_ztrax$assesment_2015,
                                                          ifelse(zillow_ztrax$ListingYear==2017,zillow_ztrax$assesment_2015*1.02,NA))))]
  
  inflation_matrix <- matrix(NA,ncol = 4,nrow=length(1996:2017),dimnames = list(1996:2017,2014:2017))
  
  for(list_year in dimnames(inflation_matrix)[[2]]) {
    for(purch_year in dimnames(inflation_matrix)[[1]]) {
      inf_fac <- 1
      for(i in (as.numeric(purch_year)):(as.numeric(list_year)-1)) {
        print(i)
        if(as.numeric(list_year)>as.numeric(purch_year)+2) {
          inf_fac <- inf_fac * inflation_factors[inflation_factors$year==i,]$inflation_factor
        }
      }
      inflation_matrix[purch_year,list_year]=inf_fac
      # inflation_matrix[paste("'",purch_year,"'",sep=""),paste("'",list_year,"'",sep="")]=inf_fac
    }
  }
  
  inflation_matrix <- data.frame(inflation_matrix)
  inflation_matrix['purchase_year'] <- row.names(inflation_matrix)
  inflation_matrix <- melt(inflation_matrix,id.vars = c("purchase_year"))
  inflation_matrix['purchase_list_year'] <- paste(inflation_matrix$purchase_year,substr(inflation_matrix$variable,2,5)) 
  inflation_matrix[,c("purchase_year","variable")]<-NULL
  names(inflation_matrix) <- c("inflation_factor_2","purchase_list_year")
  
  zillow_ztrax <- merge(zillow_ztrax,inflation_matrix,by="purchase_list_year",all.x=TRUE)
  
  zillow_ztrax[,asmt_prev_year_calc_2:=zillow_ztrax$SalesPriceAmount_prev*zillow_ztrax$inflation_factor_2]
  
  zillow_ztrax[,asmt_diff:=zillow_ztrax$asmt_prev_year_calc_2-zillow_ztrax$asmt_prev_year]
  zillow_ztrax[,asmt_diff:=zillow_ztrax$asmt_diff/zillow_ztrax$listing_amount]
  zillow_ztrax[,affected_by_prop_8:=ifelse(zillow_ztrax$asmt_diff>0.1,1,0)]
  
  saveRDS(zillow_ztrax,file=paste("C:/Users/dnratnadiwakara/Documents/sunkcost_2019/zillow_ztrax_Mar2019_5.rds",sep=""))

  
  
  
  
  temp <- zillow_ztrax[zillow_ztrax$ListingYear>2013 & zillow_ztrax$list_premium<1.5 & zillow_ztrax$ownership_years>=4 ]
  
  formula <- log(listing_amount)~log(adj_purch_price)+beds+baths+sqft+avg_school_rating+avg_school_distance+walk_score+
    log(house_age)+log(LotSizeSquareFeet)+INTR.y+LTV_prev+affected_by_prop_8+medianage+log(medianhouseholdincome)+log(totalpopulation)|
    GEOID+ListingMonth+ownership_cohort|(prop_tax_rate~log(purchase_hpi)+ownership_years)|GEOID
  
  regs <- list()
  regs[[1]] <- felm(log(listing_amount)~0|zip+ListingMonth|(prop_tax_rate~log(purchase_hpi)+ownership_years)|zip,data=temp)
  regs[[2]] <- felm(formula
                    ,data=temp[temp$house_age>0])
  stargazer(regs,type="text",no.space = TRUE,dep.var.labels.include = FALSE)
  
  
  regs <- list()
  regs[[1]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$purchase_hpi>=temp$listing_hpi])
  regs[[2]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$purchase_hpi<temp$listing_hpi])
  stargazer(regs,type="text",no.space = TRUE,dep.var.labels.include = FALSE)
  
  
  
  regs <- list()
  regs[[1]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$low_activiy==1])
  regs[[2]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$low_activiy==0])
  stargazer(regs,type="text",no.space = TRUE,dep.var.labels.include = FALSE)
  
  
 
  regs <- list()
  regs[[1]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$listing_amount>temp$listing_q2])
  regs[[2]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$listing_amount<temp$listing_q2])
  stargazer(regs,type="text",no.space = TRUE,dep.var.labels.include = FALSE)
  
  
  
  regs <- list()
  regs[[1]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$LTV_prev!=0 & temp$LTV_prev<0.5 & temp$listing_hpi<temp$purchase_hpi])
  regs[[2]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$LTV_prev!=0 & temp$LTV_prev<0.5 & temp$listing_hpi>=temp$purchase_hpi])
  regs[[3]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$LTV_prev>=0.7 & temp$listing_hpi<temp$purchase_hpi])
  regs[[4]] <- felm(formula,
                    data=temp[temp$house_age>0 & temp$LTV_prev>=0.7 & temp$listing_hpi>=temp$purchase_hpi])
  stargazer(regs,type="text",no.space = TRUE,dep.var.labels.include = FALSE)
  
  
  
  
  
  
  regs <- list()
  regs[[1]] <- felm(prop_tax_rate~log(purchase_hpi)+ownership_years|zip+ListingMonth|0|zip,data=temp)
  regs[[2]] <- felm(prop_tax_rate~log(purchase_hpi)+ownership_years+log(adj_purch_price)+beds+baths+sqft+avg_school_rating+avg_school_distance+walk_score+log(house_age)+log(LotSizeSquareFeet)+INTR.y+LTV_prev|zip+ListingMonth|0|zip,data=temp[temp$house_age>0])
  stargazer(regs,type="text",no.space = TRUE)
  
  
  regs <- list()
  regs[[1]] <- felm(log(sales_price)~0|zip+ListingMonth|(prop_tax_rate~log(purchase_hpi)+ownership_years)|zip,data=temp)
  regs[[2]] <- felm(log(sales_price)~log(adj_purch_price)+beds+baths+sqft+avg_school_rating+avg_school_distance+walk_score+log(house_age)+log(LotSizeSquareFeet)|zip+ListingMonth|(prop_tax_rate~log(purchase_hpi)+ownership_years)|zip,data=temp[temp$house_age>0])
  stargazer(regs,type="text",no.space = TRUE)
