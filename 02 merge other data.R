rm(list=ls())
library(lfe)
library(stargazer)
library(dplyr)

or <- readRDS(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztrax_processed_OR.rds")
or <- or[or$purchased_year>1997,]
or$DocumentMonth <- as.Date(or$DocumentMonth,origin = "1970-01-01")
or['ListedMonth'] <- as.Date("2019-02-01")
or['proptax'] <- or$year2016*5
or <- or[or$DocumentTypeStndCode=="WRDE",]

zhvi <- read.csv(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/Zip_Zhvi_AllHomes.csv",stringsAsFactors = FALSE)
zhvi <- zhvi[zhvi$State %in% c("OR"),]
zhvi[,c("RegionID","City","State","Metro","CountyName","SizeRank")] <- NULL
zhvi <- melt(zhvi,id.vars = c("RegionName"))
zhvi['month'] <- sapply(zhvi$variable,function(x) as.Date(paste(gsub("\\.","-",substr(x,2,8)),"-01",sep="")))
zhvi$month <- as.Date(zhvi$month,origin = "1970-01-01")
zhvi$variable <- NULL
names(zhvi) <- c("zip","purchase_hpi","DocumentMonth")


or <- merge(or,zhvi,by=c("zip","DocumentMonth"))
names(zhvi) <- c("zip","listed_hpi","ListedMonth")
or <- merge(or,zhvi,by=c("zip","ListedMonth"))

or['adj_purch_price'] <- or$SalesPriceAmount*or$listed_hpi/or$purchase_hpi
or['listing_premium'] <- or$list_price/or$adj_purch_price

or <- or[or$listing_premium>=0.8 & or$listing_premium<1.5,]

or['prop_tax_rate'] <- or$property_tax_1418/or$adj_purch_price
or['ownership_years'] <- 2019-or$purchased_year

or['age_of_property'] <- 2019 - or$YearBuilt
or['mortgage'] <- ifelse(or$LoanAmount>0,1,0)
or['loan_to_value'] <- or$LoanAmount/or$SalesPriceAmount
or['ownership_decile'] <- ntile(or$ownership_years,10)


regs <- list()
regs[[1]] <- felm(prop_tax_rate~log(purchase_hpi)+ownership_years|zip|0|zip,data=or)
regs[[2]] <- felm(prop_tax_rate~log(purchase_hpi)+ownership_years+log(adj_purch_price)+n_beds+n_baths+sqft+log(LotSizeSquareFeet)+log(age_of_property)+loan_to_value|zip|0|zip,data=or)
stargazer(regs,type="text",no.space = TRUE)


regs <- list()
regs[[1]] <- felm(listing_premium~log(purchase_hpi)+ownership_years|zip|0|zip,data=or)
regs[[2]] <- felm(listing_premium~log(purchase_hpi)+ownership_years+log(adj_purch_price)+n_beds+n_baths+sqft+log(LotSizeSquareFeet)+log(age_of_property)+loan_to_value|zip|0|zip,data=or)
stargazer(regs,type="text",no.space = TRUE)





regs[[1]] <- felm(listing_premium~prop_tax_rate+log(purchase_hpi)+ownership_years+n_beds+log(adj_purch_price)+log(age_of_property)+n_baths+log(LotSizeSquareFeet)|zip|0|zip,data=or)
regs <- list()
regs[[1]] <- felm(listing_premium~prop_tax_rate+log(purchase_hpi)+ownership_years+n_beds+log(adj_purch_price)+log(age_of_property)+n_baths+log(LotSizeSquareFeet)|zip|0|zip,data=or)
regs[[2]] <- felm(listing_premium~prop_tax_rate+n_beds+log(adj_purch_price)+n_baths+log(LotSizeSquareFeet)|zip|0|zip,data=or)
regs[[3]] <- felm(listing_premium~n_beds+log(adj_purch_price)+n_baths+log(LotSizeSquareFeet)|zip|(prop_tax_rate~log(purchase_hpi)+ownership_years)|zip,data=or)
regs[[4]] <- felm(prop_tax_rate~log(purchase_hpi)+ownership_years+n_beds+n_baths+log(adj_purch_price)+log(LotSizeSquareFeet)|zip|0|zip,data=or)

stargazer(regs,type="text",no.space = TRUE)
