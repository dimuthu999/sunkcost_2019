rm(list=ls())
setwd("E:/tax_incident")
library(plyr)
library(zoo)
library(lubridate)
library(dplyr)

trim <- function (x) gsub("^\\s+|\\s+$", "", x)
r = 0.03


sold_data <- list.files(path="E:/tax_incident/ca_anchoring",pattern = "*.csv",full.names = TRUE) #"^California.*.csv"
sold_data <- lapply(sold_data,function(x) read.csv(x, stringsAsFactors = FALSE,header = FALSE))
sold_data <- ldply(sold_data,data.frame)

sold_data <- sold_data[,1:24]
names(sold_data) <- c("price","zip","beds","baths","sqft","zest_value","zest_rent","zestimate_1year","zest_value_change","zest_rent_change","avg_school_rating","avg_school_distance","description_features","link","no_of_results","tax_history","price_history","neighborhood_desc","walk_score","walk_score_cat","description_sale","transit_score","transit_score_cat","address")
sold_data['sold_price'] <- sapply(sold_data$price,function(x) as.numeric(gsub("[^\\.0-9]", "", x)))
sold_data['city'] <- sapply(sold_data$zip, function(x) substr(x,1,regexpr(", CA",x)[1]-1))
sold_data['street'] <-sapply(sold_data$address, function(x) substr(x,1,regexpr(",_n_l_",x)[1]-1))
sold_data$zip <- sapply(sold_data$zip,function(x) as.numeric(gsub("[^\\.0-9]", "", x)))
sold_data$beds <- ifelse(sold_data$beds=="Studio",1,sold_data$beds)
sold_data$beds <- sapply(sold_data$beds,function(x) as.numeric(gsub("[^\\.0-9]", "", x)))
sold_data$baths <- sapply(sold_data$baths,function(x) as.numeric(gsub("[^\\.0-9]", "", x)))
sold_data$sqft <- sapply(sold_data$sqft,function(x) as.numeric(gsub("[^0-9]", "", x)))
sold_data$zest_rent <- sapply(sold_data$zest_rent,function(x) as.numeric(gsub("[^0-9]", "", x)))
sold_data$zest_value <- sapply(sold_data$zest_value,function(x) as.numeric(gsub("[^0-9]", "", x)))
sold_data$zestimate_1year <- NULL#as.numeric(gsub("[^0-9]", "", sold_data$zestimate_1year))
sold_data['zest_rent_change_days']<-NULL#str_split_fixed(sold_data$zest_rent_change, "Last", 2)[,2]
sold_data['zest_rent_change']<-NULL#str_split_fixed(sold_data$zest_rent_change, "Last", 2)[,1]
sold_data$zest_rent_change <- NULL#as.numeric(gsub("[^0-9\\+\\-]", "", sold_data$zest_rent_change))
sold_data['zest_value_change_days']<-NULL#str_split_fixed(sold_data$zest_value_change, "Last", 2)[,2]
sold_data['zest_value_change']<-NULL#str_split_fixed(sold_data$zest_value_change, "Last", 2)[,1]
sold_data$zest_value_change <- NULL#as.numeric(gsub("[^0-9\\+\\-]", "", sold_data$zest_value_change))
sold_data$no_of_results <- NULL#as.numeric(gsub("[^0-9\\+\\-]", "", sold_data$no_of_results))
sold_data$walk_score <- as.numeric(gsub("[^0-9]", "", sold_data$walk_score))
sold_data$transit_score <- as.numeric(gsub("[^0-9]", "", sold_data$transit_score))

sold_data$description_features <- sapply(sold_data$description_features,function (x) gsub("Lot_n_l_", "Lot:", x))
sold_data$description_features <- sapply(sold_data$description_features,function (x) gsub("Year Built_n_l_", "Built in", x))
# temp <- nchar(as.character(sold_data$description_features))
# temp <- as.character(sold_data$description_features)[which(temp==max(temp,na.rm = TRUE))]
# temp <- strsplit(temp,"_n_l_")[[1]]



sold_data <- sold_data[!duplicated(sold_data$link),]
sold_data$Lot <- ifelse(sold_data$Lot<quantile(sold_data$Lot,0.999,na.rm = TRUE) & sold_data$Lot >quantile(sold_data$Lot,0.001,na.rm = TRUE),sold_data$Lot,NA )


# Listing Data ---------------------------------------------------------

printi <-function(i)  {
  temp <- as.character(sold_data[i,'price_history'])
  temp <- strsplit(temp,"_n_l_")[[1]]
  temp
}


listing_data <- matrix(nrow = nrow(sold_data), ncol = 7)
l=0
pb <- txtProgressBar(min = 1, max = nrow(sold_data), initial = 1)
for(i in 1:nrow(sold_data)){
  setTxtProgressBar(pb, i)
  tryCatch({
    temp <- as.character(sold_data[i,'price_history'])
    temp1 <- temp[[1]]
    if(length(temp)==1) {
      temp[[1]] <- temp[[1]]
    } else {
      temp[[1]] <- temp[[2]]
    }
    
    if(is.na(temp[[1]])) temp[[1]] <- temp1
    
    if(is.na(temp)) next
    temp <- strsplit(temp[[1]],"_n_l_")[[1]]
    if(length(temp)<=2) next
    recordcomplete = FALSE
    
    for(j in 1:length(temp)){
      if(j==length(temp)) break
      if(recordcomplete) break
      if((tolower(substr(temp[j],10,13))=="sold") & (tolower(substr(temp[j],10,35)) != "sold: foreclosed to lender")) {
        for(k in (j+1):length(temp)){
          if(recordcomplete) break
          # cat(k," ")
          if(tolower(substr(temp[k],10,13))=="sold") break
          if(tolower(substr(temp[k],10,24))=="listed for sale") {
            listedinfo = temp[k]
            
            sold_date <- as.Date(substr(temp[j],1,8), "%m/%d/%y",origin="1970-01-01")
            listing_removed <- NA
            
            for(m in k:j) {
              if(tolower(substr(temp[m],10,24))=="listing removed") {
                listing_removed <- as.Date(substr(temp[m],1,8), "%m/%d/%y",origin="1970-01-01")
                listing_removed <- as.numeric(sold_date-listing_removed)
              }
            }
            
            if(listing_removed>100 & !is.na(listing_removed)) {
              l=k
              recordcomplete = TRUE
              break
            }
            
            purchasedinfo <- NA
            l=k+1
            while(l <=length(temp)){
              # cat(l," ")
              if(tolower(substr(temp[l],10,13))=="sold") {
                purchasedinfo = temp[l]
                break
              }
              l=l+1
            }
            
            if(is.na(listing_removed) | (listing_removed<100)) {
              listing_data[i,]<- c(i,sold_data[i,'link'],temp[j],listedinfo,purchasedinfo,"",1)
            }
            recordcomplete = TRUE
          }
        }
        break
      }
    }
    
    
    # j=l+1
    # if(j<(length(temp)-2) & j>2){
    #   tryCatch({
    #     recordcomplete = FALSE
    # 
    #     for(j in j:length(temp)){
    #       # cat(j)
    #       if(j==length(temp)) break
    #       if(recordcomplete) break
    #       if((tolower(substr(temp[j],10,13))=="sold" | tolower(substr(temp[j],10,24))=="listing removed") & (tolower(substr(temp[j],10,35)) != "sold: foreclosed to lender")) {
    #         for(k in (j+1):length(temp)){
    #           if(recordcomplete) break
    #           # cat(k," ")
    #           if(tolower(substr(temp[k],10,13))=="sold") break
    #           if(tolower(substr(temp[k],10,24))=="listed for sale") {
    #             listedinfo = temp[k]
    #             purchasedinfo <- NA
    #             l=k+1
    #             while(l <=length(temp)){
    #               # cat(l," ")
    #               if(tolower(substr(temp[l],10,13))=="sold") {
    #                 purchasedinfo = temp[l]
    #                 break
    #               }
    #               l=l+1
    #             }
    # 
    #             listing_data <- rbind(listing_data,c(i,sold_data[i,'link'],temp[j],listedinfo,purchasedinfo,temp[j-1],2))
    #             recordcomplete = TRUE
    #           }
    #         }
    #         break
    #       }
    #     }
    # 
    #   })
    # }
    
  })
}


listing_data <- as.data.frame(listing_data)
names(listing_data) <- c("row_no","link","transaction_info","listing_info","purchase_info","t_1","t_2")
listing_data <- data.frame(lapply(listing_data, as.character), stringsAsFactors=FALSE)
listing_data <- listing_data[!is.na(listing_data$link),]

listing_data['purchased_date'] <- sapply(listing_data$purchase_info,function(x) as.Date(substr(x,1,8), "%m/%d/%y",origin="1970-01-01"))
listing_data['purchased_amount'] <- sapply(listing_data$purchase_info,function(x) substr(x,gregexpr("\\$",x)[[1]][1]+1,nchar(x)+1))
listing_data['purchased_amount'] <- sapply(listing_data$purchased_amount,function(x) paste(gsub(",", "", x)," "))
listing_data['purchased_amount'] <- sapply(listing_data$purchased_amount,function(x) as.numeric(substr(x,1,gregexpr("[^0-9]",x)[[1]][1]-1)))

listing_data['listed_date'] <- sapply(listing_data$listing_info,function(x) as.Date(substr(x,1,8), "%m/%d/%y",origin="1970-01-01"))
listing_data['listing_amount'] <- sapply(listing_data$listing_info,function(x) substr(x,gregexpr("\\$",x)[[1]][1]+1,nchar(x)))
listing_data['listing_amount'] <- sapply(listing_data$listing_amount,function(x) paste(gsub(",", "", x)," "))
listing_data['listing_amount'] <- sapply(listing_data$listing_amount,function(x) as.numeric(substr(x,1,gregexpr("[^0-9]",x)[[1]][1]-1)))

listing_data['sale_date'] <- sapply(listing_data$transaction_info,function(x) as.Date(substr(x,1,8), "%m/%d/%y",origin="1970-01-01"))
listing_data['sales_price'] <- sapply(listing_data$transaction_info,function(x) substr(x,gregexpr("\\$",x)[[1]][1]+1,nchar(x)))
listing_data['sales_price'] <- sapply(listing_data$sales_price,function(x) paste(gsub(",", "", x)," "))
listing_data['sales_price'] <- sapply(listing_data$sales_price,function(x) as.numeric(substr(x,1,gregexpr("[^0-9]",x)[[1]][1]-1)))

# listing_data['end_type'] <- sapply(listing_data$transaction_info, function(x) substr(x,10,13))
# listing_data['successful'] <- ifelse(listing_data$end_type=="Sold",1,0)

listing_data$listed_date <- as.Date(listing_data$listed_date,origin = "1970-01-01")
listing_data['listed_year'] <- year(listing_data$listed_date )
listing_data$purchased_date <- as.Date(listing_data$purchased_date,origin = "1970-01-01")
listing_data['purchased_year'] <- year(listing_data$purchased_date )
listing_data$sale_date <- as.Date(listing_data$sale_date,origin = "1970-01-01")
listing_data['sale_year'] <- year(listing_data$sale_date )



# listing_data <- listing_data[listing_data$sale_year %in% c(2013,2014,2015,2016,2017) & listing_data$purchased_year<=2016,]
# listing_data <- listing_data[listing_data$listed_year > listing_data$purchased_year,]
# listing_data <- listing_data[listing_data$sale_year - listing_data$listed_year <=2,]

# listing_data$ZillowHomeID <- as.numeric(listing_data$ZillowHomeID)
# listing_data <- listing_data[!duplicated(listing_data$link),]

# Tax Data ----------------------------------------------------------------

tax_data <- matrix(nrow = nrow(sold_data), ncol = 26)
pb <- txtProgressBar(min = 1, max = nrow(sold_data), initial = 1)
for(i in 1:nrow(sold_data)){
  setTxtProgressBar(pb, i)
  tryCatch({
    temp <- as.character(sold_data[i,'tax_history'])
    if(is.na(temp)) next
    temp <- strsplit(temp,"_n_l_")[[1]]
    if(length(temp)<=2) next
    recordcomplete = FALSE
    
    record <- rep(0,12)
    record_assesment <- rep(0,12)
    for(j in 1:length(temp))  {
      yr = as.integer(substr(temp[j],1,4))
      if(yr %in% c(2004:2015)) {
        if(substr(temp[j],6,7)=="--")  {
          record[yr-2003] <- NA
          assesment <- substr(temp[j],gregexpr("\\$",temp[j])[[1]][1],nchar(temp[j]))
          record_assesment[yr-2003] <- as.numeric(gsub("[^\\.0-9]", "",substr(assesment,2,gregexpr("[^\\,\\.0-9]",assesment)[[1]][2]-1)))
        } else {
          tax_amount <- substr(temp[j],gregexpr("\\$",temp[j])[[1]][1],nchar(temp[j]))
          record[yr-2003] <- as.numeric(gsub("[^\\.0-9]", "",substr(tax_amount,2,gregexpr("[^\\,\\.0-9]",tax_amount)[[1]][2]-1)))
          assesment <- substr(temp[j],gregexpr("\\$",temp[j])[[1]][2],nchar(temp[j]))
          record_assesment[yr-2003] <- as.numeric(gsub("[^\\.0-9]", "",substr(assesment,2,gregexpr("[^\\,\\.0-9]",assesment)[[1]][2]-1)))
        }
        
      }
    }
    
    tax_data[i,]<- c(i,sold_data[i,'link'],t(record),t(record_assesment))
  })
}


tax_data <- as.data.frame(tax_data)
names(tax_data) <- c("row_no","link",paste("proptax_",c(2004:2015),sep = ""),paste("assesment_",c(2004:2015),sep = ""))
tax_data <- tax_data[!is.na(tax_data$link),]
tax_data <- data.frame(lapply(tax_data, as.character), stringsAsFactors=FALSE)
cols <- c(paste("proptax_",c(2004:2015),sep=""),paste("assesment_",c(2004:2015),sep = ""))
tax_data[,cols] <- sapply(tax_data[,cols],as.numeric)

# check www.zillow.com/homedetails/640-Ashby-Ln-Cambria-CA-93428/15408406_zpid/

# Merge Datasets ----------------------------------------------------------

sold_data <- merge(sold_data,listing_data,by="link",all.x = TRUE)
sold_data <- merge(sold_data,tax_data,by="link",all.x = TRUE)

sold_data <- sold_data[!is.na(sold_data$listing_amount),]

items <- c("Lot:","Built in","Last remodel year")
itemnames <- gsub("[: ]","",items)

pb <- txtProgressBar(min = 1, max = nrow(sold_data), initial = 1)
for(i in 1:nrow(sold_data))  {
  setTxtProgressBar(pb, i)
  j=1
  for(item in items)  {
    temp <- as.character(sold_data[i,'description_features'])
    temp <- strsplit(temp,"_n_l_")[[1]]
    dataitem <- temp[which(substr(temp,1,nchar(item))==item)]
    if((length(dataitem)==1)) {
      sold_data[i,itemnames[j]]<-dataitem
    }
    j=j+1
  }
}

sold_data$Lot <- sapply(sold_data$Lot, function(x) as.numeric(gsub("[^0-9]", "", x)))
sold_data$Builtin <- sapply(sold_data$Builtin,function (x) as.numeric(gsub("[^0-9]", "", x)))
sold_data$Lastremodelyear <- sapply(sold_data$Lastremodelyear,function (x) as.numeric(gsub("[^0-9]", "", x)))

keeps <- c("link","zip","beds","baths","sqft","zest_value","avg_school_rating",
           "avg_school_distance","walk_score","city","Lot","Builtin","Lastremodelyear","street",
           "purchased_date","purchased_amount","listed_date","listing_amount","sale_date",
           "sales_price","listed_year","purchased_year","sale_year","successful","t_2",paste("proptax_",c(2004:2015),sep = "")
           ,paste("assesment_",c(2004:2015),sep = ""))

sold_data <- sold_data[,names(sold_data) %in% keeps]

saveRDS(sold_data,file="ca_2016_data_Mar2019.rds")

