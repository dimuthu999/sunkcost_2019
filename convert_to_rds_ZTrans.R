rm(list=ls())
library(data.table)


# use Layout.xlsx for column names

folder = 'C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/raw/36/ZTrans'
files = list.files(path = folder, pattern = '*.*',full.names = FALSE)

variable_names <- read.csv(file="C:/Users/dnratnadiwakara/Documents/sunkcost_2019/ztraxdata/variable_names_ZTrans.csv",stringsAsFactors = FALSE)
variable_names$file <- tolower(sapply(variable_names$file,function(x) paste(substr(x,3,nchar(x)),".txt",sep="")))

options(warn = -1)

for(file in files) {
  
  print(file)

  
  tryCatch(
    {
        temp <- fread(file=paste(folder,"/",file,sep=""),sep="|")
        names(temp) <- variable_names[variable_names$file==file,]$columnname
        
        saveRDS(temp,file=paste(folder,"/",substr(file,1,nchar(file)-4),".rds",sep=""))  
        rm(temp)
        gc()
        
        
    },error=function(cond) {
      print(paste("Error",file))
    })
}


options(warn = 0)
