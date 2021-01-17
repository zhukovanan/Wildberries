#Download library
library(reticulate)
library(DBI)
library(odbc)
library(data.table)
library(jsonlite)
library(httr)
library(lubridate)
library(taskscheduleR)

taskscheduler_create(taskname = "myscriptdaily", rscript = 'C:/Users/a.shukov/Desktop/Prog/R/Wildberries_api_connect.R' , 
                     schedule = "DAILY", starttime = "20:08", 
                     startdate = format(Sys.Date(), "%d/%m/%Y"))


#Form named api vector 
api.par <- c("N" = "1234" , "M" = "1232"
, "NE" = "3456", "L" = "3456",
"RFG" = "5445" , "FM" = "4365" ,
 "SMN" = "6547")

path <- "https://suppliers-stats.wildberries.ru/api/v1/supplier/"
#Make functions for different api tables
#Sales
wb.api <- function(key, data , date = Sys.Date(), dateto = Sys.Date()+1 ) {
  url <- paste0(path , data)
  res <- GET(url, query = list( dateFrom = date ,dateto = dateto,  flag = 0, key = key))
  resp_char <- rawToChar(res$content)
  Encoding(resp_char) <- "UTF-8"
  df <-  as.data.frame(fromJSON(resp_char))
  tryCatch(df$Vendor <- names(api.par[api.par == key]), error=function(e) NULL)
  if (nrow(df) == 0) {
    return(NULL) 
  } else {
    return(df)
  }
}

#Based on api data form necessary data frames
Sales_WB <- do.call(dplyr::bind_rows, lapply(api.par, wb.api, "sales" ) )
Orders_WB <- do.call(dplyr::bind_rows, lapply(api.par, wb.api,"orders"))
Realisation_WB <- do.call(dplyr::bind_rows, lapply(api.par, wb.api,"reportDetailByPeriod", "2020-12-01" ))
Stock_WB <- do.call(dplyr::bind_rows, lapply(api.par, wb.api, "stocks" ))

#Connect to SQL Server

con <- dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "####", 
                 Database = "####", UID = "####", PWD = "####", encoding = "1251")

#Fresh sql data
dbWriteTable(con, "Sales_WB" , Sales_WB, append = TRUE)
dbWriteTable(con, "Orders_WB" , Orders_WB, append = TRUE)
dbWriteTable(con, "Realisation_WB" , Realisation_WB, append = TRUE)
dbWriteTable(con, "Stock_WB" , Stock_WB, append = TRUE)
