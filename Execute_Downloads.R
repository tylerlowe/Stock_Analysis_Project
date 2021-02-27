suppressPackageStartupMessages({
library(dplyr)
library(RobinHood)
library(RPostgres)
library(tidyr)
library(tictoc)
library(purrr)
})

rh_user <- Sys.getenv('rh_user')
rh_pw <- Sys.getenv('rh_pw')
sql_user <- Sys.getenv('sql_user')
sql_pw <- Sys.getenv('sql_pw')

#log into database
con <- dbConnect(RPostgres::Postgres(), dbname = 'Robinhood', host='localhost', port='5433', user= sql_user, password = sql_pw)

tickers_list <- readRDS("tickers_list")

try(dbExecute(con, 'DROP TABLE "Stock_Data"'), silent = TRUE) #delete old table query

for (i in 1:length(tickers_list)) {
  tic()
  write.table(data.frame(tickers_list[i]), "iteration.txt")
  system(paste0("/home/tyler/Documents/GitHub/Stock_Analysis_Project/parallel_stockdata.sh"))
  print(paste0("Iteration ", i))
  toc()
}

