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

###stop losses

#get positions
positions <- get_positions(RH)

#get list of symbols currently held
symb <- positions$symbol
names(symb) <- symb   #assign names to list

#function to calculate stop losses using high - 3*ATR  (21 day)
atr_trailing_stop <- function(sym){
  atr <- dbGetQuery(con, paste0('SELECT "date", "atr.atr", "atr.trueHigh" FROM "Stock_Data" WHERE symbol =', " '", sym, "'", 'ORDER BY "date" ASC'))
  purchase_date <- positions %>% filter(symbol == sym) %>% select(updated_at)
  atr <- atr %>% filter(date >= as.Date(purchase_date$updated_at))
  atr_stop <- atr$atr.trueHigh - atr$atr.atr*3
  max(atr_stop, na.rm = TRUE)
}

stops <- lapply(symb, atr_trailing_stop)   #apply atr function to list
stops_tbl <- as_tibble(stops)

try(dbExecute(con, 'DROP TABLE "Stops"'), silent = TRUE) #delete old table query
dbWriteTable(con, name = "Stops", value = stops_tbl, append = TRUE)  #write stops to sql table
