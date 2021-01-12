library(dplyr)
library(multidplyr)
library(tidyquant) 
library(parallel)
library(tidyr)
library(purrr)
library(BatchGetSymbols)
library(RobinHood)
library(RPostgres)
library(tibble)
library(data.table)
library(ncar)
library(tfplot)
library(tictoc)
library(tibble)
library(TTR)
library(furrr)

rh_user <- Sys.getenv("rh_user")
rh_pw <- Sys.getenv("rh_pw")
sql_user <- Sys.getenv("sql_user")
sql_pw <- Sys.getenv("sql_pw")

#log into database
con <- dbConnect(RPostgres::Postgres(), dbname = 'Robinhood', host='localhost', port='5433', user= sql_user, password = sql_pw)

#log into robinhood
RH = RobinHood(username = rh_user, password = rh_pw) 



#CREATE STOCK PRICE DATABASE

#get list of tradeable tickers
tickers = get_tickers(RH)
tickers = filter(tickers, rhs_tradability == 'tradable' & state == 'active')

tickers_good <- tibble(tickers$symbol)   #filter tickers

num_groups = 25      #create 25 groups for parallelization            

#split the list of tickers into groups to avoid ram issues
tickers_list <- tickers_good %>% 
  group_by((row_number()-1) %/% (n()/num_groups)) %>%
  nest %>% pull(data)

#rsi function with error handling
rsi_fun <- possibly(RSI, otherwise = NA)

#detect cores
cl <- detectCores() - 1  

from <- "1900-01-01"
to   <- today()


Get_Data <- function(tickers_good) {
  group <- rep(1:cl, length.out = nrow(tickers_good))
  tickers_good <- bind_cols(tibble(group), tickers_good)    
  #create clusters
  cluster <- new_cluster(cl-1)     
  #partition by group
  by_group <- tickers_good %>% group_by(group) %>% partition(cluster)
  #setup clusers
  by_group$cluster %>% cluster_library("purrr") %>% cluster_library("tidyquant") %>% cluster_assign("from" = from) %>%   cluster_assign("to" = to)
  #run parallelized code
  stockdata <- by_group %>%
    mutate(
      stock.prices = map(`tickers.symbol`,
                                function(.x) tq_get(.x,
                                                    get  = "stock.prices",
                                                    from = from,
                                                    to   = to)
      )
    ) %>%
    collect() %>% 
    unnest() %>%
    arrange(`tickers.symbol`) %>%
    ungroup()
  
  stockdata$log_ret_adj <- calc.ret(stockdata$adjusted, tickers = stockdata$symbol, type.return = 'log') #calculate log returns
  
  #add rsi
  stockdata <- stockdata %>%
    group_by(symbol) %>%
    group_map(~mutate(., rsi = rsi_fun(adjusted))) %>%
    reduce(full_join)
  
  #Push to SQL
  dbWriteTable(con, name = "Stock_Data", value = stockdata, append = TRUE)
}

tic()



try(dbExecute(con, 'DROP TABLE "Stock_Data"'), silent = TRUE) #delete old table query


#map functions to list of tickers


fix_ticker_list<- function(tickers){
  tickers_tmp <- data.frame(tickers)   
  tickers_tmp <- tibble(tickers_tmp)
  return(tickers_tmp)
}

tickers_list_fixed <- map(tickers_list, fix_ticker_list)

map(tickers_list_fixed, Get_Data)

toc()

#CALCULATE STOP LOSS VALUES
tic()
#get positions
positions <- get_positions(RH)

#get list of symbols currently held
symb <- positions$symbol
names(symb) <- symb   #assign names to list

#function to calculate stop losses using high - 3*ATR  (21 day)
atr_trailing_stop <- function(symbol){
  hlc <- dbGetQuery(con, paste0('SELECT high, low, close FROM "Stock_Data" WHERE "tickers.symbol" =', " '", symbol, "'", 'ORDER BY "date" ASC'))
  hlc <- na.omit(hlc)
  atr <-ATR(tail(hlc, 50), n = 21)
  atr_tibble <- as_tibble(atr)
  last(atr_tibble$trueHigh) - last(atr_tibble$atr)*3
}

stops <- lapply(symb, atr_trailing_stop)   #apply atr function to list
stops_tbl <- as_tibble(stops)     #convert to table

try(dbExecute(con, 'DROP TABLE "Stops"'), silent = TRUE) #delete old table query
dbWriteTable(con, name = "Stops", value = stops_tbl, append = TRUE)  #write stops to sql table

toc()

