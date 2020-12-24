library(dplyr)
library(multidplyr)
library(tidyquant) 
library(parallel)
library(tidyr)
library(purrr)
library(BatchGetSymbols)
library(RobinHood)
library(RPostgres)

#log into database
con <- dbConnect(RPostgres::Postgres(), dbname = 'Robinhood', host='localhost', port='5433', user= sql_user, password = sql_pw)

#log into robinhood
RH = RobinHood(username = rh_user, password = rh_pw) 
#get list of tradeable tickers
tickers = get_tickers(RH)
tickers = filter(tickers, rhs_tradability == 'tradable' & state == 'active')

tickers_good <- tibble(tickers$symbol)   #filter tickers

num_groups = 25                 

#split the list of tickers into groups to avoid ram issues
tickers_list <- tickers_good %>% 
  group_by((row_number()-1) %/% (n()/num_groups)) %>%
  nest %>% pull(data)

#function to get data
Get_Data <- function(tickers_good) {
  #detect cores
  cl <- detectCores() - 1           
  #add groups
  group <- rep(1:cl, length.out = nrow(tickers_good))
  tickers_good <- bind_cols(tibble(group), tickers_good)    
  #create clusters
  cluster <- new_cluster(cl)     
  #partition by group
  by_group <- tickers_good %>% group_by(group) %>% partition(cluster)              
  #setup clusers
  from <- "1900-01-01"
  to   <- today()
  
  by_group$cluster %>% cluster_library("purrr") %>% cluster_library("tidyquant") %>% cluster_assign("from" = from) %>%   cluster_assign("to" = to)
  
  #run parallelized code
  start <- proc.time() # Start clock
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
    arrange(`tickers.symbol`)
  
  stockdata$log_ret_adj <- calc.ret(stockdata$adjusted, tickers = stockdata$symbol, type.return = 'log') #calculate log returns
  #Push to SQL
  dbWriteTable(con, name = "Stock_Data", value = stockdata, append = TRUE)
}


try(dbExecute(con, 'DROP TABLE "Stock_Data"'), silent = TRUE) #delete old table query


#run first function then for loop for rest of tickers
for(i in 1:length(tickers_list)){
  tickers_tmp <- data.frame(tickers_list[i])   
  tickers_tmp <- tibble(tickers_tmp)
  Get_Data(tickers_tmp)
}