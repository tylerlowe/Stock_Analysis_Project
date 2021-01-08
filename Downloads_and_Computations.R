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


#CALCULATE STOP LOSS VALUES

#get positions
positions <- get_positions(RH)

#get list of symbols currently held
symb <- positions$symbol
names(symb) <- symb   #assign names to list

#function to calculate stop losses using high - 3*ATR  (21 day)
atr_trailing_stop <- function(symbol){
  hlc <- dbGetQuery(con, paste0('SELECT high, low, close FROM "Stock_Data" WHERE symbol =', " '", symbol, "'", 'ORDER BY "date" ASC'))
  hlc <- na.omit(hlc)
  atr <-ATR(tail(hlc, 50), n = 21)
  atr_tibble <- as_tibble(atr)
  last(atr_tibble$trueHigh) - last(atr_tibble$atr)*3
}

stops <- lapply(symb, atr_trailing_stop)   #apply atr function to list
stops_tbl <- as_tibble(stops)     #convert to table

try(dbExecute(con, 'DROP TABLE "Stops"'), silent = TRUE) #delete old table query
dbWriteTable(con, name = "Stops", value = stops_tbl, append = TRUE)  #write stops to sql table


#TECHNICAL INDICATORS

library(ncar)
library(tfplot)
library(parallel)
library(RPostgres)

obv_price_divergence <- function(symbol){
  data <- tibble(dbGetQuery(con, paste0(
    'SELECT volume, adjusted FROM "Stock_Data" WHERE  symbol =', " '", paste(symbol), "'", ' ORDER BY "date" ASC')))
  data <- na.omit(data)
  if (nrow(data)>21) {
    obv <- OBV(data$adjusted, data$volume)
    obv_ema <- EMA(na.approx(obv, n=20))
    close <- data$adjusted
    close_ema <- EMA(na.approx(data$adjusted, n=20))
    obv_diff <- ts((obv-obv_ema)/obv_ema)
    close_diff <- ts((close-close_ema)/close_ema)
    diff_obv_close <- ((obv_diff)-(close_diff))
    obv_divergence <- last(diff_obv_close)
    df <- data.frame(symbol, obv_divergence)
    dbWriteTable(con, name = "Indicators", value = df, append = TRUE)
  }
  else {NA}
}

try(dbExecute(con, 'DROP TABLE "Indicators"'), silent = TRUE) #delete old table query

mclapply(tickers_good$`tickers$symbol`, obv_price_divergence,
         mc.preschedule = TRUE, mc.set.seed = TRUE,
         mc.silent = FALSE, mc.cores = detectCores()-1,
         mc.cleanup = TRUE, mc.allow.recursive = TRUE, affinity.list = NULL)

