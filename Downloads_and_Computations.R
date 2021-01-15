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

#TTR functions with error handling
#rsi function with error handling
rsi_fun <- possibly(RSI, otherwise = NA)
aroon_fun <- possibly(aroon, otherwise = NA)
cci_fun <- possibly(CCI, otherwise = NA)
obv_fun <- possibly(OBV, otherwise = NA)
adx_fun <- possibly(ADX, otherwise = NA)
atr_fun <- possibly(ATR, otherwise = NA)
bbands_fun <- possibly(BBands, otherwise = NA)
#come back for chaikin ad
chaikinvol_fun <- possibly(chaikinVolatility, otherwise = NA)
clv_fun <- possibly(CLV, otherwise = NA)
#come back for cmf
cmo_fun <- possibly(CMO, otherwise = NA)
#come back for cti
donachianchannel_fun <- possibly(DonchianChannel, otherwise = NA)
dpo_fun <- possibly(DPO, otherwise = NA)
#come back for dvi
#come back for emv
gmma_fun <- possibly(GMMA, otherwise = NA)
kst_fun <- possibly(KST, otherwise = NA)
macd_fun <- possibly(MACD, otherwise = NA)
#come back for mfi
pbands_fun <- possibly(PBands, otherwise = NA)
sar_fun <- possibly(SAR, otherwise = NA)
stoch_fun <- possibly(stoch, otherwise = NA)
tdi_fun <- possibly(TDI, otherwise = NA)
trix_fun <- possibly(TRIX, otherwise = NA)
ultimateOscillator_fun <- possibly(ultimateOscillator, otherwise = NA)
vhf_fun <- possibly(VHF, otherwise = NA)
volatility_fun <- possibly(volatility, otherwise = NA)
williamsAD_fun <- possibly(williamsAD, otherwise = NA)
wpr_fun <- possibly(WPR, otherwise = NA)
zigzag_fun <- possibly(ZigZag, otherwise = NA)

#detect cores
cl <- detectCores() - 1  

from <- "1900-01-01"
to   <- today()


Get_Data <- function(tickers_good) {
  group <- rep(1:cl, length.out = nrow(tickers_good))
  tickers_good <- bind_cols(tibble(group), tickers_good)    
  #create clusters
  cluster <- new_cluster(cl)     
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
                                                    to   = to,
                                                    complete_cases = TRUE)
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
    mutate(rsi_price = adjusted %>% na.approx(na.rm = FALSE) %>% rsi_fun(),
           rsi_volume = volume %>% na.approx(na.rm = FALSE) %>% rsi_fun(),
           cci = tibble(high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% cci_fun(),
           obv = obv_fun(adjusted, volume),
           adx = tibble(high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% adx_fun() %>% as.data.frame(),
           aroon = tibble(high, low) %>% na.approx(na.rm = FALSE) %>% aroon_fun() %>% as.data.frame(),
           atr = tibble(high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% atr_fun() %>% as.data.frame(),
           bbands = tibble(high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% bbands_fun() %>% as.data.frame(),
           chaikinVol = tibble(high, low) %>% na.approx(na.rm = FALSE) %>% chaikinvol_fun(),
           clv = tibble(high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% clv_fun(),
           cmo_price = adjusted %>% na.approx(na.rm = FALSE) %>% cmo_fun(),
           cmo_volume = volume %>% na.approx(na.rm = FALSE) %>% cmo_fun(),
           donchannel = tibble(high, low) %>% na.approx(na.rm = FALSE) %>% donachianchannel_fun() %>% as.data.frame(), 
           dpo_price = adjusted %>% na.approx(na.rm = FALSE) %>% dpo_fun(),
           dpo_volume = volume %>% na.approx(na.rm = FALSE) %>% dpo_fun(),
           gmma_price = adjusted %>% na.approx(na.rm = FALSE) %>% gmma_fun(),
           gmma_volume = volume %>% na.approx(na.rm = FALSE) %>% gmma_fun(),
           kst = adjusted %>% na.approx(na.rm = FALSE) %>% kst_fun(),
           macd_price = adjusted %>% na.approx(na.rm = FALSE) %>% macd_fun() %>% as.data.frame(),
           macd_volume = volume %>% na.approx(na.rm = FALSE) %>% macd_fun() %>% as.data.frame(),
           pbands = adjusted %>% na.approx(na.rm = FALSE) %>% pbands_fun() %>% as.data.frame(),
           sar = tibble(high, low) %>% na.approx(na.rm = FALSE) %>% sar_fun(),
           stoch = tibble(high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% stoch_fun() %>% as.data.frame(),
           tdi = adjusted %>% na.approx(na.rm = FALSE) %>% tdi_fun() %>% as.data.frame(),
           trix = adjusted %>% na.approx(na.rm = FALSE) %>% trix_fun(),
           ultOscillator = tibble(high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% ultimateOscillator_fun(), 
           vhf = adjusted %>% na.approx(na.rm = FALSE) %>% vhf_fun(),
           volatility = tibble(open, high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% volatility_fun(),
           williamsAD = tibble(open, high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% williamsAD_fun(),
           wpr = tibble(high, low, adjusted) %>% na.approx(na.rm = FALSE) %>% wpr_fun(),
           zigzag = tibble(high, low) %>% na.approx(na.rm = FALSE) %>% zigzag_fun()
    ) %>% 
    ungroup()
  
  stockdata <- do.call("data.frame", stockdata)   #unnest dataframe variables
  
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

