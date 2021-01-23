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

sql_user <- Sys.getenv("sql_user")
sql_pw <- Sys.getenv("sql_pw")

#log into database
con <- dbConnect(RPostgres::Postgres(), dbname = 'Robinhood', host='localhost', port='5433', user= sql_user, password = sql_pw)



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

from <- "1900-01-01"
to <- today()
cl <- detectCores() - 1

tic()

Get_Data <- function(tickers_good) {
  tickers_good <- data.frame(tickers_good)
  group <- rep(1:cl, length.out = nrow(tickers_good))
  tickers_good <- bind_cols(tibble(group), tickers_good)    
  #create clusters
  cluster <- new_cluster(cl)     
  #partition by group
  by_group <- tickers_good %>% group_by(group) %>% partition(cluster)
  #setup clusers
  by_group$cluster %>% cluster_library("purrr") %>% cluster_library("tidyquant") %>% cluster_assign("from" = from) %>%   cluster_assign("to" = to)
  #run parallelized code
  by_group %>%
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
    ungroup() %>%
    subset(select = c(group, symbol, date, open, high, low, close, volume, adjusted)) %>%
    drop_na() %>%
    group_by(symbol) %>%
    mutate(log_ret_adj = calc.ret(adjusted, tickers = symbol, type.return = 'log'),
           rsi_price = adjusted  %>% rsi_fun(),
           rsi_volume = volume  %>% rsi_fun(),
           cci = tibble(high, low, adjusted)  %>% cci_fun(),
           obv = obv_fun(adjusted, volume),
           adx = tibble(high, low, adjusted)  %>% adx_fun() %>% as.data.table(),
           aroon = tibble(high, low)  %>% aroon_fun() %>% as.data.table(), 
           atr = tibble(high, low, adjusted)  %>% atr_fun() %>% as.data.table(),
           bbands = tibble(high, low, adjusted)  %>% bbands_fun() %>% as.data.table(),
           chaikinVol = tibble(high, low)  %>% chaikinvol_fun() %>% as.data.table(),
           clv = tibble(high, low, adjusted)  %>% clv_fun() %>% as.data.table(),
           cmo_price = adjusted  %>% cmo_fun() %>% as.data.table(),
           cmo_volume = volume  %>% cmo_fun() %>% as.data.table(),
           donchannel = tibble(high, low)  %>% donachianchannel_fun() %>% as.data.table(), 
           dpo_price = adjusted  %>% dpo_fun() %>% as.data.table(),
           dpo_volume = volume  %>% dpo_fun() %>% as.data.table(),
           gmma_price = adjusted  %>% gmma_fun() %>% as.data.table(),
           gmma_volume = volume  %>% gmma_fun() %>% as.data.table(),
           kst = adjusted %>% kst_fun() %>% as.data.table(),
           macd_price = adjusted  %>% macd_fun() %>% as.data.table(),
           macd_volume = volume %>% macd_fun() %>% as.data.table(),
           pbands = adjusted %>% pbands_fun() %>% as.data.table(),
           sar = tibble(high, low) %>% sar_fun() %>% as.data.table(),
           stoch = tibble(high, low, adjusted) %>% stoch_fun() %>% as.data.table(),
           tdi = adjusted %>% tdi_fun() %>% as.data.table(),
           trix = adjusted %>% trix_fun() %>% as.data.table(),
           ultOscillator = tibble(high, low, adjusted) %>% ultimateOscillator_fun() %>% as.data.table(), 
           vhf = adjusted %>% vhf_fun() %>% as.data.table(),
           volatility = tibble(open, high, low, adjusted) %>% volatility_fun() %>% as.data.table(),
           williamsAD = tibble(open, high, low, adjusted) %>% williamsAD_fun() %>% as.data.table(),
           wpr = tibble(high, low, adjusted) %>% wpr_fun() %>% as.data.table(),
           zigzag = tibble(high, low) %>% zigzag_fun() %>% as.data.table()
    ) %>%
    ungroup() %>%
    do.call(what = data.table, .) %>%  #unnest dataframe variables
    Filter(f = function(x)!all(is.na(x))) %>%
    dbWriteTable(conn = con, name = "Stock_Data", value = ., append = TRUE)  #Push to SQL
}

i <- read.table("iteration.txt")

Get_Data(i)

toc()