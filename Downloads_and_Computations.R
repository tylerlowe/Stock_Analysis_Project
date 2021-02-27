suppressPackageStartupMessages({
library(dplyr)
library(multidplyr)
library(tidyquant) 
library(parallel)
library(tidyr)
library(purrr)
library(BatchGetSymbols)
library(RPostgres)
library(tibble)
library(data.table)
library(TTR)
})

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

Get_Data <- function(tickers_good, group_num) {
  tickers_good <- data.frame(tickers_good)
  group <- rep(1:cl, length.out = nrow(tickers_good))
  tickers_good <- bind_cols(tibble(group), tickers_good)    
  tickers_good %>% 
    filter(group == group_num) %>%
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
    dbWriteTable(conn = con, name = "Stock_Data", value = ., append = TRUE)  #Push to SQL
}

i <- read.table("iteration.txt")

args <- commandArgs(trailingOnly = TRUE)
group_num <- as.numeric(args[1])

Get_Data(i, group_num)