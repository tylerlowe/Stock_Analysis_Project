suppressPackageStartupMessages({
library(dplyr)
library(RobinHood)
library(RPostgres)
library(tidyr)
library(tictoc)
})

rh_user <- Sys.getenv('rh_user')
rh_pw <- Sys.getenv('rh_pw')
sql_user <- Sys.getenv('sql_user')
sql_pw <- Sys.getenv('sql_pw')

#log into database
con <- dbConnect(RPostgres::Postgres(), dbname = 'Robinhood', host='localhost', port='5433', user= sql_user, password = sql_pw)

#log into robinhood
RH = RobinHood(username = rh_user, password = rh_pw)

tickers = get_tickers(RH)



tickers = filter(tickers, rhs_tradability == 'tradable' & state == 'active')

tickers_good <- tibble(tickers$symbol)  #filter tickers

num_groups = 35     #create 35 groups for parallelization            

#split the list of tickers into groups to avoid ram issues
tickers_list <- tickers_good %>% 
  group_by((row_number()-1) %/% (n()/num_groups)) %>%
  nest %>% pull(data)


try(dbExecute(con, 'DROP TABLE "Stock_Data"'), silent = TRUE) #delete old table query

for (i in 1:length(tickers_list)) {
  tic()
  write.table(data.frame(tickers_list[i]), "iteration.txt")
  system(paste0("/home/tyler/Documents/GitHub/Stock_Analysis_Project/parallel_stockdata.sh"))
  print(paste0("Iteration ", i))
  toc()
}
