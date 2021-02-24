import robin_stocks as rs
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

robin_user = os.environ.get("robinhood_user")
robin_pass = os.environ.get("robinhood_pass")
sql_user = os.environ.get("sql_user")
sql_pass = os.environ.get("sql_pass")

#crete postgres connection
conn = psycopg2.connect(
    host="localhost",
    port = "5433",
    database="Robinhood",
    user= sql_user,
    password= sql_pass)

#login to robinhood
rs.login(username=robin_user,
         password=robin_pass,
         expiresIn=86400,
         by_sms=True)

#get positions
positions_data = rs.build_holdings()

#query to pull stop loss prices
postgreSQL_select_Query = 'select * from "Stops"'

#execute query
stops = pd.read_sql_query(postgreSQL_select_Query, conn)

#function to execute stop loss
def execute_stop(symbol):
    last_price = positions_data[symbol]['price']
    quantity = positions_data[symbol]['quantity']
    quantity = float(quantity)
    last_price = float(last_price)
    stop_price = stops[symbol][0]
    if last_price < stop_price:   #execute sell if price < stop
        print('Selling:', symbol)
        rs.orders.order_sell_fractional_by_quantity(symbol, quantity, timeInForce='gfd')
symbols_list = list(positions_data.keys())   #get list of symbols

#apply function to list
map(execute_stop, symbols_list)

print('Success')
