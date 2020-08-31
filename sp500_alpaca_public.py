import numpy as np
import pandas as pd
import arrow
import talib
import random
import time
import datetime
from datetime import date
from datetime import datetime
import pandas_market_calendars as mcal
import math
from math import floor
import csv
import string
import tenacity
from tenacity import retry
from dateutil import parser
from scipy import stats
import alpaca_trade_api as tradeapi
from iexfinance.stocks import Stock
from slacker import Slacker

#FILL in passwords
############################################################################################
slack_token ="XXXXXXXXXXXXXXXXXXXXXXX"
alpaca_key= 'XXXXXXXXXXXXXXXXXXXXXXXXXXXX'
alpaca_secret_key='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
############################################################################################

print ("running")

def start():

    api = tradeapi.REST(
    key_id=alpaca_key,
    secret_key=alpaca_secret_key,
    base_url='https://paper-api.alpaca.markets', # remove this line to use live account
    api_version='v2')

    return api


def cancelorders():
    if len(api.list_orders())>0:
        x=api.list_orders()
        for order in x:
            print (order.id)
            api.cancel_order(order.id)
    else:
        pass

def limitorder(symbol, quantity, side):
    bid, ask=get_bid_ask(symbol)
    if side=='buy':
        price=bid
    else:
        price=ask
    if quantity>0:
        api.submit_order(symbol,quantity,side=side,
                         type='limit',limit_price=price,time_in_force='day',
            )
    else:
        pass

def strategyorder(symbol, quantity, side):
    # try placing limit orders for 15 minutes, else do market order
    try:
        if quantity>0:
            cancelorders()
            tCurrent = time.time()
            count=1

            while (count < 9):
                if time.time() <= tCurrent + 900:

                    limitorder(symbol, quantity, side)
                    print (api.list_orders())
                    time.sleep(0.25)
                    if len(api.list_orders())>0:
                        a=random.uniform(30,120)
                        time.sleep(a)
                        if len(api.list_orders())>0:
                            cancelorders()
                        else:
                            count = count + 100
                else:
                    cancelorders()
                    marketorder(symbol, quantity, side)
                    a=random.uniform(300,600)
                    time.sleep(a)
                    count = count + 100
        else:
            pass
    except:
        print('order_error')
        pass

def get_cash():
    cashx = api.get_account().cash

    return float(cashx)


def get_portfolio_value():

    portfolio = api.get_account().portfolio_value

    return float(portfolio)


def get_position(symbol):
    try:
        return float(api.get_position(symbol).qty)
    except:
        return 0

def marketorder(symbol, quantity, side):
    if quantity>0:
        api.submit_order(symbol,quantity,side,
                type='market',
                time_in_force='day',
            )
    else:
        pass

@tenacity.retry(wait=tenacity.wait_fixed(1))
def get_price(symbol):
    try:
        return float(api.polygon.last_quote(symbol).askprice)
    except:
        # try IEX data if polygon fails
        stk=Stock(symbol, output_format='pandas')
        return float(stk.get_price().loc[symbol])

@tenacity.retry(wait=tenacity.wait_fixed(1))
def get_bid_ask(symbol):
    try:
        bid=float(api.polygon.last_quote(symbol).bidprice)
        ask=float(api.polygon.last_quote(symbol).askprice)
    except:
        # try IEX data if polygon fails
        stk=Stock(symbol, output_format='pandas')
        ask= float(stk.get_quote().loc['iexAskPrice'])
        bid= float(stk.get_quote().loc['iexBidPrice'])

        if bid== 0 or ask ==0:
            ask= float(stk.get_price().loc[symbol])
            bid= float(stk.get_price().loc[symbol])
        else:
            pass

    return bid, ask

def fraction_and_percent(symbol):

    totalbalance= get_portfolio_value()
    portfolio=get_position(symbol)
    price=get_price(symbol)
    fraction=portfolio*price/totalbalance
    percent=round(fraction*100,0)
    # returns fraction and percent of portfolio of given stock
    return fraction, percent

def ordersizes(targetweight,fraction, symbol):
    currentprice=get_price(symbol)
    totalbalance=get_portfolio_value()

    buyfraction=targetweight-fraction
    stockstobuy=totalbalance*buyfraction/currentprice

    sellfraction=fraction-targetweight
    stockstosell=totalbalance*sellfraction/currentprice

    if (targetweight>fraction):
        return stockstobuy, "buy"
    elif (targetweight<fraction):
        return stockstosell, "sell"
    else:
        return [0], "pass"

def orderpercent(symbol, targetweight):

    fraction,percent=fraction_and_percent(symbol)

    ordersize,ordername=ordersizes(targetweight,fraction, symbol)


    print ('ordering')
    print (symbol)

    # break order into randomly sized smaller orders if too big to buy
    r1=random.uniform(0.2,0.4)
    r2=random.uniform(0.15,0.3)
    r3=random.uniform(0.15,0.3)
    r4=1-r1-r2-r3

    amount = int(math.floor(ordersize))

    amount_r1 = int(math.floor(ordersize*r1))
    amount_r2 = int(math.floor(ordersize*r2))
    amount_r3 = int(math.floor(ordersize*r3))
    amount_r4 = int(math.floor(ordersize*r4))

    if ordername=='buy':

        print ("buying")

        if  get_cash()>float(amount)*get_price(symbol) :
            strategyorder(symbol, amount, 'buy')

        # TRY BREAKING INTO SMALLER ORDERS IF NOT ENOUGH CASH
        else:
            if  get_cash()>float(amount_r1)*get_price(symbol):

                strategyorder(symbol, amount_r1, 'buy')
            else:
                pass

            if  get_cash()>float(amount_r2)*get_price(symbol):

                strategyorder(symbol, amount_r2, 'buy')

                a=random.uniform(1,5)
                time.sleep(a)
            else:
                pass

            if  get_cash()>float(amount_r3)*get_price(symbol):

                strategyorder(symbol, amount_r3, 'buy')

                a=random.uniform(1,5)
                time.sleep(a)
            else:
                pass

            if  get_cash()>float(amount_r4)*get_price(symbol):

                strategyorder(symbol, amount_r4, 'buy')
                a=random.uniform(1,5)
                time.sleep(a)
            else:
                pass

        # record trades
        bid, ask=get_bid_ask(symbol)
        trade = [arrow.now('US/Eastern').date(),"buy",symbol,bid,round(ordersize,4), "market"]

        with open(r'trades.csv', 'a') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(trade)


    elif ordername=='sell':

        print ("selling")

        if amount> get_position(symbol):
            amount=get_position(symbol)
        else:
            pass

        strategyorder(symbol, amount, 'sell')

        # record trades
        bid, ask=get_bid_ask(symbol)
        trade = [arrow.now('US/Eastern').date(),"sell",symbol,bid,round(ordersize,4),"market"]
        with open(r'trades.csv', 'a') as f:
           writer = csv.writer(f, delimiter=',')
           writer.writerow(trade)

    else:
        print ('pass')
        pass





def _slope(ts):
    x = np.arange(len(ts))
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, ts)
    annualized_slope = (np.power(np.exp(slope), 250) -1) *100
    return annualized_slope * (r_value ** 2) # slope* r^2. A higher r^2 has a smoother trend. A higher slope is more positive.


def getstocklist():
    # list of sp500 stocks in alpaca
    assets= (api.list_assets(status='active', asset_class='us_equity'))
    assets = [asset for asset in assets if asset.easy_to_borrow ]
    portfolio= []
    
    #sp500
    table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    df.to_csv('S&P500-Info.csv')
    df.to_csv("S&P500-Symbols.csv", columns=['Symbol'])
    

    for i in assets:

        portfolio.append(i.symbol)

    portfolio2= []
    for i in portfolio:
        for j in df['Symbol']:
            if i==j: portfolio2.append(i)
    return portfolio2

def stock_close_history(symbol):

    f=api.polygon.historic_agg_v2(symbol,1,"day",_from='2018-09-01', to=arrow.now('US/Eastern').date(), limit=None).df

    price=f['close'].values
    return price[-90:] # 90 day window




while True:

    api = start()

    g=arrow.now('US/Eastern')

    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date='2018-07-01', end_date='2030-07-10')


    marketstatus= nyse.open_at_time(schedule, pd.Timestamp(str(str(date.today())+g.time().strftime(" %H:%M")),
                                         tz='America/New_York'))


    date_today = datetime.now()
    month_first_day = date_today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)# first day of month
    firstdayopen=nyse.schedule(start_date=(month_first_day.date()), end_date='2030-07-10').iloc[0]['market_open'].date() #first market open day



    # check that market is open and today is first day of month that it's open
    if  marketstatus==True and (date.today()==firstdayopen):

        print(get_portfolio_value())
        list=getstocklist()
        df= pd.DataFrame(columns={"stock","slope"})


        for i in list:

            prices= stock_close_history(i)
            slope= _slope(np.log(prices))

            new_row = {'stock':i, 'slope':slope}


            ema1 = talib.KAMA( prices, 15)[-1] # 15 day average
            ema2 = talib.EMA( prices, 50)[-1] # 50 day average
            if ema1>ema2 and slope>45: # screen for moving average cross and high slope
                df = df.append(new_row, ignore_index=True)

        ranking_table = df.sort_values("slope",ascending=False)# sort stocks by slope


        rank_list = ranking_table[:30]# list of best 30 stocks
        print(rank_list)

        buy_list= rank_list['stock'].tolist()


        positions=api.list_positions()

        for i in positions:
            if i.symbol not in buy_list:
                orderpercent( i["symbol"], 0) # sell stocks not in buy_list

        for i in buy_list:
                    orderpercent( i,  (1.0 / 30) ) # order equal weight of each stock

        # record portfolio
        portfolio = [arrow.now('US/Eastern').date(),get_portfolio_value(), api.list_positions() ]
        with open(r'portfolio.csv', 'a') as f:
           writer = csv.writer(f, delimiter=',')
           writer.writerow(portfolio)

        #send slack notification
        message= ( str(api.list_positions()))
        slack = Slacker(slack_token)

        slack.chat.post_message(channel='algos',
                                    text=message, username= "Alpaca BOT", icon_emoji=':robot_face:')

        time.sleep(25200) # sleep until mkt close




    else:
        pass


    time.sleep(1)
    continue
