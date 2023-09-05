# OptionGunLib03  v003
# v003 - 09/04/23 - Added yahooquery to get prices to replace DARqube.
#               - Tested ivol to get real-time options data. Nor working yet.
# v002 - 03/19/23 - Added GetOptions, GetPrices, and GetERDates (need new data feed. YEC does not work.)
# v001 Created 12/31/22
# 06/19/23 - Fixed error in DATE_TIME and BuildOptionMetrics fct. where it converts Expiry string to date object.
# 03/24/23 - Wrote Bullets_STO
# 03/19/23 - Added fcts GetRootData, GetOptionStrings, BuildOptionMetrics
# 01/07/23 - Added TradeLogger
#
#

# *****************
# Import Libraries
# *****************

import csv
import copy
import json
import numpy as np
import os
import pandas as pd
import requests   # for http requests
import scipy
from   scipy import stats
from   scipy.stats import norm
import time
import yfinance   as yf
import yahooquery as yq

import datetime
from   datetime import datetime
from   datetime import date
from   datetime import timedelta
#from   datetime import fromtimestamp

from   dateutil.relativedelta  import relativedelta
#from   yahoo_earnings_calendar import YahooEarningsCalendar
#replace this wih a better earnings calendar

#import ImportLibsPython01
#import ImportLibsPowerMax01
def GetPricesfromyquery(TICKERS, root_data): # 09/03/23 - yahooquery is faster, easier than yfinance.
    print('in OptionGunLib03 in PowerMax_code...')
    all_symbols = " ".join(TICKERS)
    yquery_Info = yq.Ticker(all_symbols)
    yquery_Dict = yquery_Info.price

    prices = []
    for ticker in TICKERS:
        prices.append(yquery_Dict[ticker]['regularMarketPrice'])
                      
    df_data = {'ticker': TICKERS, 'root price': prices}
    root_data = pd.DataFrame(df_data)
    root_data.set_index('ticker', inplace = True)
    root_data['ER Date'] = '23-11-11'  #Set a bogus date. Fix when you find a source for ER dates.
     
    return root_data

def GetOptionsfromyfinance(root_data, TICKERS, EXPIRY_DELAY, TICKER_DELAY, MAX_DAYSOUT, MIN_DAYSOUT):
    bad_tickers = []
    all_options = pd.DataFrame()
    
    for ticker in TICKERS:
        print('getting data for ', ticker, ' at time ', datetime.now())
        try:                                      # get option strings from yfinance for each ticker.
                                                  # if call to yfinance fails, then just skip this expiry date.
            ticker_data = yf.Ticker(ticker)       # get option strings from yfinance for each ticker.
        except:
            print('yfinance call failed for ', ticker) 
        quote_time = datetime.now() + timedelta(hours = 3)
        quote_date = date.today()
        expiry_dates = ticker_data.options
        call_ticker_options = pd.DataFrame()
        put_ticker_options  = pd.DataFrame()
        for exp_date in expiry_dates:
            exp_daysout = (datetime.strptime(exp_date, '%Y-%m-%d').date() - quote_date).days
            if (exp_daysout < MAX_DAYSOUT and exp_daysout > MIN_DAYSOUT):
                    #If doing BTOs set MAX_DAYSOUT to high number, say 1000.0.
                    try:
                        call_data = ticker_data.option_chain(exp_date).calls
                        call_ticker_options = pd.concat([call_ticker_options, call_data], sort = False, ignore_index = True)
                        call_ticker_options.drop(columns = 'inTheMoney', inplace = True)
                        put_data = ticker_data.option_chain(exp_date).puts
                        put_ticker_options  = pd.concat([put_ticker_options,  put_data],  sort = False, ignore_index = True)
                        put_ticker_options.drop(columns = 'inTheMoney', inplace = True)
                    except:
                        print('     yfinance call failed for expiry ', exp_date)
                    time.sleep(EXPIRY_DELAY)           
       
        # calculate columns unique to each ticker
        if put_ticker_options.shape[0] == 0 and call_ticker_options.shape[0] == 0:
            print('   Yikes. No options for ', ticker)
            bad_tickers.append(ticker)
            print(bad_tickers)
        else:
            call_ticker_options['option_type'] = 'call'
            put_ticker_options['option_type']  = 'put'
            ticker_options = pd.concat([call_ticker_options, put_ticker_options], sort = False, ignore_index = True)
            ticker_options['root'] =  ticker
            ticker_options['root price'] = root_data.loc[ticker, 'root price']
            ticker_options['Quote_Time'] = quote_time
            ticker_options['ER Date'] = root_data.loc[ticker, 'ER Date']
            
            ticker_options['Expstring'] = '20' + ticker_options['contractSymbol'].str[len(ticker):len(ticker) + 6]
            ticker_options['Expiry'] = ticker_options['Expstring'].str[:4] + '-' + ticker_options['Expstring'].str[4:6] + '-' + \
                                       ticker_options['Expstring'].str[6:8]
            all_options = pd.concat([all_options, ticker_options], sort = False, ignore_index = True)

        time.sleep(TICKER_DELAY)

    print('Got all the data for all tickers. Created additional cols. time = ', datetime.now())
    if len(bad_tickers)>0:
        print('     Tickers with no data: ', bad_tickers)
    else:
        print('     Got options for every ticker.')

    return all_options

def BuildOptionMetrics(all_options, FEE_SPREAD, HIDE_TICKERS, DATE_ONLY_FORMAT, DATE_TIME_FORMAT):
    
    all_options['Quote_Time'] = pd.to_datetime(all_options['Quote_Time'], format = DATE_TIME_FORMAT)
    all_options['Expiry']     = pd.to_datetime(all_options['Expiry'], format = DATE_ONLY_FORMAT)
    all_options['fee']        = FEE_SPREAD * (all_options['ask'] - all_options['bid']) + all_options['bid']
    all_options['daysout']    = (all_options['Expiry'] - all_options['Quote_Time']) / np.timedelta64(1,'D')
    all_options['strike']     = pd.to_numeric(all_options['strike'], errors = 'coerce')
    all_options['root price'] = pd.to_numeric(all_options['root price'], errors = 'coerce')

    # TODO Find different source for ER dates. YEC not so good.
    #all_options['ER Date']    = pd.to_datetime(all_options['ER Date'], format = DATE_FORMAT)
    #all_options['ER daysout'] = (all_options['ER Date'] - all_options['Quote_Time']) / np.timedelta64(1,'D')
    #all_options['ER-Expiry']  = all_options['ER daysout'] - all_options['daysout']

    all_options['OTM']     = np.where(all_options['option_type'] == 'call', all_options['strike'] - all_options['root price'], \
                                      all_options['root price'] - all_options['strike'])

    # use np.where to calculate different ARRs based on put/call and buy/sell
    all_options['ARR']     = 100.0 * (all_options['fee'] * 365) / (all_options['daysout'] * all_options['root price'])
    all_options['PctOTM']  = 100.0 * all_options['OTM'] / all_options['root price']

    all_options['callPITM'] = 100.0 * norm.cdf(np.log(all_options['root price'] / all_options['strike']) /
                                      (all_options['impliedVolatility'] * (all_options['daysout'] / 365)**(1/2)))

    all_options['POW'] = np.where(all_options['option_type'] == 'call', 100.0 - all_options['callPITM'], all_options['callPITM'])

    all_options['PctFee'] = 100.0 * all_options['fee'] / all_options['root price']
    all_options['BidAskSpread'] = 100.0 * (all_options['ask'] - all_options['bid']) / all_options['ask']
    # Note this calc changed on 03/26/23. Previously divided by 'bid'. Now divided by 'ask' to set range 0-100
    all_options['impliedVolatility'] = 100.0 * all_options['impliedVolatility']
    #all_options.to_csv('all_options23-04-03-14.csv')

    #Clean up and simplify all_options prior to calling Bullets.
    clean_options = all_options.copy()

    drop_columns = ['contractSymbol', 'lastTradeDate', 'lastPrice', 'bid', 'ask', 'change', \
                    'percentChange', 'callPITM', 'volume', 'openInterest', 'Quote_Time', 'Expstring', 'contractSize', 'currency']
    

    #Removed ER daysout and ER-Expiry from drop_columns. Put them back after fixing the format problem.
    
    clean_options = clean_options.drop(columns = drop_columns).copy()
    clean_options.rename(columns = {'impliedVolatility': 'open_IVol'}, inplace = True)

    print('clean_options.csv')
    clean_options.replace([np.inf, -np.inf], np.nan, inplace=True)
    clean_options.dropna(how = 'all', axis = 'index', inplace = True)
#    int_columns = ['daysout', 'ARR', 'POW', 'open_IVol']
#    clean_options[int_columns] = clean_options[int_columns].astype(int, errors = 'ignore').copy() #Causes conversion error. Try removing inf and Nan

    clean_options = clean_options[~clean_options['root'].isin(HIDE_TICKERS)].copy()
 

    return all_options, clean_options

# *********
#
# Bullets_STO generates Put_STO and Call_STO csvs for short, mid, and long term options.
#    Uses screen values (min/max) from option_profiles. 
#
# *********

def Bullets_STO(all_options, profiles):  

    # Define all profiles and dfs.
    Put_STO_Short = pd.DataFrame()
    Put_STO_Mid   = pd.DataFrame()
    Put_STO_Long  = pd.DataFrame()
    Put_STO_Spike = pd.DataFrame()

    # For CSTO, FIRST screen against holdings to select ONLY trades with call coverage.
    # Make CSTO a separate Bullet.
    Call_STO_Short = pd.DataFrame()
    Call_STO_Mid   = pd.DataFrame()
    Call_STO_Long  = pd.DataFrame()
    Call_STO_Spike = pd.DataFrame()

    # Define dict lookups for option dfs. Note Bullets_STO includes ONLY STO profiles and excludes STO_spike profiles for now.
    put_profiles      = ('Put_STO_Short', 'Put_STO_Mid', 'Put_STO_Long')
    put_profiles_dict = {'Put_STO_Short': Put_STO_Short, 'Put_STO_Mid': Put_STO_Mid, 'Put_STO_Long': Put_STO_Long, 'Put_STO_Spike': Put_STO_Spike}
    call_profiles     = ('Call_STO_Short', 'Call_STO_Mid', 'Call_STO_Long')
    call_profiles_dict= {'Call_STO_Short': Call_STO_Short, 'Call_STO_Mid': Call_STO_Mid, 'Call_STO_Long': Call_STO_Long, 'Call_STO_Spike': Call_STO_Spike}
   
    for option_type in ('put', 'call'):
        options = all_options[all_options['option_type'] == option_type]
        if option_type == 'put':
            run_profiles = put_profiles
            profiles_dict = put_profiles_dict
            #options.to_csv('puts_all.csv')
        else:
            run_profiles = call_profiles
            profiles_dict = call_profiles_dict
            #options.to_csv('calls_all.csv')
            
        for profile in run_profiles: # Create option profile by profiling all_options with profile params.
            this_STO = profiles_dict[profile]                       
            this_STO = options[(((options['POW'] >= profiles.loc[profile, 'POWmin'])    & \
                                 (options['POW']  <  profiles.loc[profile, 'POWmax']))  & \
                                ((options['ARR']  >= profiles.loc[profile, 'ARRmin'])   & \
                                 (options['ARR']  <  profiles.loc[profile, 'ARRmax']))) & \
                               (((options['PctFee'] >= profiles.loc[profile, 'PctFeemin'])  & \
                                 (options['PctFee'] <  profiles.loc[profile, 'PctFeemax'])) & \
                                ((options['PctOTM'] >= profiles.loc[profile, 'PctOTMmin'])  & \
                                 (options['PctOTM'] <  profiles.loc[profile, 'PctOTMmax'])))].copy()
                
            this_STO = this_STO[((this_STO['daysout'] >= profiles.loc[profile, 'daysoutmin'])  & \
                                 (this_STO['daysout'] <  profiles.loc[profile, 'daysoutmax'])) & \
                                 (this_STO['BidAskSpread'] < profiles.loc[profile, 'BidAskSpreadmax'])].copy()

            #Sort in POWMax standard way. Then save as csv.
            this_STO.sort_values(['root', 'ARR'], ascending = [True, False], inplace = True)
            write_file = profile + '.csv'
            this_STO.to_csv(write_file, index = False, float_format = '%.2f')
            print('Wrote ', profile, ' at ', datetime.now())
                    
# TODO - Finish thee bullets. 06/19/20
#def Bullets_CCSTO(stuff) This bullet uses holding and open trades to show only calls that can be covered.

def Bullets_BTC(all_options, trade_log_open):
    Call_BTC = pd.DataFrame()
    Put_BTC  = pd.DataFrame()

    Call_BTC.iloc[0,0] = 'Not done yet. Empty for now.'
    Put_BTC.iloc[0,0]  = 'Not done yet. Empty for now.'
    
    # **********
    #
    # Bullets_Put_BTC_01
    # Started 12/14/22
    # 1. From open_options create list of open puts
    # 2. Screen all_options for only each open put.
    # 2.1 Find matching PBTC trade. From all_options, get last price  
    # 3. Annotate the put record with ARRBE price, < 20% price, etc. 
    # 4. Make recommendation - Wait, place BTC for day / GTC, at price X, Wait for price drop to close, 
    #        Wait for Expiry, Wait for Assignment.
    #
    # Make close offer IF:
    #   - matching_PBTC_fee < MIN_BID_Pct or > ARRBE (with factor)  # for profitable trades
    #           or concentration too high, or ITM is low enough, or cost_basis is higher than strike, or don't want the root.               
    #**********

    # Select only Put open trades
    PBTC_trades = open_options[open_options['option_type'] == 'Put'].copy()
    if PBTC_trades.shape[0] > 0:
        PBTC_trades.reset_index(drop = True, inplace = True)
        PBTC_trades['row_num'] = PBTC_trades.index
        
        #Find matching PBTC trade in all_options
        for i in range(PBTC_trades.shape[0]):
            strike      = PBTC_trades.loc['row_num' == i, 'strike']
            expiry      = PBTC_trades.loc['row_num' == i, 'expiry']
            root        = PBTC_trades.loc['row_num' == i, 'root']
            option_type = PBTC_trades.loc['row_num' == i, 'option_type']

            matching_PBTC = all_options.np.where[((all_options['strike'] == strike) & (all_options[expiry] == expiry)) \
                                               & ((all_options['option_type'] == option_type) & (all_options['root'] == root))]
                        
            # Get last price, IV, close from matching PBTC
            PBTC_trades.loc['row_num' == i, 'cur_IV'] = matching_PBTC.loc[0, 'Implied Volatility']
            PBTC_trades.loc['row_num' == i, 'cur_price'] = matching_PBTC.loc[0, 'last price']
            PBTC_trades.loc['row_num' == i, 'cur_bid'] = matching_PBTC.loc[0, 'fee']
                                
        #Calculate vector column values for PBTC_trades columns
        PBTC_trades['ARRBE_close_fee'] = 365.0 * PBTC_trades['ARRBE'] * PBTC_trades['strike'] / PBTC_trades['cur_days_out']
                                                                                         
        PBTC_trades['max_close_bid'] = MIN_BID_Pct * PBTC_trades['fee']
        PBTC_trades['est_close_ARR'] = MIN_ARR_Pct * est_close_ARR
                                                                                        
                                                                                         
    # **********
    #
    # Bullets_Call_BTC_01
    # Started 01/04/23
    # 1. From open_options create list of open calls.
    # 2. Screen all_options for only each open call.
    # 2.1 Find matching CBTC trade. From all_options, get last price  
    # 3. Annotate the call record with ARRBE price, < 20% price, etc. 
    # 4. Make recommendation - Wait, place BTC for day / GTC, at price X, Wait for price drop to close, 
    #        Wait for Expiry, Wait for Assignment.
    #
    # Make close offer IF:
    #   - matching_CBTC_fee < MIN_BID_Pct or > ARRBE (with factor)  # for profitable trades
    #           or concentration too high, or ITM is low enough, or cost_basis is higher than strike, or don't want the root.               
    #**********

    # Select only Put open trades
                                                                                         
    CBTC_trades = open_options[open_options['option_type'] == 'Call'].copy()
    CBTC_trades.reset_index(drop = True, inplace = True)
    CBTC_trades['row_num'] = CBTC_trades.index
    
    #Find matching CBTC trade in all_options
    for i in range(CBTC_trades.shape[0]):
        strike      = CBTC_trades.loc['row_num' == i, 'strike']
        expiry      = CBTC_trades.loc['row_num' == i, 'expiry']
        root        = CBTC_trades.loc['row_num' == i, 'root']
        option_type = CBTC_trades.loc['row_num' == i, 'option_type']

        matching_CBTC = all_options.np.where[((all_options['strike'] == strike) & (all_options[expiry] == expiry)) \
                                           & ((all_options['option_type'] == option_type) & (all_options['root'] == root))]
        
        # Get last price, IV, close from matching PBTC
        CBTC_trades.loc['row_num' == i, 'cur_IV']    = matching_CBTC.loc[0, 'Implied Volatility']
        CBTC_trades.loc['row_num' == i, 'cur_price'] = matching_CBTC.loc[0, 'last price']
        CBTC_trades.loc['row_num' == i, 'cur_bid']   = matching_CBTC.loc[0, 'fee']
    
    #Calculate vector column values for PBTC_trades columns
    CBTC_trades['ARRBE_close_fee'] = 365.0 * CBTC_trades['ARRBE'] * CBTC_trades['strike'] / CBTC_trades['cur_days_out']
    CBTC_trades['max_close_bid'] = MIN_BID_Pct * CBTC_trades['fee']
    CBTC_trades['est_close_ARR'] = MIN_ARR_Pct * est_close_ARR


# **********
# Obsolete functions
# **********

def GetPrices(TICKERS, root_data, DAR_key):
    for ticker in TICKERS:
        print('getting price for ', ticker)    
        price_rqst = 'https://api.darqube.com/data-api/market_data/quote/' + ticker + '?token=' + DAR_key

        # TODO - Use try condition to get ech stock price. If DARqube returns error, then trap out and continue instead of crashing
        response = requests.get(price_rqst)
        price_dict = response.json()
        #print('     price_dict = ', price_dict)
        root_data.loc[ticker, 'root price'] = price_dict['price']

    root_data.sort_index(inplace = True) #Save root_data with updated current market prices.

    return root_data


# Main program to verify that this runs:
print('OptionGunLib03 runs like a top.')
    


                                                                                     
                                                                                     # 


                                                                                     




                                                                                     

    




    
