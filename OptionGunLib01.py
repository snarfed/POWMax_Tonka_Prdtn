# OptionGunLib01
# v002 - 03/19/23 - Added GetOptions, GetPrices, and GetERDates (need new data feed. YEC does not work.)
#                   
# v001 Created 12/31/22
# 
# 03/24/23 - Wrote Bullets_STO
# 03/19/23 - Added fcts GetRootData, GetOptionStrings, BuildOptionMetrics
# 01/07/23 - Added TradeLogger
#
#

# *****************
#
# Import Libraries
#
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
import yfinance as yf

import datetime
from   datetime import datetime
from   datetime import date
from   datetime import timedelta
#from   datetime import fromtimestamp

from   dateutil.relativedelta  import relativedelta
from   yahoo_earnings_calendar import YahooEarningsCalendar


# Library Functions

def GetPrices(TICKERS, root_data, DAR_key):
    for ticker in TICKERS:
    
        price_rqst = 'https://api.darqube.com/data-api/market_data/quote/' + ticker + '?token=' + DAR_key
        response = requests.get(price_rqst)
        price_dict = response.json()
        root_data.loc[ticker, 'root price'] = price_dict['price']

    root_data.sort_index(inplace = True) #Save root_data with updated current market prices.

    return root_data

def GetERDates(root_data, TICKERS, YEC_DELAY):  #Yahoo Earnings calendar does not work. Find new data source.
                                                # Also, move this fct to CrystalBall, since it is used in predictions.
    for ticker in TICKERS:
        yec = YahooEarningsCalendar(YEC_DELAY)

        try:
            next_er_date = (datetime.fromtimestamp(yec.get_next_earnings_date(ticker)).strftime('%Y-%m-%d %H:%M'))
        except: 
            print(ticker, ' ER Dates - got retrieval error')
        else:
            root_data.loc[ticker, 'ER Date'] = next_er_date
            print(ticker, root_data.loc[ticker, 'ER Date'])
    root_data.sort_index(inplace = True)

    return root_data

def GetOptions(root_data, TICKERS, EXPIRY_DELAY, TICKER_DELAY, MAX_DAYSOUT, MIN_DAYSOUT):
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

def BuildOptionMetrics(all_options, FEE_SPREAD, HIDE_TICKERS, DATE_FORMAT):
    
    all_options['Quote_Time'] = pd.to_datetime(all_options['Quote_Time'], format = DATE_FORMAT)
    all_options['Expiry']     = pd.to_datetime(all_options['Expiry'], format = DATE_FORMAT)
    all_options['fee']        = FEE_SPREAD * (all_options['ask'] - all_options['bid']) + all_options['bid']
    all_options['daysout']    = (all_options['Expiry'] - all_options['Quote_Time']) / np.timedelta64(1,'D')
    all_options['strike']     = pd.to_numeric(all_options['strike'], errors = 'coerce')
    all_options['root price'] = pd.to_numeric(all_options['root price'], errors = 'coerce')
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
    # Note this clac changed on 03/26/23. Previously divided by 'bid'. Now divided by 'ask' to set range 0-100
    all_options['impliedVolatility'] = 100.0 * all_options['impliedVolatility']

    #Clean up and simplify all_options prior to calling Bullets.
    clean_options = all_options.copy()

    drop_columns = ['contractSymbol', 'lastTradeDate', 'lastPrice', 'bid', 'ask', 'change', \
                    'percentChange', 'callPITM', 'volume', 'openInterest', 'Quote_Time', 'Expstring', 'contractSize', 'currency']

    #Removed ER daysout and ER-Expiry from drop_columns. Put them back after fixing the format problem.
    
    clean_options = clean_options.drop(columns = drop_columns).copy()
    clean_options.rename(columns = {'impliedVolatility': 'open_IVol'}, inplace = True)
    int_columns = ['daysout', 'ARR', 'POW', 'open_IVol']
#    clean_options[int_columns] = clean_options[int_columns].astype(int)

    #float_columns = ['daysout', 'ARR', 'POW', 'impliedVolatility']
    #clean_options[float_columns] = all_options[float_columns].astype(float).copy()

    clean_options = clean_options[~clean_options['root'].isin(HIDE_TICKERS)].copy()
 

    return all_options, clean_options

# *********
#
# Bullets_PSTO generates Put_STO csvs for short, mid, and long term options.
#    Uses screen values (min/max) from option_profiles. 
#
# *********

def Bullets_PSTO(all_options, profiles):

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
            options.to_csv('puts_all.csv')
        else:
            run_profiles = call_profiles
            profiles_dict = call_profiles_dict
            options.to_csv('calls_all.csv')
            
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

            # Select best 3 options for each of these metrics - POW, ARR, PctOTM, PctFee, daysout
            select = 4 #Controls # options chosen to display
            col_names = this_STO.columns.tolist()
            best_options = pd.DataFrame(columns = col_names)
            this_STO['Select'] = False
            if profile == 'Put_STO_Short':
                this_STO.to_csv('Put_STO_Short_all.csv')
            #print(this_STO[['root', 'POW', 'Select', 'ARR']])
            for metric in ('POW', 'ARR', 'PctOTM', 'PctFee'):  #Add iVol?
                # Create list of roots
                roots = this_STO['root'].values.tolist()
                roots = set(roots) #Create unique list of roots
                for ticker in roots:
                    ticker_options = this_STO[this_STO['root'] == ticker].copy()
                    ticker_options.reset_index(drop = False, inplace = True)
                    if ticker_options.shape[0] <= select:
                        ticker_options['Select'] = True
                    else:
                        for metric in ('POW', 'ARR', 'PctOTM', 'PctFee'):  #Add open_iVol
                            ticker_options.sort_values(metric, axis = 0, ascending = False, inplace = True, ignore_index = True)
                            ticker_options.loc[0:select, 'Select'] = True
                    ticker_options = ticker_options[ticker_options['Select'] == True].copy()
                    best_options = best_options.merge(ticker_options, how = 'outer', copy = True)

            drop_cols = ['BidAskSpread', 'Select']
            best_options = best_options.drop(columns = drop_cols).copy()
                       
                                    
            #Sort in POWMax standard way
                    
            best_options.sort_values(['root', 'ARR'], ascending = [True, False], inplace = True)
            write_file = profile + '.csv'
            best_options.to_csv(write_file, index = False, float_format = '%.2f')
            print('Wrote ', profile, ' at ', datetime.now())


def LittleGuns(all_options, option_screens): #Obsolete now.

    # Define all option_screens and dfs.
    Put_STO_Short = pd.DataFrame()
    Put_STO_Mid   = pd.DataFrame()
    Put_STO_Long  = pd.DataFrame()
    Put_STO_Spike = pd.DataFrame()
    Put_BTC       = pd.DataFrame()
    Call_STO_Short = pd.DataFrame()
    Call_STO_Mid   = pd.DataFrame()
    Call_STO_Long  = pd.DataFrame()
    Call_STO_Spike = pd.DataFrame()
    Call_BTC       = pd.DataFrame()
    
    # Define dict lookups for option dfs
    screen_dict = {'Put_STO_Short': Put_STO_Short, 'Put_STO_Mid': Put_STO_Mid, 'Put_STO_Long': Put_STO_Long, \
                   'Put_STO_Spike': Put_STO_Spike, 'Put_BTC': Put_BTC, \
                   'Call_STO_Short': Call_STO_Short, 'Call_STO_Mid': Call_STO_Mid, 'Call_STO_Long': Call_STO_Long, \
                   'Call_STO_Spike': Call_STO_Spike, 'Call_BTC': Call_BTC \
                  }
    
    # Get screen df and active screens
    run_screens = option_screens['Profiles'][option_screens.loc[:, 'Active'] == True].values.tolist()
    
    for screen in run_screens: # Create option screen by screening all_options with screen params.
        screen_dict[screen] = all_options[(((all_options['POW'] >= option_screens.loc['screen_ID', 'POWmin'])   & \
                                            (all_options['POW'] <  option_screens.loc['screen_ID', 'POWmax']))  & \
                                           ((all_options['ARR'] >= option_screens.loc['screen_ID', 'ARRmin'])   & \
                                            (all_options['ARR']   <  option_screens.loc['screen_ID', 'ARRMax'])))    & \
                                          (((all_options['PctFee'] >= option_screens.loc['screen_ID', 'PctFeemin'])  & \
                                            (all_options['PctFee']  < option_screens.loc['screen_ID', 'PctFeemax'])) & \
                                           ((all_options['PctOTM'] >  option_screens.loc['screen_ID', 'PctOTMmin'])  & \
                                            (all_options['PctOTM'] <= option_screens.loc['screen_ID', 'PctOTMmax'])))].copy()
        
        screen_dict[screen] = screen_dict[screen][(screen_dict['daysout'] >= option_screens.loc['screen_ID', 'daysoutmin']) & \
                                                  (screen_dict['daysout'] <  option_screens.loc['screen_ID', 'daysoutmax'])].copy()
        
        int_columns = ['daysout', 'ARR', 'POW', 'impliedVolatility']
        screen_dict[screen] = screen_dict[screen][int_columns].astype(int).copy()
        
        #Sort your favorite way.
        screen_dict[screen].sort_values(['option_type', 'root', 'PctFee'], ascending = [False, True, False], inplace = True)
        write_file = screen + '.csv'
        screen_dict[screen].to_csv(write_file, index = False, float_format = '%.2f')
        print('Wrote ', screen, ' at ', datetime.now())

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
    



# Main program to verify that this runs:
print('OptionGunLib01 runs like a top.')
    


                                                                                     
                                                                                     # 


                                                                                     




                                                                                     

    




    
