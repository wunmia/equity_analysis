import re
from selenium import webdriver
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import warnings
import sqlite3
import json
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from collections import defaultdict
import ta
import numpy as np
from scipy.stats import norm

warnings.filterwarnings('ignore')

#database connection please use whatever database infrastructure you are most comfortable with
conn = sqlite3.connect('output/stock_data.sqlite')
cur = conn.cursor()

#user defined stock list, can be systematic/automated
stocklist = ["RR.L", "TSLA", "BMW.F", "VOW.DE", "GM"]

fs_codes = {"pnl": "financials", "bs": "balance-sheet", "scf": "cash-flow"}
periods = {"pnl": 5, "bs": 4, "scf": 5}
first_metric = {"pnl": "Total Rev", "bs": "Total Ass", "scf": "Operating Cash"}

####Collects and cleans, income statement, balance sheet and cashflow statement data
class FinancialStatements():
    def __init__(self):
        self.financial_sheets = []
        self.financials_list = []

    def fs_data_extract(self, stock, statement, starting_metric, period):
        url = f"https://finance.yahoo.com/quote/{stock}/{statement}?p={stock}"
        options = Options()
        options.add_argument("-headless")
        driver = webdriver.Chrome(options=options)
        driver.get(url)

        page = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        infile = urllib.request.urlopen(page).read()
        data = infile.decode('ISO-8859-1')  # Read the content as string decoded with ISO-8859-1
        soup = BeautifulSoup(data, "html.parser")

        driver.find_element("xpath", "//button[@value='agree']").click()
        time.sleep(2)
        # driver.find_element("xpath", "//*[contains(text(), 'Maybe later')]").click()
        # time.sleep(2)
        driver.find_element("xpath", "//*[contains(text(), 'Expand All')]").click()
        data = driver.find_element(By.CSS_SELECTOR, "body").text
        data = data.split('\n')

        for n, line in enumerate(data):
            if line.startswith(starting_metric):
                start = n # first line
            if line.startswith("People Also"):
                end = n  # last line

        trimmed_dataset = data[start:end]
        cleaned_dataset = {}

        for element in trimmed_dataset:
            if re.search('[a-zA-Z]', element):
                metric = element
                continue
            cleaned_dataset[metric] = element.split(" ")

        bad_data = []
        for k, v in cleaned_dataset.items():
            if len(v) < period: bad_data.append(k)
            if len(v) > period: bad_data.append(k)

        for k in bad_data:
            cleaned_dataset.pop(k, None)

        metrics = soup('span')

        for n, k in enumerate(metrics):
            if k.text == "Breakdown":
                start = n

        for n, k in enumerate(metrics[start:]):
            try:
                if k["class"][0].startswith("Ta(c)"):
                    print(k["class"])
                    break
            except:
                continue

        dates = []
        for m in metrics[start + 1:start + 1 + period]: #customisation point
            dates.append(m.text)

        #ne of the fs statement of given stock
        fs_table = pd.DataFrame(cleaned_dataset, index=dates)
        for column in fs_table.columns:
            fs_table[column] = fs_table[column].str.replace(",", "")
            fs_table[column] = pd.to_numeric(fs_table[column], errors="ignore")

        #add to a mini liast of statements for the stock
        self.financial_sheets.append(fs_table)

        self.fs_table = fs_table
        self.stock = stock

    def fs_combiner(self, stock):
        stock_financials = pd.concat(self.financial_sheets, axis=1)
        stock_financials["Stock"] = stock

        self.financials_list.append(stock_financials)
        self.financial_sheets = []

    def stock_fs_combiner(self):

        FinancialStatements.financials = pd.concat(self.financials_list)
        print(FinancialStatements.financials.columns)
        FinancialStatements.financials.to_excel("output/test.xlsx")


####computes financial ratios using all
class FinancialRatios:
    def __init__(self):
        self.fs_table  = FinancialStatements.financials
        self.keeps_columns = ['Stock','Total Revenue','Operating Revenue','Gross Profit','Operating Income','Pretax Income','Net Income','Basic EPS','Basic Average Shares','EBIT','Reconciled Depreciation','Total Assets','Current Assets','Cash, Cash Equivalents & Short Term Investments','Cash And Cash Equivalents','Inventory','Work in Process','Total non-current assets','Gross PPE','Construction in Progress','Accumulated Depreciation','Goodwill And Other Intangible Assets','Financial Assets','Total Liabilities Net Minority Interest','Current Liabilities','Current Provisions','Current Debt','Other Current Liabilities','Total Non Current Liabilities Net Minority Interest','Long Term Provisions','Long Term Debt',"Stockholders' Equity",'Common Stock','Retained Earnings','Total Capitalization','Net Tangible Assets','Working Capital','Total Debt','Net Debt','Ordinary Shares Number','Operating Cash Flow','Investing Cash Flow','Financing Cash Flow','Capital Expenditure','Free Cash Flow','Receivables','Payables']


    def metrics_pnl(self):
        fs_table = self.fs_table

        fs_table["OM %"] = fs_table["Operating Income"] / fs_table["Operating Revenue"]
        fs_table["NI %"] = fs_table["Net Income Common Stockholders"] / fs_table["Operating Revenue"]
        fs_table["EBIT %"] = fs_table["EBIT"] / fs_table["Operating Revenue"]
        fs_table["Interest cover"] = fs_table["Operating Income"] / fs_table["Interest Expense"]
        try:
            fs_table["Reinvestment Ratio"] = fs_table["Research & Development"] / fs_table["Operating Revenue"]
            fs_table["RnD Focus"] = fs_table["Research & Development"] / fs_table["Operating Expense"]
        except: pass

        self.fs_table = fs_table

    def metrics_bs(self):
        fs_table = self.fs_table
        fs_table["Current Ratio"] = fs_table["Current Assets"] / fs_table["Current Liabilities"]
        fs_table["Quick Ratio"] = (fs_table["Current Assets"] - fs_table["Inventory"]) / fs_table[
            "Current Liabilities"]
        fs_table["Cash Quality"] = (fs_table["Current Assets"] - fs_table["Inventory"]) / fs_table[
            "Current Assets"]
        fs_table["Gearing"] = (fs_table["Long Term Debt"] + fs_table["Current Debt"]) / (
                -fs_table["Stockholders' Equity"] + fs_table["Long Term Debt"] + fs_table[
            "Current Debt"])

        self.fs_table = fs_table

    def metrics_combined(self):
        fs_table = self.fs_table
        fs_table["Cash Coverage"] = fs_table["Operating Revenue"] / (fs_table["Long Term Debt"] + fs_table["Current Debt"])
        fs_table["Cash Flow Margin"] = fs_table["Operating Cash Flow"] / fs_table["Operating Revenue"]
        fs_table["Cash Flow_Current Ratio"] = fs_table["Operating Cash Flow"] / fs_table["Current Debt"]
        fs_table["Asset Turnover"] = fs_table["Operating Revenue"] / fs_table["Total Assets"]
        fs_table["Inventory Turnover"] = fs_table["Cost of Revenue"] / fs_table["Inventory"]
        fs_table["Asset Return"] = fs_table["Net Income"] / fs_table["Total Assets"]
        fs_table["Equity Return"] = fs_table["Net Income"] / fs_table["Stockholders' Equity"]
        self.fs_table = fs_table

    def data_cleanup(self):
        fs_table = self.fs_table
        fs_table.index.names = ["Date"]

        for n, col in enumerate(fs_table.columns):
            if n >= fs_table.columns.get_loc("OM %"):
                if n <= fs_table.columns.get_loc("Equity Return"):
                    self.keeps_columns.append(col)


        fs_table_comp = fs_table.copy()
        fs_table = fs_table[self.keeps_columns]
        fs_table = fs_table.reset_index() #correct columns and get data out of index to make the mapping cleaner


        fs_table.to_excel(f"output/fs_table.xlsx")
        fs_table_comp.to_excel(f"output/fs_table_comp.xlsx")
        latest_financials = fs_table
        return latest_financials

class StockPriceAnalysis:
    def __init__(self):
        self.stock_data = defaultdict(lambda:defaultdict(list))
        self.price_history = []
        self.vol_window = 60

    def stock_price_history(self, stock):
        #get stock market history to use in black scholes calculation
        stock_data = self.stock_data

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock}?region=GB&lang=en-GB&includePrePost=false&interval=1d&useYfid=true&range=2y&corsDomain=uk.finance.yahoo.com&.tsrc=finance"

        page = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        infile = urllib.request.urlopen(page).read()

        data = infile.decode('ISO-8859-1')  # Read the content as string decoded with ISO-8859-1
        soup = BeautifulSoup(data, "html.parser")
        data = json.loads(str(soup))
        dates = data["chart"]["result"][0]["timestamp"]
        prices = data["chart"]["result"][0]['indicators']['quote'][0]['close']
        dates = [datetime.fromtimestamp(date).date() for date in dates]

        stock_prices = pd.DataFrame({"date": dates, "price": prices})
        stock_prices.set_index("date").sort_index(ascending=True, inplace=True)
        stock_prices["indexed"] = ((stock_prices["price"]/stock_prices["price"][0])*100).round(2)
        stock_prices["price"] = stock_prices["price"].round(2)
        stock_prices["stock"] = stock
        stock_prices.ffill(axis=0, inplace=True)
        stock_prices.bfill(axis=0, inplace=True)

        stock_prices["r_vol"] = (stock_prices["indexed"].rolling(self.vol_window).std())/100

        stock_data[stock]["vol"] = [((stock_prices["indexed"][-self.vol_window:]).std()/100).round(3)]
        stock_data[stock]["price"] = [stock_prices["price"][-1:].values[0]]

        # print(price)
        # ma_fifty = stock_prices["price"][-50:].mean().round(2)
        stock_data[stock]["50/100_day_ma"] = [(stock_prices["price"][-50:].mean()/stock_prices["price"][-100:].mean()).round(2)]
        stock_data[stock]["50/200_day_ma"] = [(stock_prices["price"][-50:].mean()/stock_prices["price"][-200:].mean()).round(2)]
        stock_data[stock]["macd"] = [ta.trend.macd(stock_prices["price"], window_slow=26, window_fast=12, fillna=False)[-10:].mean().round(2)]
        stock_data[stock]["stc"] = [(ta.trend.stc(stock_prices["price"], window_slow=50, window_fast=23, cycle=10, smooth1=3, smooth2=3,fillna=False)[-10:].mean()/100).round(2)]
        stock_data[stock]["trix"] = [ta.trend.trix(stock_prices["price"], window=15, fillna=False)[-10:].mean().round(2)]

        #getting the risk free rate from market watch
        url = "https://ycharts.com/indicators/3_month_t_bill"

        page = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        infile = urllib.request.urlopen(page).read()
        data = infile.decode('ISO-8859-1')  # Read the content as string decoded with ISO-8859-1
        soup = BeautifulSoup(data, "html.parser")

        rfree = soup.find_all('div', attrs={"class": "key-stat-title"})[0].text
        rfree = rfree.split('%')[0].strip()

        stock_data[stock]["risk_free_rate"] = [round(float(rfree) / 100, 3)]
        stock_data[stock]["time"] = [91/365]

        self.price_history.append(stock_prices)
        # print(stock_prices)
        # print(stock_data[stock])
        self.stock_data = stock_data

    def blackScholes(self, stock):
        stock_data = self.stock_data


        #Calculate BS price of call/put
        d1 = (np.log(stock_data[stock]["price"][0] / stock_data[stock]["price"][0]) + (stock_data[stock]["risk_free_rate"][0] + stock_data[stock]["vol"][0] ** 2 / 2) * stock_data[stock]["time"][0]) / (stock_data[stock]["vol"][0] * np.sqrt(stock_data[stock]["time"][0]))
        d2 = d1 - stock_data[stock]["vol"][0] * np.sqrt(stock_data[stock]["time"][0])

        stock_data[stock]["call_price"] = [round(stock_data[stock]["price"][0] * norm.cdf(d1, 0, 1) - stock_data[stock]["price"][0] * np.exp(-stock_data[stock]["risk_free_rate"][0]* stock_data[stock]["time"][0]) * norm.cdf(d2, 0, 1),2)]
        stock_data[stock]["put_price"] = [round(stock_data[stock]["price"][0] * np.exp(-stock_data[stock]["risk_free_rate"][0] * stock_data[stock]["time"][0]) * norm.cdf(-d2, 0, 1) - stock_data[stock]["price"][0] * norm.cdf(-d1, 0, 1),2)]


        # return stock_price_metrics

    def stock_combiner(self, latest_financials):
        #cleans up stock price history data and them maps it to the already created fs table dataframe
        stock_data = self.stock_data

        total_price_history = pd.concat(self.price_history)
        total_price_history.to_sql(f'total_price_history', conn, if_exists='replace', index=False)


        df_dict = {} #converts dictionary into wierd into dictionary normal

        for stock,data in stock_data.items():
            for data_name, data_value in data.items():
                try:
                    df_dict[data_name].append(data_value[0])
                except:
                    df_dict[data_name] = data_value
        # print(df_dict)

        stock_price_metrics = pd.DataFrame(df_dict, index = list(stock_data.keys())) #creates at dict tgat
        stock_price_metrics = stock_price_metrics.reset_index()
        print(stock_price_metrics.head())
        print(latest_financials)


        combined_fs_table = pd.merge(latest_financials, stock_price_metrics,  how='left', left_on=['Stock'], right_on=['index']) #because i reset index its called index

        ##chiang ttm handle to todays date
        combined_fs_table["Date"].replace("ttm", pd.Timestamp("today").strftime("%d/%m/%Y"), inplace=True)



        self.combined_fs_table = combined_fs_table

    def combined_calculations(self):
        combined_fs_table = self.combined_fs_table
        combined_fs_table = combined_fs_table.replace("-", 0)

        combined_fs_table["PE Ratio"] = combined_fs_table["Basic Average Shares"].astype(float) * \
                                           combined_fs_table["price"].astype(float) \
                                           / combined_fs_table["Net Income"].astype(float)

        combined_fs_table.to_sql(f'combined_fs_table', conn, if_exists='replace', index=False)
        print(combined_fs_table)



def main():
    #Initialising classes that will pull financial statement data and as well as recent stock price history data, declares methods
    print("""Hello And Welcome to this stock analysis tool: It consolidates both stock market and financial statement data to
    create a condensed overview of company performance, with some relevant indicators for additional context.

    """)

    fs_object = FinancialStatements()
    stockprice_object = StockPriceAnalysis()


    #method implemenation with list of predeifined stocks
    for stock in stocklist:
        try:
            before = time.time()
            for statement, code in fs_codes.items():
                print(f"Extracting {fs_codes[statement]} data for {stock}")
                fs_object.fs_data_extract(stock, fs_codes[statement], first_metric[statement], periods[statement])
            fs_object.fs_combiner(stock)

            stockprice_object.stock_price_history(stock)
            stockprice_object.blackScholes(stock)

            after = time.time() - before
            print(f"Finished Extracting Financials for {stock}\nTime taken was {int(after)} seconds\n")
        except Exception as e:
            print(f"{e}\n{stock} has bad data - pls review\n")
            continue
    fs_object.stock_fs_combiner()


    ratios_object = FinancialRatios()
    ratios_object.metrics_pnl()
    ratios_object.metrics_bs()
    ratios_object.metrics_combined()
    latest_financials = ratios_object.data_cleanup()

    stockprice_object.stock_combiner(latest_financials)
    stockprice_object.combined_calculations()


if __name__ == "__main__":
    main()

