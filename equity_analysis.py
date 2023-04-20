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

#IMMUTABLE VARITABLES - For each of the
conn = sqlite3.connect('output/stock_data.sqlite')
cur = conn.cursor()

fs_codes = {"pnl": "financials", "bs": "balance-sheet", "scf": "cash-flow"}
periods = {"pnl": 5, "bs": 4, "scf": 5}
first_metric = {"pnl": "Total Rev", "bs": "Total Ass", "scf": "Operating Cash"}

stocklist = ["RR.L", "TSLA", "BMW.F", "FORD", "VOW.DE", "NSANY", "SZKMY", "GM", "HMC", "TM", "RACE", "0175.HK", "TTM","HOG","PAH3.DE"]


#DECORATORS/wrappers

def combiner_function(func):
    print("""Hello And Welcome to this stock analysis tool: It consolidates both stock market and financial statement data to
    create a condensed overview of company performance, with some relevant indicators for context.
    
    """)

    def wrapper(self):
        stock_packages = []
        for stock in stocklist:
            before = time.time()
            for statement, code in fs_codes.items():
                print(f"Extracting {fs_codes[statement]} data for {stock}")
                func(self, stock, fs_codes[statement], first_metric[statement], periods[statement])
            fs_object.fs_combiner()
            ratios_object = FinancialRatios()
            ratios_object.metrics_pnl()
            ratios_object.metrics_bs()
            ratios_object.metrics_combined()
            latest_financials = ratios_object.data_cleanup(stock)
            stockprice_object = StockPriceAnalysis()
            stockprice_object.stock_price_history(stock)
            lastest_stock_data = stockprice_object.blackScholes(stock)
            stock_packages.append(pd.concat([latest_financials, lastest_stock_data], axis=1))

            after = time.time() - before
            print(f"Finished Extracting Financials for {stock}\nTime taken was {int(after)} seconds\n\n")



        multi_stock_analysis = pd.concat(stock_packages)
        multi_stock_analysis.set_index("Stock")
        multi_stock_analysis["PE Ratio"] = multi_stock_analysis["Basic Average Shares"].astype(float) * multi_stock_analysis["price"].astype(float)/multi_stock_analysis["Net Income"].astype(float)
        multi_stock_analysis.to_excel(f"output/multi_stock_table.xlsx")
        multi_stock_analysis.to_sql(f'output/multi_stock_table', conn, if_exists='replace', index=True)

    return wrapper

####Collects and cleans, income statement, balance sheet and cashflow statement data
class FinancialStatements():
    def __init__(self):
        self.financial_sheets = []
        pass

    @combiner_function
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
        driver.find_element("xpath", "//*[contains(text(), 'Maybe later')]").click()
        time.sleep(2)
        driver.find_element("xpath", "//*[contains(text(), 'Expand All')]").click()

        # print(driver.page_source)
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
            if len(v) < period: bad_data.append(k)  ##may need to play with this one to get up to day b/s numbers
            if len(v) > period: bad_data.append(k)

        for k in bad_data:
            cleaned_dataset.pop(k, None)

        metrics = soup(
            'span')  # , {"class": "Va(m)"})#, "Ta(c) Py(6px) Bxz(bb) BdB Bdc($seperatorColor) Miw(120px) Miw(100px)--pnclg Bgc($lv1BgColor) fi-row:h_Bgc($hoverBgColor) D(tbc)"]})
        for n, k in enumerate(metrics):
            if k.text == "Breakdown":
                # print(n)
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

        fs_table = pd.DataFrame(cleaned_dataset, index=dates)
        for column in fs_table.columns:
            fs_table[column] = fs_table[column].str.replace(",", "")
            fs_table[column] = pd.to_numeric(fs_table[column], errors="ignore")

        self.financial_sheets.append(fs_table)
        fs_table.to_excel(f"output/{stock}_{statement}.xlsx")

        self.fs_table = fs_table
        self.stock = stock

    def fs_combiner(self):
        stock_financials = pd.concat(self.financial_sheets, axis=1)
        FinancialStatements.financials = stock_financials
        self.financial_sheets = []

####computes financial ratios using all
class FinancialRatios:
    def __init__(self):
        self.fs_table  = FinancialStatements.financials

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

    def data_cleanup(self, stock):
        fs_table = self.fs_table
        fs_table.index.names = ["Date"]
        fs_table = fs_table.reset_index()
        fs_table["Stock"] = stock
        fs_table.set_index("Stock", inplace=True)
        fs_table.to_excel(f"output/fs_table.xlsx")
        fs_table.to_sql(f'output/{stocklist[0]}_FS', conn, if_exists='replace', index=False)

        # for n, col in enumerate(fs_table.columns):
        #     if col == "OM %": cut_off = n
        #
        latest_financials = fs_table #[fs_table.columns[cut_off:]]
        latest_financials = latest_financials.iloc[1:2].reset_index()
        #df.take([2])

        return latest_financials

class StockPriceAnalysis:
    def __init__(self):
        self.stock_data = defaultdict(lambda:defaultdict(list))

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

        stock_data[stock]["vol"] = [((stock_prices["indexed"][-100:].std()) / 100).round(3)]
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

        self.stock_data = stock_data

    def blackScholes(self, stock):
        stock_data = self.stock_data

        #Calculate BS price of call/put
        d1 = (np.log(stock_data[stock]["price"][0] / stock_data[stock]["price"][0]) + (stock_data[stock]["risk_free_rate"][0] + stock_data[stock]["vol"][0] ** 2 / 2) * stock_data[stock]["time"][0]) / (stock_data[stock]["vol"][0] * np.sqrt(stock_data[stock]["time"][0]))
        d2 = d1 - stock_data[stock]["vol"][0] * np.sqrt(stock_data[stock]["time"][0])

        stock_data[stock]["call_price"] = round(stock_data[stock]["price"][0] * norm.cdf(d1, 0, 1) - stock_data[stock]["price"][0] * np.exp(-stock_data[stock]["risk_free_rate"][0]* stock_data[stock]["time"][0]) * norm.cdf(d2, 0, 1),2)
        stock_data[stock]["put_price"] = round(stock_data[stock]["price"][0] * np.exp(-stock_data[stock]["risk_free_rate"][0] * stock_data[stock]["time"][0]) * norm.cdf(-d2, 0, 1) - stock_data[stock]["price"][0] * norm.cdf(-d1, 0, 1),2)

        df_dict = {}
        for k,v in stock_data[stock].items():
            df_dict[k] = v

        stock_price_metrics = pd.DataFrame(df_dict, index = list(stock_data.keys()))
        stock_price_metrics = stock_price_metrics.reset_index()

        return stock_price_metrics

#running stock price analysis
# def main():
fs_object = FinancialStatements()
fs_object.fs_data_extract()

# if __name__ == "__main__":
#     main()