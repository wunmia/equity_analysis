# equity_analysis
(WIP) This project seeks to analysis stocks through both financial statements and alsoo equity performance

The program first scrapes income statements, balance sheets and cash flow statements from the specified list of stock, from these it calculates some performance metrics e.g. asset turnover, quick ratio, earning per share, investment as % of profits
The program then looks at the equity performance analysing returns, using technical indicators to recognise momentum and black scholes to understand how volatile the stock is
The code is still in development, the goal is to integrate these two pieces, to be able to compare metrics across stock in order to find potential mispricings

The requirements are in the req doc in this repository
Pls note I have used package SQLite3 as well as DB browser for data storage purposes.
This code uses a hybrid of Selenium and Beautifulsoup - I have been running it in Pycharm, I ecountered errors when running via the command line.
