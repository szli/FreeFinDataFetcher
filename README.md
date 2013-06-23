FreeFinDataFetcher
=======

This is a very simple Python tool to download intraday financial data from various free sources (Google, Yahoo and Netfonds). The results are saved in csv format.

Currently only SP500 stocks are downloaded but this is configurable.

Google and Yahoo data are about 1-min and Netfonds is tick data (but not complete if one day data is too large)

Please note that the websites could change the web address so this tool is not guaranteed to work in the future.

I am not responsible for any data inaccuracies.

So far there is no special dependency, only standard Python is used. Tested under Python 2.7.

But we should expect dependencies on numpy/pandas/scipy/matplotlib etc. in the future. 

To use it, simply run 
python FinDataFetcher.py


To Do:
Post process the data using pandas.