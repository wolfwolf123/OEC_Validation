
'''

@author: Chris Briere

This program serves to find possible erroneous data point in the data provided by BACI

'''
import Tools
import Plotter
import Trend_Handler
import Error_Handler

import csv
import math
import threading
import time
import matplotlib.pyplot as plt
import os
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from flask import Flask, make_response, send_file
from matplotlib.dates import DateFormatter
import code


local_shift_threshold = 1.5
# This accounts the relative size of the values to the left and right of this value

volatility_factor = 3
# This is the number of trends found in a single market for it considered "Volatile" and to ignore the errors in that market 

absolute_threshold = 50000 # 50,000,000 50 Million
# This is the threshold for a market in a country to be meaningful in thousands

market_threshold = .15
# This is the percent of the market that a trade must make up in-order to be considered relevant

trend_threashold = 1

relative_threashold = 1 

first_year = 2001
# The first year that is recorded

last_year  = 2014
# The last year that is recorded

country_codes = {}

country_names = {}

# This stores the country_id such that the hs92 value is the key and the OEC value is the value

product_codes = {}
# This stores the product_id such that the hs92 value is the key and the OEC value is the value

country_product_values = {}
# This stores the information regarding the trade between two countries to increase efficiency

product_values = {}
# This stores the information regarding the trade of a product to increase efficiency

country_values = {}

product_trends = {}

list_of_used_codes = {}

country_trends = {}

used_codes = {}

final_errors = {}

final_trends = {}

used_continents = {}

interesting_trends = {}
    
threads = []
finished_threads = 0
num_threads = 0 
errors = {}
# file_location = r'/home/chris/UROP Data/'
file_location = r'C:/Users/Chris Briere/UROP Data/'
# data_file_location = r'/media/ramdisk/'
total_loop = 0
total_csv = 0

total_country_value = 0

def getProduct(product, year):
#     for product in product_values:
#         print("Products:",product,product_values[product])
    try:
        value = product_values["%s-%s" % (year,product)]
#         print("%s-%s" % (year,product),value)
        return value
    except:
#         print("%s-%s" % (year,product))
        return 0;
def getCountry(country, year):
    try:
        value = country_values["%s-%s" % (year,country)]
        return value
    except:
        return 0
#    This will run through the collected data and return the Total Product for a given year and country
def getProductCountry(product,country,year,tag):
    try:
#         print (product,country,year,tag)
        return country_product_values["%s-%s-%s~%s" % (year,country,product,tag)]
    except:
        return 0


    
if __name__ == '__main__':
    start = time.time()
    Tools.initilize()
    file_name = file_location + 'baci92_'
    Tools.single_thread(Tools.preprocess, first_year, last_year, file_name, False)
    
    end = time.time()

    
