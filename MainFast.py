
'''

@author: Chris Briere

This program serves to find possible erroneous data point in the data provided by BACI

'''
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

first_year = 2009
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

# These allow for multi-threading

def getTrends (file_name,country,product,relative =1,absolute=50000,market=.15):
    global absolute_threshold
    global trend_threashold
    global relative_threashold
    global market_threshold
    
#     print (product)
    
    start = time.time()
    
    initilize()
    
    absolute_threshold = float(absolute)
    market_threshold = float(market)
    relative_threashold = float(relative)
    
    print (absolute_threshold,market_threshold,trend_threashold,relative_threashold)
    
    keys = []
    for code in country_codes:
        if (country) == country_codes[code]:
            keys.append(code)
            
    for code in country_names:
        if (country) == country_names[code]:
            
            keys.append(code)
    print (keys)
    single_thread(getTrendsPreProcessed,first_year,last_year,arg=(file_name,keys,product))

    findInterestingTrends(product_trends, country_trends)

    filter_trends()

    end = time.time()
    print(end - start)
    call =[]
    
    for trend in final_trends:
        print(trend,final_trends[trend])
        call.append(trend.split(","))
    return plot(country,call)

# This computes the possible errors and returns a plot
def getErrors (file_name,country,local=1.5,absolute=50000,market=.15,trend=1,volatility=3): 
    global local_shift_threshold
    global absolute_threshold
    global trend_threashold
    global volatility_factor
    global market_threshold
    
    start = time.time()
    print (file_name, country)
    initilize()
    
    local_shift_threshold = float(local)
    absolute_threshold = float(absolute)
    market_threshold = float(market)
    trend_threashold = float(trend)
    volatility_factor = float(volatility)
    

        
    keys = []
    for code in country_codes:
        if (country) == country_codes[code]:
            keys.append(code)
    for code in country_names:
        print (country_names[code])
        if (country) == country_names[code]:
            
            keys.append(code)            
    
    print (keys)
    single_thread(getErrorsPreProcessed,first_year,last_year,arg=(file_name,keys))

    findLikelyErrors()

    filter_errors()

    end = time.time()
    print(end - start)
    call =[]
    for error in final_errors:
#         print (error)
        call.append(error.split(","))
    return (plot(country,call))

def getErrorsWithData (file_name,country,codes,local=1.5,absolute=50000,market=.15,trend=1,volatility=3): 
    global local_shift_threshold
    global absolute_threshold
    global trend_threashold
    global volatility_factor
    global market_threshold
    
    start = time.time()
    print (file_name, country)
    initilize()
    
    local_shift_threshold = float(local)
    absolute_threshold = float(absolute)
    market_threshold = float(market)
    trend_threashold = float(trend)
    volatility_factor = float(volatility)
    
    findLikelyErrors(codes)

    filter_errors()

    end = time.time()
    print(end - start)
    call =[]
    for error in final_errors:
        print (error)
        call.append(error.split(","))
    return (plot(country,call))
    
# This allows a program to be multithreaded to add increased efficiency to processing
# It will run threads for discrete values within the range of the min and max values
# this is essentially used to create threads for all years over a range of years
# (i.e. with a range from 2009 to 2014 this program will create 5 threads, one for each of the years)
# Each thread will call the function passing it the val in the range and and argument arg which is 
# an argument for multi_thread. The wait argument dictates how long the sleep method will run. The num argument
# allows a function to run a number of threads less than the full range efficiently, this is useful if the full number
# of threads would overcome the computer that its run on.
def multi_thread(function,min_val,max_val,num=None, arg=None, wait=1):
    global num_threads
    global finished_threads
    global threads
    
    finished_threads = 0 
    num_threads = 0 
    for val in range(min_val,max_val+1):
        t = threading.Thread(target=function, args=(val,arg,True))
        threads.append(t)
        t.start()
        num_threads += 1

    while ( num_threads > finished_threads):
        print ('Still open %s' % (num_threads - finished_threads))
        time.sleep(wait)
# This program will run a for loop for a given range
# The given function will be called with its value in the range
# the given arg and multi, which represents whether the source of the
# call is a multi thread. This allows the program to properly update the
# number of threads  
def single_thread(function,min_val,max_val, arg="None", multi =False):

    for val in range(min_val,max_val+1):
        function(val,arg,multi)
        
def preprocess(year,file_tag,multi):
    newpath = file_tag + str(year) + "/"  
    
    print(newpath)
    
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    file_name = file_tag + str(year) + ".csv"
    
    with open(file_name) as csvfile:
        import_reader = csv.reader(csvfile, delimiter=';')
        last_key = '0'
        
        next(import_reader)
        start = 0
        end = 0
        data = []
        for row in import_reader: 
            column = row[0].split(",")
            key = column[2]
# 
#             print (column)
#             print (key)
#             print (last_key)
#             time.sleep(.5)
#             
            if (last_key != key and data != []):
                save(newpath,last_key,year,data)
                
                data = []

                start = end 
            elif (last_key != key):
                print(last_key)
#             end += 1
            data.append(column)

#             print (last_key,key)
            last_key = column[2]
            
     
    print ("saving")

    if (multi):
        end_thread()
def save (filename,key,year,data):
#     print (start,end,key)

    with open(filename +   "%s_%s" % (key,year) + ".csv", 'w') as mycsvfile:
        datawriter = csv.writer(mycsvfile, lineterminator = '\n')
        for row in data:
#                 print (row,data[row])
#             print(row,key)
#             time.sleep(.5)
            datawriter.writerow(row)


# This program initializes the code. It sets the database, defines the product_codes and country_codes dictionaries
# and fills the country_values dictionary 
def initilize():
    global country_codes
    global product_codes
    global country_names

    country_codes = {}
    country_names = {}
    product_codes = {}
    country_file = file_location + 'country_codes.csv'
    product_file = file_location + 'product_codes.csv'

    with open(country_file) as csvfile:
        import_reader = csv.reader(csvfile, delimiter=';')
        for column in import_reader:
            row = column[0].split(",")
            value = row[0]
            code  = row[3]
            if (code == ""):
                continue
#             print(code)
            codes = code.split('|')
            for c in codes:
                try:
                    label = c.strip('"')
                    while (len (label) < 3):
                        label = "0" + label
                    country_codes[label] = value
                    country_names[label] = row[6].strip('"')
#                     print (label)
                except:
                    pass
#     for i in country_codes:
#         print (i,country_codes[i])
    with open(product_file, encoding='utf-8') as csvfile:
        import_reader = csv.reader(csvfile, delimiter=';',dialect = 'unix')
        for column in import_reader:
#             print(row)
            row = column[0].split(",")
            product_codes[row[1]] = row[0]
    
# This method simply indicated that a thread that was being multithreaded has ended
def end_thread():
    global finished_threads 
    
    finished_threads +=1
def getFilesNotProcessed(year,arg,multi=False):
    
    (filename,key) = arg
    
    file_location = filename + str(year) + '.csv'
    
    with open(file_location) as csvfile:
        reader = csv.reader(csvfile)
        started = False
        
        # This skips the first line which contains the labels   
        reader.next()     
        
        for column in reader:

            if (column[2] == key):
                started = True
                
                product = column[1]
                country = country_codes[column[3]]
                value = float(column[4])
                
                # This saves all of the specific country product pairs to ensure that only the pairs that are in the list are examined
                used_codes[product +"-" + country] =  used_codes.get("%s-%s" % (product,country),0) + value

                product_values["%s-%s" % (year,product)] = product_values.get("%s-%s" % (year,product),0) + value
                country_product_values["%s-%s-%s~%s" % (year,country,product,"Import")] = value
           
            # Since the list is ordered once the last item with the correct key is called the program can exit
            
            elif (started):
                break     

          
def getErrorsPreProcessed(year,arg, multi=False):
    
    global used_codes
    global product_values
    global country_product_values
    
    (filename,keys) = arg
    for key in keys:
        file_location = filename + str(year) + "/"  + key + "_" + str(year) + '.csv'    
        print (file_location)
        try:
            with open(file_location) as csvfile:
                reader = csv.reader(csvfile)
                started = False
                
                # This skips the first line which contains the labels   
                next(reader)     
                
                for column in reader:
#                     print (column)
        
                    if (column[2] == key):
                        started = True
                        
                        product = column[1]
                        country = country_codes[column[3]]
                        value = float(column[4])
                        
                        # This saves all of the specific country product pairs to ensure that only the pairs that are in the list are examined
                        used_codes[product +"-" + country] =  used_codes.get("%s-%s" % (product,country),0) + value
        
                        product_values["%s-%s" % (year,product)] = product_values.get("%s-%s" % (year,product),0) + value
                        country_product_values["%s-%s-%s~%s" % (year,country,product,"Import")] = value
                   
                    # Since the list is ordered once the last item with the correct key is called the program can exit
                    
                    elif (started):
                        break      
        except:
            pass
def getTrendsPreProcessed(year,arg, multi=False):

    global product_trends
    global country_trends
    
    used_products = {}
    used_countries ={}
  
    (filename,keys,product) = arg

#     print(product)
    
    for key in keys:

        file_location = filename + str(year) + "/"  + key + "_" + str(year) + '.csv'    
        try:
            with open(file_location) as csvfile:
                reader = csv.reader(csvfile)
                started = False
                
                # This skips the first line which contains the labels   
                next(reader)     
                
                for column in reader:
        #             print (column)
                    if (column[2] == key):
                        started = True
                        
                        product = column[1]
                        country = country_codes[column[3]]
                        value = float(column[4])
                        
                        # This saves all of the specific country product pairs to ensure that only the pairs that are in the list are examined
                        if (product):
                            used_products [product] =  used_products.get("%s" % (product),0) + value
                        else:
                            used_countries[country] = used_countries.get("%s" % (country),0) + value
                        
                        if (product):
                            product_values["%s-%s" % (year,product)] = product_values.get("%s-%s" % (year,product),0) + value
                        else:
                            country_values["%s-%s" % (year,country)] = country_values.get("%s-%s" % (year,country),0) + value
                   
                    # Since the list is ordered once the last item with the correct key is called the program can exit
                    
                    elif (started):
                        break
                    
            used_markets ={}
            if (product):   
                for product in used_products:
                    value = product_values.get("%s-%s" % (year,product[:-2]),0) + used_products[product]
                    product_values["%s-%s" % (year,product[:-2])] = value
                    used_markets[product[:-2]] =  value
            else:    
                for country in used_countries:
                    country_values["%s-%s" % (year,country[:2])] = country_values.get("%s-%s" % (year,country[:2]),0) + used_countries[country]
                
            if (product):
                market_trends   = find_trends(used_markets,getProduct)

                product_trends  = find_product_trends(market_trends,used_products)
                
#                 for trend in market_trends:
#                     print(trend)
                product_trends.update(market_trends)

            else:
                country_trends  = find_trends(used_countries,getCountry)
            
        
        except:
            pass
    
def findInterestingTrends (product_trends,country_trends):
            
    # First it acquires the trends from the product trends 
    print (relative_threashold)
    for trend in product_trends:

        product = trend.split("-")[0]
        trendline = trend.split("-")[1]
        
        trend_val = product_trends[trend]

        year = getTrendYear(trendline)
         


#         if (abs(trend_val)/(last_year - year + 1) >= )

        # If the value of the trend (i.e. -.5 or a 50% decrease) times the size of the market (i.e. $1 billion dollars) is greater than the 
        # absolute threshold then insert the value into interesting trends
        
        
        if (abs(trend_val)/(last_year - year + 1) >= relative_threashold):

            start = getProduct(product, year)
#             print(start)
            end = getProduct(product, last_year)

            net = end -start
            
            if(abs(net) > absolute_threshold):
                
                if (len(product)<7):
                    market_val = 1
                else:
                    total = 0 
                    for y in range (year,last_year):
                        total += getProduct(product[:-2],y)
                    market_average = total/(last_year - year + 1)
                    total = 0 
                    for y in range (year,last_year):
                        total += getProduct(product,y)
                    product_average = total/(last_year - year + 1)
                    market_val = product_average/market_average
                if (market_val > market_threshold):
                    label = "%s$%s" % (trend,net)
                    
                    interesting_trends[label] = trend_val
            
            
    for trend in country_trends:
                
        trend_val = country_trends[trend]       

        trendline = trend.split("-")[1]
            
        country = trend.split("-")[0]
                     
        year = getTrendYear(trendline)
#         
#         if (not year):
#             print ("Trend:",trend)
                 
        if (abs(trend_val)/(last_year - year + 1) >= relative_threashold):


                       
            start = getCountry(country,year)
            
            end = getCountry(country,last_year)
    
            net = end - start
                    
    
            # If the value of the trend (i.e. -.5 or a 50% decrease) times the size of the market (i.e. $1 billion dollars) is greater than the 
            # absolute threshold then insert the value into interesting trends
            
            if (abs(net) > absolute_threshold):
                
                label = "%s$%s" % (trend,net)
            
                interesting_trends[label] = trend_val
    
    return interesting_trends

def find_product_trends(market_trends,products):

    output = {}

    for product in products:
        # Find all the Long, Medium, and Short term Trends in products
    
#             print (one_year_val)
        if (products[product] < absolute_threshold):
            continue

        five_year_val = getProduct(product, last_year-5)        
        first_year_val= getProduct(product, first_year)
        last_year_val = getProduct(product, last_year)      
        one_year_val  = getProduct(product, last_year-1)
        three_year_val= getProduct(product, last_year-3)

        if (one_year_val != 0):

            one_year_trend = (last_year_val - one_year_val )/ abs(one_year_val)
           
            one_year_label = "%s-%s" % (product,"one_year_trend")
            
            
            if (one_year_trend < 0):
                if (one_year_trend == -1):
                    output [one_year_label] = -100 
                else:
                    output [one_year_label] = 1/(1 - one_year_trend) - 1 - market_trends["%s-%s" % (product[:-2],"one_year_trend")]
            else:
                output [one_year_label] = one_year_trend - market_trends["%s-%s" % (product[:-2],"one_year_trend")]
                
            
        elif(last_year_val > 0):
            
            one_year_label = "%s-%s" % (product,"one_year_trend")

            output [one_year_label] = 1
                      
        if (three_year_val != 0):

            three_year_trend = (last_year_val - three_year_val )/ abs(three_year_val)
                      
            three_year_trend_label = "%s-%s" % (product,"three_year_trend")
                
            if (three_year_trend < 0):
                if (three_year_trend == -1):
                    output [three_year_trend_label] = -100
                else:
                    output [three_year_trend_label] = 1/(1 - three_year_trend) - 1 -  market_trends["%s-%s" % (product[:-2],"three_year_trend")]
            else:
                output [three_year_trend_label] = three_year_trend -  market_trends["%s-%s" % (product[:-2],"three_year_trend")]
            
        elif(last_year_val > 0):
            
            three_year_trend_label = "%s-%s" % (product,"three_year_trend")

            output [three_year_trend_label] = 1
            
        if (five_year_val != 0):

            five_year_trend = (last_year_val - five_year_val )/ abs(five_year_val)
          
            five_year_trend_label = "%s-%s" % (product,"five_year_trend")
            
            output [five_year_trend_label] = five_year_trend
            
            if (five_year_trend < 0):
                if (five_year_trend == -1):
                    output [five_year_trend_label] = -100
                else:
                    output [five_year_trend_label] = 1/(1 - five_year_trend) - 1 -  market_trends["%s-%s" % (product[:-2],"five_year_trend")]
            else:
                output [five_year_trend_label] = five_year_trend -  market_trends["%s-%s" % (product[:-2],"five_year_trend")]
            
        elif(last_year_val > 0):
        
            five_year_trend_label = "%s-%s" % (product,"five_year_trend")

            output [five_year_trend_label] = 1
                     
        if (first_year_val != 0):

            long_trend = (last_year_val - first_year_val )/ abs(first_year_val)

            long_trend_label = "%s-%s" % (product,"long_trend")

            output [long_trend_label] = long_trend
            
            if (long_trend < 0):
                if (long_trend == -1):
                    output [long_trend_label] = -100
                else:
                    output [long_trend_label] = 1/(1 - long_trend) - 1 -  market_trends["%s-%s" % (product[:-2],"long_trend")]
            else:
                output [long_trend_label] = long_trend -  market_trends["%s-%s" % (product[:-2],"long_trend")]
        elif(last_year_val > 0):
            
            long_trend_label = "%s-%s" % (product,"long_trend")

            output [long_trend_label] = 1
    
    return output


def find_trends(data,accessor):

    output = {}

    for item in data:
        # Find all the Long, Medium, and Short term Trends in products
    
#             print (one_year_val)
        if (data[item] < absolute_threshold):
            continue

        five_year_val = accessor(item, last_year-5)        
        first_year_val= accessor(item, first_year)
        last_year_val = accessor(item, last_year)      
        one_year_val  = accessor(item, last_year-1)
        three_year_val= accessor(item, last_year-3)
        
#         if (last_year_val < one_year_val):
#             print (last_year_val,one_year_val)

        # Attempts to lower the calculation requirements by not calculating for items
        # that have no values in the data set given
                        
        if (one_year_val != 0):

            one_year_trend = (last_year_val - one_year_val )/ abs(one_year_val)
           
            one_year_label = "%s-%s" % (item,"one_year_trend")
            
            
            if (one_year_trend < 0):
                if (one_year_trend == -1):
                    output [one_year_label] = -100
                else:
                    output [one_year_label] = 1/(1 - one_year_trend) - 1
            else:
                output [one_year_label] = one_year_trend
            
        elif(last_year_val > 0):
            
            one_year_label = "%s-%s" % (item,"one_year_trend")

            output [one_year_label] = 1
                      
        if (three_year_val != 0):

            three_year_trend = (last_year_val - three_year_val )/ abs(three_year_val)
                      
            three_year_trend_label = "%s-%s" % (item,"three_year_trend")
                
            if (three_year_trend < 0):
                if (three_year_trend == -1):
                    output [three_year_trend_label] = -100
                else:
                    output [three_year_trend_label] = 1/(1 - three_year_trend) - 1
            else:
                output [three_year_trend_label] = three_year_trend
            
        elif(last_year_val > 0):
            
            three_year_trend_label = "%s-%s" % (item,"three_year_trend")

            output [three_year_trend_label] = 1
            
        if (five_year_val != 0):

            five_year_trend = (last_year_val - five_year_val )/ abs(five_year_val)
          
            five_year_trend_label = "%s-%s" % (item,"five_year_trend")
            
            output [five_year_trend_label] = five_year_trend
            
            if (five_year_trend < 0):
                if (five_year_trend == -1):
                    output [five_year_trend_label] = -100
                else:
                    output [five_year_trend_label] = 1/(1 - five_year_trend) - 1
            else:
                output [five_year_trend_label] = five_year_trend
            
        elif(last_year_val > 0):
        
            five_year_trend_label = "%s-%s" % (item,"five_year_trend")

            output [five_year_trend_label] = 1
                     
        if (first_year_val != 0):

            long_trend = (last_year_val - first_year_val )/ abs(first_year_val)

            long_trend_label = "%s-%s" % (item,"long_trend")

            output [long_trend_label] = long_trend
            
            if (long_trend < 0):
                if (long_trend == -1):
                    output [long_trend_label] = -100
                else:
                    output [long_trend_label] = 1/(1 - long_trend) - 1
            else:
                output [long_trend_label] = long_trend
        elif(last_year_val > 0):
            
            long_trend_label = "%s-%s" % (item,"long_trend")

            output [long_trend_label] = 1
    
    return output

# This program will search through both the trends and the individual values of trades between two countries to ensure to detect possible errors
# Its argument note allows an appendment to the label to allow further clarification as to its meaning
def findLikelyErrors(codes = None):

    global used_codes

#     print (local_shift_threshold,absolute_threshold)
    # Runs through both True, and False
    for key in used_codes:
        (product,country) = key.split("-")
        
        total = used_codes[key] 
#             for year in range (first_year, last_year+1):
#                 total += getProductCountry(product,country, year,tag)
          
        if total/3 < absolute_threshold:
            continue
  
        totals = []

        for year in range (first_year, last_year+1):
            totals.append(getProductCountry(product,country, year,"Import"))

        three_year_averages = []
        three_year_averages.append((totals[0]+totals[1]+totals[2])/3)
        three_year_averages.append((totals[1]+totals[2]+totals[3])/3)
        three_year_averages.append((totals[2]+totals[3]+totals[4])/3)
        three_year_averages.append((totals[3]+totals[4]+totals[5])/3)

        for year in range(first_year+1,last_year):
        # This will not include 2014 as data there could be reasonable even though it seems erroneous
            three_year_average = three_year_averages.pop(0)
            
                    

            if (three_year_average > absolute_threshold):
                value = getProductCountry(product,country, year,"Import")
                local_shift = (value - three_year_average)/three_year_average
                if (local_shift < 0):
                    local_shift *= -3 
#                     print (local_shift - local_shift_threshold)
                if(abs(local_shift) > local_shift_threshold):

#                         print(local_shift)

                    # This calculates the total size of the market so that the relative size of this value to the size of the market can be determined
                        
                    market_val = getProduct(product, year)
#                    
                            
                    # If the value of the market is a sufficient part of the market (i.e. a very significant spike) or the average is a significant 
                    # portion of the market (i.e. a meaningful change with a major trade partner)
                    
                    if ( market_val == 0 or three_year_average/market_val > market_threshold):    
                        label = "%s-%s|%s" % (product,country,year)
                        index = year - first_year
#                         print (label,local_shift)

                        three_year_net = totals[min(last_year-first_year,index + 1)] - totals[max(0,index-1)]
                        
                        expected_shift = three_year_net/2
                        if (expected_shift == 0):
                            off_from_trend = trend_threashold + 1 
                        else:
                            off_from_trend = (value - expected_shift)/expected_shift

                        if (abs(off_from_trend) > trend_threashold or abs(local_shift) >= local_shift_threshold * 2):
                            errors[label]  = local_shift
def filter_errors():
    global errors
    added_errors = {}
    
    for error in errors:
#         print(error)
        label = error.split("|")[0].split("-")[0] + "~" + error.split("|")[-1].split("~")[-1].split("$")[0]
        try:
            added_errors[label] += 1
            
            # This will remove any field that coexist with another field, this will remove "High Volatility" Markets that simply react weirdly
            # (i.e. the Tanker market is made up almost entirely of 
            if (added_errors[label] >= volatility_factor - 1):
                del error[label]

        except:
            added_errors[label] = 0
#             print (label)
            label = ",," + label.split("~")[0] + "," + error.split("|")[0].split("-")[1] + "," + label.split("~")[1]

            final_errors[label] =  int(error.split("|")[1].split("~")[0])


#     for error in final_errors:
#         print (error,final_errors[error])

def filter_trends():
    global interesting_trends
    
    used_label = {}
    
    for trend in interesting_trends:
        
        trendline = trend.split("$")[0].split("-")[-1]
        
        year = getTrendYear(trendline)
#         if (not year):
#             print (trend)
        continue_bool = False
        
        
        
        try:
            if (year < used_label[trend.split("-")[0]]):
                continue_bool = True
         
                
        except:
            continue_bool = True 
        if (continue_bool):
            try:
               
                float(trend.split("-")[0])
                
                label =  str(year) + "," + str(last_year) + "," + trend.split("-")[0] + ",," + "Import"
                
                used_label[trend.split("-")[0]] = year
                
                final_trends[label] = final_trends.get(label,0) + interesting_trends[trend]
    
            except:
                
                label = str(year) + "," + str(last_year) + ",," + trend.split("-")[0] + "," + "Import"
                
                used_label[trend.split("-")[0]] = year
                
                final_trends[label] = final_trends.get(label,0) + interesting_trends[trend]
            
#     for error in final_errors:
#         print (error,final_errors[error])


def getProduct(product, year):
    try:
        value = product_values["%s-%s" % (year,product)]
        return value
    except:
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
        print (product,country,year,tag)
        return country_product_values["%s-%s-%s~%s" % (year,country,product,tag)]
    except:
        return 0
      
def getTrendYear(trendline):
    if (trendline == "five_year_trend"):
        return last_year - 5
    elif (trendline == "long_trend"):
        return first_year
    elif (trendline == "three_year_trend"):
        return last_year - 3
    elif (trendline == "one_year_trend"):
        return last_year - 1
    else:
        print ("Trend Year Failure")
        print (trendline)
        return False
def dataLookUp():
    type = input("Product or Country")

    while(True):
        
        if (type == "Product"):
            args = input("Insert product and year separated by comma, print 'quit' to exit").split(",")
            if args[0] == 'quit':
                break
            try:
                print (getProduct(args[0],args[1]))
            except:
                print (args, "Not found")
        else:
            args = input("Insert product, country, year, is_export separated by comma, print 'quit' to exit").split(",")
            if args[0] == 'quit':
                break
            try:
                print (getProductCountry(args[0],args[1],args[2],args[3]))
            except:
                print (args, "Not found")
def plotter():

    while(True):
        inputs = input("Enter First Year, Last Year, Product, Country, Tag seperated by comma if not applicable then write noting (e.g. ',,'). Add |to add another plot").split("|")
        args = []
        for call in inputs:
            call = call.split(",")
            
            print (call)
            if call[0] == 'quit':
                break
            args.append(call)
            print (call, "Not found")
        plot (args)

def plot(inp_country,inputs):
        
    fig, ax = plt.subplots()
    
    # We need to draw the canvas, otherwise the labels won't be positioned and 
    # won't have values yet.
    fig.canvas.draw()
    fig.set_size_inches(17, 10.5)
    first_year = 2009
    last_year = 2014
#     print (inputs)
    for arg in inputs:
        points = []
        point_market = []
        point_overmarket = []
        (min,max,product,country, tag) = arg

        if (min != "" or max != ""): 
            first_year = int(min)
            last_year = int(max)


        x = range(first_year,last_year+1)
        
        ax.set_xticklabels(x)
    
        for year in range(first_year,last_year+1):
            if ((country == None or country == "") and (product == None or product == "")):
                return None
            elif(country == None or country == "" ):
                points.append(math.log( 1 + getProduct(product,year)* 1000))
#                 point_market.append(getProduct(product[:-2],year)* 1000)
#                 point_overmarket.append(getProduct(product[:-2],year)* 1000)
            elif(product == None or product == ""):
                points.append(math.log( 1 + getCountry(country,year) * 1000))
            else:
                points.append(math.log( 1 + getProductCountry(product,country,year,"Import") * 1000))
#                 point_market.append(getProduct(product,year) * 1000)
#                 if (getProduct(product[:-2],year) > getProduct(product,year) * too_large_market):
#                 point_overmarket.append(getProduct(product,year) * 1000)
#                 else:
#                     point_overmarket.append(getProduct(product[:-2],year)* 1000)
#         print (arg)

#         print (points) 
        plt.plot(x,points)
#         plt.plot(x,point_market)
#         plt.plot(x,point_overmarket)
    
    first_year = 2009
    last_year = 2014
    plt.ylabel('Value of Trade ($)')
    plt.xlabel('Country:%s,Years from %s to %s' % (inp_country,first_year,last_year))
    fig.tight_layout()
#     plt.show()

    return (plt)
# def plot(min,max,product= None,country = None, tag = "Export"):
#     points = []
#     
#     first_year = int(min)
#     last_year = int(max)
#     
#     product = int(product)
#     
#     fig, ax = plt.subplots()
#     
#     # We need to draw the canvas, otherwise the labels won't be positioned and 
#     # won't have values yet.
#     fig.canvas.draw()
#     
#     x = range(first_year,last_year+1)
#     
#     ax.set_xticklabels(x)
# 
#     for year in range(first_year,last_year+1):
#         if (country == None or country == "" and product == None or product == ""):
#             return None
#         elif(country == None or country == "" ):
#             points.append(getProduct(product,year))
# #         DEPRECATED
# #         elif(product == None):
# #             points.append(Main.getCountry(country,year))
#         else:
#             points.append(getProductCountry(product,country,year,tag))
#             
#     plt.plot(x,points)
#     plt.ylabel('Value of Trade (thousands of $)')
#     plt.xlabel('Country:%s,Product:%s,Years from %s to %s' % (country,product,min,max))
#     plt.show()
    
if __name__ == '__main__':
    start = time.time()
    initilize()
    file_name = file_location + 'baci92_'
    single_thread(preprocess, first_year, last_year, file_name, False)
    
    end = time.time()

#     getTrends(file_name, 'eunld')
#     getErrors(file_name, 'eunld')
#     single_country_run(file_name, False, False, "eunld")
#     import plotly.plotly as py
#     import plotly.graph_objs as go
#     
#     # Sign in to plotly
#     py.sign_in('DemoAccount', '1gf0wk3') # Replace the username, and API key with your credentials.
#     trace0 = Scatter(
#         x=[1, 2, 3, 4],
#         y=[10, 15, 13, 17]
#     )
#     trace1 = Scatter(
#         x=[1, 2, 3, 4],
#         y=[16, 5, 11, 9]
#     )
#     data = Data([trace0, trace1])
# 
#     # Create a simple chart..
# #     trace = go.Bar(x=[2, 4, 6], y= [10, 12, 15])
# #     data = [trace]
#     layout = go.Layout(title='A Simple Plot', width=800, height=640)
#     fig = go.Figure(data=data, layout=layout)
    
#     py.image.ishow(fig)
#     
#     py.image.save_as(fig, filename='a-simple-plot.png')
#         

# def single_country_run (file_name,datalookup,plot,country,database,type):
    
