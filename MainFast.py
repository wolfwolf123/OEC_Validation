
'''

@author: Chris Briere

This program serves to find possible erroneous data point in the data provided by BACI

'''
import csv
import plotly
from plotly.graph_objs import Scatter, Layout
import tempfile
import plotly.plotly as py
from plotly.graph_objs import *
import threading
import time
import matplotlib.pyplot as plt
import os
import StringIO
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
used_codes = {}
used_products = {}
used_countries = {}
# This stores the country_id such that the hs92 value is the key and the OEC value is the value

product_codes = {}
# This stores the product_id such that the hs92 value is the key and the OEC value is the value

country_product_values = {}
# This stores the information regarding the trade between two countries to increase efficiency

product_values = {}
# This stores the information regarding the trade of a product to increase efficiency

country_values = {}

final_errors = {}

threads = []
finished_threads = 0
num_threads = 0 
errors = {}
file_location = r'/home/chris/UROP Data/'
# data_file_location = r'/media/ramdisk/'
total_loop = 0
total_csv = 0

# These allow for multi-threading


# This computes the relevant data for a single given country
def single_country_run (file_name,datalookup,ploting,country,local,absolute,market,trend,volatility):
    global local_shift_threshold
    global absolute_threshold
    global trend_threashold
    global volatility_factor
    global market_threshold
    
    # These Inputs are booleans, with the exception of file_name which denotes the file from which to retrieve the data, of what parts of the code
    # Should be used. If a value is denoted false the associated table WILL BE DELETED to be replaced with new values. Saved, if True, will save all
    # Other values that are denoted as completed, e.g. are True. Datalookup will allow you to search collected data
    start = time.time()
    initilize()
    
    local_shift_threshold = float(local)
    absolute_threshold = float(absolute)
    market_threshold = float(market)
    trend_threashold = float(trend)
    volatility_factor = float(volatility)
        
    key = '000'
    for code in country_codes:
        if (country) == country_codes[code]:
            key = code
            break
#     print (key)
    print("Initilizing...")
#     single_thread(preprocess,first_year,last_year, arg = file_name)
    single_thread(getFilesPreProcessed,first_year,last_year,arg=(file_name,key))
#  
    findLikelyErrors()
# #     if (not final_errors):
    filter_errors()
#     if (datalookup):
#         dataLookUp()
#     if (plot):
#         plotter()
    end = time.time()
    print(end - start)
    call =[]
    for error in final_errors:
        call.append(error.split(","))
    return plot(country,call)

    
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
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    file_name = file_tag + str(year) + ".csv"
    
    with open(file_name) as csvfile:
        import_reader = csv.reader(csvfile, delimiter=',')
        last_key = '0'
        
        import_reader.next()
        start = 0
        end = 0
        data = []
        for column in import_reader: 

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
    if (multi):
        end_thread()
def save (filename,key,year,data):
#     print (start,end,key)

    with open(filename +   "%s_%s" % (key,year) + ".csv", 'w') as mycsvfile:
        datawriter = csv.writer(mycsvfile)
        for row in data:
#             print(row,key)
#             time.sleep(10)
            datawriter.writerow(row)

# This program initializes the code. It sets the database, defines the product_codes and country_codes dictionaries
# and fills the country_values dictionary 
def initilize():
    global country_codes
    global product_codes

    country_codes = {}
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
#                     print (label)
                except:
                    pass
#     for i in country_codes:
#         print (i,country_codes[i])

    with open(product_file) as csvfile:
        import_reader = csv.reader(csvfile, delimiter=';')
        for column in import_reader:
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
            
def getFilesPreProcessedErrors(year,arg,key, multi=False):

    (filename,key) = arg

    file_location = filename + str(year) + "//"  + key + "_" + str(year) + '.csv'    
    
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

def getFilesPreProcessedTrends(year,arg,key, multi=False):

    (filename,key) = arg

    file_location = filename + str(year) + "//"  + key + "_" + str(year) + '.csv'    
    
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
                used_products [product] =  used_products.get("%s" % (product),0) + value
                used_countries[country] = used_countries.get("%s" % (country),0) + value
                
                country_values["%s-%s" % (year,country)] = country_values.get("%s-%s" % (year,country),0) + value
                product_values["%s-%s" % (year,product)] = product_values.get("%s-%s" % (year,product),0) + value
           
            # Since the list is ordered once the last item with the correct key is called the program can exit
            
            elif (started):
                break      
# This program will search through both the trends and the individual values of trades between two countries to ensure to detect possible errors
# Its argument note allows an appendment to the label to allow further clarification as to its meaning
def findLikelyErrors():
                
    for tag in ["Export","Import"]:
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
                totals.append(getProductCountry(product,country, year,tag))

            three_year_averages = []
            three_year_averages.append((totals[0]+totals[1]+totals[2])/3)
            three_year_averages.append((totals[1]+totals[2]+totals[3])/3)
            three_year_averages.append((totals[2]+totals[3]+totals[4])/3)
            three_year_averages.append((totals[3]+totals[4]+totals[5])/3)

            for year in range(first_year+1,last_year):
            # This will not include 2014 as data there could be reasonable even though it seems erroneous
                three_year_average = three_year_averages.pop(0)
                
                        

                if (three_year_average > absolute_threshold):
                    value = getProductCountry(product,country, year,tag)
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
                            label = "%s-%s|%s~%s" % (product,country,year,tag)
                            index = year - first_year
                            print (label,local_shift)

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


def getProduct(product, year):
    try:
        value = product_values["%s-%s" % (year,product)]
        return value
    except:
        return 0;

#    This will run through the collected data and return the Total Product for a given year and country
def getProductCountry(product,country,year,tag):
    try:
        return country_product_values["%s-%s-%s~%s" % (year,country,product,tag)]
    except:
        return 0
      

def dataLookUp():
    type = raw_input("Product or Country")

    while(True):
        
        if (type == "Product"):
            args = raw_input("Insert product and year separated by comma, print 'quit' to exit").split(",")
            if args[0] == 'quit':
                break
            try:
                print (getProduct(args[0],args[1]))
            except:
                print (args, "Not found")
        else:
            args = raw_input("Insert product, country, year, is_export separated by comma, print 'quit' to exit").split(",")
            if args[0] == 'quit':
                break
            try:
                print (getProductCountry(args[0],args[1],args[2],args[3]))
            except:
                print (args, "Not found")
def plotter():

    while(True):
        inputs = raw_input("Enter First Year, Last Year, Product, Country, Tag seperated by comma if not applicable then write noting (e.g. ',,'). Add |to add another plot").split("|")
        args = []
        for call in inputs:
            call = call.split(",")
            
            print (call)
            if call[0] == 'quit':
                break
            args.append(call)
            print (call, "Not found")
        plot (args)
def plotWithPlotly():
    
    plotly.offline.plot({
        "data": [Scatter(x=[1, 2, 3, 4], y=[4, 3, 2, 1])],
        "layout": Layout(title="hello world")
    })
def plot(inp_country,inputs):
        
    fig, ax = plt.subplots()
    
    # We need to draw the canvas, otherwise the labels won't be positioned and 
    # won't have values yet.
    fig.canvas.draw()
    fig.set_size_inches(17, 10.5)
    first_year = 2009
    last_year = 2014
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
            if (country == None or country == "" and product == None or product == ""):
                return None
            elif(country == None or country == "" ):
                points.append(getProduct(product,year)* 1000)
#                 point_market.append(getProduct(product[:-2],year)* 1000)
#                 point_overmarket.append(getProduct(product[:-2],year)* 1000)
    #         DEPRECATED
    #         elif(product == None):
    #             points.append(Main.getCountry(country,year))
            else:
                points.append(getProductCountry(product,country,year,tag) * 1000)
#                 point_market.append(getProduct(product,year) * 1000)
#                 if (getProduct(product[:-2],year) > getProduct(product,year) * too_large_market):
#                 point_overmarket.append(getProduct(product,year) * 1000)
#                 else:
#                     point_overmarket.append(getProduct(product[:-2],year)* 1000)

#         print (points) 
        plt.plot(x,points)
#         plt.plot(x,point_market)
#         plt.plot(x,point_overmarket)

    plt.ylabel('Value of Trade ($)')
    plt.xlabel('Country:%s,Years from %s to %s' % (inp_country,first_year,last_year))
    fig.tight_layout()
   
#     png_output = StringIO.StringIO()
#     canvas.print_png(png_output)
#     response = make_response(png_output.getvalue())
#     response.headers['Content-Type'] = 'image/png'
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
        
    file_name = file_location + 'baci92_'
    single_country_run(file_name, False, False, "eunld")
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
    
