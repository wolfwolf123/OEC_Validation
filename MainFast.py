
'''

@author: Chris Briere

This program serves to find possible erroneous data point in the data provided by BACI

'''
import csv
import threading
import time
from itertools import * 
import matplotlib.pyplot as plt

start = {2009 : 3924821 , 2010 :  4014977 , 2011 : 4055105 ,2012  : 4077214, 2013:4168986,2014 : 4161735}
end   = {2009 : 4129893 , 2010 :  4232191 , 2011 : 4274539 ,2012  : 4298880, 2013:4397963,2014 : 4390427}

local_shift_threshold = 1.5
# This accounts the relative size of the values to the left and right of this value

zero_threshold = 3
# This is the number of zeros in a single product,country,year trend before a zero becomes an expected value

too_large_market = 10
# This is the point at which the over market is no longer considered due to its relative size (Only used in ploting)

volatility_factor = 3
# This is the number of trends found in a single market for it considered "Volatile" and to ignore the errors in that market 

absolute_threshold = 50000 # 50,000,000 50 Million
# This is the threshold for a market in a country to be meaningful in thousands

relative_threashold = 1
# This is the threshold for a change to be considered relevant

market_threshold = .15
# This is the percent of the market that a trade must make up in-order to be considered relevant

error_factor = 5
# This is the additional factor that turns a point from being interesting into being a possible error

first_year = 2009
# The first year that is recorded

last_year  = 2014
# The last year that is recorded

country_codes = {}
used_codes = {}
# This stores the country_id such that the hs92 value is the key and the OEC value is the value

product_codes = {}
# This stores the product_id such that the hs92 value is the key and the OEC value is the value

country_product_values = {}
# This stores the information regarding the trade between two countries to increase efficiency

saved_trends = {}
# This stores the information regarding trends in trade between two countries to increase efficiency

product_values = {}
# This stores the information regarding the trade of a product to increase efficiency

saved_product_trends = {}
# This stores the information regarding product trends in trade between two countries to increase efficiency

overmarket_factor = 2
# This is the tolerance amount that the scale (over market size/ product market size) is multiplied by to to account for other products in the market  
threads = []
finished_threads = 0
num_threads = 0 
errors = {}
file_location = r'/home/chris/Downloads/'
# data_file_location = r'/media/ramdisk/'
total_loop = 0
total_csv = 0

# These allow for multi-threading


# This computes the relevant data for a single given country
def single_country_run (file_name,datalookup,plot,country):
    # These Inputs are booleans, with the exception of file_name which denotes the file from which to retrieve the data, of what parts of the code
    # Should be used. If a value is denoted false the associated table WILL BE DELETED to be replaced with new values. Saved, if True, will save all
    # Other values that are denoted as completed, e.g. are True. Datalookup will allow you to search collected data
    start = time.time()

    print("Initilizing...")
    initilize()
    for i in range (100):
        single_thread(getFilesNotProcessed,first_year,last_year,arg=file_name)
    
        findLikelyErrors()
#     if (not final_errors):
#         filter_errors()
#     if (datalookup):
#         dataLookUp()
#     if (plot):
#         plotter()
    end = time.time()
    print(end - start)


    
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
def getFilesNotProcessed(year,arg,key,multi=False):
        
    file_location = arg + str(year) + '.csv'
    
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
                used_codes[product +"-" + country] = product +"-" + country

                product_values["%s-%s" % (year,product)] = product_values.get("%s-%s" % (year,product),0) + value
                country_product_values["%s-%s-%s~%s" % (year,country,product,"Import")] = value
           
            # Since the list is ordered once the last item with the correct key is called the program can exit
            
            elif (started):
                break     
            
def getFilesPreProcessed(year,arg,key, multi=False):
    file_location = file_name +  key + "_" + str(year) + '.csv'
    file_location = arg + str(year) + '.csv'
    
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
                used_codes[product +"-" + country] = product +"-" + country

                product_values["%s-%s" % (year,product)] = product_values.get("%s-%s" % (year,product),0) + value
                country_product_values["%s-%s-%s~%s" % (year,country,product,"Import")] = value
           
            # Since the list is ordered once the last item with the correct key is called the program can exit
            
            elif (started):
                break      
def findStart(filename,key):
    with open(filename) as csvfile:
        reader = csv.reader(csvfile)
 
        value = search(filename,0,6902706,key)  
         
        print (value)
         
        return value
         
def search (filename,low,high,key):
    with open(filename) as csvfile:
        reader = csv.reader(csvfile)    
        value = -1 
        while (value != key):
            mid = (low + high)/2
            itr = None 
            itr = islice(reader, mid, mid+1)
            value =  next(itr)
            print(mid)
            print (value)
         
            mid_val = value[2]
            if ( mid_val == key):
                print ("success" + mid )
                return mid
            elif (mid_val >  key):
                high = mid -1 
            else:
                low = mid + 1 
     
# This program will search through both the trends and the individual values of trades between two countries to ensure to detect possible errors
# Its argument note allows an appendment to the label to allow further clarification as to its meaning
def findLikelyErrors():

#     print("Calculating Errors")

                
    for tag in ["Export","Import"]:
    # Runs through both True, and False
        for key in used_codes:
#                 print(product)
            (product,country) = key.split("-")
            # First year value is used later to account for a any trends across multiple years that might explain some shift (i.e. if a country's
            # trade is growing 200% over the course of 5 years then a single year growth of 75% is within a reasonable range while if a country's 
            # trade is shrinking 200% over the course of 5 years then a single year growth of 50% is likely erroneous

            total = 0 

            for year in range (first_year, last_year+1):
                total += getProductCountry(product,country, year,tag)
              
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

                three_year_average = three_year_averages.pop(0)
           # This will not include 2014 as data there could be reasonable even though it seems erroneous
                
                
                # out_average represents the average of the given range removing the value that is being examined from the average
                # For example imagine a range of [1,1,1000,1,1] the average would be 200.8 where as the out_average would be 1
                
                
                # If there are no other data points in the range then set the out_average to be .01. This will result in EXTREMELY
                # high ranges that will make these errors stand out
                

                # The multiplier will take into account the predicted shift from the trend so that values that follow a general trend are not taken
                # into account in the same manner 
                # i.e. in the dataset [100,200,300,600,500] the average 340 would indicate that the second to last data point should be an error as it
                # is nearly double the average, and with an out_average of (340  - (600/5))  * 5/4 = 275. However, the trend value of 4 which leads to a 400% increase of a given period. In the fourth year this
                # would lead to and additional modifier of (4/5) * (-4/2 + 4) = 8/5. So where as the unadjusted difference is 600 - 275 = 325, the 
                # adjusted difference is 600 - 275 - 160 = 165 which nearly cuts the increase in half.    
                
                
                
                # If the value of the shift from the average, not including the value in question, minus the expected trend shift is greater than the absolute_threshold
                # times the error_factor and the shift is greater or equal to the relative_threshold times the error_factor
                # or the value is 0 and the average for the period is greater than the absolute_threshold times error_factor

                        

                if (three_year_average > absolute_threshold):
                    value = getProductCountry(product,country, year,tag)
                    local_shift = (value - three_year_average)/three_year_average
                    if (local_shift < 0):
                        local_shift *= 3 

                    if(abs(local_shift) > local_shift_threshold):

                        # This calculates the total size of the market so that the relative size of this value to the size of the market can be determined
                            
                        market_val = getProduct(product, year)
#                             if (product == "16851782" and country == "euita" and year == 2011):
#                                 print(year)
#                                 print(counter)
#                                 print(out_average)
#                                 print(value)
#                                 print(value - out_average - first_year_value * multiplier,absolute_threshold)
#                                 print(percent_change,relative_threashold*error_factor)
#                                 print(value/market_val)
#                                 print(market_val)
#                                 print(average)
#                                 print(average/market_val)
#                                 print(local_shift)
#                                 print(three_year_average)
                                
                        # If the value of the market is a sufficient part of the market (i.e. a very significant spike) or the average is a significant 
                        # portion of the market (i.e. a meaningful change with a major trade partner)
                        
                        if ( market_val == 0 or three_year_average/market_val > market_threshold):    
                            label = "%s-%s|%s~%s" % (product,country,year,tag)
                        
                         
                            errors[label]  = local_shift
def filter_errors():
    global errors
    added_errors = {}
    
    for error in errors:
        label = error[0].split("|")[0].split("-")[0] + "~" + error[0].split("|")[-1].split("~")[-1].split("$")[0]
        try:
            added_errors[label] += 1
            
            # This will remove any field that coexist with another field, this will remove "High Volatility" Markets that simply react weirdly
            # (i.e. the Tanker market is made up almost entirely of 
            if (added_errors[label] >= volatility_factor - 1):
                SQL_Handler.deleteLike("final_errors",database,[error[0].split("|")[0].split("-")[0],error[0].split("|")[-1].split("~")[-1].split("$")[0]])


        except:
            added_errors[label] = 0

            label = ",," + label.split("~")[0] + "," + error[0].split("|")[0].split("-")[1] + "," + label.split("~")[1]
#             SQL_Handler.deleteLike("final_errors",database,[error[0].split("|")[0].split("-")[0],error[0].split("|")[-1].split("~")[-1].split("$")[0]])

            SQL_Handler.insert("final_errors",label,int(error[0].split("|")[1].split("~")[0]),database)




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
        return last_year

#    This will run through the collected data and return the Total Product for a given year        
def getProductCode(product):
    try:
        return product_codes[str(int(product))]
    except:
        for i in product_codes:
            if (i == int(product)):
                return product_codes[i]

def getProduct(product, year):
    try:
        value = product_values["%s-%s" % (year,product)]
#         print (value,"%s-%s" % (year,product))
#             print ("Value",value)
#         print(product)
#         print(value)
        return value
    except:
#         print ("%s,%s" % (year,product))
#         for i in product_values:
#             print(i)
        return 0;

#    This will run through the collected data and return the Total Product for a given year and country
def getProductCountry(product,country,year,tag):
    try:
#         print (country_product_values["%s-%s-%s~%s" % (year,country,product,tag)],"%s-%s-%s~%s" % (year,country,product,tag))

        return country_product_values["%s-%s-%s~%s" % (year,country,product,tag)]

    except:
#         print ("%s,%s,%s~%s" % (year,country,product,tag))
#         for i in country_product_values:
#             print(i)
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
def plot(inputs):
        
    fig, ax = plt.subplots()
    
    # We need to draw the canvas, otherwise the labels won't be positioned and 
    # won't have values yet.
    fig.canvas.draw()
    
    for arg in inputs:
        points = []
        point_market = []
        point_overmarket = []
        (min,max,product,country, tag) = arg
        if (min != "" or max != ""): 
            first_year = int(min)
            last_year = int(max)
        else:
            first_year = 2009
            last_year = 2014

        x = range(first_year,last_year+1)
        
        ax.set_xticklabels(x)
    
        for year in range(first_year,last_year+1):
            if (country == None or country == "" and product == None or product == ""):
                return None
            elif(country == None or country == "" ):
                points.append(getProduct(product,year)* 1000)
                point_market.append(getProduct(product[:-2],year)* 1000)
                point_overmarket.append(getProduct(product[:-2],year)* 1000)
    #         DEPRECATED
    #         elif(product == None):
    #             points.append(Main.getCountry(country,year))
            else:
                points.append(getProductCountry(product,country,year,tag) * 1000)
                point_market.append(getProduct(product,year) * 1000)
#                 if (getProduct(product[:-2],year) > getProduct(product,year) * too_large_market):
                point_overmarket.append(getProduct(product,year) * 1000)
#                 else:
#                     point_overmarket.append(getProduct(product[:-2],year)* 1000)

        print (points) 
        plt.plot(x,points)
        plt.plot(x,point_market)
        plt.plot(x,point_overmarket)

    plt.ylabel('Value of Trade ($)')
    plt.xlabel('Country:%s,Product:%s,Years from %s to %s' % (country,product,first_year,last_year))
    plt.show()   
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
# def single_country_run (file_name,datalookup,plot,country,database,type):
    
