
'''

@author: Chris Briere

This program serves to find possible erroneous data point in the data provided by BACI

'''
import csv
import threading
import time
import SQL_Handler
import Trend_Handler

absolute_threshold = 50000
# This is the threshold for a market in a country to be meaningful in thousands

relative_threashold = 1
# This is the threshold for a change to be considered relevent

market_threshold = .25
# This is the percent of the market that a trade must make up inorder to be considered relevent

error_factor = 10
# This is the additional factor that turns a point from being interesting into being a possible error

first_year = 2009
# The first year that is recorded

last_year  = 2014
# The last year that is recorded

country_codes = {}
# This stores the country_id such that the hs92 value is the key and the OEC value is the value

product_codes = {}
# This stores the product_id such that the hs92 value is the key and the OEC value is the value

country_product_values = {}
# This stores the information regarding the trade between two countries to increase efficency

saved_trends = {}
# This stores the information regarding trends in trade between two countries to increase efficency

overmarket_factor = 10

threads = []
finished_threads = 0
num_threads = 0 

# These allow for multi-threading

database = ""

# This denotes the database that is being connected to as a global variable

    
# This serves to initialize the Im_Ex_Data. It should only be called once for a given period of time. This will essentially transfer all of the data from
# csv files into mysql databases so that it can be processed more quickly. It is separated to ensure that it is not over-used or its data overwritten. 
def setup ():
    
    setup_initilize("OEC_DB")

    multi_thread(getFiles,first_year,last_year,arg=file_name)

# This computes the relevant data for a single given country
def single_country_run (file_name,values,product_trends,trends,interesting_trends,errors,final_errors,saved,datalookup,country,database):
    # These Inputs are booleans, with the exception of file_name which denotes the file from which to retrieve the data, of what parts of the code
    # Should be used. If a value is denoted false the associated table WILL BE DELETED to be replaced with new values. Saved, if True, will save all
    # Other values that are denoted as completed, e.g. are True. Datalookup will allow you to search collected data
    start = time.time()

    print("Initilizing...")
    initilize(values,product_trends,trends,interesting_trends,errors,final_errors,database)

    if (not values):
        single_thread(populate_values,first_year,last_year,arg=country)

        multi_thread(single_country_over_values,first_year,last_year)

        print ("Finished Calculating")
    if (not product_trends):
        Trend_Handler.find_product_trends(database,product_codes)
    if (not trends):
        Trend_Handler.find_trends(country_codes,product_codes,True)
        Trend_Handler.find_trends(country_codes,product_codes,False)
    if (not interesting_trends):
        findInterestingTrends()
    if (not errors):
        findLikelyErrors()
    if (not final_errors):
        filter_errors()
    if (not saved):
        save_tables(values,product_trends,trends,interesting_trends,errors)
    if (datalookup):
        dataLookUp()
    
    end = time.time()
    print(end - start)

# This method will increment through all countries and acquire their export data only and saving a compiled list of interesting_trends and errors
def all_country_run(file_name,database_name):
    # These Inputs are booleans, with the exception of file_name which denotes the file from which to retrieve the data, of what parts of the code
    # Should be used. If a value is denoted false the associated table WILL BE DELETED to be replaced with new values. Saved, if True, will save all
    # Other values that are denoted as completed, e.g. are True. 
    start = time.time()

    print("Initilizing...")
    initilize(False,False,False,False,False,False,database_name)
    for id,country in country_codes:
        print (country)
        single_thread(multiple_country_populate_values,first_year,last_year,arg=country)
        multi_thread(single_country_over_values,first_year,last_year)
        Trend_Handler.find_product_trends(database,product_codes)
        Trend_Handler.find_trends(country_codes,product_codes,True)
        findInterestingTrends(country)
        findLikelyErrors(country)
        reset(database_name)
    save_tables(False,False,False,True,True)


    
    end = time.time()
    print(end - start)
def multi_thread(function,min_val,max_val,num=None, arg=None, wait=60):
    global num_threads
    global finished_threads
    global threads
    global done 
    
    finished_threads = 0 
    num_threads = 0 
    if (num == None):
        for val in range(min_val,max_val+1):
            t = threading.Thread(target=function, args=(val,arg))
            threads.append(t)
            t.start()
            num_threads += 1
    else:

        delta = (int(((max_val-min_val)+1)/num)) - 1
        
        for x in range(num-1):
            t = threading.Thread(target=single_thread, args=(function,min_val + (x*(delta+1)),min_val + (x*(delta+1) + delta),arg))
            threads.append(t)
            t.start()
            num_threads += 1
        t = threading.Thread(target=single_thread, args=(function,min_val + ((num-1) * (delta + 1)),max_val,arg))
        threads.append(t)
        t.start()
        num_threads += 1

    while ( num_threads > finished_threads):
        print ('Still open %s' % (num_threads - finished_threads))
        time.sleep(wait)
    
def single_thread(function,min_val,max_val, arg="None"):
    finished_threads = 0 
    num_threads = 0 
    for val in range(min_val,max_val+1):
        function(val,arg)
def setup_initilize (database_name = "OEC_DB" ):

    single_thread(SQL_Handler.make_table, first_year, last_year, ("Im_Ex_Data",database))
    
    initilize(False,False,False,False,False,False,database_name)
    
def initilize(values,product_trends,trends,interesting_trends,errors,final_errors,database_name="OEC_DB"):
    global country_codes
    global product_codes
    global database
    
    database = database_name
                       
    make_tables(values,product_trends,trends,interesting_trends,errors,final_errors)

    country_codes = {}
    product_codes = {}
    country_file = r'/home/chris/Downloads/country_codes.csv'
    product_file = r'/home/chris/Downloads/product_codes.csv'

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
                    country_codes[int(c.strip('"'))] = value
                except:
                    pass

    with open(product_file) as csvfile:
        import_reader = csv.reader(csvfile, delimiter=';')
        for column in import_reader:
            row = column[0].split(",")
            product_codes[row[1]] = row[0]
    fill_country_values(database_name)
def reset(database_name="OEC_DB"):
    
    initilize(False,False,False,True,True,True,database_name)

def fill_country_values(database_name):
    global database 
    
    database = database_name
        
    for year in range (first_year,last_year+1):
        country_product_values_year = SQL_Handler.readAll("country_product_values_%s" % (year),database)
        for value in country_product_values_year:
            country_product_values[value[0]] = value[-1]
        trend = SQL_Handler.readAll("trends",database)
        for value in trend:
            saved_trends[value[0]] = value[-1]            
def make_tables(values,product_trends,trends,interesting_trends,errors,final_errors):    
    if(not values):
        single_thread(SQL_Handler.make_table, first_year, last_year, ("product_values",database))
        single_thread(SQL_Handler.make_table, first_year, last_year, ("country_values",database))
        single_thread(SQL_Handler.make_table, first_year, last_year, ("country_product_values",database))
    if(not product_trends):
        SQL_Handler.make_table(None,("product_trends",database))
    if (not trends):
        SQL_Handler.make_table(None,("trends",database))
    if (not interesting_trends):
        SQL_Handler.make_table(None,("interesting_trends",database))
    if (not errors):
        SQL_Handler.make_table(None,("errors",database))
    if (not final_errors):
        SQL_Handler.make_table(None,("final_errors",database))

        
def save_tables(values,product_trends,trends,interesting_trends,errors):    
    if(values):
        single_thread(save, first_year, last_year, "product_values")
        single_thread(save, first_year, last_year, "country_values")
        single_thread(save, first_year, last_year, "country_product_values")
    if(product_trends):
        save(None,"product_trends")
    if (trends):
        save(None,"trends")
#        if (Im_Ex):
#            single_thread(save, first_year, last_year, "Im_Ex_Data")
#       This file is too large for github
    if (interesting_trends):
        save(None,"interesting_trends")
    if (errors):
        save(None,"errors")


def end_thread():

    global finished_threads 
    
    # This will indicate the end of a thread for the purposes of multithreading
    finished_threads +=1        
def save(year,table_name):
    if (year == None):
        data = SQL_Handler.readAll(table_name,database)
        with open(table_name + ".csv", 'w') as mycsvfile:
            datawriter = csv.writer(mycsvfile)
            for row in data:
                datawriter.writerow(row)
    else:
        data = SQL_Handler.readAll("%s_%s" % (table_name,year),database)
        with open(table_name + ".csv", 'w') as mycsvfile:
            datawriter = csv.writer(mycsvfile)
            for row in data:
                datawriter.writerow(row)

def getFiles(year,arg):
    file_name = arg
    print (year)
    file_location = file_name + str(year) + '.csv'
    with open(file_location) as csvfile:
        import_reader = csv.reader(csvfile, delimiter=';')
        for column in import_reader:
            try:
                row = column[0].split(",")
                importer = country_codes[int(row[3])]
                exporter = country_codes[int(row[2])]
                product = product_codes[str(int(row[1]))]
#                 if (importer == country or exporter == country):
#                     populate_value(year,importer,exporter,product_codes[str(int(row[1]))],row[4])
                SQL_Handler.insert("Im_Ex_Data_%s" % (year),'Year:%s,Importer:%s,Exporter:%s,Product:%s' % (row[0],importer,exporter,product),row[4],database)
            except:    
                print ('Year:%s,Importer:%s,Exporter:%s,Product:%s' % (row[0],row[3],row[2],row[1]))
    end_thread()
    
def populate_values(year, country = None):
    
    print("Populating Tables...")
    
    if (country == None):
        Im_Ex_Data = SQL_Handler.readAll("Im_Ex_Data_%s" %(year),database)
    else:
        Im_Ex_Data = SQL_Handler.readAllCountryExport("Im_Ex_Data_%s" %(year),country,database)

    print("Parsing Data...")
    total = len(Im_Ex_Data)
    increment = 0
    for row in Im_Ex_Data:
        increment += 1
        if ((total - increment) % 50000 == 0):
            print ("Progress = %f,%f,%f" % (increment,total,100*(float(increment)/float(total))))
        split_label = row[0].split(",")
        year = split_label[0].split(":")[1]
        Im = split_label[1].split(":")[1]
        Ex = split_label[2].split(":")[1]
        
        
        product = split_label[3].split(":")[1]
        value = row[1]
                
        SQL_Handler.insert("product_values_%s" % (year),"%s,%s" % (year,product),value,database)
         
        SQL_Handler.insert("country_values_%s" %(year),"%s,%s~%s" %(year,Im,"Import"),value,database)
         
        SQL_Handler.insert("country_values_%s" %(year),"%s,%s~%s" % (year,Ex,"Export"),value,database)
         
        SQL_Handler.insert("country_product_values_%s" %(year),"%s,%s,%s~%s" % (year,Im,product,"Import"),value,database)
 
        SQL_Handler.insert("country_product_values_%s" %(year),"%s,%s,%s~%s" % (year,Ex,product,"Export"),value,database)
        
         
    end_thread()

    
def multiple_country_populate_values(year, country):
        
    Im_Ex_Data = SQL_Handler.readAllCountryExport("Im_Ex_Data_%s" %(year),country,database)

    for row in Im_Ex_Data:
        split_label   =  row[0].split(",")
        year   = split_label[0].split(":")[1]
        Ex     = split_label[2].split(":")[1]
        product= split_label[3].split(":")[1]
        value = row[1]
                
        SQL_Handler.insert("product_values_%s" % (year),"%s,%s" % (year,product),value,database)
                  
        SQL_Handler.insert("country_values_%s" %(year),"%s,%s~%s" % (year,Ex,"Export"),value,database)
          
        SQL_Handler.insert("country_product_values_%s" %(year),"%s,%s,%s~%s" % (year,Ex,product,"Export"),value,database)
         
    end_thread()
def single_country_over_values(year, arg = None):
    print("Calculating Overhead")
    for hs,product in product_codes.items():
        if (len(product) > 6):
            total = 0
            products = SQL_Handler.getOverSize(database,product,year)
            for p in products:
                total += p[1]
            SQL_Handler.insert("product_values_%s" % (year),"%s,%s" % (year,product[:-2]),total,database)
    end_thread()

# This method combs through the data to see if it can find interesting long-term trends, i.e. The oil market contracting severely in size.
# Some of these trends may be so severe that they indicate a possible error in the data    
# Its argument note allows an appendment to the label to allow further clarification as to its meaning
def findInterestingTrends (note = None):
 
    print ("Getting Interesting Trends...")
        
    product_trends = SQL_Handler.readAll("product_trends",database)
    # First it acquires the trends from the product trends 
    
    for trend in product_trends:
        
        product = trend[0].split("-")[0]
        trendline = trend[0].split("-")[1]

        year = getTrendYear(trendline)

        start = getProduct(product, year)
        
        end = getProduct(product, last_year)

        net = end -start
        
        trend_val = trend[1]
        
        # If the value of the trend (i.e. -.5 or a 50% decrease) times the size of the market (i.e. $1 billion dollars) is greater than the 
        # absolute threshold then insert the value into interesting trends
        
        if (abs(net) > absolute_threshold):
            if (note == None):
                label = "%s$%s" % (trend[0],net)
            else:
                label = "%s$%s@%s" % (trend[0],net,note)

            SQL_Handler.insert("interesting_trends",label,trend[1],database)
    
    print ("Finished Products...")
    trends = SQL_Handler.readAll("trends",database)
            
    for trend in trends:
                
        product = trend[0].split("|")[0].split("-")[0]
        trendline = trend[0].split("|")[0].split("-")[1]
        country = trend[0].split("|")[1].split("~")[0]
        
        tag = trend[0].split("|")[1].split("~")[1]
     
        getTrendYear(trendline)
                   
        start = getProductCountry(product,country,year,tag)
        
        end = getProductCountry(product,country,last_year,tag)

        net = end -start
                
        trend_val   = trend[1]

        # If the value of the trend (i.e. -.5 or a 50% decrease) times the size of the market (i.e. $1 billion dollars) is greater than the 
        # absolute threshold then insert the value into interesting trends
        
        if (abs(net * trend_val) > absolute_threshold):
            
            if (note == None):
                label = "%s-%s|%s~%s$%s" % (product,trendline,country,tag,net)
            else:
                label = "%s-%s|%s~%s$%s@%s" % (product,trendline,country,tag,net,note)

            SQL_Handler.insert("interesting_trends",label,trend[1],database)

# This program will search through both the trends and the individual values of trades between two countries to ensure to detect possible errors
# Its argument note allows an appendment to the label to allow further clarification as to its meaning
def findLikelyErrors(note=None):
    market_val_total = 0
    diff = last_year-first_year

    print("Calculating Errors")
        
    interesting_trends = SQL_Handler.readAll("interesting_trends",database)
    # Acquires the interesting_trends
     
    for trend in interesting_trends:
        product = trend[0].split("-")[0]
        net = float(trend[0].split("$")[-1])
        trendline = trend[0].split("-")[1].split("|")[0].split("$")[0]

        # Retrieves the product value that is appended to the trend
        trend_val = trend[-1]

        year = getTrendYear(trendline)
        
        
        # If the value of the trend (i.e. -.5 or a 50% decrease) times the size of the market (i.e. $1 billion dollars) is greater than the 
        # absolute threshold times the error threshold and the shift is at the relative threshold shift over the given period then insert the value into errors        
        
        if (abs(net) > absolute_threshold * error_factor * (last_year - year) and abs(trend_val) >= relative_threashold):
            
            # Computes the total market size for a given product
            


            # Calculate the total value of the market over the period of time then find its  average over that period of time 
            for y in range(year,last_year+1):               
                market_val_total += getProduct(product, y)
                        
            market_val = market_val_total/ (last_year - year + 1)
            
            # Ensures that this trend is a significant change in the market (i.e. a country with a small percentage of trade might grow 200% over the
            # The course of 5 years but that is likely not an erroneous trend 
            if (net/market_val > market_threshold):
                label = trend[0]
                if (note != None and not trend[0].contains('@')):
                    label = trend[0].append('@%s' % (note))
              
                product_total = 0
                for y in range(first_year,last_year+1):               
                    product_total += getProduct(product[:-2], y)
                
                if (product_total != 0):
                                
                    product_val     = getProduct(product[:-2], year)
                    product_average = (product_total-product_val)/ (diff)
                    product_percent_change = (product_val - product_average) / product_average
    
                    if (abs(product_percent_change) * overmarket_factor > abs(trend_val)): 
                        SQL_Handler.insert("errors",trend[0],trend_val,database)
                    
                
    print ("Finished Trends")
    for bool in [True,False]:
    # Runs through both True, and False
        tag = getExport(bool)
        labels = []
        for id,country in country_codes.items():
            for hs,product in product_codes.items():
                
                market_val_total = 0

                total = 0
                
                # First year value is used later to account for a any trends across multiple years that might explain some shift (i.e. if a country's
                # trade is growing 200% over the course of 5 years then a single year growth of 75% is within a reasonable range while if a country's 
                # trade is shrinking 200% over the course of 5 years then a single year growth of 50% is likely erroneous
                first_year_value = getProductCountry(product,country, first_year,tag)
                
                for year in range(first_year+1,last_year+1):               
                    total += getProductCountry(product,country, year,tag)
                total += first_year_value
                # If the total size of the market is zero then the rest of the calculations can be skipped
                if (total == 0):
                    continue
                average = total/(diff+1)
                for year in range(first_year,last_year):
                    
                # This will not include 2014 as data there could be reasonable even though it seems erroneous
                    value = getProductCountry(product,country, year,tag)
                    trend_val = getLongTrend(product,country,tag) 
                    
                    # out_average represents the average of the given range removing the value that is being examined from the average
                    # For example imagine a range of [1,1,1000,1,1] the average would be 200.8 where as the out_average would be 1
                    
                    out_average = abs((total - value)/(diff))
                    
                    # If there are no other data points in the range then set the out_average to be .01. This will result in EXTREMELY
                    # high ranges that will make these errors stand out
                    
                    if (out_average == 0):
                        out_average = .01
                    # The multiplier will take into account the predicted shift from the trend so that values that follow a general trend are not taken
                    # into account in the same manner 
                    # i.e. in the dataset [100,200,300,600,500] the average 340 would indicate that the second to last data point should be an error as it
                    # is nearly double the average, and with an out_average of (340  - (600/5))  * 5/4 = 275. However, the trend value of 4 which leads to a 400% increase of a given period. In the fourth year this
                    # would lead to and additional modifier of (4/5) * (-4/2 + 4) = 8/5. So where as the unadjusted difference is 600 - 275 = 325, the 
                    # adjusted difference is 600 - 275 - 160 = 165 which nearly cuts the increase in half.    
                    
                    multiplier = (trend_val/(diff+1)) * (-diff/2 + (year - first_year))
                    
                    percent_change = (value-out_average)/out_average
                    
                    # If the value of the shift from the average, not including the value in question, minus the expected trend shift is greater than the absolute_threshold
                    # times the error_factor and the shift is greater or equal to the relative_threshold times the error_factor
                    # or the value is 0 and the average for the period is greater than the absolute_threshold times error_factor
                    
                    
                    
                    if (abs(value - out_average  - first_year_value * multiplier)  > absolute_threshold and abs(percent_change) >= relative_threashold * error_factor
                        or value == 0 and average > absolute_threshold * error_factor):
                        
                        # This calculates the total size of the market so that the relative size of this value to the size of the market can be determined
                            
                        market_val = getProduct(product, year)
                        
                        
                        # If the value of the market is a sufficient part of the market (i.e. a very significant spike) or the average is a significant 
                        # portion of the market (i.e. a meaningful change with a major trade partner)
                        
                        if (market_val != 0 and (value/market_val > market_threshold or average/market_val > market_threshold)):    
                            label = "%s-%s|%s~%s" % (product,country,year,tag)
                            if(note != None):
                                label = "%s-%s|%s~%s@%s" % (product,country,year,tag,note)
                            product_total = 0
                            for y in range(first_year,last_year+1):               
                                product_total += getProduct(product[:-2], y)
                                
                            product_val     = getProduct(product[:-2], year)
                            product_average = (product_total-product_val)/ (diff)
                            product_percent_change = (product_val - product_average) / product_average

                            if (product == "5271121" and country == "eudeu"):
                                print(year)
                                print(out_average)
                                print(value)
                                print(value - out_average - first_year_value * multiplier,absolute_threshold)
                                print(percent_change,relative_threashold*error_factor)
                                print(abs(product_percent_change) * overmarket_factor,percent_change)

                            if (abs(product_percent_change) * overmarket_factor > abs(percent_change)): 
                                output = (value-out_average)/out_average
                                labels.append(label)
                                for i in labels:
                                    if (i == label): 
                                        continue
                                SQL_Handler.insert("errors",label,output,database)
def filter_errors():
    errors = SQL_Handler.readAll("errors", database)
    
    added_errors = {}
    
    for error in errors:
        label = error[0].split("|")[0].split("-")[0] + "~" + error[0].split("|")[1].split("~")[1].split("$")[0]
        try:
            added_errors[label]
        except:
            SQL_Handler.insert("final_errors",label,0,database)
            added_errors[label] = error[1]

def getDatabase():
    return database

def getExport(is_export):
    tag = "Import"
    if (is_export):
        tag = "Export"
    return tag  
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
def getLongTrend(product,country,tag):
    try:
        output = saved_trends["%s-%s|%s~%s" % (product,"long_trend",country,tag)]
    except:
        output = 0
    return output

def getProductTrend(label):
    try:
        return SQL_Handler.read("product_trends",label,database)[0][-1]
    except:
        return 0

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
        value = SQL_Handler.read("product_values_%s" % (year),"%s,%s" % (year,product),database)[0][-1]
#             print ("Value",value)
        return value
    except:
        return 0;

#    This will run through the collected data and return the Total Product for a given year and country
def getProductCountry(product,country,year,tag):
    try:
        return country_product_values["%s,%s,%s~%s" % (year,country,product,tag)]
    except:
        return 0
      

def dataLookUp():
    type = raw_input("Product or Country")

    while(True):
        
        if (type == "Product"):
            args = raw_input("Insert product and year seperated by comma, print 'quit' to exit").split(",")
            if args[0] == 'quit':
                break
            try:
                print (getProduct(args[0],args[1]))
            except:
                print (args, "Not found")
        else:
            args = raw_input("Insert product, country, year, is_export seperated by comma, print 'quit' to exit").split(",")
            if args[0] == 'quit':
                break
            try:
                print (getProductCountry(args[0],args[1],args[2],args[3]))
            except:
                print (args, "Not found")
        
if __name__ == '__main__':
        
    file_name = r'/home/chris/Downloads/baci92_'
    single_country_run(file_name, True, True, True, True, False,False, False, False,"eunld","OEC_DB")
    # Inputs = calculated_values,calculated_product_trends,calculated_trends,interesting trends, errors,saved,datalookup
    
