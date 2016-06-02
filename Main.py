
'''
Created on Oct 7, 2015

@author: Chris Briere

This program is used to get a predefined variable from the census API
Right now it is setup to find the percent of the first variable, white population, in the 
Second variable.
'''
import csv
import threading
from decimal import Decimal
import time
import SQL_Handler
import Trend_Handler

# This size determines the portion of the file that each thread is processing
# The smaller this number the faster the program should run but the larger the number of program stalling collisions 

size = 1000;
absolute_threshold = 5000
# This is the threshold for a market in a country to be meaningful

relative_threashold = .25
# This is the relative threshold for a trend to be considered meaningful
error_factor = 10
# rsrc = resource.RLIMIT_DATA

# resource.setrlimit(rsrc, (1073741824, 1610612736))
first_year = 2009

last_year  = 2014

country_codes = {}
product_codes = {}
country_product_values = {}
saved_trends = {}
threads = []
finished_threads = 0
num_threads = 0 


finished_threads = 0
    
    
# Initializes MYSQL Server 


        
# This will run the program
def run (file_name,Im_Ex,values,product_trends,trends,interesting_trends,errors,saved,datalookup):
    # Inputs = calculated_Im_Ex,calculated_values,calculated_product_trends,calculated_trends
    print("Initilizing...")
    initilize(Im_Ex,values,product_trends,trends,interesting_trends,errors)
    if (not Im_Ex):
        print("Getting Files...")
        multi_thread(getFiles,first_year,last_year,arg=file_name)
    if (not values):
        print("Populating Tables...")
        multi_thread(populate_values,first_year,last_year,num=1)
        print ("Finished Calculating")

    if (not product_trends):
        print("Getting Product Trends...")
        Trend_Handler.find_product_trends(product_codes)
    if (not trends):
        print ("Getting Export Trends...")
        Trend_Handler.find_trends(country_codes,product_codes,True)
        print ("Getting Import Trends...")
        Trend_Handler.find_trends(country_codes,product_codes,False)
    if (not interesting_trends):
        print ("Getting Interesting Trends...")
        interesting_trends = findInterestingTrends()
    if (not errors):
        print("Calculating Errors")
        findLikelyErrors()
    if (not saved):
        save_tables(Im_Ex,values,product_trends,trends,interesting_trends,errors)
    if (datalookup):
        dataLookUp()
def multi_thread(function,min_val,max_val,num=None, arg=None):
    global num_threads
    global finished_threads
    global threads
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

    while (not num_threads - finished_threads == 0):
        print ('Still open %s' % (num_threads - finished_threads))
        time.sleep(5)
        
def single_thread(function,min_val,max_val, arg="None"):
    finished_threads = 0 
    num_threads = 0 
    for val in range(min_val,max_val+1):
        function(val,arg)
             
def initilize(Im_Ex,values,product_trends,trends,interesting_trends,errors,database_name="OEC_DB"):
    global country_codes
    global product_codes
    global database
    
    database = database_name
                       
    make_tables(Im_Ex,values,product_trends,trends,interesting_trends,errors)

    country_codes = {}
    product_codes = {}
    country_file = r'/home/chris/Downloads/country_code_baci92.csv'
    product_file = r'/home/chris/Downloads/product_code_baci92.csv'

    with open(country_file) as csvfile:
        import_reader = csv.reader(csvfile, delimiter=';')
        for column in import_reader:
            row = column[0].split(",")
            value = row[0]
            if (row[0] == ''):
                value = row [2]
            country_codes[row[3]] = value

    with open(product_file) as csvfile:
        import_reader = csv.reader(csvfile, delimiter=';')
        for column in import_reader:
            row = column[0].split(",")
            product_codes[row[0]] = row[1]
    for year in range (first_year,last_year+1):
        country_product_values_year = SQL_Handler.readAll("country_product_values_%s" % (year),database)
        for value in country_product_values_year:
            country_product_values[value[0]] = value[-1]
        trend = SQL_Handler.readAll("trends",database)
        for value in trend:
            saved_trends[value[0]] = value[-1]
            
def make_tables(Im_Ex,values,product_trends,trends,interesting_trends,errors):
    if(not values):
        single_thread(SQL_Handler.make_table, first_year, last_year, ("product_values",database))
        single_thread(SQL_Handler.make_table, first_year, last_year, ("country_values",database))
        single_thread(SQL_Handler.make_table, first_year, last_year, ("country_product_values",database))
    if(not product_trends):
        SQL_Handler.make_table(None,("product_trends",database))
    if (not trends):
        SQL_Handler.make_table(None,("trends",database))
    if (not Im_Ex):
        single_thread(SQL_Handler.make_table, first_year, last_year, ("Im_Ex_Data",database))
    if (not interesting_trends):
        SQL_Handler.make_table(None,("interesting_trends",database))
    if (not errors):
        SQL_Handler.make_table(None,("errors",database))
def save_tables(Im_Ex,values,product_trends,trends,interesting_trends,errors):    
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
def populate_values(year, value):

    Im_Ex_Data = SQL_Handler.readAllCountry("Im_Ex_Data_%s" %(year),"NLD",database)

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


    
def findInterestingTrends ():

    interesting_trends = {}
    
    product_trends = SQL_Handler.readAll("product_trends",database)
    
    for trend in product_trends:
        product = trend[0].split("-")[0]
        trendline = trend[0].split("-")[1]

        if (trendline == "five_year_trend"):
            year = last_year - 5
        elif (trendline == "long_trend"):
            year = first_year
        elif (trendline == "three_year_trend"):
            year = last_year - 3
        elif (trendline == "one_year_trend"):
            year = last_year - 1
        else:
            year = last_year

        total = 0    
        for y in range (year,last_year+1):
            total += getProduct(product,year)
        product_val = total
        if (last_year-year > 0):
            product_val = total/(last_year-year)                
        
        trend_val   = trend[1]
        
        
        if (abs(product_val*trend_val) > absolute_threshold):
            label = "%s$%s" % (trend[0],product_val)

            SQL_Handler.insert("interesting_trends",label,trend[1],database)
    print ("Finished Products...")
    trends = SQL_Handler.readAll("trends",database)
            
    for trend in trends:
        product = trend[0].split("|")[0].split("-")[0]
        trendline = trend[0].split("|")[0].split("-")[1]
        country = trend[0].split("|")[1].split("~")[0]
        
        tag = trend[0].split("|")[1].split("~")[1]
        if (trendline == "five_year_trend"):
            year = last_year - 5
        elif (trendline == "long_trend"):
            year = first_year
        elif (trendline == "three_year_trend"):
            year = last_year - 3
        elif (trendline == "one_year_trend"):
            year = last_year - 1
        else:
            print ("Horrible Failure")
            year = last_year
        
        total = 0    
        for y in range (year,last_year+1):
            total += getProductCountry(product,country,y,tag)
        product_val = total/(last_year-year + 1) 
        trend_val   = trend[1]
        
        if (abs(product_val * trend_val) > absolute_threshold):
            if (isCountry(country)):
                label = "%s-%s|%s~%s$%s" % (product,trendline,realCountry(country),tag,product_val)
                SQL_Handler.insert("interesting_trends",label,trend[1],database)

    return interesting_trends
# This will return the relevent files
def findLikelyErrors():

        
    interesting_trends = SQL_Handler.readAll("interesting_trends",database)
    
    for trend in interesting_trends:
        product_val = float(trend[0].split("$")[-1])
        trend_val = trend[-1]
        if (abs(product_val * trend_val) > absolute_threshold * error_factor and trend_val > 1):
                SQL_Handler.insert("errors",trend[0],trend_val,database)
    print ("Finished Trends")
    for bool in [True,False]:
        for country in country_codes:
            for product in product_codes:
                total = 0
                real = realCountry(country)
                for year in range(first_year,last_year+1):               
                    total += getProductCountry(product,real, year,bool)
                average = abs(total/ (last_year-first_year))
                for year in range(first_year,last_year+1):
                    value = getProductCountry(product,real, year,bool)
                    trend_val = getTrend(product,real,bool) 
                    
                    try:
                        multiplier = (1 + (trend_val/(last_year-first_year)) * (first_year - last_year)/2 + (year - first_year))
                    except:
                        multiplier = 1
                    tag = getExport(bool)

                    if (abs(value - multiplier * average)  > absolute_threshold * error_factor and abs((value-average)/average) > 1):
                        label = "%s-%s|%s~%s" % (product,real,year,tag)
                        SQL_Handler.insert("errors",label,(value-average)/average,database)


def getFiles(year,file_name):
    global country_codes
    global product_codes
    print (year)
    file_location = file_name + str(year) + '.csv'
    with open(file_location) as csvfile:
        import_reader = csv.reader(csvfile, delimiter=';')
        for column in import_reader:
        
            row = column[0].split(",")
            importer = row[3]
            exporter = row[2]
            
            real_importer = realCountry(row[3])
            
            if (real_importer != False):
                importer = real_importer
                
            real_exporter = realCountry(row[2])
            if (real_exporter != False):
                exporter = real_exporter


            SQL_Handler.insert("Im_Ex_Data_%s"% (year),'Year:%s,Importer:%s,Exporter:%s,Product:%s' % (row[0],importer,exporter,row[1]),row[4],database)
    end_thread()
def getDatabase():
    return database
def getExport(is_export):
    tag = "Import"
    if (is_export):
        tag = "Export"
    return tag  
def getTrend(product,country,is_export):
    try:
        tag = getExport(is_export)          
        output = saved_trends["%s-%s|%s~%s" % (product,"long_trend",'USA',tag)]
    except:
        output = 0
    return output

def getProductTrend(label):
    try:
        return SQL_Handler.read("product_trends",label,database)[0][-1]
    except:
        return 0
#    This will run through the collected data and return the Total Product for a given year        
def getProduct(product, year):
    try:
        value = SQL_Handler.read("product_values_%s" % (year),"%s,%s" % (year,product),database)[0][-1]
#             print ("Value",value)
        return value
    except:
        return 0;
#         product_label = "Year:%s,Product:%s" % (year,product)
#         output = product_values[product_label]
#         return output

#    This will run through the collected data and return the Total Product for a given year and country
def getProductCountry(product,country,year,is_export):
    tag = "Import"
    if (is_export):
        tag = "Export"
    try:
        return country_product_values["%s,%s,%s~%s" % (year,country,product,tag)]
    except:
        return 0

#         country_label = "Year:%s,County:%s,Product:%s~%s" % (year,country,product,tag)
#         return country_product_values[country_label]

#    This will run through the collected data and return a dictionary of all products for a given year and country        
def getExportsCountry(country,year,is_export):
    tag = "Import"
    if (is_export):
        tag = "Export"
    
    try:
        return(SQL_Handler.read("country_product_values_%s" % (year),"%s,%s~%s" % (year,country,tag),database)[0][-1])
    except:
        return 0
     
    
def isProduct(product):
    for real_product in product_codes:
        if (real_product == product):
            return True
    return False

def isCountry(country):
    for real_country in country_codes:
        if (real_country == country):
            return True
    return False

def realCountry(country):
    try:
        return country_codes[str(int(country))]
    except:
        try:
            for real_country in country_codes:
                if (real_country == str(int(country))):
                    return country_codes[real_country]
        except:
            pass
    return False
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

#     run(file_name,True,True,False,False,False,True,True,False)
    # Inputs = calculated_Im_Ex,calculated_values,calculated_product_trends,calculated_trends
    
