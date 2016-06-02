
'''
Created on Oct 7, 2015

@author: Chris Briere

This program is used to get a predefined variable from the census API
Right now it is setup to find the percent of the first variable, white population, in the 
Second variable.
'''
import MySQLdb

import csv
import threading
from decimal import Decimal
import time
import unittest

# This size determines the portion of the file that each thread is processing
# The smaller this number the faster the program should run but the larger the number of program stalling collisions 

size = 1000;
absolute_threshold = 5000
# This is the threshold for a market in a country to be meaningful

relative_threashold = .25
# This is the relative threshold for a trend to be considered meaningful
error_factor = 10
# rsrc = resource.RLIMIT_DATA

database = ""

# resource.setrlimit(rsrc, (1073741824, 1610612736))

first_year = 2009

last_year  = 2014

finished_threads = 0
if __name__ == '__main__':

    country_codes = {}
    product_codes = {}
    country_product_values = {}
    saved_trends = {}
    threads = []
    finished_threads = 0
    num_threads = 0 

    # Initializes MYSQL Server 
    

            
   # This will run the program
    def run (file_name,Im_Ex,values,product_trends,trends,interesting_trends,errors,saved,datalookup):
        make_tables(Im_Ex,values,product_trends,trends,interesting_trends,errors)
        # Inputs = calculated_Im_Ex,calculated_values,calculated_product_trends,calculated_trends
        print("Initilizing...")
        initilize()
        if (not Im_Ex):
            print("Getting Files...")
            multi_thread(getFiles,first_year,last_year,arg=file_name)
        if (not values):
            print("Populating Tables...")
            multi_thread(populate_values,first_year,last_year,num=1)
            print ("Finished Calculating")

        if (not product_trends):
            print("Getting Product Trends...")
            find_product_trends()
        if (not trends):
            print ("Getting Export Trends...")
            find_trends(True)
            print ("Getting Import Trends...")
            find_trends(False)
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
            time.sleep(100)
    def single_thread(function,min_val,max_val, arg="None"):
        finished_threads = 0 
        num_threads = 0 
        for val in range(min_val,max_val+1):
            function(val,arg)
 
    def initilize():
        global country_codes
        global product_codes
        global database
        
        database = "OEC_DB"
        
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
            country_product_values_year = readAll("country_product_values_%s" % (year))
            for value in country_product_values_year:
                country_product_values[value[0]] = value[-1]
            trend = readAll("trends")
            for value in trend:
                saved_trends[value[0]] = value[-1]
    def make_tables(Im_Ex,values,product_trends,trends,interesting_trends,errors):    
        if(not values):
            single_thread(make_table, first_year, last_year, "product_values")
            single_thread(make_table, first_year, last_year, "country_values")
            single_thread(make_table, first_year, last_year, "country_product_values")
        if(not product_trends):
            make_table(None,"product_trends")
        if (not trends):
            make_table(None,"trends")
        if (not Im_Ex):
            single_thread(make_table, first_year, last_year, "Im_Ex_Data")
        if (not interesting_trends):
            make_table(None,"interesting_trends")
        if (not errors):
            make_table(None,"errors")
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

    def make_table(year,table_name):
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db=database)        
        mysql_cur = db.cursor()
        if (year == None):    
            create_table = "CREATE TABLE %s (LABEL VARCHAR(255) PRIMARY KEY, VALUE FLOAT)"
            delete_table = "DROP TABLE %s"
            try:
                mysql_cur.execute(create_table % table_name)
                db.commit()
            except:
                try:
                    mysql_cur.execute(delete_table % (table_name))
                    mysql_cur.execute(create_table % (table_name))
                except:
                    print("Failed %s_%s Creation" % (table_name))
                    db.rollback()
        else:
            create_table = "CREATE TABLE %s_%s (LABEL VARCHAR(255) PRIMARY KEY, VALUE FLOAT)"
            delete_table = "DROP TABLE %s_%s"
            try:
                mysql_cur.execute(create_table % (table_name,year))
                db.commit()
            except:
                try:
                    mysql_cur.execute(delete_table % (table_name,year))
                    mysql_cur.execute(create_table % (table_name,year))
                    db.commit()
                except:
                    print("Failed %s_%s Creation" % (table_name,year))
                    db.rollback()
        db.close()
    def insert(table_name, label, value):
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db=database)
        mysql_cur = db.cursor()
        try:
            sql ="""INSERT INTO %s VALUES ('%s',%f)
                  ON DUPLICATE KEY UPDATE VALUE=VALUE+%f;""" % (table_name,label,float(value),float(value))
        except:
            print (table_name,label,value)
            return False
        try:
        # Execute the SQL command
            mysql_cur.execute(sql)
#             print("UPDATE")
            # Commit your changes in the database
            db.commit()
        except:
            # Rollback in case there is any error
            db.rollback()
        db.close()
    def read(table_name,label):
#         print ("SELECT * FROM %s WHERE LABEL = '%s'" % (table_name,label))
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db=database)
        mysql_cur = db.cursor()
        mysql_cur.execute("SELECT * FROM %s WHERE LABEL = '%s'" % (table_name,label))
        
        output = mysql_cur.fetchall()
        db.close()


        return output
    def readAll (table_name):
        
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db=database)
        mysql_cur = db.cursor()
        mysql_cur.execute("SELECT * FROM %s" % (table_name))
        
        output = mysql_cur.fetchall()
        
        db.close()
        
        return output
    def readAllCountry (table_name,country):
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db=database)
        
        mysql_cur = db.cursor()
        search = []
        search.append("'%")
        search.append(country)
        search.append("%'")
        
        value = "".join(search)
        print ("SELECT * FROM %s WHERE Label LIKE %s" % (table_name,value))
        mysql_cur.execute( "SELECT * FROM %s WHERE Label LIKE %s" % (table_name,value))
               
        output = mysql_cur.fetchall()
        
        db.close()
        
        return output
    def getTrend(product,country,is_export):
        try:
            tag = getExport(is_export)
            output = saved_trends["%s-%s|%s~%s" % (product,"long_trend",'USA',tag)]
        except:
            output = 0
        return output

    def end_thread():
    
        global finished_threads 
        
        # This will indicate the end of a thread for the purposes of multithreading
        finished_threads +=1        
    def save(year,table_name):
        if (year == None):
            data = readAll(table_name)
            with open(table_name + ".csv", 'w') as mycsvfile:
                datawriter = csv.writer(mycsvfile)
                for row in data:
                    datawriter.writerow(row)
        else:
            data = readAll("%s_%s" % (table_name,year))
            with open(table_name + ".csv", 'w') as mycsvfile:
                datawriter = csv.writer(mycsvfile)
                for row in data:
                    datawriter.writerow(row)
    def populate_values(year, value):
  
        Im_Ex_Data = readAllCountry("Im_Ex_Data_%s" %(year),"NLD")

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
            
            insert("product_values_%s" % (year),"%s,%s" % (year,product),value)
             
            insert("country_values_%s" %(year),"%s,%s~%s" %(year,Im,"Import"),value)
             
            insert("country_values_%s" %(year),"%s,%s~%s" % (year,Ex,"Export"),value)
             
            insert("country_product_values_%s" %(year),"%s,%s,%s~%s" % (year,Im,product,"Import"),value)
     
            insert("country_product_values_%s" %(year),"%s,%s,%s~%s" % (year,Ex,product,"Export"),value)
             
        end_thread()

    def find_product_trends():
        for product in product_codes:
            # Find all the Long, Medium, and Short term Trends in products
        
            first_year_val = getProduct(product, first_year)
            last_year_val = getProduct(product, last_year)
            one_year_val  = getProduct(product, last_year-1)
#             print (one_year_val)
            
            total = 0
            for y in range(last_year,first_year-1,-1):
                total += getProduct(product, y)
                if (y == last_year - 5):
                    five_year_average = abs(total/6)
                if (y == last_year - 3):
                    three_year_average = abs(total/4)
                if (y == last_year - 1):
                    one_year_average = abs(total/2)
            long_average = abs(total/(last_year + 1 - first_year))
            
            if (one_year_val == 0):
                one_year_trend = 0
            else:
                one_year_trend = (last_year_val - one_year_val )/ one_year_average
            one_year_label = "%s-%s" % (product,"one_year_trend")
            
            insert("product_trends",one_year_label,one_year_trend)
             
            if (first_year_val == 0):
                long_trend = 0
            else:
                long_trend = (last_year_val - first_year_val )/ long_average
            long_trend_label = "%s-%s" % (product,"long_trend")

            insert("product_trends",long_trend_label,long_trend)
              
            five_year_val = getProduct(product, last_year-5)
            if (five_year_val == 0):
                five_year_trend = 0
            else:
                five_year_trend = (last_year_val - five_year_val )/ five_year_average
              
            five_year_trend_label = "%s-%s" % (product,"five_year_trend")

            insert("product_trends",five_year_trend_label,five_year_trend)
                         
            three_year_val = getProduct(product, last_year-3)
            if (three_year_val == 0):
                three_year_trend = 0
            else:
                three_year_trend = (last_year_val - three_year_val )/ three_year_average
                          
            three_year_trend_label = "%s-%s" % (product,"three_year_trend")

            insert("product_trends",three_year_trend_label,three_year_trend)
            
#   
    def getExport(is_export):
        tag = "Import"
        if (is_export):
            tag = "Export"
        return tag     
    def find_trends(is_export):
        # Find the Total Product Trends
        
        initilize()
        
        trends = {}
        
        tag = getExport(is_export)
        
        for country_code in country_codes:
            for product in product_codes:
                  
                country = realCountry(country_code)
                                          
                first_year_val = getProductCountry(product,country, first_year,is_export)
                
                last_year_val  = getProductCountry(product,country, last_year,is_export)
                total = 0
                for y in range(last_year,first_year-1,-1):
                    total += getProductCountry(product,country, y,is_export)
                    if (y == last_year - 5):
                        five_year_average = abs(total/6)
                    if (y == last_year - 3):
                        three_year_average = abs(total/4)
                    if (y == last_year - 1):
                        one_year_average = abs(total/2)
                long_average = abs(total/(last_year + 1 - first_year))
                
                if (first_year_val == 0 and last_year_val == 0):
                    continue
                elif (first_year_val == 0):
                    long_trend = 0
                else:
                    long_trend = ((last_year_val - first_year_val )/ long_average)
                
                if (abs(long_trend) > 10):
                    print (product)
                    print (country)
                    print (first_year_val)
                    print (last_year_val)
                    print (long_average)
                
                # This will normalize the trends for individual countries based on shifting product trends
                # For example as coal consumption increases gloabally 5% a country increasing coal exports 3% 
                # is actually 2% below what one would expect so the normalized trend would be -2%
                
#                     long_trend = long_trend_raw - getProductTrend("%s-%s" % (product,"long_trend"))

                long_trend_label = "%s-%s|%s~%s" % (product,"long_trend",country,tag)
            
                insert("trends",long_trend_label,long_trend)
                
                five_year_val = getProductCountry(product,country, last_year-5,is_export)
                
                if (five_year_val == 0):
                    five_year_trend = 0
                else:
                    five_year_trend = (last_year_val - five_year_val )/ five_year_average
                    
#                     five_year_trend = five_year_trend_raw - getProductTrend("%s-%s" % (product,"five_year_trend"))
        
                five_year_trend_label = "%s-%s|%s~%s" % (product,"five_year_trend",country,tag)
            
                insert("trends",five_year_trend_label,five_year_trend)
                    
                three_year_val = getProductCountry(product,country, last_year-3,is_export)
                
                if (three_year_val == 0):
                    three_year_trend = 0
                else:
                    three_year_trend = ((last_year_val - three_year_val )/ three_year_average) 
                    
#                     three_year_trend = three_year_trend_raw - getProductTrend("%s-%s" % (product,"three_year_trend"))
                
                three_year_trend_label  = "%s-%s|%s~%s" % (product,"three_year_trend",country,tag)
            
                insert("trends",three_year_trend_label,three_year_trend)
                                
                one_year_val  = getProductCountry(product,country, last_year-1,is_export)
                
                if (one_year_val == 0):
                    one_year_trend = 0
                else:
                    one_year_trend = (last_year_val - one_year_val )/ one_year_average
                    
#                     one_year_trend = one_year_trend_raw - getProductTrend("%s-%s" % (product,"one_year_trend"))
                
                one_year_trend_label  = "%s-%s|%s~%s" % (product,"one_year_trend",country,tag)
            
                insert("trends",one_year_trend_label,one_year_trend)
                        
        return trends
        
    def findInterestingTrends ():
    
        interesting_trends = {}
        
        product_trends = readAll("product_trends")
        
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

                insert("interesting_trends",label,trend[1])
        print ("Finished Products...")
        trends = readAll("trends")
                
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
                    insert("interesting_trends",label,trend[1])

        return interesting_trends
    # This will return the relevent files
    def findLikelyErrors():

            
        interesting_trends = readAll("interesting_trends")
        
        for trend in interesting_trends:
            product_val = float(trend[0].split("$")[-1])
            trend_val = trend[-1]
            if (abs(product_val * trend_val) > absolute_threshold * error_factor and trend_val > 1):
                    insert("errors",trend[0],trend_val)
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
                            insert("errors",label,(value-average)/average)

 
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
   

                insert("Im_Ex_Data_%s"% (year),'Year:%s,Importer:%s,Exporter:%s,Product:%s' % (row[0],importer,exporter,row[1]),row[4])
        end_thread()
    def getProductTrend(label):
        try:
            return read("product_trends",label)[0][-1]
        except:
            return 0
    #    This will run through the collected data and return the Total Product for a given year        
    def getProduct(product, year):
        try:
            value = read("product_values_%s" % (year),"%s,%s" % (year,product))[0][-1]
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
            return(read("country_product_values_%s" % (year),"%s,%s~%s" % (year,country,tag))[0][-1])
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
            
        
    file_name = r'/home/chris/Downloads/baci92_'

#     run(file_name,True,True,True,True,True,True,True,False)
    # Inputs = calculated_Im_Ex,calculated_values,calculated_product_trends,calculated_trends
    
    
    class Tester(unittest.TestCase):
        def setUp(self):
            global database
            
            datebase = "Test_DB"
            
            print ("Beginnig Test")
            
        def test_tester(self):
            self.assertEqual(1, 1)
            
        def test_tester(self):
            self.assertEqual(1, 1)
        
        def tearDown(self):
            print("End Test")
                
    unittest.main()
