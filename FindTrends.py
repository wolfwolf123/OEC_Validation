
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
import resource

# This size determines the portion of the file that each thread is processing
# The smaller this number the faster the program should run but the larger the number of program stalling collisions 

size = 1000;
absolute_threshold = 5000
# This is the threshold for a market in a country to be meaningful

relative_threashold = .25
# This is the relative threshold for a trend to be considered meaningful
extreme_relative_threashold = 3
# rsrc = resource.RLIMIT_DATA


# resource.setrlimit(rsrc, (1073741824, 1610612736))

first_year = 2009

last_year  = 2014

finished_threads = 0
if __name__ == '__main__':

    country_codes = {}
    product_codes = {}
    country_product_values = {}
    threads = []
    finished_threads = 0
    num_threads = 0 

    # Initializes MYSQL Server 
    

            
   # This will run the program
    def run (file_name,Im_Ex,values,product_trends,trends,interesting_trends,errors,saved):
        make_tables(Im_Ex,values,product_trends,trends,interesting_trends,errors)
        # Inputs = calculated_Im_Ex,calculated_values,calculated_product_trends,calculated_trends
        print("Initilizing...")
        initilize()
        print("Getting Files...")
        if (not Im_Ex):
            multi_thread(getFiles,first_year,last_year,arg=file_name)
        print("Populating Tables...")
        if (not values):
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
        if (Im_Ex):
            single_thread(save, first_year, last_year, "Im_Ex_Data")
        if (interesting_trends):
            save(None,"interesting_trends")
        if (errors):
            save(None,"errors")

    def make_table(year,table_name):
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db="OEC_DB")        
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
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db="OEC_DB")
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
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db="OEC_DB")
        mysql_cur = db.cursor()
        mysql_cur.execute("SELECT * FROM %s WHERE LABEL = '%s'" % (table_name,label))
        
        output = mysql_cur.fetchall()
        db.close()


        return output
    def readAll (table_name):
        
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db="OEC_DB")
        mysql_cur = db.cursor()
        mysql_cur.execute("SELECT * FROM %s" % (table_name))
        
        output = mysql_cur.fetchall()
        
        db.close()
        
        return output
    def readAllCountry (table_name,country):
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db="OEC_DB")
        
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
            split_label = row[0]
            split_label = split_label.split(",")
            year = split_label[0].split(":")[1]
            Im = split_label[1].split(":")[1]
            Ex = split_label[2].split(":")[1]
            product = split_label[3].split(":")[1]
 
            product_label = "%s,%s" % (year,product)
            insert("product_values_%s" % (year),product_label,row[1])
             
            country_label = "%s,%s~%s" %(year,Im,"Import")
            insert("country_values_%s" %(year),country_label,row[1])
             
            country_label = "%s,%s~%s" % (year,Ex,"Export")
            insert("country_values_%s" %(year),country_label,row[1])
             
            country_label = "%s,%s,%s~%s" % (year,Im,product,"Import")
            insert("country_product_values_%s" %(year),country_label,row[1])
     
            country_label = "%s,%s,%s~%s" % (year,Ex,product,"Export")
            insert("country_product_values_%s" %(year),country_label,row[1])
             
        end_thread()

    def find_product_trends():
        for product in product_codes:
            # Find all the Long, Medium, and Short term Trends in products
        
            first_year_val = getProduct(product, first_year)
            last_year_val = getProduct(product, last_year)
            one_year_val  = getProduct(product, last_year-1)
#             print (one_year_val)
            if (one_year_val == 0):
                one_year_trend = 0
            else:
                one_year_trend = (last_year_val - one_year_val )/ one_year_val
            one_year_label = "%s-%s" % (product,"one_year_trend")
            
            insert("product_trends",one_year_label,one_year_trend)
             
            if (first_year_val == 0):
                long_trend = 0
            else:
                long_trend = (last_year_val - first_year_val )/ first_year_val
            long_trend_label = "%s-%s" % (product,"long_trend")

            insert("product_trends",long_trend_label,long_trend)
              
            five_year_val = getProduct(product, last_year-5)
            if (five_year_val == 0):
                five_year_trend = 0
            else:
                five_year_trend = (last_year_val - five_year_val )/ five_year_val
              
            five_year_trend_label = "%s-%s" % (product,"five_year_trend")

            insert("product_trends",five_year_trend_label,five_year_trend)
                         
            three_year_val = getProduct(product, last_year-3)
            if (three_year_val == 0):
                three_year_trend = 0
            else:
                three_year_trend = (last_year_val - three_year_val )/ three_year_val
                          
            three_year_trend_label = "%s-%s" % (product,"three_year_trend")

            insert("product_trends",three_year_trend_label,three_year_trend)
            
#             
    def find_trends(is_export):
        # Find the Total Product Trends
        trends = {}
        tag = "Import"
        if (is_export):
            tag = "Export"
                  
        for country in country_codes:
            for product in product_codes:
                first_year_val = getProductCountry(product,country, first_year,is_export)
                
                last_year_val  = getProductCountry(product,country, last_year,is_export)

                if (first_year_val == 0 and last_year_val == 0):
                    continue
                elif (first_year_val == 0):
                    long_trend = 0
                else:
                    long_trend_raw = ((last_year_val - first_year_val )/ first_year_val)
                
                # This will normalize the trends for individual countries based on shifting product trends
                # For example as coal consumption increases gloabally 5% a country increasing coal exports 3% 
                # is actually 2% below what one would expect so the normalized trend would be -2%
                
                    long_trend = long_trend_raw - getProductTrend("%s-%s" % (product,"long_trend"))

                long_trend_label = "%s-%s|%s~%s" % (product,"long_trend",country,tag)
            
                insert("trends",long_trend_label,long_trend)
                
                five_year_val = getProductCountry(product,country, last_year-5,is_export)
                
                if (five_year_val == 0):
                    five_year_trend = 0
                else:
                    five_year_trend_raw = (last_year_val - five_year_val )/ five_year_val
                    
                    five_year_trend = five_year_trend_raw - getProductTrend("%s-%s" % (product,"five_year_trend"))
        
                five_year_trend_label = "%s-%s|%s~%s" % (product,"five_year_trend",country,tag)
            
                insert("trends",five_year_trend_label,five_year_trend)
                    
                three_year_val = getProductCountry(product,country, last_year-3,is_export)
                
                if (three_year_val == 0):
                    three_year_trend = 0
                else:
                    three_year_trend_raw = ((last_year_val - three_year_val )/ three_year_val) 
                    
                    three_year_trend = three_year_trend_raw - getProductTrend("%s-%s" % (product,"three_year_trend"))
                
                three_year_trend_label  = "%s-%s|%s~%s" % (product,"three_year_trend",country,tag)
            
                insert("trends",three_year_trend_label,three_year_trend)
                                
                one_year_val  = getProductCountry(product,country, last_year-1,is_export)
                
                if (one_year_val == 0):
                    one_year_trend = 0
                else:
                    one_year_trend_raw = (last_year_val - one_year_val )/ one_year_val
                    
                    one_year_trend = one_year_trend_raw - getProductTrend("%s-%s" % (product,"one_year_trend"))
                
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
            elif (trendline == "long_year_trend"):
                year = first_year
            elif (trendline == "three_year_trend"):
                year = last_year - 3
            elif (trendline == "one_year_trend"):
                year = last_year - 1
            else:
                year = last_year

                
            
            product_val = getProduct(product,year)
            trend_val   = trend[1]
            
            if (product_val > absolute_threshold):
                if (trend_val > relative_threashold):
                    if (isProduct(trend[0])):
                        insert("interesting_trends",trend[0],trend[1])
        print ("Finished Products...")
        trends = readAll("trends")
                
        for trend in trends:
            product = trend[0].split("|")[0].split("-")[0]
            trendline = trend[0].split("|")[0].split("-")[1]
            country = trend[0].split("|")[1].split("~")[0]
            tag = trend[0].split("|")[1].split("~")[1]
            if (trendline == "five_year_trend"):
                year = last_year - 5
            elif (trendline == "long_year_trend"):
                year = first_year
            elif (trendline == "three_year_trend"):
                year = last_year - 3
            elif (trendline == "one_year_trend"):
                year = last_year - 1
            else:
                year = last_year
            product_val = getProductCountry(product,country,year,tag)
            trend_val   = trend[1]
            
            if (product_val > absolute_threshold):
                if (trend_val > relative_threashold):
                    if (isCountry(country) and isProduct(product)):
                        label = "%s-%s|%s~%s" % (product,trendline,realCountry(country),tag)
                        insert("interesting_trends",label,trend[1])

        return interesting_trends
    # This will return the relevent files
    def findLikelyErrors():
        
        interesting_trends = readAll("interesting_trends")
        
        for trend in interesting_trends:
            if trend[-1] > 1:
                insert("errors",trend[0],trend[-1])

        
        for country in country_codes:
            for product in product_codes:
                total = 0
                real = realCountry(country)
                for year in range(first_year,last_year+1):               
                    total += getProductCountry(product,real, last_year,True)
                average = total/ (last_year-first_year)
                for year in range(first_year,last_year+1):
                    value = getProductCountry(product,real, last_year,True)               
                    if (average > 0  and (value == 0 and average > extreme_relative_threashold or value/average > extreme_relative_threashold)):
                        label = "%s-%s~%s" % (product,real,"Export")
                        insert("errors",label,value)
            print(real)

        for country in country_codes:
            for product in product_codes:
                total = 0
                real = realCountry(country)
                for year in range(first_year,last_year+1):               
                    total += getProductCountry(product,real, last_year,False)
                average = total/ (last_year-first_year)
                for year in range(first_year,last_year+1):
                    value = getProductCountry(product,real, last_year,False)               
                    if (average > 0  and (value == 0 and average > extreme_relative_threashold or value/average > extreme_relative_threashold)):
                        label = "%s-%s~%s" % (product,real,"Import")
                        insert("errors",label,value)
            
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
                else:
                    print (exporter)

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
        while(True):
            args = raw_input("Insert product, country, year, is_export seperated by comma, print 'quit' to exit").split(",")
            if args[0] == 'quit':
                break
            try:
                print (getProductCountry(args[0],args[1],args[2],args[3]))
            except:
                print (args, "Not found")
        
        
    file_name = r'/home/chris/Downloads/baci92_'
    run(file_name,True,False,True,True,True,True,True)
#  