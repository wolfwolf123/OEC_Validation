'''
Created on Jun 2, 2016

@author: chris
'''

first_year = 2009

last_year  = 2014

import SQL_Handler  
import Main

def find_product_trends(database,product_codes):
    
    print("Getting Product Trends...")
    
    Main.fill_country_values(database)


    for hs,product in product_codes.items():
        # Find all the Long, Medium, and Short term Trends in products
    
#             print (one_year_val)

        total = 0
        for y in range(last_year,first_year-1,-1):
            total += Main.getProduct(product, y)
            if (y == last_year - 5):
                five_year_average = abs(total/6)
            if (y == last_year - 3):
                three_year_average = abs(total/4)
            if (y == last_year - 1):
                one_year_average = abs(total/2)
        long_average = abs(total/(last_year + 1 - first_year))
    
        if (total == 0):
            continue
        
        first_year_val =Main.getProduct(product, first_year)
        last_year_val = Main.getProduct(product, last_year)
        one_year_val  = Main.getProduct(product, last_year-1)
        
        print (product)
        
        if (one_year_val == 0):
            one_year_trend = 0
        else:
            one_year_trend = (last_year_val - one_year_val )/ one_year_average
        one_year_label = "%s-%s" % (product,"one_year_trend")
        
        SQL_Handler.insert("product_trends",one_year_label,one_year_trend,Main.database)
         
        if (first_year_val == 0):
            long_trend = 0
        else:
            long_trend = (last_year_val - first_year_val )/ long_average
        long_trend_label = "%s-%s" % (product,"long_trend")

        SQL_Handler.insert("product_trends",long_trend_label,long_trend,Main.database)
          
        five_year_val = Main.getProduct(product, last_year-5)
        if (five_year_val == 0):
            five_year_trend = 0
        else:
            five_year_trend = (last_year_val - five_year_val )/ five_year_average
          
        five_year_trend_label = "%s-%s" % (product,"five_year_trend")

        SQL_Handler.insert("product_trends",five_year_trend_label,five_year_trend,Main.database)
                     
        three_year_val = Main.getProduct(product, last_year-3)
        if (three_year_val == 0):
            three_year_trend = 0
        else:
            three_year_trend = (last_year_val - three_year_val )/ three_year_average
                      
        three_year_trend_label = "%s-%s" % (product,"three_year_trend")

        SQL_Handler.insert("product_trends",three_year_trend_label,three_year_trend,Main.database)
        

def find_trends(country_codes,product_codes,is_export):
    # Find the Total Product Trends
                
    trends = {}
    
    tag = Main.getExport(is_export)
    
    print ("Getting %s Trends..." % (tag))    
    
    for id,country in country_codes.items():
        for hs,product in product_codes.items():
                                                       
            total = 0
            for y in range(last_year,first_year,-1):
                total += Main.getProductCountry(product,country, y-1,tag)
                if (y == last_year - 4):
                    five_year_average = abs(total/5) 
                if (y == last_year - 2):
                    three_year_average = abs(total/3)
                if (y == last_year):
                    one_year_average = abs(total)
            long_average = abs(total/(last_year - first_year))
            if (total == 0):
                continue


            first_year_val = Main.getProductCountry(product,country, first_year,tag)
            
            last_year_val  = Main.getProductCountry(product,country, last_year,tag)
            


            if (first_year_val == 0):
                long_trend = 0
            else:
                long_trend = ((last_year_val - first_year_val )/ long_average)
            
            
            # This will normalize the trends for individual countries based on shifting product trends
            # For example as coal consumption increases gloabally 5% a country increasing coal exports 3% 
            # is actually 2% below what one would expect so the normalized trend would be -2%
            
#                     long_trend = long_trend_raw - Main.getProductTrend("%s-%s" % (product,"long_trend"))

            long_trend_label = "%s-%s|%s~%s" % (product,"long_trend",country,tag)
        
            SQL_Handler.insert("trends",long_trend_label,long_trend,Main.database)
            
            five_year_val = Main.getProductCountry(product,country, last_year-5,tag)
            
            if (five_year_val == 0):
                five_year_trend = 0
            else:
                five_year_trend = (last_year_val - five_year_val )/ five_year_average
                
#                     five_year_trend = five_year_trend_raw - Main.getProductTrend("%s-%s" % (product,"five_year_trend"))
    
            five_year_trend_label = "%s-%s|%s~%s" % (product,"five_year_trend",country,tag)
        
            SQL_Handler.insert("trends",five_year_trend_label,five_year_trend,Main.database)
                
            three_year_val = Main.getProductCountry(product,country, last_year-3,tag)
            
            if (three_year_val == 0):
                three_year_trend = 0
            else:
                three_year_trend = ((last_year_val - three_year_val )/ three_year_average) 
                
#                     three_year_trend = three_year_trend_raw - Main.getProductTrend("%s-%s" % (product,"three_year_trend"))
            
            three_year_trend_label  = "%s-%s|%s~%s" % (product,"three_year_trend",country,tag)
        
            SQL_Handler.insert("trends",three_year_trend_label,three_year_trend,Main.database)
                            
            one_year_val  = Main.getProductCountry(product,country, last_year-1,tag)
            
            if (one_year_val == 0):
                one_year_trend = 0
            else:
                one_year_trend = (last_year_val - one_year_val )/ one_year_average
                
#                     one_year_trend = one_year_trend_raw - Main.getProductTrend("%s-%s" % (product,"one_year_trend"))
            
            one_year_trend_label  = "%s-%s|%s~%s" % (product,"one_year_trend",country,tag)
        
            SQL_Handler.insert("trends",one_year_trend_label,one_year_trend,Main.database)
    
    return trends