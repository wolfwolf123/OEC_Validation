'''
Created on Jun 2, 2016

@author: chris
'''
from Main import country_codes

first_year = 2009

last_year  = 2014

import SQL_Handler  
import Main

def find_product_trends(database,product_codes):
    
    print("Getting Product Trends...")
    
    Main.fill_values(database)


    for hs,product in product_codes.items():
        # Find all the Long, Medium, and Short term Trends in products
    
#             print (one_year_val)

        five_year_val = Main.getProduct(product, last_year-5)        
        first_year_val= Main.getProduct(product, first_year)
        last_year_val = Main.getProduct(product, last_year)      
        one_year_val  = Main.getProduct(product, last_year-1)
        three_year_val= Main.getProduct(product, last_year-3)

        # Attempts to lower the calculation requirements by not calculating for products
        # that have no values in the data set given
        
        total = first_year_val + last_year_val + one_year_val + five_year_val + three_year_val
        
        if (total == 0):
            continue
                
        if (one_year_val != 0):

            one_year_trend = (last_year_val - one_year_val )/ one_year_val
           
            one_year_label = "%s-%s" % (product,"one_year_trend")
            
            SQL_Handler.insert("product_trends",one_year_label,one_year_trend,Main.database)
          
        if (three_year_val != 0):

            three_year_trend = (last_year_val - three_year_val )/ three_year_val
                      
            three_year_trend_label = "%s-%s" % (product,"three_year_trend")
    
            SQL_Handler.insert("product_trends",three_year_trend_label,three_year_trend,Main.database)
            
        if (five_year_val != 0):

            five_year_trend = (last_year_val - five_year_val )/ five_year_val
          
            five_year_trend_label = "%s-%s" % (product,"five_year_trend")
            
            SQL_Handler.insert("product_trends",five_year_trend_label,five_year_trend,Main.database)
                     
        if (first_year_val != 0):

            long_trend = (last_year_val - first_year_val )/ first_year_val

            long_trend_label = "%s-%s" % (product,"long_trend")

            SQL_Handler.insert("product_trends",long_trend_label,long_trend,Main.database)
        

def find_trends(country_codes,product_codes,is_export):
    # Find the Total Product Trends
                
    trends = {}
    
    tag = Main.getExport(is_export)
    
    print ("Getting %s Trends..." % (tag))    
    
    for id,country in country_codes.items():
        for hs,product in product_codes.items():
                                                       

            five_year_val = Main.getProductCountry(product,country, last_year-5,tag)        
            first_year_val= Main.getProductCountry(product,country, first_year ,tag)
            last_year_val = Main.getProductCountry(product,country, last_year  ,tag)      
            one_year_val  = Main.getProductCountry(product,country, last_year-1,tag)
            three_year_val= Main.getProductCountry(product,country, last_year-3,tag)
    
            # Attempts to lower the calculation requirements by not calculating for products
            # that have no values in the data set given
            
            total = first_year_val + last_year_val + one_year_val + five_year_val + three_year_val
        
            if (total == 0):
                continue
                      
            
            if (one_year_val != 0):
               
                one_year_trend = (last_year_val - one_year_val )/ one_year_val
                            
                one_year_trend_label  = "%s-%s|%s~%s" % (product,"one_year_trend",country,tag)
            
                SQL_Handler.insert("trends",one_year_trend_label,one_year_trend,Main.database)
            if (three_year_val != 0):

                three_year_trend = ((last_year_val - three_year_val )/ three_year_val) 
                
                three_year_trend_label  = "%s-%s|%s~%s" % (product,"three_year_trend",country,tag)
            
                SQL_Handler.insert("trends",three_year_trend_label,three_year_trend,Main.database)
        
            if (first_year_val != 0):

                long_trend = ((last_year_val - first_year_val )/ first_year_val)
                
                long_trend_label = "%s-%s|%s~%s" % (product,"long_trend",country,tag)
            
                SQL_Handler.insert("trends",long_trend_label,long_trend,Main.database)
            
            
            if (five_year_val != 0):
                
                five_year_trend = (last_year_val - five_year_val )/ five_year_val
                    
                five_year_trend_label = "%s-%s|%s~%s" % (product,"five_year_trend",country,tag)
        
                SQL_Handler.insert("trends",five_year_trend_label,five_year_trend,Main.database)
                
            
     
    
    return trends