'''
Created on Jun 2, 2016

@author: chris
'''
from Main import country_codes

first_year = 2009

last_year  = 2014

import SQL_Handler  
import Main
import MainFast

country_trends = {}
product_trends = {}
interesting_trends = {}



def find_product_trends(database,product_codes):

    for product in product_codes:
        # Find all the Long, Medium, and Short term Trends in products
    
#             print (one_year_val)
        if (product_codes[product] < MainFast.absolute_threshold):
            continue

        five_year_val = Main.getProduct(product, last_year-5)        
        first_year_val= Main.getProduct(product, first_year)
        last_year_val = Main.getProduct(product, last_year)      
        one_year_val  = Main.getProduct(product, last_year-1)
        three_year_val= Main.getProduct(product, last_year-3)

        # Attempts to lower the calculation requirements by not calculating for products
        # that have no values in the data set given
                        
        if (one_year_val != 0):

            one_year_trend = (last_year_val - one_year_val )/ one_year_val
           
            one_year_label = "%s-%s" % (product,"one_year_trend")
            
            product_trends [one_year_label] = one_year_trend
            
        elif(last_year_val > 0):
            
            one_year_label = "%s-%s" % (product,"one_year_trend")

            product_trends [one_year_label] = 1
                      
        if (three_year_val != 0):

            three_year_trend = (last_year_val - three_year_val )/ three_year_val
                      
            three_year_trend_label = "%s-%s" % (product,"three_year_trend")
    
            product_trends [three_year_trend_label] = three_year_trend
            
        elif(last_year_val > 0):
            
            three_year_trend_label = "%s-%s" % (product,"three_year_trend")

            product_trends [three_year_trend_label] = 1
            
        if (five_year_val != 0):

            five_year_trend = (last_year_val - five_year_val )/ five_year_val
          
            five_year_trend_label = "%s-%s" % (product,"five_year_trend")
            
            product_trends [five_year_trend_label] = five_year_trend
            
        elif(last_year_val > 0):
        
            five_year_trend_label = "%s-%s" % (product,"five_year_trend")

            product_trends [five_year_trend_label] = 1
                     
        if (first_year_val != 0):

            long_trend = (last_year_val - first_year_val )/ first_year_val

            long_trend_label = "%s-%s" % (product,"long_trend")

            product_trends [long_trend_label] = long_trend
        elif(last_year_val > 0):
            
            long_trend_label = "%s-%s" % (product,"long_trend")

            product_trends [long_trend_label] = long_trend
        
def find_country_trends(database,country_codes):
   
    for country in country_codes:
        # Find all the Long, Medium, and Short term Trends in countrys
    
#             print (one_year_val)
        if (country_codes[country] < MainFast.absolute_threshold):
            continue
        five_year_val = Main.getCountry(country, last_year-5)        
        first_year_val= Main.getCountry(country, first_year)
        last_year_val = Main.getCountry(country, last_year)      
        one_year_val  = Main.getCountry(country, last_year-1)
        three_year_val= Main.getCountry(country, last_year-3)

        # Attempts to lower the calculation requirements by not calculating for countrys
        # that have no values in the data set given
   
        if (one_year_val != 0):

            one_year_trend = (last_year_val - one_year_val )/ one_year_val
           
            one_year_label = "%s-%s" % (country,"one_year_trend")
            
            country_trends [one_year_label] = one_year_trend
            
        elif(last_year_val > 0):
            
            one_year_label = "%s-%s" % (country,"one_year_trend")

            country_trends [one_year_label] = 1
                      
        if (three_year_val != 0):

            three_year_trend = (last_year_val - three_year_val )/ three_year_val
                      
            three_year_trend_label = "%s-%s" % (country,"three_year_trend")
    
            country_trends [three_year_trend_label] = three_year_trend
            
        elif(last_year_val > 0):
            
            three_year_trend_label = "%s-%s" % (country,"three_year_trend")

            country_trends [three_year_trend_label] = 1
            
        if (five_year_val != 0):

            five_year_trend = (last_year_val - five_year_val )/ five_year_val
          
            five_year_trend_label = "%s-%s" % (country,"five_year_trend")
            
            country_trends [five_year_trend_label] = five_year_trend
            
        elif(last_year_val > 0):
        
            five_year_trend_label = "%s-%s" % (country,"five_year_trend")

            country_trends [five_year_trend_label] = 1
                     
        if (first_year_val != 0):

            long_trend = (last_year_val - first_year_val )/ first_year_val

            long_trend_label = "%s-%s" % (country,"long_trend")

            country_trends [long_trend_label] = long_trend
        elif(last_year_val > 0):
            
            long_trend_label = "%s-%s" % (country,"long_trend")

            country_trends [long_trend_label] = long_trend
# This method combs through the data to see if it can find interesting long-term trends, i.e. The oil market contracting severely in size.
# Some of these trends may be so severe that they indicate a possible error in the data    
# Its argument note allows an appendment to the label to allow further clarification as to its meaning
def findInterestingTrends (note = None):
 
    print ("Getting Interesting Trends...")
        
    # First it acquires the trends from the product trends 
    
    for trend in product_trends:
        
        product = trend[0].split("-")[0]
        trendline = trend[0].split("-")[1]
        
        trend_val = trend[1]
        
        # If the value of the trend (i.e. -.5 or a 50% decrease) times the size of the market (i.e. $1 billion dollars) is greater than the 
        # absolute threshold then insert the value into interesting trends
        
        if (abs(trend_val) >= MainFast.relative_threashold):
            year = getTrendYear(trendline)

            start = MainFast.getProduct(product, year)
        
            end = MainFast.getProduct(product, last_year)

            net = end -start
            
            if(abs(net) > MainFast.absolute_threshold):
                label = "%s$%s" % (trend[0],net)
                
                interesting_trends[label] = trend_val
            
            
    for trend in country_trends:
                
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
        
        if (abs(net) > absolute_threshold and abs(trend_val) >= .25):
            
            if (note == None):
                label = "%s-%s|%s~%s$%s" % (product,trendline,country,tag,net)
            else:
                label = "%s-%s|%s~%s$%s@%s" % (product,trendline,country,tag,net,note)

            SQL_Handler.insert("interesting_trends",label,trend_val,database)
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