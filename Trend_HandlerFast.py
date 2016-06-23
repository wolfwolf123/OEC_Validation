'''
Created on Jun 2, 2016

@author: chris
'''

first_year = 2009

last_year  = 2014

import SQL_Handler  
import MainFast

country_trends = {}
product_trends = {}
interesting_trends = {}



def find_trends(data,accessor):

    output = {}

    for item in data:
        # Find all the Long, Medium, and Short term Trends in products
    
#             print (one_year_val)
        if (data[item] < MainFast.absolute_threshold):
            continue

        five_year_val = accessor(item, last_year-5)        
        first_year_val= accessor(item, first_year)
        last_year_val = accessor(item, last_year)      
        one_year_val  = accessor(item, last_year-1)
        three_year_val= accessor(item, last_year-3)

        # Attempts to lower the calculation requirements by not calculating for items
        # that have no values in the data set given
                        
        if (one_year_val != 0):

            one_year_trend = (last_year_val - one_year_val )/ one_year_val
           
            one_year_label = "%s-%s" % (item,"one_year_trend")
            
            output [one_year_label] = one_year_trend
            
        elif(last_year_val > 0):
            
            one_year_label = "%s-%s" % (item,"one_year_trend")

            output [one_year_label] = 1
                      
        if (three_year_val != 0):

            three_year_trend = (last_year_val - three_year_val )/ three_year_val
                      
            three_year_trend_label = "%s-%s" % (item,"three_year_trend")
    
            output [three_year_trend_label] = three_year_trend
            
        elif(last_year_val > 0):
            
            three_year_trend_label = "%s-%s" % (item,"three_year_trend")

            output [three_year_trend_label] = 1
            
        if (five_year_val != 0):

            five_year_trend = (last_year_val - five_year_val )/ five_year_val
          
            five_year_trend_label = "%s-%s" % (item,"five_year_trend")
            
            output [five_year_trend_label] = five_year_trend
            
        elif(last_year_val > 0):
        
            five_year_trend_label = "%s-%s" % (item,"five_year_trend")

            output [five_year_trend_label] = 1
                     
        if (first_year_val != 0):

            long_trend = (last_year_val - first_year_val )/ first_year_val

            long_trend_label = "%s-%s" % (item,"long_trend")

            output [long_trend_label] = long_trend
        elif(last_year_val > 0):
            
            long_trend_label = "%s-%s" % (item,"long_trend")

            output [long_trend_label] = long_trend
    
    return output
        
def find_country_trends(database,country_codes):
   
    for country in country_codes:
        # Find all the Long, Medium, and Short term Trends in countrys
    
#             print (one_year_val)
        if (country_codes[country] < MainFast.absolute_threshold):
            continue
        five_year_val = MainFast.getCountry(country, last_year-5)        
        first_year_val= MainFast.getCountry(country, first_year)
        last_year_val = MainFast.getCountry(country, last_year)      
        one_year_val  = MainFast.getCountry(country, last_year-1)
        three_year_val= MainFast.getCountry(country, last_year-3)

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
                
                if (len(product)<7):
                    market_val = 1
                else:
                    total = 0 
                    for y in range (year,last_year):
                        total += MainFast.getProduct(product[:-2],y)
                    market_average = total/(last_year - year + 1)
                    total = 0 
                    for y in range (year,last_year):
                        total += MainFast.getProduct(product,y)
                    product_average = total/(last_year - year + 1)
                    market_val = product_average/market_average
                if (market_val > MainFast.market_threshold):
                    label = "%s$%s" % (trend[0],net)
                    
                    interesting_trends[label] = trend_val
            
            
    for trend in country_trends:
        
        trend_val = trend[1]       
         
        if (trend_val >= MainFast.absolute_threshold):

            trendline = trend[0].split("-")[0]
            
            country = trend[0].split("-")[1]
                     
            year = getTrendYear(trendline)
                       
            start = MainFast.getCountry(country,year)
            
            end = MainFast.getCountry(country,last_year)
    
            net = end - start
                    
    
            # If the value of the trend (i.e. -.5 or a 50% decrease) times the size of the market (i.e. $1 billion dollars) is greater than the 
            # absolute threshold then insert the value into interesting trends
            
            if (abs(net) > MainFast.absolute_threshold):
                
                label = "%s$%s" % (trend[0],net)
            
                interesting_trends[label] = trend_val
    
    return interesting_trends
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