'''
Created on Jun 30, 2016

@author: Chris Briere
'''
import Tools
import time
import Error_Handler
import Plotter
import csv

main = None 
last_year  = -1
first_year =  0 
country_codes = {}

##################################################################################################
#                                                                                                #
#               INITIALIZING FUNCTION (ONLY THIS FUNCTION CAN BE CALLED EXTERNALLY)              #
#                                                                                                #
##################################################################################################

def getTrends (file_name,country,product,relative =1,absolute=50000,market=.15,trendline = "All"):
   
    global main
    
    main = Tools.initilize()
    
    global last_year
    global first_year
    global country_codes
    global country_names
    
    country_names = main.country_names
    country_codes = main.country_codes
    last_year = main.last_year
    first_year= main.first_year
        
    start = time.time()
    
    main.product_trends = {}
    main.country_trends = {}
    main.product_values = {}
    main.country_values = {}
    main.market_trends = {}
   
#     absolute_threshold = float(absolute)
    main.market_threshold = float(market)
    main.relative_threashold = float(relative)
    main.absolute_threshold = float(absolute) / (last_year - first_year + 1)
    
    print (product,main.absolute_threshold,main.market_threshold,main.trend_threashold,main.relative_threashold,trendline)
    
    keys = []
    for code in country_codes:
        if (country) == country_codes[code]:
            keys.append(code)
            
    for code in country_names:
        if (country) == country_names[code]:
            
            keys.append(code)
    print (keys)
    
    Tools.single_thread(Tools.getTotalCountry, first_year, last_year, arg=(file_name,keys))
    
    absolute_threshold = (float(absolute) / (last_year - first_year + 1)) * main.total_country_value
    
    print ("Absolute", absolute_threshold)

    print("Total Country Value", main.total_country_value)
    
    Tools.single_thread(__getTrendsPreProcessed,first_year,last_year,arg=(file_name,keys,product,trendline))

#     absolute_threshold = float(absolute) * total_country_value

    main.interesting_trends = {}
    
#     for i in interesting_trends:
#         print ("Interesting Before:",i)
    
    __findInterestingTrends()

#     for i in interesting_trends:
#         print ("Interesting After:",i);
    (main,final_errors) = Error_Handler.getErrors(file_name, country, 1.5, absolute_threshold, market, .75, 3, main)

    end = time.time()
    print(end - start)
    
    return (main,__filter_trends(main,final_errors))

##################################################################################################
#                                                                                                #
#               INITIALIZING FUNCTION (ONLY THIS FUNCTION CAN BE CALLED EXTERNALLY)              #
#                                                                                                #
##################################################################################################

def getTrendsPlot(file_name,country,product,relative =1,absolute=50000,market=.15,trendline = "All"):
    
    final_trends = getTrends(file_name, country, product, relative, absolute, market, trendline)
    
    call =[]
    
    for trend in final_trends:
#         print(trend,final_trends[trend])
        call.append(trend.split(","))
    return Plotter.plot(country,call,main)

##################################################################################################
#                                                                                                #
#               INITIALIZING FUNCTION (ONLY THIS FUNCTION CAN BE CALLED EXTERNALLY)              #
#                                                                                                #
##################################################################################################

def getTrendsList(file_name,country,product,relative =1,absolute=.0002,market=.15,trendline = "All",type = "All"):
    
    (main,trends) = getTrends(file_name, country, product, relative, absolute, market, trendline)
    if(type == "All"):
        return (main,trends)
    elif (type == "Net Gain"):
        top_five = {"a":0,"b":0,"c":0,"d":0,"e":0}
        
        for trend in trends:
#             print (trend)
            for index in top_five:
                if (trends[trend] > top_five[index]):
                    del top_five[index]
                    top_five[trend] = trends[trend]
                    break
        return (main,top_five)
    
    

def __getTrendsPreProcessed(year,arg, multi=False):

    used_products = {}
    used_countries ={}

    
  
    (filename,keys,productBool,trendline) = arg

#     print(productBool)
    
    for key in keys:

        file_location = filename + str(year) + "/"  + key + "_" + str(year) + '.csv'    
#         print (file_location)
        
        try:
            with open(file_location) as csvfile:
                reader = csv.reader(csvfile)
                started = False
                
                # This skips the first line which contains the labels   
                next(reader)     
                
                for column in reader:
    #                     print (column)
                    if (column[2] == key):
                        started = True
                        
                        product = column[1]
                        country = country_codes[column[3]]
                        value = float(column[4])
                        
                        # This saves all of the specific country product pairs to ensure that only the pairs that are in the list are examined
                        if (productBool):
                            used_products [product] =  used_products.get("%s" % (product),0) + value
                        else:
                            used_countries[country] = used_countries.get("%s" % (country),0) + value
                        
                        if (productBool):
                            main.product_values["%s-%s" % (year,product)] = main.product_values.get("%s-%s" % (year,product),0) + value
                        else:
                            main.country_values["%s-%s" % (year,country)] = main.country_values.get("%s-%s" % (year,country),0) + value
    
    
                    # Since the list is ordered once the last item with the correct key is called the program can exit
                    
                    elif (started):
                        print ("Failed Values")
                        break
            
            used_markets ={}
            if (productBool):   
                for product in used_products:
                    value = main.product_values.get("%s-%s" % (year,product[:-2]),0) + used_products[product]
    
                    main.product_values["%s-%s" % (year,product[:-2])] = value
                    used_markets[product[:-2]] =  value
            else:    
                for country in used_countries:
                    main.country_values["%s-%s" % (year,country[:2])] = main.country_values.get("%s-%s" % (year,country[:2]),0) + used_countries[country]
                
            if (productBool):
    #                 print ("Get Trends")
                market_trends   = __find_trends(used_markets,main.getProduct,trendline)
                print ("Get Trends")
    
                main.product_trends  = __find_product_trends(market_trends,used_products,trendline)
                
                for trend in market_trends:
                    print(trend)
                main.product_trends.update(market_trends)
    
            else:
                main.country_trends  = __find_trends(used_countries,main.getCountry,trendline)
        
    #             for product in product_values:
    #                 if (product_values[product] == 0):
    #                     print (product_values[product])
        
        except:
            print("Failed Trends")
            pass
def __findInterestingTrends ():
    
    main.interesting_trends = {}
    
    # First it acquires the trends from the product trends 
#     print (relative_threashold)
    
    for trend in main.product_trends:
        print (trend)
        product = trend.split("-")[0]
        trendline = trend.split("-")[1]
        
        trend_val = main.product_trends[trend]

        year = Tools.getTrendYear(trendline)
         


#         if (abs(trend_val)/(last_year - year + 1) >= )

        # If the value of the trend (i.e. -.5 or a 50% decrease) times the size of the market (i.e. $1 billion dollars) is greater than the 
        # absolute threshold then insert the value into interesting trends
        
        
        if (abs(trend_val)/(last_year - year + 1) >= main.relative_threashold):

            start = main.getProduct(product, year)
#             print(start)
            end = main.getProduct(product, last_year)

            net = end -start
#             print ("Absolute",absolute_threshold,abs(net))

            if(abs(net) > main.absolute_threshold and start > main.absolute_threshold):
                
#                 print ("Absolute",absolute_threshold,abs(net))
                
                if (len(product)==4):
                    market_val = 1.1
                else:
                    total = 0 
                    for y in range (year,last_year):
                        total += main.getProduct(product[:-2],y)
                    market_average = total/(last_year - year + 1)

                    total = 0 
                    for y in range (year,last_year):
                        total += main.getProduct(product,y)

                    product_average = total/(last_year - year + 1)
                    if (product_average == 0):
                        market_val = 0
                    else:
                        market_val = product_average/market_average
#                     print (market_val)
                if (market_val > main.market_threshold):
                    label = "%s$%s" % (trend,net)
#                     print ("Absolute",absolute_threshold,abs(net),label)
                    main.interesting_trends[label] = trend_val
            
            
    for trend in main.country_trends:
                
        trend_val = main.country_trends[trend]       

        trendline = trend.split("-")[1]
            
        country = trend.split("-")[0]
                     
        year = main.getTrendYear(trendline)
#         
#         if (not year):
#             print ("Trend:",trend)
                 
        if (abs(trend_val)/(last_year - year + 1) >= main.relative_threashold):


                       
            start = main.getCountry(country,year)
            
            end = main.getCountry(country,last_year)
    
            net = end - start
                    
    
            # If the value of the trend (i.e. -.5 or a 50% decrease) times the size of the market (i.e. $1 billion dollars) is greater than the 
            # absolute threshold then insert the value into interesting trends
            
            if (abs(net) > main.absolute_threshold and start > main.absolute_threshold):

#                 print ("Absolute",absolute_threshold,abs(net))
                
                label = "%s$%s" % (trend,net)
                
#                 print ("Absolute",absolute_threshold,abs(net),label)

                main.interesting_trends[label] = trend_val
    
    return main.interesting_trends

def __find_product_trends(market_trends,products,trendline):
    print ("Starting Product Trends")

    output = {}
    print (trendline)

    print ("One Year Threshold", main.absolute_threshold)

    for product in products:
        print (product)
        # Find all the Long, Medium, and Short term Trends in products
    
#             print (one_year_val)
        if (products[product] < main.absolute_threshold):
            continue

        last_year_val = main.getProduct(product, last_year)      
        
        
        if (trendline == "One Year" or trendline == "All"):
            one_year_val  = main.getProduct(product, last_year-1)

            if (one_year_val != 0):
    
                one_year_trend = (last_year_val - one_year_val )/ abs(one_year_val)
               
                one_year_label = "%s-%s" % (product,"one_year_trend")
                
                
                if (one_year_trend < 0):
                    if (one_year_trend == -1):
                        output [one_year_label] = -100 
                    else:
                        output [one_year_label] = 1/(1 - one_year_trend) - 1 - market_trends["%s-%s" % (product[:-2],"one_year_trend")]
                else:
                    output [one_year_label] = one_year_trend - market_trends["%s-%s" % (product[:-2],"one_year_trend")]
                    
                
            elif(last_year_val > 0):
                
                one_year_label = "%s-%s" % (product,"one_year_trend")
    
                output [one_year_label] = 1
                          
        if (trendline == "Three Years" or trendline == "All"):
            three_year_val= main.getProduct(product, last_year-3)

            if (three_year_val != 0):
    
                three_year_trend = (last_year_val - three_year_val )/ abs(three_year_val)
                          
                three_year_trend_label = "%s-%s" % (product,"three_year_trend")
                    
                if (three_year_trend < 0):
                    if (three_year_trend == -1):
                        output [three_year_trend_label] = -100
                    else:
                        output [three_year_trend_label] = 1/(1 - three_year_trend) - 1 -  market_trends["%s-%s" % (product[:-2],"three_year_trend")]
                else:
                    output [three_year_trend_label] = three_year_trend -  market_trends["%s-%s" % (product[:-2],"three_year_trend")]
                
            elif(last_year_val > 0):
                
                three_year_trend_label = "%s-%s" % (product,"three_year_trend")
    
                output [three_year_trend_label] = 1
                     
        if (trendline == "Five Years" or trendline == "All"):

            five_year_val = main.getProduct(product, last_year-5)        

            if (five_year_val != 0):
            
                five_year_trend = (last_year_val - five_year_val )/ abs(five_year_val)
              
                five_year_trend_label = "%s-%s" % (product,"five_year_trend")
                
                output [five_year_trend_label] = five_year_trend
                
                if (five_year_trend < 0):
                    if (five_year_trend == -1):
                        output [five_year_trend_label] = -100
                    else:
                        output [five_year_trend_label] = 1/(1 - five_year_trend) - 1 -  market_trends["%s-%s" % (product[:-2],"five_year_trend")]
                else:
                    output [five_year_trend_label] = five_year_trend -  market_trends["%s-%s" % (product[:-2],"five_year_trend")]
                
            elif(last_year_val > 0):
            
                five_year_trend_label = "%s-%s" % (product,"five_year_trend")

                output [five_year_trend_label] = 1

        if (trendline == "All"):
            
            first_year_val= main.getProduct(product, first_year)
    
           
           
                         
            if (first_year_val != 0):
    
                long_trend = (last_year_val - first_year_val )/ abs(first_year_val)
    
                long_trend_label = "%s-%s" % (product,"long_trend")
    
                output [long_trend_label] = long_trend
                
                if (long_trend < 0):
                    if (long_trend == -1):
                        output [long_trend_label] = -100
                    else:
                        output [long_trend_label] = 1/(1 - long_trend) - 1 -  market_trends["%s-%s" % (product[:-2],"long_trend")]
                else:
                    output [long_trend_label] = long_trend -  market_trends["%s-%s" % (product[:-2],"long_trend")]
            elif(last_year_val > 0):
                
                long_trend_label = "%s-%s" % (product,"long_trend")
    
                output [long_trend_label] = 1
        
    return output


def __find_trends(data,accessor,trendline):

    output = {}
    
#     print (absolute_threshold_one_year)
    
    for item in data:
        # Find all the Long, Medium, and Short term Trends in products
    
#             print (one_year_val)
        if (data[item] < main.absolute_threshold):
            continue

        last_year_val = accessor(item, last_year)      
        
#         if (last_year_val < one_year_val):
#             print (last_year_val,one_year_val)

        # Attempts to lower the calculation requirements by not calculating for items
        # that have no values in the data set given
        if (trendline == "One Year" or trendline == "All"):

            one_year_val  = accessor(item, last_year-1)

            if (one_year_val != 0):
    
                one_year_trend = (last_year_val - one_year_val )/ abs(one_year_val)
               
                one_year_label = "%s-%s" % (item,"one_year_trend")
                
                
                if (one_year_trend < 0):
                    if (one_year_trend == -1):
                        output [one_year_label] = -100
                    else:
                        output [one_year_label] = 1/(1 - one_year_trend) - 1
                else:
                    output [one_year_label] = one_year_trend
                
            elif(last_year_val > 0):
                
                one_year_label = "%s-%s" % (item,"one_year_trend")
    
                output [one_year_label] = 1
                      
        if (trendline == "Three Years" or trendline == "All"):

            three_year_val= accessor(item, last_year-3)

    
            if (three_year_val != 0):
    
                three_year_trend = (last_year_val - three_year_val )/ abs(three_year_val)
                          
                three_year_trend_label = "%s-%s" % (item,"three_year_trend")
                    
                if (three_year_trend < 0):
                    if (three_year_trend == -1):
                        output [three_year_trend_label] = -100
                    else:
                        output [three_year_trend_label] = 1/(1 - three_year_trend) - 1
                else:
                    output [three_year_trend_label] = three_year_trend
                
            elif(last_year_val > 0):
                
                three_year_trend_label = "%s-%s" % (item,"three_year_trend")
    
                output [three_year_trend_label] = 1
            
        if (trendline == "Five Years" or trendline == "All"):

            five_year_val = accessor(item, last_year-5)        

            if (five_year_val != 0):
    
                five_year_trend = (last_year_val - five_year_val )/ abs(five_year_val)
              
                five_year_trend_label = "%s-%s" % (item,"five_year_trend")
                
                output [five_year_trend_label] = five_year_trend
                
                if (five_year_trend < 0):
                    if (five_year_trend == -1):
                        output [five_year_trend_label] = -100
                    else:
                        output [five_year_trend_label] = 1/(1 - five_year_trend) - 1
                else:
                    output [five_year_trend_label] = five_year_trend
                
            elif(last_year_val > 0):
            
                five_year_trend_label = "%s-%s" % (item,"five_year_trend")
    
                output [five_year_trend_label] = 1
        if (trendline == "All"):

            first_year_val= accessor(item, first_year)
                         
            if (first_year_val != 0):
    
                long_trend = (last_year_val - first_year_val )/ abs(first_year_val)
    
                long_trend_label = "%s-%s" % (item,"long_trend")
    
                output [long_trend_label] = long_trend
                
                if (long_trend < 0):
                    if (long_trend == -1):
                        output [long_trend_label] = -100
                    else:
                        output [long_trend_label] = 1/(1 - long_trend) - 1
                else:
                    output [long_trend_label] = long_trend
            elif(last_year_val > 0):
                
                long_trend_label = "%s-%s" % (item,"long_trend")
    
                output [long_trend_label] = 1
        
    return output
def __filter_trends(main,errors):
    
    final_trends = {}
        
    used_label = {}
    product_errors = {}
    country_errors = {}
    
    for error in errors:
        product_errors[error.split(",")[2]] = True
        country_errors[error.split(",")[3]] = True
    for trend in main.interesting_trends:
        
        trendline = trend.split("$")[0].split("-")[-1]
        
        
        year = Tools.getTrendYear(trendline)

        continue_bool = False
        
        
        
        try:
            if (year < used_label[trend.split("-")[0]]):
                continue_bool = True
         
                
        except:
            continue_bool = True 
        if (continue_bool):
            try:
               
                float(trend.split("-")[0])
                
                try:
                    product_errors[trend.split("-")[0]]          
                except:    
                    label =  str(year) + "," + str(last_year) + "," + trend.split("-")[0] + ",," + "Import"
            
                                            
                    used_label[trend.split("-")[0]] = year
                    
                    if (final_trends.get(label,0) != 0):
                        print ("Trend Found",label)
                        print ("Trend Found",label)
                        print ("Trend Found",label)
                        print ("Trend Found",label)

                    final_trends[label] = final_trends.get(label,0) + main.interesting_trends[trend]
    
            except:
                try:
                    country_errors[trend.split("-")[0]]
                except:
                    label = str(year) + "," + str(last_year) + ",," + trend.split("-")[0] + "," + "Import"
                    
                    used_label[trend.split("-")[0]] = year
                    
                    final_trends[label] = final_trends.get(label,0) + main.interesting_trends[trend]
    return final_trends

