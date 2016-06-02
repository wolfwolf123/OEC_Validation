'''
Created on Jun 2, 2016

@author: chris
'''
import SQL_Handler
import Main

sql_hand = SQL_Handler("None")

product_codes = {}
country_codes = {}
main = Main()

first_year = 2009

last_year  = 2014
    
class Trend_Handler(object):
    
    def init(self,SQL_Hand,product,country, main_in):
        global sql_hand
        global product_codes
        global country_codes
        global main
        
        sql_hand = SQL_Hand
        product_codes = product
        country_codes = country 
        main = main_in
    
    def find_product_trends(self):
        for product in product_codes:
            # Find all the Long, Medium, and Short term Trends in products
        
            first_year_val =main.getProduct(product, first_year)
            last_year_val = main.getProduct(product, last_year)
            one_year_val  = main.getProduct(product, last_year-1)
#             print (one_year_val)
            
            total = 0
            for y in range(last_year,first_year-1,-1):
                total += main.getProduct(product, y)
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
            
            sql_hand.insert("product_trends",one_year_label,one_year_trend)
             
            if (first_year_val == 0):
                long_trend = 0
            else:
                long_trend = (last_year_val - first_year_val )/ long_average
            long_trend_label = "%s-%s" % (product,"long_trend")

            sql_hand.insert("product_trends",long_trend_label,long_trend)
              
            five_year_val = main.getProduct(product, last_year-5)
            if (five_year_val == 0):
                five_year_trend = 0
            else:
                five_year_trend = (last_year_val - five_year_val )/ five_year_average
              
            five_year_trend_label = "%s-%s" % (product,"five_year_trend")

            sql_hand.insert("product_trends",five_year_trend_label,five_year_trend)
                         
            three_year_val = main.getProduct(product, last_year-3)
            if (three_year_val == 0):
                three_year_trend = 0
            else:
                three_year_trend = (last_year_val - three_year_val )/ three_year_average
                          
            three_year_trend_label = "%s-%s" % (product,"three_year_trend")

            sql_hand.insert("product_trends",three_year_trend_label,three_year_trend)
            
   
    def find_trends(self,is_export):
        # Find the Total Product Trends
                
        trends = {}
        
        tag = "import"
        if (is_export):
            tag = "Export"
        
        for country_code in country_codes:
            for product in product_codes:
                  
                country = main.realCountry(country_code)
                                          
                first_year_val = main.getProductCountry(product,country, first_year,is_export)
                
                last_year_val  = main.getProductCountry(product,country, last_year,is_export)
                total = 0
                for y in range(last_year,first_year-1,-1):
                    total += main.getProductCountry(product,country, y,is_export)
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
                
#                     long_trend = long_trend_raw - main.getProductTrend("%s-%s" % (product,"long_trend"))

                long_trend_label = "%s-%s|%s~%s" % (product,"long_trend",country,tag)
            
                sql_hand.insert("trends",long_trend_label,long_trend)
                
                five_year_val = main.getProductCountry(product,country, last_year-5,is_export)
                
                if (five_year_val == 0):
                    five_year_trend = 0
                else:
                    five_year_trend = (last_year_val - five_year_val )/ five_year_average
                    
#                     five_year_trend = five_year_trend_raw - main.getProductTrend("%s-%s" % (product,"five_year_trend"))
        
                five_year_trend_label = "%s-%s|%s~%s" % (product,"five_year_trend",country,tag)
            
                sql_hand.insert("trends",five_year_trend_label,five_year_trend)
                    
                three_year_val = main.getProductCountry(product,country, last_year-3,is_export)
                
                if (three_year_val == 0):
                    three_year_trend = 0
                else:
                    three_year_trend = ((last_year_val - three_year_val )/ three_year_average) 
                    
#                     three_year_trend = three_year_trend_raw - main.getProductTrend("%s-%s" % (product,"three_year_trend"))
                
                three_year_trend_label  = "%s-%s|%s~%s" % (product,"three_year_trend",country,tag)
            
                sql_hand.insert("trends",three_year_trend_label,three_year_trend)
                                
                one_year_val  = main.getProductCountry(product,country, last_year-1,is_export)
                
                if (one_year_val == 0):
                    one_year_trend = 0
                else:
                    one_year_trend = (last_year_val - one_year_val )/ one_year_average
                    
#                     one_year_trend = one_year_trend_raw - main.getProductTrend("%s-%s" % (product,"one_year_trend"))
                
                one_year_trend_label  = "%s-%s|%s~%s" % (product,"one_year_trend",country,tag)
            
                sql_hand.insert("trends",one_year_trend_label,one_year_trend)
                        
        return trends