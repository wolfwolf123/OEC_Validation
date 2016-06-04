'''
Created on Jun 2, 2016

@author: chris
'''
import unittest
import SQL_Handler
import Main 
import Trend_Handler

first_year = 2009

last_year  = 2014
database = ""
file_name = r'/home/chris/Downloads/Test_'


# This will allow the program to skip tests that are mutually inclusive

class Tester(unittest.TestCase):

    def setUp(self):  
        global database  
        print ("Beginnig Test")
        Main.initilize(False,False,False,False,False,False,"Test_DB")
        database = Main.getDatabase()
     
    def test_find_errors(self):
          
        Main.multi_thread(Main.getFiles,first_year,last_year,arg=file_name)
           
        Main.multi_thread(Main.populate_values,first_year,last_year,num=1)
   
        Trend_Handler.find_product_trends(Main.product_codes)
           
        Trend_Handler.find_trends(Main.database,Main.country_codes,Main.product_codes,True)
           
        Trend_Handler.find_trends(Main.database,Main.country_codes,Main.product_codes,False)
           
        Main.findInterestingTrends()
          
        Main.findLikelyErrors()
          
        for i in SQL_Handler.readAll('errors',database):
            print(i)
          
        print (SQL_Handler.readAll('errors',database))
         
        self.assertEqual(SQL_Handler.readAll('errors',database), (('710811-five_year_trend|LAO~Import$833333.333333', -1.0), 
      ('710811-five_year_trend|MAR~Import$833333.333333', -1.0), ('710811-five_year_trend|MDG~Import$833333.333333', -1.0),
      ('710811-five_year_trend|MEX~Import$833333.333333', -1.0), ('710811-five_year_trend|MLT~Import$2500000.0', 9.0), 
      ('710811-five_year_trend|NPL~Import$833333.333333', -1.0), ('710811-five_year_trend|NRU~Import$1833333.33333', -0.5),
      ('710811-LAO|2014~Import', -1.0), ('710811-long_trend|LAO~Import$833333.333333', -1.0), ('710811-long_trend|MAR~Import$833333.333333', -1.0),
      ('710811-long_trend|MDG~Import$833333.333333', -1.0), ('710811-long_trend|MEX~Import$833333.333333', -1.0), 
      ('710811-long_trend|MLT~Import$2500000.0', 9.0), ('710811-long_trend|NPL~Import$833333.333333', -1.0), 
      ('710811-long_trend|NRU~Import$1833333.33333', -0.5), ('710811-MAR|2014~Import', -1.0), ('710811-MDG|2014~Import', -1.0), 
      ('710811-MEX|2014~Import', -1.0), ('710811-MLT|2009~Import', -0.642857), ('710811-MLT|2010~Import', -0.642857), 
      ('710811-MLT|2011~Import', -0.642857), ('710811-MLT|2012~Import', -0.642857), ('710811-MLT|2013~Import', -0.642857), 
      ('710811-MLT|2014~Import', 9.0), ('710811-NPL|2014~Import', -1.0), ('710811-NRU|2014~Import', -0.5), 
      ('710811-one_year_trend|LAO~Import$500000.0', -1.0), ('710811-one_year_trend|MAR~Import$500000.0', -1.0),
      ('710811-one_year_trend|MDG~Import$500000.0', -1.0), ('710811-one_year_trend|MEX~Import$500000.0', -1.0), 
      ('710811-one_year_trend|MLT~Import$5500000.0', 9.0), ('710811-one_year_trend|NPL~Import$500000.0', -1.0), 
      ('710811-one_year_trend|NRU~Import$1500000.0', -0.5), ('710811-three_year_trend|LAO~Import$750000.0', -1.0),
      ('710811-three_year_trend|MAR~Import$750000.0', -1.0), ('710811-three_year_trend|MDG~Import$750000.0', -1.0), 
      ('710811-three_year_trend|MEX~Import$750000.0', -1.0), ('710811-three_year_trend|MLT~Import$3250000.0', 9.0),
      ('710811-three_year_trend|NPL~Import$750000.0', -1.0), ('710811-three_year_trend|NRU~Import$1750000.0', -0.5)))
 
 
 
    def test_find_interesting_trends(self):
             
        Main.multi_thread(Main.getFiles,first_year,last_year,arg=file_name)
             
        Main.multi_thread(Main.populate_values,first_year,last_year,num=1)
     
        Trend_Handler.find_product_trends(Main.product_codes)
             
        Trend_Handler.find_trends(Main.database,Main.country_codes,Main.product_codes,True)
             
        Trend_Handler.find_trends(Main.database,Main.country_codes,Main.product_codes,False)            
             
        Main.findInterestingTrends()
           
        for i in SQL_Handler.readAll('interesting_trends',database):
            print(i)
            
        print ("Interesting Trends:", SQL_Handler.readAll('interesting_trends',database))
     
        self.assertEqual(SQL_Handler.readAll('interesting_trends',database), 
                            (('710811-five_year_trend$48000001.0', 0.074074), ('710811-five_year_trend|LAO~Import$833333.333333', -1.0),
                             ('710811-five_year_trend|MAR~Import$833333.333333', -1.0), ('710811-five_year_trend|MDG~Import$833333.333333', -1.0),
                             ('710811-five_year_trend|MEX~Import$833333.333333', -1.0), ('710811-five_year_trend|MLT~Import$2500000.0', 9.0), 
                             ('710811-five_year_trend|NPL~Import$833333.333333', -1.0), ('710811-five_year_trend|NRU~Import$1833333.33333', -0.5), 
                             ('710811-five_year_trend|USA~Export$40500000.0', 0.075), ('710811-long_trend$48000001.0', 0.074074), 
                             ('710811-long_trend|LAO~Import$833333.333333', -1.0), ('710811-long_trend|MAR~Import$833333.333333', -1.0), 
                             ('710811-long_trend|MDG~Import$833333.333333', -1.0), ('710811-long_trend|MEX~Import$833333.333333', -1.0), 
                             ('710811-long_trend|MLT~Import$2500000.0', 9.0), ('710811-long_trend|NPL~Import$833333.333333', -1.0), 
                             ('710811-long_trend|NRU~Import$1833333.33333', -0.5), ('710811-long_trend|USA~Export$40500000.0', 0.075),
                             ('710811-one_year_trend$80000001.0', 0.072289), ('710811-one_year_trend|LAO~Import$500000.0', -1.0), 
                             ('710811-one_year_trend|MAR~Import$500000.0', -1.0), ('710811-one_year_trend|MDG~Import$500000.0', -1.0),
                             ('710811-one_year_trend|MEX~Import$500000.0', -1.0), ('710811-one_year_trend|MLT~Import$5500000.0', 9.0), 
                             ('710811-one_year_trend|NPL~Import$500000.0', -1.0), ('710811-one_year_trend|NRU~Import$1500000.0', -0.5), 
                             ('710811-one_year_trend|USA~Export$41500000.0', 0.075), ('710811-three_year_trend$53333334.3333', 0.07362), 
                             ('710811-three_year_trend|LAO~Import$750000.0', -1.0), ('710811-three_year_trend|MAR~Import$750000.0', -1.0),
                             ('710811-three_year_trend|MDG~Import$750000.0', -1.0), ('710811-three_year_trend|MEX~Import$750000.0', -1.0),
                             ('710811-three_year_trend|MLT~Import$3250000.0', 9.0), ('710811-three_year_trend|NPL~Import$750000.0', -1.0), 
                             ('710811-three_year_trend|NRU~Import$1750000.0', -0.5), ('710811-three_year_trend|USA~Export$40750000.0', 0.075)))
    
        
    def test_find_trends(self):
            
        Main.multi_thread(Main.getFiles,first_year,last_year,arg=file_name)
            
        Main.multi_thread(Main.populate_values,first_year,last_year,num=1)
            
        Trend_Handler.find_product_trends(Main.product_codes)
            
        Trend_Handler.find_trends(Main.database,Main.country_codes,Main.product_codes,True)
                    
        self.assertNotEqual(SQL_Handler.readAll('trends',database), ())
    
            
    def test_find_product_trends(self):
             
        Main.multi_thread(Main.getFiles,first_year,last_year,arg=file_name)
             
        Main.multi_thread(Main.populate_values,first_year,last_year,num=1)
     
        Trend_Handler.find_product_trends(Main.product_codes)
             
        self.assertNotEqual(SQL_Handler.readAll('product_trends',database), ())
     
    def test_populate_values(self):
             
        Main.multi_thread(Main.getFiles,first_year,last_year,arg=file_name)
             
        Main.multi_thread(Main.populate_values,first_year,last_year,num=1)
     
        self.assertNotEqual(SQL_Handler.readAll('product_values_2014',database), ())  
                
    def test_getfiles(self):
                       
        Main.multi_thread(Main.getFiles,first_year,last_year,arg=file_name)
     
        self.assertNotEqual(SQL_Handler.readAll('Im_Ex_Data_2014',database), ())
     
    def test_initialize(self):
        tables = SQL_Handler.getTables(database)
              
        correct_tables = (('Im_Ex_Data_2009',), ('Im_Ex_Data_2010',), ('Im_Ex_Data_2011',),
                          ('Im_Ex_Data_2012',), ('Im_Ex_Data_2013',), ('Im_Ex_Data_2014',), 
                          ('country_product_values_2009',), ('country_product_values_2010',),
                          ('country_product_values_2011',), ('country_product_values_2012',), 
                          ('country_product_values_2013',), ('country_product_values_2014',), 
                          ('country_values_2009',), ('country_values_2010',), ('country_values_2011',),
                          ('country_values_2012',), ('country_values_2013',), ('country_values_2014',), 
                          ('errors',), ('interesting_trends',), ('product_trends',), ('product_values_2009',), 
                          ('product_values_2010',), ('product_values_2011',), ('product_values_2012',),
                          ('product_values_2013',), ('product_values_2014',), ('trends',))
     
        self.assertEqual(tables,correct_tables)
             
        self.assertEqual(SQL_Handler.readAll('Im_Ex_Data_2014',database), ())
     
        self.assertEqual(SQL_Handler.readAll('product_values_2014',database), ())
     
        self.assertEqual(SQL_Handler.readAll('product_trends',database), ())
                     
        self.assertEqual(SQL_Handler.readAll('trends',database), ())
             
        self.assertEqual(SQL_Handler.readAll('interesting_trends',database), ())
     
        self.assertEqual(SQL_Handler.readAll('errors',database), ())
    
     
    def test_tester(self):
        self.assertEqual(1, 1)
      
   
   
    def tearDown(self):
        print("End Test")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()