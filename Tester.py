'''
Created on Jun 2, 2016

@author: chris
'''
import unittest
import SQL_Handler
import Main 
    
first_year = 2009

last_year  = 2014


class Tester(unittest.TestCase):


    def setUp(self):    
        print ("Beginnig Test")
        
        Main.initilize(False,False,False,False,False,False,"Test_DB")
         
    def test_tester(self):
        self.assertEqual(1, 1)
    def test_initialize(self):
        tables = SQL_Handler.getTables(Main.getDatabase())
         
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
    def test_getfiles(self):
         
        file_name = r'/home/chris/Downloads/Test_'
         
        Main.multi_thread(Main.getFiles,first_year,last_year,arg=file_name)


        self.assertEqual(1, 1)
     
    def tearDown(self):
        print("End Test")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()