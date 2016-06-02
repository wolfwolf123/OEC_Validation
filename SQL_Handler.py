'''
Created on Jun 2, 2016

@author: chris
'''
import MySQLdb

database = ""

class SQL_Handler(object):


    def init(self,database_name):
        global database
        database = database_name
        
    def make_table(self,year,table_name):
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
                    print (Exception)
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
    def insert(self,table_name, label, value):
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
    def getTables(self):
#         print ("SELECT * FROM %s WHERE LABEL = '%s'" % (table_name,label))
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db=database)
        mysql_cur = db.cursor()
        mysql_cur.execute("show tables")
        
        output = mysql_cur.fetchall()
        db.close()


        return output
    def read(self,table_name,label):
#         print ("SELECT * FROM %s WHERE LABEL = '%s'" % (table_name,label))
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db=database)
        mysql_cur = db.cursor()
        mysql_cur.execute("SELECT * FROM %s WHERE LABEL = '%s'" % (table_name,label))
        
        output = mysql_cur.fetchall()
        db.close()


        return output
    def readAll (self,table_name):
        
        db = MySQLdb.connect(host="localhost",user="root",passwd="enKibc43",db=database)
        mysql_cur = db.cursor()
        mysql_cur.execute("SELECT * FROM %s" % (table_name))
        
        output = mysql_cur.fetchall()
        
        db.close()
        
        return output
    def readAllCountry (self,table_name,country):
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


        