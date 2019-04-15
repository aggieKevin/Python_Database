import mysql.connector
from my_stock_list import *
import datetime

mydb=mysql.connector.connect(
    host="localhost",
    user='root',
    passwd='hejia123',
    database='stocks'
        )

# create table
mycursor=mydb.cursor()
create_sql='CREATE TABLE stockList (stockName VARCHAR(255))'
mycursor.execute(create_sql)

# read list to table
for stock in stock_list:
    insert_sql="INSERT INTO stockList (stockName) VALUES (%s)"
    val=(stock,)
    mycursor.execute(insert_sql,val)
    

mydb.commit()  # must do this one, or the data would not be saved  
mycursor.close()
mydb.close()

