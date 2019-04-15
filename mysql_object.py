"""
@author: hejia
"""
import pandas as pd
import sqlalchemy
import fix_yahoo_finance as yf
import datetime
import myList
from sqlalchemy.types import String
#engine=sqlalchemy.create_engine('mysql+pymysql://root:hejia123@127.0.0.1:3306/stocks')

class mysqlOperation(object):
    def __init__(self):
        self.engine=sqlalchemy.create_engine('mysql+pymysql://root:hejia123@127.0.0.1:3306/stocks')
        
    def writeStockInfo(self,stockList,startDate=datetime.date(2017,6,1)):
        if type(stockList)==str: # if just input a single stock
            stockList=[stockList]
            
        for stock in stockList:
            try:
                stock_data=yf.download(stock,start=startDate)
                stock_data.to_sql(
                name=stock.lower(),
                con=self.engine,
                index=True,
                if_exists='replace'
                )
            except Exception as e:

                print(e)
                print('cannot write info of stock {0}'.format(stock))
                
    def readStcokInfo(self,stock):
        try:
            sql='select * from '+stock
            stockInfo=pd.read_sql(sql,self.engine,index_col='Date') 
            return stockInfo               
        except Exception as e:
            print(e)
            print('have exception reading from database {0}'.format(stock))
                
        
    
    def readDelta(self,stock):
        try:
            sql="select * from delta where name='{}'".format(stock)
            stockDelta=pd.read_sql(sql,self.engine,index_col='name')
            return stockDelta
        except Exception as e:
            print(e)
            print('exception when read stock {0} from table delta'.format(stock))

    def writeDf(self,df,stockName,index_name):
        try:
            df.to_sql(
                    name=stockName,
                    con=self.engine,
                    index=True,
                    if_exists='replace',
                    dtype={index_name:String(255)}
                    )
        except Exception as e:
            print(e)
            print('cannot write df to dababase')
    def writeTable(self,tableName):
        pass
     
if __name__=='__main__':
    #update data to mysql database
    mysql=mysqlOperation()
    mysql.writeStockInfo(myList.stock_list)
    
    # read data from database
    stockBasic=[]
    for name in myList.stock_list:
        data=mysql.readStcokInfo(name)
        mean=data['Close'].mean()
        
        minmium=data['Low'].min()
        maxmium=data['High'].max()
        volitility=((data['High']-data['Low'])/data['Close']).mean()
        vol_value_mean=(data['High']-data['Low']).mean()
        vol_value_median=(data['High']-data['Low']).median()
        currentToHighest=(data['Close'].iloc[-1]-maxmium)/maxmium
        currentToLowest=(data['Close'].iloc[-1]-minmium)/minmium
        newRow=[name,mean,minmium,maxmium,volitility,vol_value_mean,vol_value_median,currentToHighest,currentToLowest]
        for i in range(1,len(newRow)):
            newRow[i]=round(newRow[i],2)
        
        stockBasic.append(newRow)
    name="name mean minimum maximum volitility vol_value_mean vol_value_median currentToHighest currentToLowest".split()
    df=pd.DataFrame(stockBasic,columns=name)
    df.set_index('name',inplace=True)
    mysql.writeDf(df,'delta','name')    
            
            
        
    