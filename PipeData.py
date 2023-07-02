import requests
import pandas as pd
import re
import pymongo
from bs4 import BeautifulSoup
import time
import datetime
cliant = pymongo.MongoClient()
db = cliant['roundBours']


def GetTse():
    tseTable = pd.read_excel(requests.get('http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d=0').content,header=2)
    tseTable = tseTable[~tseTable['نماد'].apply(lambda x: bool(re.search(r'\d', str(x))))]
    now = datetime.datetime.now()
    tseTable['datetime'] = now
    tseTable = tseTable.to_dict('records')
    db['historyTse'].insert_many(tseTable)


def GetMsgTse():
    msg = requests.get('http://old.tsetmc.com/Loader.aspx?ParTree=151313&Flow=0').text
    soup = BeautifulSoup(msg, 'html.parser')
    table = soup.find('table')
    df = pd.read_html(str(table))[0]
    dff = pd.DataFrame([df.columns]).rename(columns={0:df.columns[0],1:df.columns[1]})
    df = pd.concat([dff,df]).rename(columns={df.columns[0]:'title',df.columns[1]:'time'}).reset_index()
    df['context'] = ''
    for i  in df.index:
        if i%2 != 0:
            df['context'][i-1] = df['title'][i]
            df = df.drop(index=i)
    df = df.to_dict('records')
    db['msgTse'].insert_many(df)



