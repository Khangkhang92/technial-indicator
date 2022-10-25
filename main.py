import pandas as pd
import pandas_ta as ta
from webdatamodel.model import StockPrice
from sqlalchemy.orm import Session
from base.db.session import ScopedSession
from fastapi import FastAPI, Depends
from sqlalchemy import select, asc

app = FastAPI()


@app.get("/")
async def root(session: Session = Depends(ScopedSession)):
    close = []
    rough_datas = (
        session.execute(
            select(StockPrice)
            .where(StockPrice.code == "TCB")
            .order_by(asc(StockPrice.time))
        )
        .scalars()
        .fetchall()
    )
    for item in rough_datas:
        close.append(item.close)
    close_series = pd.Series(close)
    rsi = ta.rsi(close_series).fillna(0).tolist()
    rsi_series = ta.rsi(close_series)
    sma9 = ta.ma("sma", close_series, length=9).fillna(0)
    sma20 = ta.ma("sma", close_series, length=20).fillna(0)
    macd = ta.macd(close_series).fillna(0)
    bbands = ta.bbands(close_series).fillna(0)
    golden_cross = ta.cross(sma9,sma20).fillna(0).tolist()
    death_cross = ta.cross(sma9,sma20,above = False).fillna(0).tolist()
    
    print("golden_cross!")  
    for index, item in enumerate(rsi_series):
        if golden_cross[index] > 0:
          print(f"sma9 = {sma9[index]},sma20 = {sma20[index]},date = {rough_datas[index].time.date()}")
    print("death_cross!")      
    for index, item in enumerate(rsi_series):     
        if death_cross[index] > 0:
          print(f"sma9 = {sma9[index]},sma20 = {sma20[index]},date = {rough_datas[index].time.date()}")  
      
   

    
