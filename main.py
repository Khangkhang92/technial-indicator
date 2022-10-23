import pandas as pd
import pandas_ta as ta
from webdatamodel.model import StockPrice
from sqlalchemy.orm import Session
from base.db.session import ScopedSession
from fastapi import FastAPI,Depends
from sqlalchemy import select,asc

app = FastAPI()


@app.get("/")
async def root(session: Session = Depends(ScopedSession)):
    close = []
    rough_datas = session.execute(select(StockPrice).where(StockPrice.code == 'FLC').order_by(asc(StockPrice.time))).scalars().fetchall()
    for item in rough_datas:
        close.append(item.close)
    close_series = pd.Series(close)
    rsi = ta.rsi(close_series).fillna(0).tolist()
    sma7= ta.ma("sma", close_series, length=7 ).fillna(0).tolist()
    sma20= ta.ma("sma", close_series, length=20 ).fillna(0).tolist()
    sma200= ta.ma("sma", close_series, length=200 ).fillna(0).tolist()
    macd= ta.macd(close_series).fillna(0)
    bbands= ta.bbands(close_series).fillna(0)


    results = []
    for index,rsi in enumerate(rsi):
        result = {}
        result["date"] = rough_datas[index].time.date()
        result["rsi"] = rsi
        result["sma20"] = sma7[index]
        result["sma50"] = sma20[index]
        result["sma200"] = sma200[index]
        result["macd"] = macd.loc[index].to_dict()
        result["bbands"] = bbands.loc[index].to_dict()
        results.append(result) 
    return results         
    