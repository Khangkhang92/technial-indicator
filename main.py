from unittest import result
import pandas as pd
import pandas_ta as ta
from sqlalchemy.orm import Session
from base.db.session import ScopedSession
from fastapi import FastAPI, Depends


app = FastAPI()


def indicators_caculate(rough_data_by_ticker):
    close = []
    results = []
    close_price_d1 = rough_data_by_ticker.d1
    for day in close_price_d1:
        close.append(day["close"])
    close_series = pd.Series(close)

    rsi = ta.rsi(close_series).fillna(0).tolist()
    sma7 = ta.ma("sma", close_series, length=7).fillna(0).tolist()
    sma60 = ta.ma("sma", close_series, length=60).fillna(0).tolist()
    macd = ta.macd(close_series).fillna(0)
    bbands = ta.bbands(close_series).fillna(0)

    for index, rsi in enumerate(rsi):
        result = {}
        result["stock_id"] = rough_data_by_ticker.stock_id
        result["ticker"] = rough_data_by_ticker.ticker
        result["date"] = close_price_d1[index]["date"]
        result["rsi"] = rsi
        result["sma7"] = sma7[index]
        result["sma60"] = sma60[index]
        result["macd"] = macd.loc[index].to_dict()
        result["bbands"] = bbands.loc[index].to_dict()
        results.append(result)
    return results


@app.get("/")
async def root(session: Session = Depends(ScopedSession)):

    stmt = """
                select
                    stock_id,
                    code as ticker ,
                    jsonb_agg(jsonb_build_object('close', close , 'date', date(time)) order by time asc) d1
                from
                    stock_prices
                group by
                    stock_id ,
                    code
    """

    stockprice_alldata = session.execute(stmt).fetchall()

    result_list = []
    for rough_data_by_ticker in stockprice_alldata:
        results_by_ticker = indicators_caculate(rough_data_by_ticker)
        result_list.extend(results_by_ticker)
    print(len(result_list))
    return result_list
