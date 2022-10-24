import pandas as pd
import pandas_ta as ta
from sqlalchemy.orm import Session
from base.db.session import ScopedSession
from fastapi import FastAPI, Depends
from webdatamodel.model import IndicatorsResults


app = FastAPI()


def indicators_caculate(rough_data_by_ticker, session):
    close = []
    indicator_results = []
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
        sma = {}
        sma["sma7"] = sma7[index]
        sma["sma60"] = sma60[index]
        indicator_result = IndicatorsResults(
            stock_id=rough_data_by_ticker.stock_id,
            date=close_price_d1[index]["date"],
            rsi=rsi,
            sma=sma,
            macd=macd.loc[index].to_dict(),
            bbands=bbands.loc[index].to_dict(),
        )
        indicator_results.append(indicator_result)
        print(index)
    session.bulk_save_objects(indicator_results)
    session.commit()
    print(f"caculate for {rough_data_by_ticker.stock_id} is ok")


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

    for rough_data_by_ticker in stockprice_alldata:
        if len(rough_data_by_ticker.d1) >= 60:
            indicators_caculate(rough_data_by_ticker, session)
    return {"msg": "sucessful saving data"}
