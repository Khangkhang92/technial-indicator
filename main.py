import pandas as pd
import pandas_ta as ta
from sqlalchemy.orm import Session
from sqlalchemy import select, update, desc, and_
from base.db.session import ScopedSession
from fastapi import FastAPI, Depends
from webdatamodel.model import IndicatorsResults, AlertCriteria
from sqlalchemy.dialects.postgresql import insert


app = FastAPI()


def _get_stock_prices(session):

    stmt = """
                with t1 as (select
                                stock_id,
                                code as ticker ,
                                jsonb_agg(jsonb_build_object('close', close , 'date', date(time)) order by time asc) d1
                            from
                                stock_prices
                            group by
                                stock_id ,
                                code
                            ), 
                t2 as (
                            select max(d1[-1]::json->>'date') as last_date from t1  
                            )
                            select * from t1 where d1[-1]::json->>'date' = (select * from t2)
            """

    stockprice_alldata = session.execute(stmt).fetchall()

    return stockprice_alldata


def _indicators_signal(
    rsi_list,
    close_series,
    sma9_series,
    sma20_series,
    bbands_df,
    macd_df,
    rough_data_by_ticker,
    session,
):
    close_price_d1 = rough_data_by_ticker.d1
    sma_golden_cross = ta.cross(sma9_series, sma20_series).fillna(0).tolist()
    sma_death_cross = (
        ta.cross(sma9_series, sma20_series, above=False).fillna(0).tolist()
    )
    bbupper_series = pd.Series(bbands_df["BBU_5_2.0"])
    bblower_series = pd.Series(bbands_df["BBL_5_2.0"])
    bbands_golden_cross = ta.cross(close_series, bbupper_series).fillna(0).tolist()
    bbands_death_cross = (
        ta.cross(close_series, bblower_series, above=False).fillna(0).tolist()
    )
    macd_df = ta.macd(close_series).fillna(0)
    macd_series = pd.Series(macd_df["MACD_12_26_9"])
    macd_signal_series = pd.Series(macd_df["MACDs_12_26_9"])
    macd_golden_cross = ta.cross(macd_series, macd_signal_series).fillna(0).tolist()
    macd_death_cross = (
        ta.cross(macd_series, macd_signal_series, above=False).fillna(0).tolist()
    )

    sma_golden_cross = ta.cross(sma9_series, sma20_series).fillna(0).tolist()
    sma_death_cross = (
        ta.cross(sma9_series, sma20_series, above=False).fillna(0).tolist()
    )

    last_index = len(close_price_d1) - 1

    sma_cross = macd_cross = bbands_cross = None
    latest_rsi = None
    if rsi_list[last_index] > 70:
        latest_rsi = "overbought"
    elif rsi_list[last_index] < 30:
        latest_rsi = "oversold"
    if sma_golden_cross[last_index] > 0:
        print("sma_golden_cross", rough_data_by_ticker.ticker)
        print(
            f"sma9 = {sma9_series[last_index]},sma20 = {sma20_series[last_index]},date = {close_price_d1[last_index]['date']}"
        )
        sma_cross = "golden_cross"
    elif sma_death_cross[last_index] > 0:
        print("sma_death_cross", rough_data_by_ticker.ticker)
        print(
            f"sma9 = {sma9_series[last_index]},sma20 = {sma20_series[last_index]},date = {close_price_d1[last_index]['date']}"
        )
        sma_cross = "death_cross"
    if macd_golden_cross[last_index] > 0:
        print("macd_golden_cross", rough_data_by_ticker.ticker)
        print(f"date = {close_price_d1[last_index]['date']}")
        macd_cross = "golden_cross"
    elif macd_death_cross[last_index] < 0:
        print("macd_death_cross", rough_data_by_ticker.ticker)
        print(f"date = {close_price_d1[last_index]['date']}")
        macd_cross = "death_cross"
    if bbands_golden_cross[last_index] > 0:
        print("bbands_golden_cross", rough_data_by_ticker.ticker)
        print(f"date = {close_price_d1[last_index]['date']}")
        bbands_cross = "golden_cross"
    elif bbands_death_cross[last_index] < 0:
        print("bbands_death_cross", rough_data_by_ticker.ticker)
        print(f"date = {close_price_d1[last_index]['date']}")
        bbands_cross = "death_cross"
    if sma_cross is not None or macd_cross is not None or bbands_cross is not None:
        stmt = (
            update(AlertCriteria)
            .where(AlertCriteria.stock_id == rough_data_by_ticker.stock_id)
            .values(
                rsi=latest_rsi,
                sma_cross=sma_cross,
                macd_cross=macd_cross,
                bbands_cross=bbands_cross,
            )
        )

        session.execute(stmt)


def create_signals(rough_data_by_ticker, session):
    close = []
    close_price_d1 = rough_data_by_ticker.d1
    for day in close_price_d1:
        close.append(day["close"])
    close_series = pd.Series(close)
    rsi_list = ta.rsi(close_series).fillna(0).tolist()
    sma9_series = ta.ma("sma", close_series, length=9).fillna(0)
    sma20_series = ta.ma("sma", close_series, length=20).fillna(0)
    bbands_df = ta.bbands(close_series).fillna(0)
    macd_df = ta.macd(close_series).fillna(0)
    _indicators_signal(
        rsi_list,
        close_series,
        sma9_series,
        sma20_series,
        bbands_df,
        macd_df,
        rough_data_by_ticker,
        session,
    )


def _create_indicators_chart(
    rsi, sma9_series, sma20_series, bbands_df, macd_df, rough_data_by_ticker
):
    indicator_results = []
    close_price_d1 = rough_data_by_ticker.d1
    start = len(rsi) - 60
    for index, rsi in enumerate(rsi):
        if index >= start:
            sma = {}
            sma["sma9"] = sma9_series[index]
            sma["sma20"] = sma20_series[index]
            macd = macd_df.loc[index].to_dict()
            macd["macd"] = macd.pop("MACD_12_26_9")
            macd["signal"] = macd.pop("MACDs_12_26_9")
            macd["histogram"] = macd.pop("MACDh_12_26_9")
            bbands = bbands_df.loc[index].to_dict()
            bbands["upper"] = bbands.pop("BBU_5_2.0")
            bbands["mid"] = bbands.pop("BBM_5_2.0")
            bbands["lower"] = bbands.pop("BBL_5_2.0")
            bbands["bandwidth"] = bbands.pop("BBB_5_2.0")
            bbands["percent"] = bbands.pop("BBP_5_2.0")
            # indicator_result = IndicatorsResults(
            #     stock_id=rough_data_by_ticker.stock_id,
            #     date=close_price_d1[index]["date"],
            #     rsi=rsi,
            #     sma=sma,
            #     macd=macd,
            #     bbands=bbands,
            # )
            indicator_result = {}
            indicator_result["stock_id"] = (rough_data_by_ticker.stock_id,)
            indicator_result["date"] = (close_price_d1[index]["date"],)
            indicator_result["rsi"] = (rsi,)
            indicator_result["sma"] = (sma,)
            indicator_result["macd"] = (macd,)
            indicator_result["bbands"] = (bbands,)
            indicator_results.append(indicator_result)

    print(f"caculate for {rough_data_by_ticker.stock_id} is ok")
    return indicator_results


def create_charts(rough_data_by_ticker):
    close = []
    close_price_d1 = rough_data_by_ticker.d1
    for day in close_price_d1:
        close.append(day["close"])
    close_series = pd.Series(close)
    rsi = ta.rsi(close_series).fillna(0).tolist()
    sma9_series = ta.ma("sma", close_series, length=9).fillna(0)
    sma20_series = ta.ma("sma", close_series, length=20).fillna(0)
    bbands_df = ta.bbands(close_series).fillna(0)
    macd_df = ta.macd(close_series).fillna(0)
    return _create_indicators_chart(
        rsi,
        sma9_series,
        sma20_series,
        bbands_df,
        macd_df,
        rough_data_by_ticker,
    )


@app.get("/signals_trading")
async def signals(session: Session = Depends(ScopedSession)):
    stockprice_alldata = _get_stock_prices(session)
    for rough_data_by_ticker in stockprice_alldata:
        if len(rough_data_by_ticker.d1) >= 60:
            create_signals(rough_data_by_ticker, session)


@app.get("/chart")
async def create_indicator_chart(session: Session = Depends(ScopedSession)):
    """
    https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.dml.Insert.on_conflict_do_update
    https://stackoverflow.com/questions/57046011/multiple-inserts-at-a-time-using-sqlalchemy-with-postgres
    """
    results = []
    stockprice_alldata = _get_stock_prices(session)
    for rough_data_by_ticker in stockprice_alldata:
        if len(rough_data_by_ticker.d1) >= 60:
            result = create_charts(rough_data_by_ticker)
            results.extend(result)

    print("saving indicator results")

    insert_stmt = insert(IndicatorsResults, results)
    # session.bulk_save_objects(results)
    # session.commit() indicators_results_un
    # do_update_stmt = insert_stmt.on_conflict_do_update(
    #             constraint='indicators_results_un',
    #             set_=results
    # )
    # do_update_stmt = insert_stmt.on_conflict_do_nothing()
    # session.execute(do_update_stmt)
    return {"msg": "sucessful saving data"}
