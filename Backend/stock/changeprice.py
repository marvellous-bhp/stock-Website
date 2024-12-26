import pandas as pd


def changeinfo(df):
    df['dates'] = pd.to_datetime(df['dates'])
    df = df.sort_values(by='dates', ascending=True)
    change_7d = df['close'].pct_change(periods=7).iloc[-1] * 100
    change_20d = df['close'].pct_change(periods=20).iloc[-1] * 100
    change_1m = df['close'].pct_change(periods=30).iloc[-1] * 100
    change_3m = df['close'].pct_change(periods=3*30).iloc[-1] * 100
    change_1y = df['close'].pct_change(periods=365).iloc[-1] * 100

    return {"change_7d": f"{change_7d:.2f}%", "change_20d": f"{change_20d:.2f}%", "change_1m": f"{change_1m:.2f}%", "change_3m": f"{change_3m:.2f}%", "change_1y": f"{change_1y:.2f}%"}


def changeprice(stock_infos):
    dates = []
    opens = []
    highs = []
    lows = []
    closes = []
    volumes = []
    for stock_info in stock_infos:
        dates.append(stock_info.date.strftime('%Y-%m-%d'))
        opens.append(stock_info.open)
        highs.append(stock_info.high)
        lows.append(stock_info.low)
        closes.append(stock_info.close)
        volumes.append(stock_info.volume)
    data = {
        "dates": dates,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
    }
    df = pd.DataFrame(data)

    change_price = changeinfo(df)
    return change_price
