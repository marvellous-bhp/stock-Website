from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from stock.labeling.label import predict, combine_labels
from io import BytesIO
import base64


def chart_stock(stock_infos):
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
    data_label = predict(df)
    label_combine_feature = ["ema_point",
                             "cusum_point", "peak_point", "tri_barr_point"]
    data_label["combined_label"] = data_label[label_combine_feature].apply(
        combine_labels, axis=1
    )
    chart_data = {
        "dates": df["dates"].tolist(),
        "open": df["open"].tolist(),
        "high": df["high"].tolist(),
        "low": df["low"].tolist(),
        "close": df["close"].tolist(),
        "volume": df["volume"].tolist(),
    }
    dataSource = []
    for i in range(len(chart_data["dates"])):
        data_point = {
            "Date": chart_data["dates"][i],
            "Open": chart_data["open"][i],
            "High": chart_data["high"][i],
            "Low": chart_data["low"][i],
            "Close": chart_data["close"][i],
            "Volume": chart_data["volume"][i],
        }
        dataSource.append(data_point)

    close_prices = data_label["close"].values
    combined_labels = data_label["combined_label"].values
    last_label = combined_labels[-1]
    num_points = 20
    x_values = np.linspace(
        len(close_prices) - 1, len(close_prices) - 1 + num_points, num_points
    )
    plt.figure(figsize=(6, 3))
    plt.plot(close_prices, label="Close Prices")
    if last_label == 1:
        y_values_parabol = (
            5 * (x_values - len(close_prices) + 1) ** 2 + close_prices[-1]
        )
        plt.plot(
            x_values,
            y_values_parabol,
            color="green",
            linestyle="--",
            label="line prediction",
        )
    elif last_label == -1:
        y_values_parabol = (
            -5 * (x_values - len(close_prices) + 1) ** 2 + close_prices[-1]
        )
        plt.plot(
            x_values,
            y_values_parabol,
            color="red",
            linestyle="--",
            label="line prediction",
        )
    else:
        y_values_parabol = np.full_like(x_values, fill_value=close_prices[-1])
        plt.plot(x_values, y_values_parabol,
                 linestyle="--", label="line prediction")

    plt.xlabel("Time")
    plt.ylabel("Close Prices")
    plt.title("Trend Prediction")
    plt.legend()
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png')
    img_buffer.seek(0)
    plt.close()
    img_predict = base64.b64encode(img_buffer.getvalue()).decode("utf-8")

    return dataSource, img_predict
