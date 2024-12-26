import numpy as np


def cusum_filter(dataset, threshold):
    pos_dates, neg_dates = [], []
    pos_sum, neg_sum = 0, 0
    dataset["differences"] = dataset["close"].diff()
    for i, r in dataset.iloc[1:].iterrows():
        pos_sum = max(0, pos_sum + r["differences"])
        neg_sum = min(0, neg_sum + r["differences"])
        if pos_sum > threshold:
            pos_sum = 0
            pos_dates.append(i)
        elif neg_sum < -threshold:
            neg_sum = 0
            neg_dates.append(i)
    return pos_dates, neg_dates


def detect_peaks(y, lag, threshold, influence):
    signals = np.zeros(len(y))
    filtered_y = np.copy(y)
    avg_filter = np.mean(y[:lag])
    std_filter = np.std(y[:lag])

    for i in range(lag, len(y)):
        if np.abs(y[i] - avg_filter) > threshold * std_filter:
            if y[i] > avg_filter:
                signals[i] = 1
            else:
                signals[i] = -1
            if i < len(y) - 1:
                filtered_y[i + 1] = (
                    influence * y[i + 1] + (1 - influence) * filtered_y[i]
                )
        else:
            signals[i] = 0
            if i < len(y) - 1:
                filtered_y[i + 1] = y[i + 1]

        avg_filter = np.mean(filtered_y[max(i - lag + 1, 0): i + 1])
        std_filter = np.std(filtered_y[max(i - lag + 1, 0): i + 1])

    return signals


def label_tripple_barrier_method(data, length):
    data["tri_barr_point"] = 0
    for i in range(length, len(data) - 1):
        volatility = data["close"].iloc[i - length: i].std()

        upper_barrier = data["close"].iloc[i] + volatility
        lower_barrier = data["close"].iloc[i] - volatility

        if data.iloc[i + 1, 4] > upper_barrier:
            data.loc[i, "tri_barr_point"] = 1
        elif data.iloc[i + 1, 4] < lower_barrier:
            data.loc[i, "tri_barr_point"] = -1
    return data


def predict(df):
    dataema = df.tail(365)
    dataema["ema_point"] = 0
    dataema["EMA5"] = dataema.close.ewm(span=5, adjust=False).mean()
    dataema["EMA20"] = dataema.close.ewm(span=20, adjust=False).mean()
    dataema["EMA50"] = dataema.close.ewm(span=50, adjust=False).mean()
    dataema.loc[
        (dataema["EMA5"] < dataema["EMA20"])
        & (dataema["EMA5"].shift(1) >= dataema["EMA20"].shift(1)),
        "ema_point",
    ] = 1
    dataema.loc[
        (dataema["EMA5"] > dataema["EMA20"])
        & (dataema["EMA5"].shift(1) <= dataema["EMA20"].shift(1)),
        "ema_point",
    ] = -1

    dataema.loc[
        (dataema["EMA20"] < dataema["EMA50"])
        & (dataema["EMA20"].shift(1) >= dataema["EMA50"].shift(1)),
        "ema_point",
    ] = 1
    dataema.loc[
        (dataema["EMA20"] > dataema["EMA50"])
        & (dataema["EMA20"].shift(1) <= dataema["EMA50"].shift(1)),
        "ema_point",
    ] = -1

    threshold_value = 1000
    dataframe = df
    positive_dates, negative_dates = cusum_filter(dataframe, threshold_value)
    df["cusum_point"] = 0
    df.loc[df.index.isin(positive_dates), "cusum_point"] = 1
    df.loc[df.index.isin(negative_dates), "cusum_point"] = -1
    df["ema_point"] = dataema["ema_point"]

    lag = 10
    threshold = 2.5
    influence = 0.3
    df["peak_point"] = detect_peaks(df["close"], lag, threshold, influence)

    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)

    data_copy = df.copy()
    data_label = label_tripple_barrier_method(data_copy, 20)
    return data_label


def combine_labels(row):
    if row.sum() >= 1:
        return 1
    elif row.sum() <= -1:
        return -1
    else:
        return 0
