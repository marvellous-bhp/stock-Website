import Chart, {
  Series,
  Aggregation,
  ArgumentAxis,
  Grid,
  Label,
  ValueAxis,
  Margin,
  Legend,
  Tooltip,
  CommonSeriesSettings,
} from "devextreme-react/chart";
import RangeSelector, {
  Size,
  Scale,
  Chart as RsChart,
  ValueAxis as RsValueAxis,
  Series as RsSeries,
  Aggregation as RsAggregation,
  Behavior,
  RangeSelectorTypes,
} from "devextreme-react/range-selector";

import React, { FC, useCallback, useState, useEffect } from "react";
import { getStockImage } from "./CandlestickReq";
import classes from "./Candlestick.module.css";

interface CandlestickProps {
  symbol: string;
}
const Candlestick: React.FC<CandlestickProps> = ({ symbol }) => {
  const [chartImage, setChartImage] = useState<any | null>(null);
  const [visualRange, setVisualRange] = useState({});
  // const [SMAdata, setSMAdata] = useState<string  | null>(null);
  const updateVisualRange = useCallback(
    (e: RangeSelectorTypes.ValueChangedEvent) => {
      setVisualRange(e.value);
    },
    [setVisualRange]
  );
  useEffect(() => {
    const fetchImage = async () => {
      try {
        const imageJson = await getStockImage(symbol);
        const stockDict = JSON.parse(imageJson);
        // setSMAdata(stockDict.chart_SMA);
        setChartImage(stockDict.chart_data);
      } catch (error) {
        console.error("Error fetching stock image:", error);
      }
    };

    fetchImage();
  }, [symbol]);
  return (
    <div className={classes.chart}>
      <Chart id="zoomedChart" dataSource={chartImage} title="">
        <Series
          type="candlestick"
          openValueField="Open"
          highValueField="High"
          lowValueField="Low"
          closeValueField="Close"
          argumentField="Date"
        >
          <Aggregation enabled={true} />
        </Series>
        <ArgumentAxis
          visualRange={visualRange}
          valueMarginsEnabled={false}
          argumentType="datetime"
        >
          <Grid visible={true} />
          <Label visible={false} />
        </ArgumentAxis>
        <ValueAxis valueType="numeric" />
        <Margin right={10} />
        <Legend visible={false} />
        <Tooltip enabled={true} />
      </Chart>
      <RangeSelector dataSource={chartImage} onValueChanged={updateVisualRange}>
        <Size height={120} />
        <RsChart>
          <RsValueAxis valueType="numeric" />
          <RsSeries type="line" valueField="Open" argumentField="Date">
            <RsAggregation enabled={true} />
          </RsSeries>
        </RsChart>
        <Scale
          placeholderHeight={20}
          minorTickInterval="day"
          tickInterval="month"
          valueType="datetime"
          aggregationInterval="week"
        />
        <Behavior snapToTicks={false} valueChangeMode="onHandleMove" />
      </RangeSelector>
      {/* <div>
      {SMAdata && (
        <img src={`data:image/png;base64,${SMAdata}`} alt="SMA Chart"  style={{ width: '900px', height: 'auto' }}/>
      )}
      </div> */}
    </div>
  );
};

export default Candlestick;