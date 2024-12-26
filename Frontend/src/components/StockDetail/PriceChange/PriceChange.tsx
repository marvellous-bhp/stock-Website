import React, { useState, useEffect } from "react";
import { getPriceChange } from "./PriceChangeReq";
import classes from "./PriceChange.module.css";
import ArrowDropDownIcon from "@mui/icons-material/ArrowDropDown";
import ArrowDropUpIcon from "@mui/icons-material/ArrowDropUp";
interface StockProps {
  symbol: string;
}

const StockChange: React.FC<StockProps> = ({ symbol }) => {
  const [PriceChange, setPreiceChange] = useState<any | null>(null);
  useEffect(() => {
    const fetchData = async () => {
      try {
        const data = await getPriceChange(symbol);
        const stockDict = JSON.parse(data);
        setPreiceChange(stockDict.change_price);
      } catch (error) {
        console.error("Error fetching company data:", error);
      }
    };

    fetchData();
  }, [symbol]);
  return (
    <div className={classes.stockchange}>
      <div className={classes.changetitle}>
        <span>Biến động giá</span>
      </div>
      <div className={classes.list_change}>
        <div className={classes.change}>
          <span>7 ngày</span>
          {PriceChange ? (
            <div>
              {parseFloat(PriceChange.change_7d) < 0 ? (
                <React.Fragment>
                  <ArrowDropDownIcon className={classes.down} />
                  <span className={classes.down}>{PriceChange.change_7d}</span>
                </React.Fragment>
              ) : (
                <React.Fragment>
                  <ArrowDropUpIcon className={classes.up} />
                  <span className={classes.up}>{PriceChange.change_7d}</span>
                </React.Fragment>
              )}
            </div>
          ) : (
            "loading ..."
          )}
        </div>
        <div className={classes.change}>
          <span>20 ngày</span>
          {PriceChange ? (
            <div>
              {PriceChange.change_20d < 0 ? (
                <React.Fragment>
                  <ArrowDropDownIcon className={classes.down} />
                  <span className={classes.down}>{PriceChange.change_20d}</span>
                </React.Fragment>
              ) : (
                <React.Fragment>
                  <ArrowDropUpIcon className={classes.up} />
                  <span className={classes.up}>{PriceChange.change_20d}</span>
                </React.Fragment>
              )}
            </div>
          ) : (
            "loading ..."
          )}
        </div>
        <div className={classes.change}>
          <span>1 tháng</span>
          {PriceChange ? (
            <div>
              {parseFloat(PriceChange.change_1m) < 0 ? (
                <React.Fragment>
                  <ArrowDropDownIcon className={classes.down} />
                  <span className={classes.down}>{PriceChange.change_1m}</span>
                </React.Fragment>
              ) : (
                <React.Fragment>
                  <ArrowDropUpIcon className={classes.up} />
                  <span className={classes.up}>{PriceChange.change_1m}</span>
                </React.Fragment>
              )}
            </div>
          ) : (
            "loading ..."
          )}
        </div>
        <div className={classes.change}>
          <span>3 tháng</span>
          {PriceChange ? (
            <div>
              {parseFloat(PriceChange.change_3m) < 0 ? (
                <React.Fragment>
                  <ArrowDropDownIcon className={classes.down} />
                  <span className={classes.down}>{PriceChange.change_3m}</span>
                </React.Fragment>
              ) : (
                <React.Fragment>
                  <ArrowDropUpIcon className={classes.up} />
                  <span className={classes.up}>{PriceChange.change_3m}</span>
                </React.Fragment>
              )}
            </div>
          ) : (
            "loading ..."
          )}
        </div>
        <div className={classes.change}>
          <span>1 năm</span>
          {PriceChange ? (
            <div>
              {parseFloat(PriceChange.change_1y) < 0 ? (
                <React.Fragment>
                  <ArrowDropDownIcon className={classes.down} />
                  <span className={classes.down}>{PriceChange.change_1y}</span>
                </React.Fragment>
              ) : (
                <React.Fragment>
                  <ArrowDropUpIcon className={classes.up} />
                  <span className={classes.up}>{PriceChange.change_1y}</span>
                </React.Fragment>
              )}
            </div>
          ) : (
            "loading ..."
          )}
        </div>
      </div>
    </div>
  );
};

export default StockChange;
