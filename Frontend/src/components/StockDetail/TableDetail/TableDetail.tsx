import classes from "./TableDetail.module.css";
import React, { useState, useEffect } from "react";
import { getStockdata } from "./TableDetailReq";
interface DetailProps {
  symbol: string;
}
const TableDetail: React.FC<DetailProps> = ({ symbol }) => {
  const [StockData, setStockData] = useState<any | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const data = await getStockdata(symbol);
        setStockData(data);
      } catch (error) {
        console.error("Error fetching company data:", error);
      }
    };

    fetchData();
  }, [symbol]);

  return (
    <div className={classes.tabledetail}>
      <div className={classes.header}>
        <span>Ngày</span>
        <span className={classes.datetime}>
          {StockData
            ? new Date(StockData.date).toLocaleDateString()
            : "Loading..."}
        </span>
      </div>
      <div className={classes.open}>
        <span>Giá mở cửa</span>
        <span className={classes.info}>
          {StockData ? StockData.open : "Loading..."}
        </span>
      </div>
      <div className={classes.high}>
        <span>Giá cao</span>
        <span className={classes.info}>
          {StockData ? StockData.high : "Loading..."}
        </span>
      </div>
      <div className={classes.low}>
        <span>Giá thấp</span>
        <span className={classes.info}>
          {StockData ? StockData.low : "Loading..."}
        </span>
      </div>
      <div className={classes.close}>
        <span>Giá đóng cửa</span>
        <span className={classes.info}>
          {StockData ? StockData.previous_close_price : "Loading..."}
        </span>
      </div>
    </div>
  );
};

export default TableDetail;
