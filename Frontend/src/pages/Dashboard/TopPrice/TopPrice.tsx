import React, { useEffect, useState } from "react";
import classes from "./TopPrice.module.css";
import { Link } from "react-router-dom";
import { getTopStocks } from "./TopPriceReq";
const TopStock = () => {
  const [TopIncrease, setTopStock] = useState<any | null>(null);
  const [TopDecrease, setTopDecrease] = useState<any | null>(null);
  useEffect(() => {
    const fetchData = async () => {
      try {
        const data = await getTopStocks();
        setTopStock(data.top_increase);
        setTopDecrease(data.top_decrease);
      } catch (error) {
        console.error("Error fetching all stock data:", error);
      }
    };
    fetchData();
  }, []);

  const [showFirstDiv, setShowFirstDiv] = useState(true);

  useEffect(() => {
    const intervalId = setInterval(() => {
      setShowFirstDiv((prev) => !prev);
    }, 3000);

    return () => clearInterval(intervalId);
  }, []);
  const [isLogedIn, setIsLogedIn] = useState(false);
  let token = localStorage.getItem("token");
  useEffect(() => {
    if (token) {
      setIsLogedIn(true);
    }
  }, []);

  return (
    <div className={classes.containertopstock}>
      {TopIncrease && (
        <div className={classes.topstock}>
          <div
            className={showFirstDiv ? classes.slideenter : classes.slideleave}
          >
            {showFirstDiv ? (
              <div className={classes.topincrease}>
                <div className={classes.toptitle}>Top tăng giá</div>
                <div className={classes.field}>
                  <div className={classes.itemfield}>
                    <span>Mã cổ phiếu</span>
                  </div>
                  <div className={classes.itemfield}>
                    <span>% thay đổi giá</span>
                  </div>
                  <div className={classes.itemfield}>
                    <span>Khối lượng giao dịch</span>
                  </div>
                </div>
                {TopIncrease.map(
                  (
                    stock: { symbol: string; percent: string; volume: string },
                    index: number
                  ) => {
                    const percentValue = parseFloat(stock.percent);
                    return (
                      <Link to={isLogedIn ? `/stock/${stock.symbol}` : "#"}>
                        <div key={index}>
                          <div className={classes.ItemStock}>
                            <span className={classes.symbol}>
                              {stock.symbol}
                            </span>
                            <span className={classes.percent}>
                              {percentValue >= 0
                                ? `+${percentValue.toFixed(2)}%`
                                : `${percentValue.toFixed(2)}%`}
                            </span>
                            <span className={classes.volume}>
                              {stock.volume}
                            </span>
                          </div>
                        </div>
                      </Link>
                    );
                  }
                )}
              </div>
            ) : (
              <div className={classes.topdecrease}>
                <div className={classes.toptitle}>Top giảm giá</div>
                <div className={classes.field}>
                  <div className={classes.itemfield}>
                    <span>Mã cổ phiếu</span>
                  </div>
                  <div className={classes.itemfield}>
                    <span>% thay đổi giá</span>
                  </div>
                  <div className={classes.itemfield}>
                    <span>Khối lượng giao dịch</span>
                  </div>
                </div>
                {TopDecrease.map(
                  (
                    stock: { symbol: string; percent: string; volume: string },
                    index: number
                  ) => {
                    const percentValue = parseFloat(stock.percent);
                    return (
                      <Link to={isLogedIn ? `/stock/${stock.symbol}` : "#"}>
                        <div key={index}>
                          <div className={classes.ItemStock}>
                            <span className={classes.symbol}>
                              {stock.symbol}
                            </span>
                            <span className={classes.percent}>
                              {percentValue >= 0
                                ? `+${percentValue.toFixed(2)}%`
                                : `${percentValue.toFixed(2)}%`}
                            </span>
                            <span className={classes.volume}>
                              {stock.volume}
                            </span>
                          </div>
                        </div>
                      </Link>
                    );
                  }
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default TopStock;
