import { memo, useEffect, useState } from "react";
import type { FC } from "react";
import {
  DataGrid,
  GridColDef,
  GridValueGetterParams,
  GridToolbar,
} from "@mui/x-data-grid";
import { Button } from "@mui/material";
import resets from "../../components/_resets.module.css";
import classes from "./Dashboard.module.css";
import Header from "../../components/Header/Header";
import { getStockbyDate, getFollowStock } from "../../services/api/stock.api";

import { crawlDataStock } from "../../services/api/crawl.api";
import { Link } from "react-router-dom";
import { ToastContainer, toast } from "react-toastify";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import Checkbox from "@mui/material/Checkbox";
import FormGroup from "@mui/material/FormGroup";
import FormControlLabel from "@mui/material/FormControlLabel";
import dayjs from "dayjs";
import TopPrice from "./TopPrice/TopPrice";

interface Props {
  className?: string;
}

export const Dashboard: FC<Props> = memo(function Dashboard(props = {}) {
  const [stocks, setStocks] = useState([]);
  const currentDate = dayjs();
  const [selectedDate, setSelectedDate] = useState<Date>(currentDate);
  const [isLogedIn, setIsLogedIn] = useState(false);
  const [isFollow, setIsFollow] = useState([false]);
  const [tokenTimeout, setTokenTimeout] = useState<NodeJS.Timeout | null>(null);
  const [isLoading, setIsLoading] = useState(false);  // New loading state
  const [isCrawlRequested, setIsCrawlRequested] = useState(false);
  const [isCrawlSuccess, setIsCrawlSuccess] = useState(false);
  let token = localStorage.getItem("token");

  useEffect(() => {
    if (token) {
      setIsLogedIn(true);
    }
  }, []);

  const [windowSize, setWindowSize] = useState({
    width: window.innerWidth,
    height: window.innerHeight,
  });

  useEffect(() => {
    const fetchStockData = (date: Date) => {
      if (!isFollow) {
        console.log(isFollow, "isfollow getFollowStock");
        getFollowStock(date)
          .then((data) => {
            setStocks(data);
          })
          .catch((error) => {
            console.log("Error fetching stocks:", error);
          });
      } else {
        console.log(isFollow, "isfollow getStockbyDate");
        getStockbyDate(date)
          .then((data) => {
            setStocks(data);
          })
          .catch((error) => {
            console.log("Error fetching stocks:", error);
          });
      }
    };

    fetchStockData(selectedDate);

    const handleResize = () => {
      setWindowSize({
        width: window.innerWidth,
        height: window.innerHeight,
      });
    };

    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
    };
  }, [selectedDate, isFollow]); // Add windowSize to the dependency array

  // handle toast
  useEffect(() => {
    const isLoggedIn = localStorage.getItem("isLoggedIn");
    if (isLoggedIn === "true") {
      toast.success("Welcome");

      const timeout = setTimeout(() => {
        localStorage.removeItem("isLoggedIn");
      }, 20 * 1000);

      setTokenTimeout(timeout);
    }

    return () => {
      if (tokenTimeout) {
        clearTimeout(tokenTimeout);
      }
    };
  }, []);

  const handleDateChange = (date: Date | null) => {
    setSelectedDate(date || new Date());
  };

  const handleShowFollow = () => {
    setIsFollow((prevIsFollow) => !prevIsFollow);
  };

  // Function to handle the API call for /api/crawl
  const handleCrawl = async () => {
    if (isLoading) return; // Ngăn chặn việc gọi request khi đang loading
  
    setIsLoading(true);
    setIsCrawlRequested(false); // Đặt lại ngay lập tức để tránh nhiều lần gọi
  
    try {
      const data = await crawlDataStock(); 
      const check_crawl = data.success
      console.log('check_crawl',check_crawl);
      
      if (check_crawl) {
        toast.success(data.message);
        setIsCrawlSuccess(true);
      } else {
        toast.error(data.message);
        setIsCrawlSuccess(false);
      }
    } catch (error) {
      toast.error("An error occurred while crawling. " + error);
      setIsCrawlSuccess(false);
    } finally {
      setIsLoading(false);
    }
  };
  
  // Loại bỏ useEffect không cần thiết
  // Thay vào đó, sử dụng trực tiếp handleCrawl khi nhấn nút
  const handleCrawlClick = () => {
    if (!isLoading) {
      handleCrawl();
    }
  };

  const columns: GridColDef[] = [
    {
      field: "Mã chứng khoán",
      headerName: "Mã CK",
      width: windowSize.width * 0.09,
      valueGetter: (params: GridValueGetterParams) =>
        `${params.row.symbol || ""}`,
    },
    {
      field: "Giá trần",
      headerName: "Giá trần",
      width: windowSize.width * 0.08,
      valueGetter: (params: GridValueGetterParams) => {
        if (typeof params.row.high === "number") {
          return params.row.high.toFixed(0);
        } else {
          return "0";
        }
      },
    },
    {
      field: "Giá đáy",
      headerName: "Giá đáy",
      width: windowSize.width * 0.08,
      valueGetter: (params: GridValueGetterParams) => {
        if (typeof params.row.low === "number") {
          return params.row.low.toFixed(0);
        } else {
          return "0";
        }
      },
    },
    {
      field: "Giá mở cửa",
      headerName: "Giá mở cửa",
      width: windowSize.width * 0.08,
      valueGetter: (params: GridValueGetterParams) => {
        if (typeof params.row.open === "number") {
          return params.row.open.toFixed(0);
        } else {
          return "0";
        }
      },
    },
    {
      field: "Giá đóng cửa hôm qua",
      headerName: "Giá đóng cửa hôm qua",
      width: windowSize.width * 0.08,
      valueGetter: (params: GridValueGetterParams) => {
        if (typeof params.row.previous_close_price === "number") {
          return params.row.previous_close_price.toFixed(0);
        } else {
          return "0";
        }
      },
    },
    {
      field: "Tăng giảm",
      headerName: "+/-",
      width: windowSize.width * 0.06,
      align: "right",
      renderCell: (params: GridValueGetterParams) => {
        const value =
          typeof params.row.diffirence === "number" ? params.row.diffirence : 0;
        let textColor = "white";

        if (value > 0) {
          textColor = "green";
        } else if (value < 0) {
          textColor = "red";
        } else {
          textColor = "#FFC300";
        }

        return <span style={{ color: textColor }}>{value.toFixed(0)}</span>;
      },
    },
    {
      field: "Tỉ lệ %",
      headerName: "%",
      width: windowSize.width * 0.05,
      align: "right",
      renderCell: (params: GridValueGetterParams) => {
        const value =
          typeof params.row.percent === "number" ? params.row.percent : 0;
        let textColor = "white";

        if (value > 0) {
          textColor = "green";
        } else if (value < 0) {
          textColor = "red";
        } else {
          textColor = "#FFC300";
        }

        return (
          <span style={{ color: textColor }}>
            {params.row.percent.toFixed(2) + "%"}
          </span>
        );
      },
    },
    {
      field: "Tổng khối lượng",
      headerName: "Tổng khối lượng",
      width: windowSize.width * 0.08,
      valueGetter: (params: GridValueGetterParams) => {
        if (typeof params.row.volume === "number") {
          return params.row.volume.toFixed(0);
        } else {
          return "0";
        }
      },
      align: "right",
      cellClassName: "vollume",
    },
    {
      field: "Hành động",
      headerName: "Hành động",
      sortable: false,
      width: windowSize.width * 0.09,
      renderCell: (params) =>
        isLogedIn && (
          <Button variant="outlined">
            <Link to={`/stock/${params.row.symbol}`} style={{ color: "black" }}>
              Chi tiết
            </Link>
          </Button>
        ),
    },
  ];

  switch (true) {
    case windowSize.width <= 576:
      columns.splice(1, 7);
      columns.forEach((column) => {
        column.width = windowSize.width * 0.3;
      });
      break;
    case windowSize.width <= 992:
      columns.splice(1, 4);
      columns.forEach((column, index) => {
        if (index !== 0 && index !== columns.length - 1) {
          column.width = windowSize.width * 0.1;
        } else {
          column.width = windowSize.width * 0.195;
        }
      });
      break;
    case windowSize.width <= 1200:
      columns.splice(1, 2);
      columns.forEach((column, index) => {
        if (index !== 0 && index !== columns.length - 1) {
          column.width = windowSize.width * 0.085;
        } else {
          column.width = windowSize.width * 0.13;
        }
      });
      break;
    default:
      break;
  }

  const rows = stocks;

  return (
    <div className={`${resets.storybrainResets} ${classes.root}`}>
      <div className={classes.container}>
        <ToastContainer />
        <div className={classes.dashboard}>
          <Header />
          <div className={classes.main}>
            <div className={classes.topblock}>
              <Button 
                variant="contained" 
                color="primary" 
                onClick={handleCrawl} 
                disabled={isLoading}
              >
                {isLoading ? "Đang crawl..." : "Bắt đầu crawl"}
              </Button>
              {isCrawlSuccess && (
                <div style={{ 
                  color: 'green', 
                  marginLeft: '10px', 
                  display: 'inline-block' 
                }}>
                  Crawl dữ liệu thành công!
                </div>
              )}
              {!isCrawlSuccess && isLoading && (
                <div style={{ 
                  color: 'orange', 
                  marginLeft: '10px', 
                  display: 'inline-block' 
                }}>
                  Đang xử lý...
                </div>
              )}
            </div>
            {isLogedIn && (
              <FormGroup
                aria-label="position"
                row
                className={classes.datetimebox}
              >
                <LocalizationProvider dateAdapter={AdapterDayjs}>
                  <DatePicker
                    value={selectedDate}
                    onChange={handleDateChange}
                    label="Chọn ngày"
                  />
                </LocalizationProvider>
                <FormControlLabel
                  value={isFollow}
                  control={<Checkbox onChange={handleShowFollow} />}
                  label="Đang theo dõi"
                  labelPlacement="start"
                />
              </FormGroup>
            )}
            <div className={classes.tableclose}>
              <DataGrid
                rows={rows}
                columns={columns}
                style={{ width: windowSize.width * 0.7, margin: "0 auto" }}
                initialState={{
                  pagination: {
                    paginationModel: { page: 0, pageSize: 10 },
                  },
                }}
                pageSizeOptions={[5, 10]}
                slots={{ toolbar: GridToolbar }}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
});
