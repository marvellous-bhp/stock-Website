import React, { useState, useEffect } from "react";
import classes from "./CompanyInfo.module.css";
import { Star } from "@mui/icons-material";
import { ToastContainer, toast } from "react-toastify";
import {
  changefollowstatus,
  getCompanyData,
  isfollowstock,
} from "./CompanyInfoReq";

interface CompanyInfoProps {
  symbol: string;
  follow: boolean;
}

const CompanyInfo: React.FC<CompanyInfoProps> = ({ symbol, follow }) => {
  const [companyData, setCompanyData] = useState<any | null>(null);
  const [isFollow, setIsFollow] = useState<any | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const data = await getCompanyData(symbol);
        const isfollowdata = await isfollowstock(symbol);
        setCompanyData(data);
        setIsFollow(isfollowdata);
      } catch (error) {
        console.error("Error fetching company data:", error);
      }
    };

    fetchData();
  }, [symbol]);
  const handlefollow = async () => {
    try {
      const isfollowdata = await changefollowstatus(symbol, isFollow);
      setIsFollow(isfollowdata);
      if (isFollow) {
        toast.success("unfollowed");
      } else {
        toast.success("followed");
      }
    } catch (error) {
      console.error("Error change follow data:", error);
    }
  };
  return (
    <div className={classes.companyinfo}>
      <div className={classes.namecompany}>
        {companyData ? companyData.company_name : "Loading..."}
      </div>
      <div className={classes.symbol}>
        {symbol}
        <div
          className={`${
            isFollow !== null && isFollow ? classes.follow : classes.unfollow
          }`}
          onClick={handlefollow}
        >
          <span>{isFollow}</span>
          <Star />
        </div>
      </div>
    </div>
  );
};

export default CompanyInfo;
