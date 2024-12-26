import React, {  useState,useEffect  } from "react";
import classes from "./CompanyInfo.module.css";
import { Star } from "@mui/icons-material";
import {getCompanyData} from "./CompanyInfoReq"

interface CompanyInfoProps {
  symbol: string;
  follow: boolean;
}

const CompanyInfo: React.FC<CompanyInfoProps> = ({
  symbol,
  follow,
}) => {
  const [companyData, setCompanyData] = useState<any | null>(null);
  const [isFollow, setIsFollow] = useState(follow);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const data = await getCompanyData(symbol);
        setCompanyData(data);
      } catch (error) {
        console.error('Error fetching company data:', error);
      }
    };

    fetchData();
  }, [symbol]);


  const handlefollow = () => {
    setIsFollow(!isFollow);
    // add code
  };
  return (
    <div className={classes.companyinfo}>
      <div className={classes.namecompany}>{companyData ? companyData.company_name : 'Loading...'}</div>
      <div className={classes.symbol}>
        {symbol}
        <div
          className={`${classes.follow} ${isFollow ? classes.unfollow : ""}`}
          onClick={handlefollow}
        >
          <Star />
        </div>
      </div>
    </div>
  );
};

export default CompanyInfo;