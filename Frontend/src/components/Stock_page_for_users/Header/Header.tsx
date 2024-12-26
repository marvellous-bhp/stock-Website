import SearchBox from "../SearchBox/SearchBox";
import { UserCircle } from "../../Header/UserCircle/UserCircle";
import classes from "./Header.module.css";

const Header = () => {
  return (
    <div className={classes.header}>
      <div className={classes.leftheader}>
        <div className={classes.logo}></div>
        <div className={classes.homepage}>Trang chủ</div>
      </div>
      <div className={classes.rightheader}>
        <div className={classes.darkmode}></div>
        <div className={classes.search}>
          <SearchBox />
        </div>
        <UserCircle className={classes.userCircle} />
      </div>
    </div>
  );
};
export default Header;
