import React, { useState, useEffect } from "react";
import Button from "@mui/material/Button";
import { UserCircle } from "./UserCircle/UserCircle";
import classes from "./Header.module.css";
import DropdownMenu from "./DropdownMenu/DropdownMenu";
import { Link } from "react-router-dom";
import Divider from "@mui/material/Divider";

const Header: React.FC = () => {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [openDropdown, setOpenDropdown] = useState<boolean>(false);

  useEffect(() => {
    const token = localStorage.getItem("token");
    if (token) {
      setIsLoggedIn(true);
    }
  }, []);

  if (!isLoggedIn) {
    return (
      <div className={classes.headercontainer}>
        <div className={classes.header}>
          <div className={classes.leftheader}>
            <Link className={classes.logo} to={"/"}></Link>
          </div>
          <div className={classes.rightheader}>
            <div className={classes.search}>
              <Button variant="outlined">
                <Link to={"/login"}>Đăng nhập</Link>
              </Button>
            </div>
          </div>
        </div>
        <Divider></Divider>
      </div>
    );
  } else {
    return (
      <div className={classes.headercontainer}>
        <div className={classes.header}>
          <div className={classes.leftheader}>
            <Link to="/" className={classes.logo}></Link>
          </div>
          <div className={classes.rightheader}>
            <UserCircle
              className={classes.userCircle}
              onClick={() => {
                setOpenDropdown((openDropdown) => !openDropdown);
              }}
            />
            {openDropdown && <DropdownMenu />}
          </div>
        </div>
        <Divider></Divider>
      </div>
    );
  }
};

export default Header;
