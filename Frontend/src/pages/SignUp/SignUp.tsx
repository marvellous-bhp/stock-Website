import { memo } from "react";
import type { FC } from "react";
import React, { useState } from "react";
import axios from "axios";
import { useNavigate, Link } from "react-router-dom";
import resets from "../../components/_resets.module.css";
import { ChevronRight } from "../../components/ChevronRight/ChevronRight";
import { EnvelopeLightSolid } from "../../components/EnvelopeLightSolid/EnvelopeLightSolid";
import { IconlyBoldProfileIcon } from "./IconlyBoldProfileIcon.js";
import { Line20Icon } from "./Line20Icon.js";
import { InterfaceEssentialLock_StyleFi } from "../Password_Login/InterfaceEssentialLock_StyleFi/InterfaceEssentialLock_StyleFi";
import { ShapeIcon2 } from "./ShapeIcon2.js";
import { ShapeIcon } from "./ShapeIcon.js";
import classes from "./SignUp.module.css";
import { useSignUpForm } from "../../services/api/authencication.api";
import { ToastContainer } from "react-toastify";

interface Props {
  className?: string;
}
export const SignUp: FC<Props> = memo(function SignUp(props: Props = {}) {
  const {
    signUpForm,
    handleChange,
    signUp,
    fullnameError,
    emailError,
    passwordError,
    confirmPasswordError,
  } = useSignUpForm();
  return (
    <div className={`${resets.storybrainResets} ${classes.root}`}>
      <ToastContainer />
      <div className={classes.line20}>
        <Line20Icon className={classes.icon3} />
      </div>
      <Link className={classes.image10} to={"/"}></Link>

      <div className={classes.registerForm}>
        <div className={classes.frame2}>
          <div className={classes.AngKi}>Đăng kí</div>
        </div>
        <form className={classes.frame3} onSubmit={signUp}>
          <label className={classes.labelRegister}>Họ và tên</label>
          <div
            className={`${classes.rectangle} ${classes.rectangleName} ${
              fullnameError ? classes.inputerror : ""
            }`}
          >
            <div className={classes.iconlyBoldProfile}>
              <IconlyBoldProfileIcon
                className={`${classes.icon4} ${classes.iconName}`}
              />
            </div>
            <input
              className={`${classes.input} ${classes.inputFullname}`}
              onChange={handleChange}
              placeholder="Họ và tên"
              name="fullname"
              value={signUpForm.fullname}
            />
            {fullnameError && (
              <span className={classes.errorMessage}>
                Họ và tên không chứa kí tự số
              </span>
            )}
          </div>

          <label className={classes.labelRegister}>Email</label>
          <div
            className={`${classes.rectangle} ${classes.rectangleEmail} ${
              emailError ? classes.inputerror : ""
            }`}
          >
            <EnvelopeLightSolid
              className={classes.envelopeLightSolid}
              swap={{
                shape: <ShapeIcon className={classes.icon} />,
              }}
            />
            <input
              className={`${classes.input} ${classes.inputEmail}`}
              onChange={handleChange}
              placeholder="Email"
              name="email"
              value={signUpForm.email}
              type="email"
            />
            {emailError && (
              <span className={classes.errorMessage}>Email đã tồn tại</span>
            )}
          </div>

          <label className={classes.labelRegister}>Mật khẩu</label>
          <div
            className={`${classes.rectangle} ${classes.rectanglePassword} ${
              passwordError ? classes.inputerror : ""
            }`}
          >
            <InterfaceEssentialLock_StyleFi
              className={classes.interfaceEssentialLock}
            />
            <input
              onChange={handleChange}
              type="password"
              name="password"
              value={signUpForm.password}
              className={`${classes.input} ${classes.inputPassword}`}
              placeholder="Mật khẩu"
            />
            {passwordError && (
              <span className={classes.errorMessage}>
                Mật khẩu ít nhất 8 ký tự, có ít nhất một kí tự hoa, số và ký tự
                đặc biệt
              </span>
            )}
          </div>
          <label className={classes.labelRegister}>Xác nhận mật khẩu</label>
          <div
            className={`${classes.rectangle} ${classes.rectanglePassword} ${
              confirmPasswordError ? classes.inputerror : ""
            }`}
          >
            <InterfaceEssentialLock_StyleFi
              className={classes.interfaceEssentialLock}
            />
            <input
              onChange={handleChange}
              type="password"
              name="confirmpassword"
              value={signUpForm.confirmpassword}
              className={`${classes.input} ${classes.inputPassword}`}
              placeholder="Nhập lại mật khẩu"
            />
            {confirmPasswordError && (
              <span className={classes.errorMessage}>
                Mật khẩu xác nhận không trùng khớp
              </span>
            )}
          </div>

          <div className={classes.next_BTN}>
            <button className={classes.next_Icon} type="submit">
              <div className={classes.next}>
                <div className={classes.AngNhap2}>Đăng kí</div>
                <div className={classes.icon3}>
                  <ChevronRight />
                </div>
              </div>
            </button>
          </div>
        </form>

        <div className={classes.loginGroup}>
          <div className={classes.line2}></div>
          <div className={classes.loginAction}>
            <Link to="/login" className={classes.banACoTaiKhoan}>
              Bạn đã có tài khoản?
            </Link>
            <Link to="/login" className={classes.AngNhap}>
              Đăng nhập{" "}
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
});
