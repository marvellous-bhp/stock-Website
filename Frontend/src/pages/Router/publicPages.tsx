import { Password_Login } from "../Password_Login/Password_Login";
import { SignUp } from "../SignUp/SignUp";
import { Dashboard } from "../Dashboard/Dashboard";
import { routerType } from "../../types/Router/router.types";

const publicPages: routerType[] = [
  {
    path: "/login",
    element: <Password_Login />,
    title: "Login",
  },
  {
    path: "/signup",
    element: <SignUp />,
    title: "/signup",
  },
  {
    path: "/",
    element: <Dashboard />,
    title: "Dashboard",
  },
];
export default publicPages;
