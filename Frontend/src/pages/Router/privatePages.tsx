import { Stock_page_for_users } from "../Stock_page_for_users/Stock_page_for_users";
import { UserProfile } from "../UserProfile/UserProfile";
import { routerType } from "../../types/Router/router.types";

const privatePages: routerType[] = [
  {
    path: "/stock/:stocks",
    element: <Stock_page_for_users />,
    title: "Stock detail",
  },
  {
    path: "/userprofile",
    element: <UserProfile />,
    title: "User profile",
  },
];

export default privatePages;
