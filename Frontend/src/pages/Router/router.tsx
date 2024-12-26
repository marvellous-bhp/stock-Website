import { Route, Routes } from "react-router-dom";
import { routerType } from "../../types/Router/router.types";
import publicPages from "./publicPages";
import privatePages from "./privatePages";
import { useEffect, useState } from "react";

const Router = () => {
  const [isLogedIn, setIsLogedIn] = useState(false);
  let token = localStorage.getItem("token");
  useEffect(() => {
    if (token) {
      setIsLogedIn(true);
    }
  }, []);
  const pagesPublic = publicPages.map(
    ({ path, title, element }: routerType) => {
      return <Route key={title} path={`/${path}`} element={element} />;
    }
  );
  const pagesPrivate = privatePages.map(
    ({ path, title, element }: routerType) => {
      return (
        isLogedIn && <Route key={title} path={`/${path}`} element={element} />
      );
    }
  );

  return <Routes>{[pagesPublic, pagesPrivate]}</Routes>;
};

export default Router;
