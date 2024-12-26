import { Box } from "@mui/material";
import { Link } from "react-router-dom";
import styled from "styled-components";

export const TopBarContainer = styled.div`
  width: 100%;
  height: 50px;
  background-color: white;
  position: sticky;
  top: 0;
  z-index: 999;
  box-shadow: rgba(0, 0, 0, 0.16) 0px 3px 6px, rgba(0, 0, 0, 0.23) 0px 3px 6px;
`;

export const TopBarWrapper = styled.div`
  height: 100%;
  padding: 0px 20px;
  display: flex;
  align-items: center;
  justify-content: space-between;
`;

export const TopLeft = styled.div``;

export const LogoImage = styled.img`
  width: 116px;
  height: auto;
`;

export const TopRight = styled.button`
  display: flex;
  align-items: center;
  background: transparent;
  border: none;
  cursor: pointer;
  width: 3rem;
`;

export const LoginLink = styled(Link)`
  color: black;
  text-decoration: none;
  font-size: 1rem;
`;

export const DropdownContainer = styled(Box)`
  position: absolute;
  top: 3.5rem;
  right: 1rem;
  width: 14rem;
  border-color: rgb(156 163 175);
  border-radius: 0.5rem;
  z-index: 20;
  background-color: rgb(247 247 247);
  padding: 0.5rem;

  .link {
    display: flex;
    margin-bottom: 0.5rem;
    width: 100%;
  }

  .avatar {
    padding-bottom: 0.25rem;
    margin-right: 1rem;
  }
`;
