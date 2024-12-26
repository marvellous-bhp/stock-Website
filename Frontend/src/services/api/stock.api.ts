import axios from "axios";
import { toast } from "react-toastify";
const API_BASE_URL = "http://127.0.0.1:5000";
const userToken = localStorage.getItem("token");
const headers = {
  "Content-Type": "application/json",
  Authorization: `Bearer ${userToken}`,
};

export const getAllStocks = async () => {
  try {
    const response = await axios.get(`${API_BASE_URL}/getAllStocks`);
    return response.data;
  } catch (error) {
    throw error;
  }
};

export const getStockbyDate = async (selectedDate: Date | null) => {
  try {
    if (selectedDate) {
      let year = selectedDate.$y;
      let month = (selectedDate.$M + 1).toString().padStart(2, "0");
      let day = selectedDate.$d.getDate().toString().padStart(2, "0");
      let dateString = year + "-" + month + "-" + day;
      const response = await axios.get(
        `${API_BASE_URL}/getAllStocks/${dateString}`,
        {
          headers: headers,
        }
      );
      return response.data;
    } else {
      console.log("Please select a date");
    }
  } catch (error) {
    console.log("error fetch", error);
    throw error;
  }
};

export const getFollowStock = async (selectedDate: Date | null) => {
  if (userToken) {
    if (selectedDate) {
      let year = selectedDate.$y;
      let month = (selectedDate.$M + 1).toString().padStart(2, "0");
      let day = selectedDate.$d.getDate().toString().padStart(2, "0");
      let dateString = year + "-" + month + "-" + day;

      const response = await axios.get(
        `${API_BASE_URL}/getAllStocks/follow/${dateString}`,
        {
          headers: headers,
        }
      );
      return response.data;
    }
  } else {
    toast.error("Vui lòng đăng nhập.");
  }
};
