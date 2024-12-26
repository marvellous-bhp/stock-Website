import axios from "axios";
import { toast } from "react-toastify";
const API_BASE_URL = "http://127.0.0.1:5000";
export const getCompanyData = async (symbol: string): Promise<any> => {
  try {
    const response = await axios.get(`${API_BASE_URL}/stock/${symbol}`);
    return response.data;
  } catch (error) {
    console.error("Error fetching company data:", error);
    throw error;
  }
};

export const isfollowstock = async (symbol: string): Promise<any> => {
  const userToken = localStorage.getItem("token");
  if (userToken) {
    try {
      const response = await fetch(`${API_BASE_URL}/user/follow/${symbol}`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${userToken}`,
        },
      });
      if (response.ok) {
        const data = await response.json();
        return data.is_follow;
      } else {
        toast.error("follow feature response false");
      }
    } catch (error) {
      toast.error("Error getting follow status:");
    }
  }
};

export const changefollowstatus = async (
  symbol: string,
  is_follow: boolean
): Promise<any> => {
  const userToken = localStorage.getItem("token");
  if (userToken) {
    try {
      const response = await fetch(`${API_BASE_URL}/user/follow/${symbol}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${userToken}`,
        },
        body: JSON.stringify({ isfollow: is_follow }),
      });
      if (response.ok) {
        const data = await response.json();
        return data.is_follow;
      } else {
        toast.error("follow feature response false");
      }
    } catch (error) {
      toast.error("Error getting follow status:");
    }
  }
};
