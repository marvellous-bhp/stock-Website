import { toast } from "react-toastify";
import axios from "axios";

const API_BASE_URL = "http://127.0.0.1:5000";
const userToken = localStorage.getItem("token");

export const crawlDataStock = async () => {
  if (!userToken) {
    toast.error("You do not have permission.");
    return false;
  }

  try {
    const response = await axios.get(  
      `${API_BASE_URL}/crawl_data`,
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${userToken}`,
        },
      }
    );
    
    console.log("Response:", response);
    return response.data;
  } catch (error: any) {
    if (error.response) {
      if (error.response.status === 401) {
        toast.error("Unauthorized. Please log in again.");
        localStorage.removeItem("token");
        window.location.href = "/login";
      } else {
        toast.error(`Crawl error: ${error.response.data.message || 'Unknown error'}`);
      }
    } else if (error.request) {
      toast.error("No response received from server.");
    } else {
      toast.error("An error occurred while crawling data.");
    }
    
    console.error("Error:", error);
    return false;
  }
};