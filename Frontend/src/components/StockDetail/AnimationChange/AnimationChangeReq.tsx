import axios from "axios";
const API_BASE_URL = "http://127.0.0.1:5000";
export const getAllStocks = async () => {
  try {
    const response = await axios.get(`${API_BASE_URL}/getAllStocks`);
    return response.data;
  } catch (error) {
    throw error;
  }
};
