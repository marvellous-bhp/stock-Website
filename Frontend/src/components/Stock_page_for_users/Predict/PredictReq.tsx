
import axios from "axios";
const API_BASE_URL = "http://127.0.0.1:5000";


export const PostPredictText = async (userid:string,stockCode: string, textPrediction: string): Promise<string> => {
    try {
      const response = await axios.post(`${API_BASE_URL}/admin/predictions/${stockCode}`, {
        userid:userid,
        textPrediction: textPrediction,
      });
  
      // Assuming the server responds with the prediction id
      const predictionId: string = response.data.predictionId;
      
      return predictionId;
    } catch (error) {
      console.error("Error posting prediction text:", error);
      throw error;
    }
  };
export const getPredictText = async (symbol: string): Promise<any> => {
  try {
    const response = await axios.get(
      `${API_BASE_URL}/user/predictions/${symbol}`
    );
    return response.data;
  } catch (error) {
    console.error("Error fetching company data:", error);
    throw error;
  }
};
export const getimagePredict = async (symbol: string): Promise<any> => {
  try {
    const response = await fetch(`${API_BASE_URL}/stock-chart/${symbol}`);
    const data = await response.text();
    console.log(data);
    return data;
  } catch (error) {
    console.error("Error fetching stock data:", error);
    throw error;
  }
};
