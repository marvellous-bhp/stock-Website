
import axios from 'axios';

const API_BASE_URL = 'http://127.0.0.1:5000';

export const getStockdata = async (symbol: string): Promise<any> => {
  try {
    const response = await axios.get(`${API_BASE_URL}/stock/${symbol}`);
    return response.data;
  } catch (error) {
    console.error('Error fetching company data:', error);
    throw error;
  }
};
