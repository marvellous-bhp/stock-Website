import axios from 'axios';

const API_BASE_URL = "http://127.0.0.1:5000";

export const getAllComments = async (symbol: any): Promise<any> => {
    try {
        let param = symbol.stocks
        const response = await axios.get(`${API_BASE_URL}/comment/showAll/${param}`);
        return response.data;
    } catch (error) {
        console.error('Error fetching company data:', error);
        throw error;
    }
  };
