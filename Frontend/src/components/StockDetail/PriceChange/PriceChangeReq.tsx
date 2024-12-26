const API_BASE_URL = "http://127.0.0.1:5000";
export const getPriceChange = async (symbol: string): Promise<string> => {
  try {
    const response = await fetch(`${API_BASE_URL}/stock/change/${symbol}`);
    const data = await response.text();
    return data;
  } catch (error) {
    console.error("Error fetching stock image:", error);
    throw error;
  }
};
