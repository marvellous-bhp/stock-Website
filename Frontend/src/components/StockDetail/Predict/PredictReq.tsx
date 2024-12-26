import axios from "axios";
const API_BASE_URL = "http://127.0.0.1:5000";
import React, { useState } from "react";
import { toast } from "react-toastify";
export const PostPredictText = async (
  stockCode: string,
  textPrediction: string
): Promise<any> => {
  const userToken = localStorage.getItem("token");
  if (userToken) {
    fetch(`${API_BASE_URL}/admin/predictions/${stockCode}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${userToken}`,
      },
      body: JSON.stringify({ textpredict: textPrediction }),
    })
      .then((response) => {
        if (response.ok) {
          toast.success("Cập nhật dự đoán thành công!");
        } else {
          toast.info("Bạn không có quyền cập nhật");
        }
      })
      .catch((error) => {
        console.error("Error sending comment:", error);
        toast.error("Lỗi.");
      });
  } else {
    toast.info("Vui lòng đăng nhập để bình luận.");
  }
};
export const deletePredictText = async (stockCode: String): Promise<any> => {
  const userToken = localStorage.getItem("token");
  if (userToken) {
    fetch(`${API_BASE_URL}/admin/predictions/${stockCode}`, {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${userToken}`,
      },
    })
      .then((response) => {
        if (response.ok) {
          toast.success("Xóa dự đoán thành công!");
        } else {
          toast.error("Bạn không có quyền xóa dự đoán hoặc có lỗi xảy ra.");
        }
      })
      .catch((error) => {
        console.error("Error deleting prediction:", error);
        toast.error("Lỗi khi xóa dự đoán.");
      });
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
    return data;
  } catch (error) {
    console.error("Error fetching stock data:", error);
    throw error;
  }
};
