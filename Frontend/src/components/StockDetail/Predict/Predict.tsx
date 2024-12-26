import React, { useState, useEffect } from "react";
import classes from "./Predict.module.css";
import {
  PostPredictText,
  getPredictText,
  getimagePredict,
  deletePredictText,
} from "./PredictReq";

interface PredictInfoProps {
  symbol: string;
}
const Predict: React.FC<PredictInfoProps> = ({ symbol }) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editedContent, setEditedContent] = useState<string>("");
  const [PredictText, setPredictText] = useState<any | null>(null);
  const [PredictImage, setPredictImage] = useState<string | null>(null);
  const handleEditClick = () => {
    setIsEditing(true);
    setEditedContent(PredictText?.textPrediction || "");
  };

  const handleSaveClick = async () => {
    const stockCode = symbol;
    await PostPredictText(stockCode, editedContent);
    setIsEditing(false);
    fetchData();
  };

  const handleCancelClick = () => {
    setIsEditing(false);
  };
  const handleDeleteClick = async () => {
    const stockCode = symbol;
    const confirmDelete = window.confirm(
      "Bạn có chắc chắn muốn xóa dự đoán không?"
    );
    if (confirmDelete) {
      await deletePredictText(stockCode);
      setPredictText("");
      fetchData();
    }
  };

  const handleTextareaChange = (
    event: React.ChangeEvent<HTMLTextAreaElement>
  ) => {
    setEditedContent(event.target.value);
  };

  const fetchData = async () => {
    try {
      const imagepredict = await getimagePredict(symbol);
      const image = JSON.parse(imagepredict);
      setPredictImage(image.img_predict);
      const predictext = await getPredictText(symbol);
      setPredictText(predictext);
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  };

  useEffect(() => {
    fetchData();
  }, [symbol]);

  return (
    <div className={classes.predict}>
      <div className={classes.chartpredict}>
        <div className={classes.title}>
          <span>Biểu đồ dự đoán</span>
        </div>
        {PredictImage && (
          <img src={`data:image/png;base64,${PredictImage}`} alt="SMA Chart" />
        )}
      </div>
      <div className={classes.contentpredict}>
        <div className={classes.title}>
          <span>Nhận định</span>
        </div>
        {isEditing ? (
          <textarea
            className={classes.contentbox}
            value={editedContent}
            onChange={handleTextareaChange}
            spellCheck={false}
          />
        ) : (
          <span className={classes.content}>{PredictText?.textPrediction}</span>
        )}
        <div className={classes.updatecontent}>
          {isEditing ? (
            <>
              <span className={classes.save} onClick={handleSaveClick}>
                Lưu
              </span>
              <span className={classes.cancel} onClick={handleCancelClick}>
                Hủy
              </span>
            </>
          ) : (
            <span className={classes.update} onClick={handleEditClick}>
              Sửa nhận định
            </span>
          )}
          <span className={classes.delete} onClick={handleDeleteClick}>
            Xóa
          </span>
        </div>
      </div>
    </div>
  );
};

export default Predict;
