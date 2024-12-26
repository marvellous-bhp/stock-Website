import React, { useState, useEffect } from "react";
import classes from "./Predict.module.css";
import { PostPredictText, getPredictText, getimagePredict } from "./PredictReq";

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
    const userId = "111";
    await PostPredictText(userId, stockCode, editedContent);
    setIsEditing(false);

    alert("Nhận định về cổ phiếu đã được cập nhật");
    fetchData();
  };

  const handleCancelClick = () => {
    setIsEditing(false);
  };

  const handleTextareaChange = (
    event: React.ChangeEvent<HTMLTextAreaElement>
  ) => {
    setEditedContent(event.target.value);
  };

  const fetchData = async () => {
    try {
      // const data = await getPredictText(symbol);
      // setPredictText(data);
      // setEditedContent(data?.textPrediction || "");

      const imagepredict = await getimagePredict(symbol);
      const image = JSON.parse(imagepredict);
      console.log(image);
      setPredictImage(image.img_predict);
    } catch (error) {
      console.error("Error fetching company data:", error);
    }
  };

  useEffect(() => {
    fetchData();
  }, [symbol]);

  return (
    <div className={classes.predict}>
      <div className={classes.chartpredict}>
        {PredictImage && (
          <img
            src={`data:image/png;base64,${PredictImage}`}
            alt="SMA Chart"
            style={{ width: "600px", height: "auto" }}
          />
        )}
        <div>
          <ul>
            <li>Đường line cong lên: xu hướng tăng</li>
            <li>Đường line cong xuống: xu hướng giảm</li>
            <li>Đường line nằm ngang: không có dự báo về xu hướng</li>
          </ul>
        </div>
      </div>
      <div className={classes.contentpredict}>
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
          <span className={classes.delete}>Xóa</span>
        </div>
      </div>
    </div>
  );
};

export default Predict;