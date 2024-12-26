import React, { useEffect, useState } from "react";
import classes from "./Discuss.module.css";
import { Button } from "@mui/material";
import { toast } from "react-toastify";
import { API_BASE_URL } from "../../../services/api/baseURL";

interface CommentProps {
  id: string;
  username: string;
  time: string;
  commenttext: string;
  tokenUser: string;
  onUpdate: () => void; // Add onUpdate prop
}

const Discuss: React.FC<CommentProps> = ({
  id,
  username,
  time,
  commenttext,
  tokenUser,
  onUpdate,
}) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editedComment, setEditedComment] = useState(commenttext);
  const [isComment, setIsComment] = useState(false);
  const userToken = localStorage.getItem("token");

  useEffect(() => {
    if (userToken === tokenUser) {
      setIsComment(true);
    } else {
      setIsComment(false);
    }
  }, []);

  const handleEditClick = () => {
    setIsEditing(true);
  };

  const handleSaveCommentEdit = () => {
    if (userToken) {
      fetch(`${API_BASE_URL}/comment/update/${id}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${userToken}`,
        },
        body: JSON.stringify({
          id: id,
          commenttext: editedComment,
          token: userToken,
        }),
      })
        .then((response) => {
          if (response.ok) {
            toast.success("Bình luận thành công!");
            setEditedComment("");
            setIsEditing(false);
            onUpdate();
          } else {
            toast.error("Lỗi bình luận.");
          }
        })
        .catch((error) => {
          console.error("Error sending comment: checkk", error);
          toast.error("Lỗi.");
        });
    } else {
      toast.error("Vui lòng đăng nhập.");
    }
  };

  const handleCancelClick = () => {
    setEditedComment(commenttext);
    setIsEditing(false);
  };

  const handleDelClick = () => {
    if (userToken) {
      fetch(`${API_BASE_URL}/comment/delete/${id}`, {
        method: "DELETE",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${userToken}`,
        },
      })
        .then((response) => {
          if (response.ok) {
            toast.success("Xóa bình luận thành công!");
            onUpdate(); // Gọi hàm onUpdate để cập nhật danh sách bình luận
          } else {
            toast.error("Lỗi khi xóa bình luận.");
          }
        })
        .catch((error) => {
          console.error("Error deleting comment:", error);
          toast.error("Lỗi khi xóa bình luận.");
        });
    } else {
      toast.error("Vui lòng đăng nhập để xóa bình luận.");
    }
  };

  return (
    <div className={classes.discuss}>
      <div className={classes.commentinfo}>
        <span className={classes.username}>{username}</span>
        <span className={classes.time}>{time}</span>
      </div>
      <div className={classes.commenttext}>
        {isEditing ? (
          <textarea
            className={classes.textcomment}
            value={editedComment}
            onChange={(event) => setEditedComment(event.target.value)}
            placeholder="Nhập bình luận của bạn"
          />
        ) : (
          <div>{commenttext}</div>
        )}
      </div>
      <div className={classes.feature}>
        {isEditing ? (
          <>
            <Button className={classes.save} onClick={handleSaveCommentEdit}>
              Lưu
            </Button>
            <Button className={classes.cancel} onClick={handleCancelClick}>
              Hủy
            </Button>
          </>
        ) : (
          <Button className={classes.edit} onClick={handleEditClick}>
            <span>Sửa</span>
          </Button>
        )}
        <Button className={classes.delete} onClick={handleDelClick}>
          <span>Xóa</span>
        </Button>
      </div>
    </div>
  );
};

export default Discuss;
