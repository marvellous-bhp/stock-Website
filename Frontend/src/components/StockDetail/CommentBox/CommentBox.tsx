import React, { useState } from "react";
import { useSpring, animated } from "react-spring";
import classes from "./CommentBox.module.css";
import { useParams } from "react-router-dom";
import { ToastContainer, toast } from "react-toastify";
import { Button } from "@mui/material";

const CommentBox = ({ onUpdateComments }) => {
  const [comment, setComment] = useState("");
  const [isCommenting, setIsCommenting] = useState(false);
  const symbol = useParams();
  const API_BASE_URL = "http://127.0.0.1:5000";

  const slideInAnimation = useSpring({
    opacity: isCommenting ? 1 : 0,
    marginTop: isCommenting ? 0 : -50,
  });

  const handleCommentToggle = () => {
    setIsCommenting(!isCommenting);
  };

  const handleCommentSubmit = () => {
    if (comment.trim() !== "") {
      const userToken = localStorage.getItem("token");
      if (userToken) {
        fetch(`${API_BASE_URL}/comment/create`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${userToken}`,
          },
          body: JSON.stringify({ comment: comment, symbol: symbol }),
        })
          .then((response) => {
            if (response.ok) {
              toast.success("Bình luận thành công!");
              setComment("");
              setIsCommenting(!isCommenting);
              onUpdateComments();
            } else {
              toast.error("Lỗi bình luận.");
            }
          })
          .catch((error) => {
            console.error("Error sending comment:", error);
            toast.error("Lỗi.");
          });
      } else {
        toast.error("Vui lòng đăng nhập để bình luận.");
      }
    } else {
      toast.info("Vui lòng nhập nội dung bình luận trước khi gửi.");
    }
  };

  return (
    <div className={classes.writecomment}>
      <span className={classes.writecommentlabel} onClick={handleCommentToggle}>
        Thêm bình luận
      </span>
      <ToastContainer />
      <animated.div style={slideInAnimation} className={classes.commentGroup}>
        {isCommenting && (
          <div className={`${classes.commentbox} ${classes.editorContainer}`}>
            <textarea
              className={classes.commenttext}
              value={comment}
              onChange={(e) => setComment(e.target.value)}
              placeholder="Nhập bình luận của bạn"
            ></textarea>

            <Button
              onClick={handleCommentSubmit}
              className={classes.submitComment}
            >
              Gửi bình luận
            </Button>
          </div>
        )}
      </animated.div>
    </div>
  );
};

export default CommentBox;
