// CommentBox.js
import React, { useState } from "react";
import { useSpring, animated } from "react-spring";
import { CKEditor } from "@ckeditor/ckeditor5-react";
import ClassicEditor from "@ckeditor/ckeditor5-build-classic";
import classes from "./CommentBox.module.css"; // Import CSS file
import { useParams } from "react-router-dom";

const CommentBox = () => {
  const [comment, setComment] = useState("");
  const [isCommenting, setIsCommenting] = useState(false);
  const symbol = useParams();

  const slideInAnimation = useSpring({
    opacity: isCommenting ? 1 : 0,
    marginTop: isCommenting ? 0 : -50,
  });

  const handleCommentToggle = () => {
    setIsCommenting(!isCommenting);
  };

  const handleCommentSubmit = () => {
    if (comment.trim() !== "") {
      const userToken = localStorage.getItem('token');
      if (userToken) {
        fetch('http://127.0.0.1:5000/comment/create', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${userToken}`,
          },
          body: JSON.stringify({ comment: comment, symbol: symbol }),
        })
          .then(response => {
            if (response.ok) {
              alert('Bình luận thành công!');
              setComment("");
            } else {
              alert('Lỗi bình luận.');
            }
          })
          .catch(error => {
            console.error('Error sending comment:', error);
            alert('Lỗi.');
          });
      } else {
        alert('Vui lòng đăng nhập để bình luận.');
      }
    } else {
      alert("Vui lòng nhập nội dung bình luận trước khi gửi.");
    }
  };

  return (
    <div className={classes.writecomment}>
      <span className={classes.writecommentlabel} onClick={handleCommentToggle}>
        Thêm bình luận
      </span>

      <animated.div style={slideInAnimation} className={classes.commentGroup}>
        {isCommenting && (
          <div className={`${classes.commentbox} ${classes.editorContainer}`}> {/* Apply CSS class */}
            <CKEditor
              editor={ClassicEditor}
              data={comment}
              onChange={(event, editor) => {
                const data = editor.getData().replace(/<[^>]+>/g, '');
                setComment(data);
              }}
            />
            <button onClick={handleCommentSubmit}>Gửi</button>
          </div>
        )}
      </animated.div>
    </div>
  );
};

export default CommentBox;
