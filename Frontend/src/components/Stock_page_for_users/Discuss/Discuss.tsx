import React, { useEffect, useState } from "react";
import classes from "./Discuss.module.css";
import { CKEditor } from "@ckeditor/ckeditor5-react";
import ClassicEditor from "@ckeditor/ckeditor5-build-classic";
import { Button } from "@mui/material";

interface CommentProps {
  id:string,
  username: string;
  time: string;
  commenttext: string;
  tokenUser:string;
}

const Discuss: React.FC<CommentProps> = ({ id, username, time, commenttext, tokenUser }) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editedComment, setEditedComment] = useState(commenttext);
  const API_BASE_URL = "http://127.0.0.1:5000";
  const [isComment, setIsComment] = useState(false);
  const userToken = localStorage.getItem('token');
  useEffect(() => {
    if (userToken === tokenUser) {
      setIsComment(true);
    } else {
      setIsComment(false);
    }
  }, []);
  const handleEditClick = () => {
    console.log(id,'id cmt',username);
    setIsEditing(true);
  };

  const handleSaveCommentEdit = () => {
    if (userToken) {
      fetch(`${API_BASE_URL}/comment/update/${id}`, {
          method: 'PUT',
          headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${userToken}`, 
          },
          body: JSON.stringify({ commentid: id, commenttext: editedComment, token: userToken }),
      })
      .then(response => {
          if (response.ok) {
          alert('Bình luận thành công!');
          setEditedComment(""); 
          } else {
          alert('Lỗi bình luận.');
          }
      })
      .catch(error => {
          console.error('Error sending comment:', error);
          alert('Lỗi.');
      });
      } 
      else {
      alert('Vui lòng đăng nhập.');
      }
    } 

  
  // const handleSaveClick = () => {
  //   // Thêm logic xử lý khi lưu bình luận sau khi sửa
  //   console.log('editedComment9',editedComment);
  //   alert("Nội dung đã được cập nhật");
  //   setIsEditing(false);
  // };

  const handleCancelClick = () => {
    setEditedComment(commenttext);
    setIsEditing(false);
  };

  const handleTextChange = (
    event: React.ChangeEvent<HTMLTextAreaElement>
  ) => {
    setEditedComment(event.target.value);
  };

  return (
    <div className={classes.discuss}>
      <div className={classes.commentinfo}>
        <div className={classes.username}>{username}</div>
        <div className={classes.time}>{time}</div>
      </div>
      <div className={classes.commenttext}>
        {isEditing ? (
          <CKEditor
          editor={ClassicEditor}
          data={editedComment}
          onChange={(event, editor) => {
            const data = editor.getData().replace(/<[^>]+>/g, '');
            setEditedComment(data);
          }}
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
        <Button className={classes.delete}>
          <span>Xóa</span>
        </Button>
      </div>
    </div>
  );
};

export default Discuss;
