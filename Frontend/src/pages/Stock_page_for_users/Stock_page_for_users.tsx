import { memo } from "react";
import type { FC } from "react";
import Line from "./Line";
import classes from "./Stock_page_for_users.module.css";
import CompanyInfo from "../../components/StockDetail/CompanyInfo/CompanyInfo";
import TableDetail from "../../components/StockDetail/TableDetail/TableDetail";
import Predict from "../../components/StockDetail/Predict/Predict";
import Discuss from "../../components/StockDetail/Discuss/Discuss";
import CommentBox from "../../components/StockDetail/CommentBox/CommentBox";
import Header from "../../components/Header/Header";
import Candlestick from "../../components/StockDetail/Candlestick/Candlestick";
import { useParams } from "react-router-dom";
import { getAllComments } from "../../services/api/comment.api";
import React, { useEffect, useState } from "react";
import StockChange from "../../components/StockDetail/PriceChange/PriceChange";
import AnimationChange from "../../components/StockDetail/AnimationChange/AnimationChange";

interface Props {
  className?: string;
}

export const Stock_page_for_users: FC<Props> = memo(
  function Stock_page_for_users(props = {}) {
    const { stocks } = useParams();
    const [comments, setComments] = useState([]);
    const [updateTrigger, setUpdateTrigger] = useState(false);

    useEffect(() => {
      fetchData();
    }, [updateTrigger]);

    const fetchData = async () => {
      try {
        const data = await getAllComments({ stocks });
        setComments(data);
      } catch (error) {
        console.error("Error fetching comments:", error);
      }
    };

    const handleCommentUpdate = async () => {
      try {
        await fetchData();
        setUpdateTrigger((prevState) => !prevState);
        console.log(updateTrigger, "updateTrigger");
      } catch (error) {
        console.error("Error updating comments:", error);
      }
    };

    return (
      <div className={` ${classes.root}`}>
        <div className={classes.maincontainer}>
          <div className={classes.content}>
            <Header />
            <div className={classes.companyinfo}>
              {stocks && <CompanyInfo symbol={stocks} follow={false} />}
            </div>
            <div className={classes.animation}>
              <AnimationChange />
            </div>

            <div className={classes.container}>
              <Line />
              <div className={classes.detail}>
                <div className={classes.chart}>
                  {stocks && <Candlestick symbol={stocks} />}
                </div>
                <div className={classes.tabledetail}>
                  {stocks && <TableDetail symbol={stocks} />}
                  {stocks && <StockChange symbol={stocks} />}
                </div>
              </div>
              <Line />
              <div className={classes.predictcontent}>
                <div className={classes.labelpredict}>
                  <span>Dự đoán</span>
                </div>
                {stocks && <Predict symbol={stocks} />}
              </div>
              <div className={classes.discusscomment}>
                <Line />
                <div className={classes.text}>Thảo luận</div>
                <div className={classes.wiritecomment}>
                  <div className={classes.commentbox}>
                    <CommentBox onUpdateComments={handleCommentUpdate} />
                  </div>
                </div>
                <div className={classes.discussGroup}>
                  {comments.map((commentItem, index) => (
                    <React.Fragment key={index}>
                      <Discuss
                        id={commentItem.commentid}
                        username={commentItem.name}
                        time={commentItem.updated_at}
                        commenttext={commentItem.comment_text}
                        tokenUser={commentItem.userToken}
                        onUpdate={handleCommentUpdate}
                      />
                    </React.Fragment>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
);

export default Stock_page_for_users;
