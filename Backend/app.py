import pandas as pd
from flask_jwt_extended import create_access_token, get_jwt, get_jwt_identity, unset_jwt_cookies, jwt_required, JWTManager
from stock.changeprice import changeprice
from stock.getStocks import get_stock_history, get_follow_stock_by_date, get_top_stock
from stock.predict import chart_stock
from datetime import datetime, timedelta, timezone
from stock.crawl import *
from database.write_json_data import write_crawl_data_to_db, banks
import json
import uuid
from flask import Flask, request, jsonify
from models import (
    StockFollow,
    db,
    StockList,
    StockHistory,
    StockPrediction,
    Users,
    Comments,
)
from sqlalchemy import desc
from flask_cors import CORS
import matplotlib
from dotenv import load_dotenv
import os
from config import *
matplotlib.use('Agg')
load_dotenv()

SQL_STRING = os.getenv('SQL_STRING')

app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = 'python'
app.config["SQLALCHEMY_DATABASE_URI"] = (SQL_STRING)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1)
db.init_app(app)
jwt = JWTManager(app)
CORS(app, supports_credentials=True)

now = datetime.now(timezone.utc)
@app.after_request
def refresh_expiring_jwts(response):
    try:
        exp_timestamp = get_jwt()["exp"]
        target_timestamp = datetime.timestamp(now + timedelta(minutes=30))
        if target_timestamp > exp_timestamp:
            access_token = create_access_token(identity=get_jwt_identity())
            data = response.get_json()
            if type(data) is dict:
                data["access_token"] = access_token
                response.data = json.dumps(data)
        return response
    except (RuntimeError, KeyError):
        return response


@app.route('/stock-chart/<stockCode>')
def stockchart(stockCode):
    stock = StockList.query.filter_by(symbol=stockCode).first()
    if not stock:
        return jsonify({'error': STOCK_NOT_FOUND}), 404
    stock_infos = (
        StockHistory.query.filter_by(stockid=stock.stockid)
        .order_by(desc(StockHistory.date))
        .all()
    )
    dataSource, img_predict = chart_stock(stock_infos)
    # print(img_predict,"img_predict")
    return jsonify({"chart_data": dataSource, "img_predict": img_predict})


@app.route('/stock/change/<stockCode>', methods=['GET'])
def get_change_price(stockCode):
    stock = StockList.query.filter_by(symbol=stockCode).first()
    if not stock:
        return jsonify({'error': STOCK_NOT_FOUND}), 404
    stock_infos = (
        StockHistory.query.filter_by(stockid=stock.stockid)
        .order_by(desc(StockHistory.date))
        .all()
    )
    change_price = changeprice(stock_infos)
    return jsonify({"change_price": change_price})


@app.route('/stock/<stockCode>', methods=['GET', 'POST', 'UPDATE'],)
def get_stock_list(stockCode):
    stock = StockList.query.filter_by(symbol=stockCode).first()
    if request.method == 'POST':
        data = request.get_json()
        new_text_prediction = data.get('text_prediction')
        if not stock:
            return jsonify({'error': STOCK_NOT_FOUND}), 404
        stock_prediction = StockPrediction.query.filter_by(
            stockid=stock.stockid).first()
        if not stock_prediction:
            stock_prediction = StockPrediction(
                stockid=stock.stockid,
                userid='user_id_placeholder',
                date=now,
                text_prediction=new_text_prediction
            )
            db.session.add(stock_prediction)
        else:
            stock_prediction.text_prediction = new_text_prediction
        db.session.commit()
        return jsonify({'success': True}), 200
    if request.method == 'GET':
        stock_info = (
            StockHistory.query.filter_by(stockid=stock.stockid)
            .order_by(StockHistory.date.desc())
            .first()
        )
        if not stock:
            return jsonify({'error': STOCK_NOT_FOUND}), 404
        stock_dict = {
            "stockid": stock.stockid,
            "symbol": stock.symbol,
            "company_name": stock.company_name,
            "company_detail": stock.company_detail,
            "previous_close_price": stock.previous_close_price,
            "date": str(stock_info.date),
            "open": stock_info.open,
            "high": stock_info.high,
            "low": stock_info.low,
            "close": stock_info.close,
            "volume": stock_info.volume,
        }
    return (
        json.dumps(stock_dict, ensure_ascii=False).encode("utf8"),
        200,
        {"Content-Type": "application/json; charset=utf-8"},
    )


@app.route('/admin/predictions/<stockCode>', methods=['POST', 'DELETE'])
@jwt_required()
def predictions(stockCode):
    user = Users.query.filter_by(
        email=get_jwt_identity(), type="admin").first()
    stock = StockList.query.filter_by(symbol=stockCode).first()

    if not user:
        return jsonify({'error': UN_AUTHORIZED_ACCESS}), 403
    if not stock:
        return jsonify({'error': STOCK_NOT_FOUND}),  404
    if request.method == 'POST':
        data = request.get_json()
        new_text_prediction = data.get('textpredict')
        stock_prediction = StockPrediction.query.filter_by(
            stockid=stock.stockid).first()

        if not stock_prediction:
            stock_prediction = StockPrediction(
                stockid=stock.stockid,
                userid=user.userid,
                date=now,
                text_prediction=new_text_prediction
            )
            db.session.add(stock_prediction)
        else:
            stock_prediction.text_prediction = new_text_prediction
            db.session.flush()

        db.session.commit()
        return jsonify({'success': True}), 200

    if request.method == "DELETE":
        stock_prediction = StockPrediction.query.filter_by(
            stockid=stock.stockid).first()
        if stock_prediction:
            db.session.delete(stock_prediction)
            db.session.commit()

        return jsonify({'success': True}), 200


@app.route('/user/predictions/<stockCode>')
def get_predictions(stockCode):
    stock = StockList.query.filter_by(symbol=stockCode).first()
    if not stock:
        return jsonify({'error': STOCK_NOT_FOUND}), 404
    stock_prediction = StockPrediction.query.filter_by(
        stockid=stock.stockid).first()
    if not stock_prediction:
        return jsonify({'textPrediction': ''}), 200
    return jsonify({'textPrediction': stock_prediction.text_prediction}), 200


@app.route("/user/follow/<stockCode>", methods=["GET", "POST"])
@jwt_required()
def follow_data(stockCode):
    email_user = get_jwt_identity()
    user = Users.query.filter_by(email=email_user).first()
    stock = StockList.query.filter_by(symbol=stockCode).first()
    followed = StockFollow.query.filter_by(
        userid=user.userid, stockid=stock.stockid
    ).first()

    if request.method == "GET":
        if followed:
            return jsonify({"is_follow": True}), 200
        else:
            return jsonify({"is_follow": False}), 200

    if request.method == "POST":
        if followed:
            db.session.delete(followed)
            db.session.commit()
            return jsonify({"message": f"{UN_FOLLOWED}{stockCode}.", "is_follow": False}), 200
        else:
            new_followed = StockFollow(
                followid=str(uuid.uuid4()), userid=user.userid, stockid=stock.stockid
            )
            db.session.add(new_followed)
            db.session.commit()
            return jsonify({"message": f"{FOLLOWED}{stockCode}.", "is_follow": True}), 200


@app.route('/token', methods=["POST"])
def create_token():
    email = request.json.get("email", None)
    password = request.json.get("password", None)
    user = Users.query.filter_by(email=email).first()
    if user is None:
        return jsonify({"error": UN_AUTHORIZED_ACCESS}), 401
    if email != user.email or password != user.password:
        return {"msg": INCORRECT_LOGIN}, 401
    access_token = create_access_token(identity=email)
    response = {"access_token": access_token}
    return response


@app.route("/logout", methods=["POST"])
def logout():
    response = jsonify({"msg": LOGOUT_SUCCESSFUL})
    unset_jwt_cookies(response)
    return response


@app.route("/signup", methods=["POST"])
def signup():
    fullname = request.json.get("fullname", None)
    email = request.json.get("email", None)
    password = request.json.get("password", None)
    if fullname == "":
        return jsonify({"error": VALIDATE_SIGNUP_FORM["full_name"]}), 400
    if email == "":
        return jsonify({"error": VALIDATE_SIGNUP_FORM["email"]}), 400
    if password == "":
        return jsonify({"error": VALIDATE_SIGNUP_FORM["pass_word"]}), 400
    user_exists = Users.query.filter_by(email=email).first()
    if user_exists:
        return jsonify({"error": VALIDATE_SIGNUP_FORM["email_exists"]}), 400
    new_user = Users(
        userid=str(uuid.uuid4()),
        username=email.split('@')[0],
        password=password,
        email=email,
        fullname=fullname,
        type='user',
        created_at=now,
        updated_at=now
    )
    db.session.add(new_user)
    db.session.commit()
    return jsonify({"success": REGISTER_SUCCESSFUL})


@app.route('/getAllStocks', methods=['GET'])
def get_stock_lists():
    currentDate = "2024-11-07"
    dataFilterByDate = get_stock_history(currentDate)
    return (
        jsonify(dataFilterByDate)
    )


@app.route('/getAllStocks/<date>', methods=['GET'])
def get_stock_date(date):
    dataFilterByDate = get_stock_history(date)

    return (
        jsonify(dataFilterByDate)
    )


@app.route('/getTopStock', methods=['GET'])
def get_top_stock_lists():
    currentDate = "2024-10-24"
    topincrease, topdecrease = get_top_stock(currentDate)
    return (
        jsonify({"top_increase": topincrease, "top_decrease": topdecrease})
    )


@app.route('/getAllStocks/follow/<date>', methods=['GET'])
@jwt_required()
def get_follow_stock(date):
    email_user = get_jwt_identity()
    user = Users.query.filter_by(email=email_user).first()
    userID = user.userid
    followStockList = get_follow_stock_by_date(userID, date)
    return (
        jsonify(followStockList)
    )


@app.route('/userprofile', methods=['GET'])
@jwt_required()
def user_profile():
    email_user = get_jwt_identity()
    user = Users.query.filter_by(email=email_user).first()
    user_get_by_email = {
        "fullname": user.fullname,
        "email": user.email,
        "password": user.password
    }
    return jsonify(user_get_by_email)


@app.route('/userprofile', methods=['POST'])
@jwt_required()
def update_user_profile():
    email_user = get_jwt_identity()
    email = request.json.get('email')
    if (email_user == email):
        fullname = request.json.get('fullname')
        password = request.json.get('password')
        user = Users.query.filter_by(email=email).first()
        user.fullname = fullname
        user.password = password
        db.session.commit()
        return jsonify({"message": "Cập nhật thông tin thành công."}), 200
    else:
        return jsonify({"error": "Cập nhật thông tin thất bại"}), 401


@app.route('/comment/showAll/<symbol>', methods=['GET'])
def get_comment_lists(symbol):
    stock = StockList.query.filter_by(symbol=symbol).first()
    commentArr = Comments.query.filter_by(stockid=stock.stockid).all()
    current_time = now
    commentStock = []
    for commentObject in commentArr:
        user = Users.query.filter_by(userid=commentObject.userid).first()
        tokenUser = create_access_token(identity=user.email)
        time_difference = current_time - commentObject.updated_at
        total_seconds = time_difference.total_seconds()
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        if (hours > 24):
            days = int(hours//24)
            time = f"{days} ngày"
        else:
            time = f"{hours} giờ, {minutes} phút"
        comment = {
            "commentid": commentObject.commentid,
            "name": str(user.fullname),
            "userToken": str(tokenUser),
            "stockid": commentObject.stockid,
            "comment_text": commentObject.comment_text,
            "updated_at": time,
            "second": total_seconds,
        }
        commentStock.append(comment)
    commentStock = sorted(commentStock, key=lambda x: x['second'])
    return (
        jsonify(commentStock)
    )


@app.route('/comment//admin/predictions/TPB', methods=["POST"])
@jwt_required()
def comment():
    comment = request.json.get("comment", None)
    symbol = request.json.get("symbol", None)
    emailUser = get_jwt_identity()
    user = Users.query.filter_by(email=emailUser).first()
    stock = StockList.query.filter_by(symbol=symbol['stocks']).first()
    new_cmt = Comments(
        commentid=str(uuid.uuid4()),
        userid=user.userid,
        stockid=stock.stockid,
        comment_text=comment,
        created_at=now,
        updated_at=now
    )
    db.session.add(new_cmt)
    db.session.commit()
    return jsonify({'a': 'b'})


@app.route('/comment/update/<commentid>', methods=["PUT"])
@jwt_required()
def updateComment(commentid):
    emailUser = get_jwt_identity()
    user = Users.query.filter_by(email=emailUser).first()
    comment = Comments.query.filter_by(commentid=commentid).first()
    if (user.userid == comment.userid):
        update_comment_text = request.json.get("commenttext", None)
        comment.comment_text = update_comment_text
        db.session.commit()
        return jsonify({"message": COMMENT_SUCCESSFULL}), 200
    else:
        return jsonify({"error": "You are not authorized to update this comment"}), 401


@app.route("/comment/delete/<comment_id>", methods=["DELETE"])
@jwt_required()
def delete_comment(comment_id):
    emailUser = get_jwt_identity()
    user = Users.query.filter_by(email=emailUser).first()
    comment = Comments.query.get(comment_id)
    if user.userid == comment.userid:
        db.session.delete(comment)
        db.session.commit()
    return jsonify({"message": "Comment deleted successfully"}), 200


@app.route('/crawl_data', methods=['GET'])
@jwt_required()
def crawl_stock():
    emailUser = get_jwt_identity()
    # print(emailUser,'emailUser')
    user = Users.query.filter_by(email=emailUser).first()
    print(user.type, 'user')
    if user.type != 'admin' or not user:
        return jsonify({
            'success': False,
            'message': 'You do not have permission to crawl data',
            'data': None
        }), 403
    date_his = now.strftime('%d/%m/%Y')
    # date_his = '15/12/2024'
    newest_day_data = StockHistory.query.order_by(StockHistory.date.desc()).first()
    print(date_his,'today')
    print(newest_day_data.date.strftime('%d/%m/%Y'),'newest_day_data')
    data = {}
    check = False
    if (newest_day_data.date.strftime('%d/%m/%Y')!=date_his):
        stock_ids = []
        for bank in banks:
            print(f"Crawling data for bank: {bank}")

            crawled_data = crawl_stock_data(bank)
            stock = StockList.query.filter_by(symbol=bank).first()
            stock_id = stock.stockid
            stock_ids.append(stock_id)
            time.sleep(1)
            # Debug: Print the structure of crawled_data
            print(f"Crawled data for {bank}:", crawled_data)
            
            # Validate crawled_data structure
            if crawled_data is None or crawled_data['data'] is None:
                print(f"No data returned for {bank}")
                continue
            
            if 'data' not in crawled_data or bank not in crawled_data['data']:
                print(f"Unexpected data structure for {bank}")
                continue
            
            data[bank] = crawled_data['data'][bank]
            time.sleep(1)
            print("Full crawled data:", data)
        check = write_crawl_data_to_db(data, stock_ids)
        print('check',check)
        if check:
            return jsonify({
                'success': True,
                'message': 'Data crawled and stored successfully',
                'data': data if banks else None
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Error adding data to database',
                'data': None
            }), 500

    else:    
        return jsonify({
            'success': True,
            'message': 'Data is the newest!',
            'data': None
        })
    



@app.route("/", methods=["get"])
def hello():
    return "hello"


if __name__ == '__main__':
    app.run(debug=True)
