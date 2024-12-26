from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Users(db.Model):
    __tablename__ = "users"

    userid = db.Column(db.String(50), primary_key=True, nullable=False)
    username = db.Column(db.String(20), nullable=False)
    password = db.Column(db.String(30), nullable=False)
    email = db.Column(db.String(50), unique=True, nullable=False)
    fullname = db.Column(db.String(30), nullable=False)
    type = db.Column(db.String(15), nullable=False)
    created_at = db.Column(
        db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(
        db.DATETIME,
        server_default=db.func.current_timestamp(),
        onupdate=db.func.current_timestamp(),
    )


class StockHistory(db.Model):
    __tablename__ = "stockhistory"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    stockid = db.Column(db.String(50), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    open = db.Column(db.Float, nullable=False)
    high = db.Column(db.Float, nullable=False)
    low = db.Column(db.Float, nullable=False)
    close = db.Column(db.Float, nullable=False)
    volume = db.Column(db.Float, nullable=False)
    stocklist = db.relationship("StockList", backref="stockhistory", lazy=True)
    stockid = db.Column(db.String(50), db.ForeignKey("stocklist.stockid"))


class StockList(db.Model):
    __tablename__ = "stocklist"

    stockid = db.Column(db.String(50), primary_key=True, nullable=False)
    symbol = db.Column(db.String(10), unique=True, nullable=False)
    company_name = db.Column(db.String(255, collation="utf8mb4_unicode_ci"))
    company_detail = db.Column(db.String(200))
    previous_close_price = db.Column(db.Float, nullable=False)
    predictions = db.relationship(
        "StockPrediction", back_populates="stock", lazy=True)


class StockPrediction(db.Model):
    __tablename__ = "stockprediction"
    predictionid = db.Column(db.Integer, primary_key=True, autoincrement=True)
    stockid = db.Column(
        db.String(50), db.ForeignKey("stocklist.stockid"), nullable=False
    )
    userid = db.Column(db.String(50), db.ForeignKey(
        "users.userid"), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    text_prediction = db.Column(db.String(300), nullable=False)
    stock = db.relationship(
        "StockList",
        back_populates="predictions",
        lazy=True,
        primaryjoin="StockPrediction.stockid == StockList.stockid",
    )


class Comments(db.Model):
    __tablename__ = "comments"

    commentid = db.Column(db.String(50), primary_key=True, nullable=False)
    userid = db.Column(db.String(50), db.ForeignKey(
        "users.userid"), nullable=False)
    stockid = db.Column(
        db.String(50), db.ForeignKey("stocklist.stockid"), nullable=False
    )
    comment_text = db.Column(db.String(200))
    created_at = db.Column(
        db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(
        db.DateTime,
        server_default=db.func.current_timestamp(),
        onupdate=db.func.current_timestamp(),
    )
    user = db.relationship("Users")
    stock = db.relationship("StockList")

    def __repr__(self):
        return f"<Comment(commentid='{self.commentid}', userid='{self.userid}', stockid='{self.stockid}', comment_text='{self.comment_text}', created_at='{self.created_at}', updated_at='{self.updated_at}')>"


class StockFollow(db.Model):
    __tablename__ = "stockfollow"

    followid = db.Column(db.String(50), primary_key=True, nullable=False)
    userid = db.Column(db.String(50), db.ForeignKey(
        "users.userid"), nullable=False)
    stockid = db.Column(
        db.String(50), db.ForeignKey("stocklist.stockid"), nullable=False
    )
    user = db.relationship("Users")
    stock = db.relationship("StockList")
